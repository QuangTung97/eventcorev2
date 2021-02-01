package eventcorev2

import (
	"context"
	"github.com/cheekybits/genny/generic"
)

// Event represent events
type Event generic.Type

// Sequence ...
type Sequence uint64

// GetSequence ...
type GetSequence func(e Event) Sequence

// SetSequence ...
type SetSequence func(e Event, seq Sequence) Event

//===========================================
// Event Queue
//===========================================
type eventQueue struct {
	len        int
	frontIndex int
	data       []Event
}

func newEventQueue(cap int) *eventQueue {
	return &eventQueue{
		len:        0,
		frontIndex: 0,
		data:       make([]Event, cap),
	}
}

func (q *eventQueue) push(e Event) {
	index := (q.frontIndex + q.len) % len(q.data)
	q.data[index] = e
	if q.len < len(q.data) {
		q.len++
	} else {
		q.frontIndex = (q.frontIndex + 1) % len(q.data)
	}
}

func (q *eventQueue) front() Event {
	return q.data[q.frontIndex]
}

func (q *eventQueue) back() Event {
	index := (q.frontIndex + q.len - 1) % len(q.data)
	return q.data[index]
}

func (q *eventQueue) getAll() []Event {
	result := make([]Event, 0, q.len)
	for i := 0; i < q.len; i++ {
		index := (q.frontIndex + i) % len(q.data)
		result = append(result, q.data[index])
	}
	return result
}

//===========================================
// Input
//===========================================

type processInput struct {
	signalChan <-chan struct{}
	outputChan chan<- Event
}

type processConfig struct {
	getSequence          GetSequence
	setSequence          SetSequence
	getUnprocessedEvents func(ctx context.Context) ([]Event, error)
	updateEvents         func(ctx context.Context, events []Event) error
}

type processState struct {
	lastSequence Sequence
}

type fetchRequest struct {
	fromSequence Sequence
	data         []Event
	limit        Sequence
	responseChan chan<- fetchResponse
}

type fetchResponse struct {
	existed bool
	data    []Event
}

type fetchHandlerInput struct {
	eventChan   <-chan Event
	requestChan <-chan fetchRequest
}

type fetchHandlerConfig struct {
	getSequence GetSequence
}

type fetchHandlerState struct {
	firstSequence Sequence
	lastSequence  Sequence
	data          []Event
	waitList      []fetchRequest
}

//===========================================
// Processor
//===========================================

func newProcessState(lastEvents []Event, config *processConfig) *processState {
	sequence := Sequence(0)
	if len(lastEvents) > 0 {
		sequence = config.getSequence(lastEvents[len(lastEvents)-1])
	}
	return &processState{
		lastSequence: sequence,
	}
}

func processDBEvents(ctx context.Context, input processInput, state *processState, config *processConfig) error {
	select {
	case <-input.signalChan:
	DrainLoop:
		for {
			select {
			case <-input.signalChan:
				continue
			default:
				break DrainLoop
			}
		}
		events, err := config.getUnprocessedEvents(ctx)
		if err != nil {
			return err
		}

		lastSequence := state.lastSequence

		for i := range events {
			lastSequence++
			events[i] = config.setSequence(events[i], lastSequence)
		}

		err = config.updateEvents(ctx, events)
		if err != nil {
			return err
		}

		state.lastSequence = lastSequence

		for _, e := range events {
			input.outputChan <- e
		}

		return nil

	case <-ctx.Done():
		return nil
	}
}

func newFetchHandlerState(lastEvents []Event, cap int, config *fetchHandlerConfig) *fetchHandlerState {
	state := &fetchHandlerState{
		data: make([]Event, cap),
	}

	for _, e := range lastEvents {
		state.appendEvent(e, config)
	}

	return state
}

func (s *fetchHandlerState) appendEvent(event Event, config *fetchHandlerConfig) {
	seq := config.getSequence(event)
	n := Sequence(len(s.data))
	index := seq % n
	s.data[index] = event

	if s.firstSequence == 0 {
		s.firstSequence = seq
	}
	if seq >= s.firstSequence+n {
		s.firstSequence = seq + 1 - n
	}
	s.lastSequence = seq
}

func copyStateData(stateData []Event, fromSequence Sequence,
	lastSequence Sequence, limit Sequence, data []Event,
) []Event {
	end := lastSequence
	if end >= fromSequence+limit {
		end = fromSequence + limit - 1
	}
	for seq := fromSequence; seq <= end; seq++ {
		index := seq % Sequence(len(stateData))
		data = append(data, stateData[index])
	}
	return data
}

func returnFetchResponse(request fetchRequest, state *fetchHandlerState) {
	if request.fromSequence < state.firstSequence {
		request.responseChan <- fetchResponse{
			existed: false,
		}
		return
	}
	request.responseChan <- fetchResponse{
		existed: true,
		data:    copyStateData(state.data, request.fromSequence, state.lastSequence, request.limit, request.data),
	}
}

func clearWaitList(waitList []fetchRequest) []fetchRequest {
	for i := range waitList {
		waitList[i] = fetchRequest{}
	}
	return waitList[:0]
}

func runFetchHandler(ctx context.Context, input fetchHandlerInput,
	state *fetchHandlerState, config *fetchHandlerConfig,
) {
	select {
	case event := <-input.eventChan:
		state.appendEvent(event, config)
	BatchLoop:
		for {
			select {
			case e := <-input.eventChan:
				state.appendEvent(e, config)
			default:
				break BatchLoop
			}
		}
		for _, req := range state.waitList {
			returnFetchResponse(req, state)
		}
		state.waitList = clearWaitList(state.waitList)
		return

	case request := <-input.requestChan:
		if request.fromSequence > state.lastSequence+1 {
			panic("fromSequence MUST <= lastSequence + 1")
		}
		if state.firstSequence+1 == request.fromSequence {
			state.waitList = append(state.waitList, request)
			return
		}
		returnFetchResponse(request, state)
		return

	case <-ctx.Done():
		return
	}
}
