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
	queue *eventQueue
}

type fetchHandlerInput struct {
	eventChan <-chan Event
}

type fetchHandlerConfig struct {
	getSequence GetSequence
}

type fetchHandlerState struct {
	firstSequence Sequence
	lastSequence  Sequence
	data          []Event
}

//===========================================
// Processor
//===========================================

func newProcessState(lastEvents []Event, cap int) *processState {
	q := newEventQueue(cap)
	for _, e := range lastEvents {
		q.push(e)
	}

	return &processState{
		queue: q,
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

		lastSequence := Sequence(0)
		if state.queue.len > 0 {
			lastSequence = config.getSequence(state.queue.back())
		}

		for i := range events {
			lastSequence++
			events[i] = config.setSequence(events[i], lastSequence)
		}

		err = config.updateEvents(ctx, events)
		if err != nil {
			return err
		}

		for _, e := range events {
			state.queue.push(e)
		}

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
	if seq >= s.firstSequence+n-1 {
		s.firstSequence = seq + 1 - n
	}
	s.lastSequence = seq
}

func runFetchHandler(ctx context.Context, input fetchHandlerInput, state *fetchHandlerState, config *fetchHandlerConfig) {
	select {
	case event := <-input.eventChan:
		state.appendEvent(event, config)
		return

	case <-ctx.Done():
		return
	}
}
