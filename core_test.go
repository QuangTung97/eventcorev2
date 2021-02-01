package eventcorev2

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

// x Processor
// x FetchService
// - Publisher
// - AsyncPublisher??
// x EventQueue

type testEvent struct {
	id  int
	seq Sequence
}

func getSequence(event Event) Sequence {
	e := event.(testEvent)
	return e.seq
}

func setSequence(event Event, seq Sequence) Event {
	e := event.(testEvent)
	e.seq = seq
	return e
}

func TestProcessDBEvents_Context(t *testing.T) {
	table := []struct {
		name string
	}{
		{
			name: "cancel",
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			cancel()

			err := processDBEvents(ctx, processInput{}, nil, nil)
			assert.Nil(t, err)
		})
	}
}

func drainOutputChan(ch <-chan Event) []Event {
	var result []Event
	for {
		select {
		case e := <-ch:
			result = append(result, e)
		default:
			return result
		}
	}
}

func TestProcessDBEvents_Signal(t *testing.T) {
	table := []struct {
		name        string
		lastEvents  []Event
		signalCount int

		getEvents   []Event
		getError    error
		updateError error

		expected           error
		getCalled          int
		updateCalled       int
		updateCalledEvents []Event
		outputEvents       []Event
		lastSequence       Sequence
	}{
		{
			name:     "get-unprocessed-error",
			getError: errors.New("get error"),

			getCalled: 1,
			expected:  errors.New("get error"),
		},
		{
			name: "update-error-without-last-events",
			getEvents: []Event{
				testEvent{id: 12},
				testEvent{id: 11},
				testEvent{id: 15},
			},
			updateError: errors.New("update error"),

			getCalled:    1,
			updateCalled: 1,
			updateCalledEvents: []Event{
				testEvent{id: 12, seq: 1},
				testEvent{id: 11, seq: 2},
				testEvent{id: 15, seq: 3},
			},
			expected: errors.New("update error"),
		},
		{
			name: "update-error-with-last-events",
			lastEvents: []Event{
				testEvent{id: 1, seq: 5},
				testEvent{id: 3, seq: 6},
			},
			getEvents: []Event{
				testEvent{id: 12},
				testEvent{id: 11},
				testEvent{id: 15},
			},
			updateError: errors.New("update error"),

			getCalled:    1,
			updateCalled: 1,
			updateCalledEvents: []Event{
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
			expected:     errors.New("update error"),
			lastSequence: 6,
		},
		{
			name: "update-ok-with-last-events",
			lastEvents: []Event{
				testEvent{id: 1, seq: 5},
				testEvent{id: 3, seq: 6},
			},
			getEvents: []Event{
				testEvent{id: 12},
				testEvent{id: 11},
				testEvent{id: 15},
			},

			getCalled:    1,
			updateCalled: 1,
			updateCalledEvents: []Event{
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
			outputEvents: []Event{
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
			lastSequence: 9,
		},
		{
			name: "update-ok-without-last-events",
			getEvents: []Event{
				testEvent{id: 12},
				testEvent{id: 11},
				testEvent{id: 15},
			},

			getCalled:    1,
			updateCalled: 1,
			updateCalledEvents: []Event{
				testEvent{id: 12, seq: 1},
				testEvent{id: 11, seq: 2},
				testEvent{id: 15, seq: 3},
			},
			outputEvents: []Event{
				testEvent{id: 12, seq: 1},
				testEvent{id: 11, seq: 2},
				testEvent{id: 15, seq: 3},
			},
			lastSequence: 3,
		},
		{
			name:        "multiple-signals",
			signalCount: 5,
			lastEvents: []Event{
				testEvent{id: 1, seq: 5},
				testEvent{id: 3, seq: 6},
			},
			getEvents: []Event{
				testEvent{id: 12},
				testEvent{id: 11},
				testEvent{id: 15},
			},

			getCalled:    1,
			updateCalled: 1,
			updateCalledEvents: []Event{
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
			outputEvents: []Event{
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
			lastSequence: 9,
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()

			var signalChan chan struct{}
			if e.signalCount <= 1 {
				signalChan = make(chan struct{}, 1)
				signalChan <- struct{}{}
			} else {
				signalChan = make(chan struct{}, e.signalCount)
				for i := 0; i < e.signalCount; i++ {
					signalChan <- struct{}{}
				}
			}

			outChan := make(chan Event, 10)

			var getCalled int
			getEvents := func(ctx context.Context) ([]Event, error) {
				getCalled++
				return e.getEvents, e.getError
			}

			var updateCalled int
			var updateCalledEvents []Event
			updateEvents := func(ctx context.Context, events []Event) error {
				updateCalled++
				updateCalledEvents = events
				return e.updateError
			}

			conf := &processConfig{
				getSequence:          getSequence,
				setSequence:          setSequence,
				getUnprocessedEvents: getEvents,
				updateEvents:         updateEvents,
			}

			state := newProcessState(e.lastEvents, conf)

			err := processDBEvents(ctx, processInput{
				signalChan: signalChan,
				outputChan: outChan,
			}, state, conf)

			outputEvents := drainOutputChan(outChan)

			assert.Equal(t, 0, len(signalChan))
			assert.Equal(t, e.expected, err)
			assert.Equal(t, e.getCalled, getCalled)
			assert.Equal(t, e.updateCalled, updateCalled)
			assert.Equal(t, e.updateCalledEvents, updateCalledEvents)
			assert.Equal(t, e.outputEvents, outputEvents)
			assert.Equal(t, e.lastSequence, state.lastSequence)
		})
	}
}

func TestNewProcessState(t *testing.T) {
	config := &processConfig{
		getSequence: getSequence,
	}

	p := newProcessState([]Event{
		testEvent{id: 1, seq: 5},
		testEvent{id: 3, seq: 6},
	}, config)

	assert.Equal(t, Sequence(6), p.lastSequence)

	p = newProcessState(nil, config)
	assert.Equal(t, Sequence(0), p.lastSequence)
}

func TestRunFetchHandler_Context(t *testing.T) {
	table := []struct {
		name string
	}{
		{
			name: "cancel",
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)
			cancel()

			state := newFetchHandlerState(nil, 4, nil)
			runFetchHandler(ctx, fetchHandlerInput{}, state, nil)
		})
	}
}

func TestRunFetchHandler_Event(t *testing.T) {
	table := []struct {
		name        string
		events      []Event
		stateBefore fetchHandlerState
		stateAfter  fetchHandlerState
		responses   []fetchResponse
	}{
		{
			name: "first",
			events: []Event{
				testEvent{id: 10, seq: 2},
			},
			stateBefore: fetchHandlerState{
				data: make([]Event, 4),
			},
			stateAfter: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  2,
				data: []Event{
					nil,
					nil,
					testEvent{id: 10, seq: 2},
					nil,
				},
			},
		},
		{
			name: "second",
			events: []Event{
				testEvent{id: 9, seq: 3},
			},
			stateBefore: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  2,
				data: []Event{
					nil,
					nil,
					testEvent{id: 10, seq: 2},
					nil,
				},
			},
			stateAfter: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  3,
				data: []Event{
					nil,
					nil,
					testEvent{id: 10, seq: 2},
					testEvent{id: 9, seq: 3},
				},
			},
		},
		{
			name: "wrap",
			events: []Event{
				testEvent{id: 18, seq: 6},
			},
			stateBefore: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  5,
				data: []Event{
					testEvent{id: 12, seq: 4},
					testEvent{id: 13, seq: 5},
					testEvent{id: 10, seq: 2},
					testEvent{id: 11, seq: 3},
				},
			},
			stateAfter: fetchHandlerState{
				firstSequence: 3,
				lastSequence:  6,
				data: []Event{
					testEvent{id: 12, seq: 4},
					testEvent{id: 13, seq: 5},
					testEvent{id: 18, seq: 6},
					testEvent{id: 11, seq: 3},
				},
			},
		},
		{
			name: "near-wrap",
			events: []Event{
				testEvent{id: 13, seq: 5},
			},
			stateBefore: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  4,
				data: []Event{
					testEvent{id: 12, seq: 4},
					nil,
					testEvent{id: 10, seq: 2},
					testEvent{id: 11, seq: 3},
				},
			},
			stateAfter: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  5,
				data: []Event{
					testEvent{id: 12, seq: 4},
					testEvent{id: 13, seq: 5},
					testEvent{id: 10, seq: 2},
					testEvent{id: 11, seq: 3},
				},
			},
		},
		{
			name: "multiple-events",
			events: []Event{
				testEvent{id: 10, seq: 2},
				testEvent{id: 13, seq: 3},
			},
			stateBefore: fetchHandlerState{
				data: make([]Event, 4),
			},
			stateAfter: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  3,
				data: []Event{
					nil,
					nil,
					testEvent{id: 10, seq: 2},
					testEvent{id: 13, seq: 3},
				},
			},
		},
		{
			name: "multiple-events-wrap",
			events: []Event{
				testEvent{id: 12, seq: 2},
				testEvent{id: 13, seq: 3},
				testEvent{id: 14, seq: 4},
				testEvent{id: 15, seq: 5},
				testEvent{id: 16, seq: 6},
			},
			stateBefore: fetchHandlerState{
				data: make([]Event, 4),
			},
			stateAfter: fetchHandlerState{
				firstSequence: 3,
				lastSequence:  6,
				data: []Event{
					testEvent{id: 14, seq: 4},
					testEvent{id: 15, seq: 5},
					testEvent{id: 16, seq: 6},
					testEvent{id: 13, seq: 3},
				},
			},
		},
		{
			name: "multiple-events-with-wait-list",
			events: []Event{
				testEvent{id: 17, seq: 7},
				testEvent{id: 18, seq: 8},
			},
			stateBefore: fetchHandlerState{
				firstSequence: 3,
				lastSequence:  6,
				data: []Event{
					testEvent{id: 14, seq: 4},
					testEvent{id: 15, seq: 5},
					testEvent{id: 16, seq: 6},
					testEvent{id: 13, seq: 3},
				},
				waitList: []fetchRequest{
					{
						fromSequence: 7,
						limit:        2,
					},
					{
						fromSequence: 7,
						limit:        1,
					},
				},
			},
			stateAfter: fetchHandlerState{
				firstSequence: 5,
				lastSequence:  8,
				data: []Event{
					testEvent{id: 18, seq: 8},
					testEvent{id: 15, seq: 5},
					testEvent{id: 16, seq: 6},
					testEvent{id: 17, seq: 7},
				},
				waitList: []fetchRequest{},
			},
			responses: []fetchResponse{
				{
					existed: true,
					data: []Event{
						testEvent{id: 17, seq: 7},
						testEvent{id: 18, seq: 8},
					},
				},
				{
					existed: true,
					data: []Event{
						testEvent{id: 17, seq: 7},
					},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			eventChan := make(chan Event, len(e.events))

			for _, event := range e.events {
				eventChan <- event
			}

			responseCount := len(e.stateBefore.waitList)
			responseChan := make(chan fetchResponse, responseCount)
			for i := range e.stateBefore.waitList {
				e.stateBefore.waitList[i].responseChan = responseChan
			}

			config := &fetchHandlerConfig{
				getSequence: getSequence,
			}

			state := e.stateBefore

			runFetchHandler(ctx, fetchHandlerInput{
				eventChan: eventChan,
			}, &state, config)

			var responses []fetchResponse
			for i := 0; i < responseCount; i++ {
				resp := <-responseChan
				responses = append(responses, resp)
			}

			assert.Equal(t, e.stateAfter, state)
			assert.Equal(t, 0, len(eventChan))
			assert.Equal(t, e.responses, responses)
		})
	}
}

func TestRunFetchHandler_Request(t *testing.T) {
	table := []struct {
		name        string
		request     fetchRequest
		stateBefore fetchHandlerState
		stateAfter  *fetchHandlerState
		expected    fetchResponse
		notWaitResp bool
	}{
		{
			name: "state-empty-request-from-1",
			request: fetchRequest{
				fromSequence: 1,
				data:         nil,
				limit:        3,
			},
			stateBefore: fetchHandlerState{
				firstSequence: 0,
				lastSequence:  0,
			},
			stateAfter: &fetchHandlerState{
				waitList: []fetchRequest{
					{
						fromSequence: 1,
						data:         nil,
						limit:        3,
					},
				},
			},
			notWaitResp: true,
		},
		{
			name: "existed-single-request-from-not-exist",
			request: fetchRequest{
				fromSequence: 1,
				data:         nil,
				limit:        3,
			},
			stateBefore: fetchHandlerState{
				firstSequence: 2,
				lastSequence:  2,
				data: []Event{
					nil,
					nil,
					testEvent{id: 10, seq: 2},
					nil,
				},
			},
			expected: fetchResponse{
				existed: false,
			},
		},
		{
			name: "existed-single",
			request: fetchRequest{
				fromSequence: 1,
				data:         nil,
				limit:        3,
			},
			stateBefore: fetchHandlerState{
				firstSequence: 1,
				lastSequence:  1,
				data: []Event{
					nil,
					testEvent{id: 10, seq: 1},
					nil,
					nil,
				},
			},
			expected: fetchResponse{
				existed: true,
				data: []Event{
					testEvent{id: 10, seq: 1},
				},
			},
		},
		{
			name: "existed-multiple",
			request: fetchRequest{
				fromSequence: 1,
				limit:        3,
			},
			stateBefore: fetchHandlerState{
				firstSequence: 1,
				lastSequence:  2,
				data: []Event{
					nil,
					testEvent{id: 10, seq: 1},
					testEvent{id: 9, seq: 2},
					nil,
				},
			},
			expected: fetchResponse{
				existed: true,
				data: []Event{
					testEvent{id: 10, seq: 1},
					testEvent{id: 9, seq: 2},
				},
			},
		},
		{
			name: "reach-limit",
			request: fetchRequest{
				fromSequence: 1,
				limit:        2,
			},
			stateBefore: fetchHandlerState{
				firstSequence: 1,
				lastSequence:  3,
				data: []Event{
					nil,
					testEvent{id: 10, seq: 1},
					testEvent{id: 9, seq: 2},
					testEvent{id: 11, seq: 3},
				},
			},
			expected: fetchResponse{
				existed: true,
				data: []Event{
					testEvent{id: 10, seq: 1},
					testEvent{id: 9, seq: 2},
				},
			},
		},
		{
			name: "reach-limit-wrap-around",
			request: fetchRequest{
				fromSequence: 6,
				limit:        3,
			},
			stateBefore: fetchHandlerState{
				firstSequence: 6,
				lastSequence:  9,
				data: []Event{
					testEvent{id: 12, seq: 8},
					testEvent{id: 14, seq: 9},
					testEvent{id: 9, seq: 6},
					testEvent{id: 11, seq: 7},
				},
			},
			expected: fetchResponse{
				existed: true,
				data: []Event{
					testEvent{id: 9, seq: 6},
					testEvent{id: 11, seq: 7},
					testEvent{id: 12, seq: 8},
				},
			},
		},
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()

			responseChan := make(chan fetchResponse, 1)
			e.request.responseChan = responseChan

			if e.stateAfter != nil {
				for i := range e.stateAfter.waitList {
					e.stateAfter.waitList[i].responseChan = responseChan
				}
			}

			requestChan := make(chan fetchRequest, 1)
			requestChan <- e.request

			state := e.stateBefore
			runFetchHandler(ctx, fetchHandlerInput{
				requestChan: requestChan,
			}, &state, nil)

			var response fetchResponse
			if !e.notWaitResp {
				response = <-responseChan
			}

			if e.stateAfter != nil {
				assert.Equal(t, *e.stateAfter, state)
			} else {
				assert.Equal(t, e.stateBefore, state)
			}

			assert.Equal(t, e.expected, response)
		})
	}
}

func TestRunFetchHandler_Panic(t *testing.T) {
	state := fetchHandlerState{
		firstSequence: 1,
		lastSequence:  1,
		data: []Event{
			nil,
			testEvent{id: 12, seq: 1},
			nil,
			nil,
		},
	}

	requestChan := make(chan fetchRequest, 1)
	requestChan <- fetchRequest{
		fromSequence: 3,
		limit:        3,
		responseChan: make(chan fetchResponse, 1),
	}

	defer func() {
		if msg, ok := recover().(string); ok {
			assert.Equal(t, "fromSequence MUST <= lastSequence + 1", msg)
		} else {
			assert.Fail(t, "must panic")
		}
	}()

	ctx := context.Background()
	runFetchHandler(ctx, fetchHandlerInput{
		requestChan: requestChan,
	}, &state, nil)

}

func TestNewFetchHandlerState(t *testing.T) {
	lastEvents := []Event{
		testEvent{id: 10, seq: 3},
		testEvent{id: 12, seq: 4},
		testEvent{id: 9, seq: 5},
		testEvent{id: 18, seq: 6},
	}
	s := newFetchHandlerState(lastEvents, 3, &fetchHandlerConfig{
		getSequence: getSequence,
	})
	expected := &fetchHandlerState{
		firstSequence: 4,
		lastSequence:  6,
		data: []Event{
			testEvent{id: 18, seq: 6},
			testEvent{id: 12, seq: 4},
			testEvent{id: 9, seq: 5},
		},
	}
	assert.Equal(t, expected, s)
}

func TestEventQueue(t *testing.T) {
	q := newEventQueue(3)

	q.push(testEvent{id: 1})
	assert.Equal(t, 1, q.len)
	assert.Equal(t, []Event{
		testEvent{id: 1},
		nil,
		nil,
	}, q.data)

	q.push(testEvent{id: 2})
	assert.Equal(t, 2, q.len)
	assert.Equal(t, []Event{
		testEvent{id: 1},
		testEvent{id: 2},
		nil,
	}, q.data)

	q.push(testEvent{id: 3})
	assert.Equal(t, 3, q.len)
	assert.Equal(t, []Event{
		testEvent{id: 1},
		testEvent{id: 2},
		testEvent{id: 3},
	}, q.data)

	q.push(testEvent{id: 4})
	assert.Equal(t, 3, q.len)
	assert.Equal(t, []Event{
		testEvent{id: 4},
		testEvent{id: 2},
		testEvent{id: 3},
	}, q.data)

	assert.Equal(t, testEvent{id: 2}, q.front())
	assert.Equal(t, testEvent{id: 4}, q.back())

	assert.Equal(t, []Event{
		testEvent{id: 2},
		testEvent{id: 3},
		testEvent{id: 4},
	}, q.getAll())
}
