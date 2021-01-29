package eventcorev2

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

// x Processor
// . FetchService
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
		queueEvents        []Event
	}{
		{
			name:     "get-unprocessed-error",
			getError: errors.New("get error"),

			getCalled:   1,
			expected:    errors.New("get error"),
			queueEvents: []Event{},
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
			expected:    errors.New("update error"),
			queueEvents: []Event{},
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
			expected: errors.New("update error"),
			queueEvents: []Event{
				testEvent{id: 1, seq: 5},
				testEvent{id: 3, seq: 6},
			},
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
			queueEvents: []Event{
				testEvent{id: 3, seq: 6},
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
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
			queueEvents: []Event{
				testEvent{id: 3, seq: 6},
				testEvent{id: 12, seq: 7},
				testEvent{id: 11, seq: 8},
				testEvent{id: 15, seq: 9},
			},
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

			state := newProcessState(e.lastEvents, 4)

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
			assert.Equal(t, e.queueEvents, state.queue.getAll())
		})
	}
}

func TestNewProcessState(t *testing.T) {
	p := newProcessState([]Event{
		testEvent{id: 1, seq: 5},
		testEvent{id: 3, seq: 6},
	}, 3)

	assert.Equal(t, []Event{
		testEvent{id: 1, seq: 5},
		testEvent{id: 3, seq: 6},
	}, p.queue.getAll())
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
		event       Event
		stateBefore fetchHandlerState
		stateAfter  fetchHandlerState
	}{
		{
			name:  "first",
			event: testEvent{id: 10, seq: 2},
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
			name:  "second",
			event: testEvent{id: 9, seq: 3},
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
			name:  "wrap",
			event: testEvent{id: 18, seq: 6},
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
			name:  "near-wrap",
			event: testEvent{id: 13, seq: 5},
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
	}

	for _, e := range table {
		t.Run(e.name, func(t *testing.T) {
			ctx := context.Background()
			eventChan := make(chan Event, 1)

			eventChan <- e.event

			config := &fetchHandlerConfig{
				getSequence: getSequence,
			}

			state := e.stateBefore

			runFetchHandler(ctx, fetchHandlerInput{
				eventChan: eventChan,
			}, &state, config)

			assert.Equal(t, state, e.stateAfter)
		})
	}
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
