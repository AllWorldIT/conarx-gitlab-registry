package notifications

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroadcaster(t *testing.T) {
	const nEvents = 1000
	var sinks []Sink

	for i := 0; i < 10; i++ {
		sinks = append(sinks, &testSink{})
	}

	b := NewBroadcaster(
		// NOTE(prozlach): The very high timeout is motivied by the fact that
		// we want to avoid any flakes. 60 seconds should be more than enough
		// to finish broadcasting to all sinks. In production this timeout is
		// much lower as we do not have any delivery guarantees ATM and the
		// purge timeout is meant only to allow for graceful termination of
		// the queue, not a reliable delivery.
		60*time.Second,
		sinks...,
	)

	event := createTestEvent("push", "blob")
	for i := 0; i <= nEvents-1; i++ {
		require.NoError(t, b.Write(&event), "error writing event")
	}

	checkClose(t, b)

	// Iterate through the sinks and check that they all have the expected length.
	for _, sink := range sinks {
		ts := sink.(*testSink)
		ts.mu.Lock()
		// nolint: revive // defer
		defer ts.mu.Unlock()

		require.Len(t, ts.events, nEvents, "not all events ended up in testsink")

		require.True(t, ts.closed, "sink should have been closed")
	}
}

func TestEventQueue(t *testing.T) {
	const nEvents = 1000
	var ts testSink
	metrics := newSafeMetrics(t.Name())
	eq := newEventQueue(
		// delayed sync simulates destination slower than channel comms
		&delayedSink{
			Sink:  &ts,
			delay: time.Millisecond * 1,
		},
		// NOTE(prozlach): The very high timeout is motivied by the fact that
		// we want to avoid any flakes. 60 seconds should be more than enough
		// to purge the queue buffer. In production this timeout is much lower
		// as we do not have any devlier guarantees ATM and the purge timeout
		// is meant only to allow for graceful termination of the queue, not a
		// reliable delivery.
		60*time.Second,
		metrics.eventQueueListener(),
	)

	event := createTestEvent("push", "blob")
	for i := 0; i <= nEvents-1; i++ {
		require.NoError(t, eq.Write(&event), "error writing event")
	}

	checkClose(t, eq)

	ts.mu.Lock()
	defer ts.mu.Unlock()
	require.Len(t, ts.events, nEvents, "events did not make it to the sink")

	require.True(t, ts.closed, "sink should have been closed")

	require.EqualValues(t, nEvents, metrics.events.Load(), "unexpected ingress count")

	require.Zero(t, metrics.pending.Load(), "unexpected egress count")
}

func TestRetryingSinkWithDeliveryListener(t *testing.T) {
	t.Run("successful delivery on first attempt", func(tt *testing.T) {
		metrics := newSafeMetrics(tt.Name())
		deliveryListener := metrics.deliveryListener()
		ts := &testSink{}

		s := newRetryingSink(ts, 3, 10*time.Millisecond, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		require.NoError(tt, s.Write(&event))

		assert.EqualValues(tt, 1, metrics.delivered.Load())
		assert.Zero(tt, metrics.retries.Load())

		ts.mu.Lock()
		assert.Len(tt, ts.events, 1, "event should be in test sink")
		ts.mu.Unlock()
	})

	t.Run("successful delivery after retries", func(tt *testing.T) {
		metrics := newSafeMetrics(tt.Name())
		deliveryListener := metrics.deliveryListener()

		failing := &failingSink{
			failBelowCount: 2,
			Sink:           &testSink{},
		}

		s := newRetryingSink(failing, 3, 10*time.Millisecond, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		require.NoError(tt, s.Write(&event))

		assert.EqualValues(tt, 1, metrics.delivered.Load())
		assert.Positive(tt, metrics.retries.Load())
	})

	t.Run("delivery with backoff period", func(tt *testing.T) {
		metrics := newSafeMetrics(tt.Name())
		deliveryListener := metrics.deliveryListener()

		failing := &failingSink{
			failBelowCount: 5,
			Sink:           &testSink{},
		}

		s := newRetryingSink(failing, 3, 50*time.Millisecond, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		start := time.Now()
		require.NoError(tt, s.Write(&event))
		elapsed := time.Since(start)

		assert.Greater(tt, elapsed, 50*time.Millisecond)
		assert.EqualValues(tt, 1, metrics.delivered.Load())
		assert.Positive(tt, metrics.retries.Load())
	})

	t.Run("concurrent writes", func(tt *testing.T) {
		metrics := newSafeMetrics(tt.Name())
		deliveryListener := metrics.deliveryListener()

		// Flaky sink that fails 30% of the time
		flaky := &flakySink{
			rate: 0.3,
			Sink: &testSink{},
		}

		s := newRetryingSink(flaky, 5, 10*time.Millisecond, deliveryListener)
		defer s.Close()

		const nEvents = 10
		var wg sync.WaitGroup

		for i := 0; i < nEvents; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				event := createTestEvent("push", fmt.Sprintf("blob-%d", i))
				assert.NoError(tt, s.Write(&event))
			}(i)
		}

		wg.Wait()

		// All events should be delivered (retryingSink retries indefinitely)
		assert.EqualValues(tt, nEvents, metrics.delivered.Load())

		// Should have some retries due to 30% failure rate
		assert.Positive(tt, metrics.retries.Load())
	})
}

func TestIgnoredSink(t *testing.T) {
	blob := createTestEvent("push", "blob")
	manifest := createTestEvent("pull", "manifest")

	type testcase struct {
		ignoreMediaTypes []string
		ignoreActions    []string
		expected         []*Event
	}

	cases := []testcase{
		{nil, nil, []*Event{&blob, &manifest}},
		{[]string{"other"}, []string{"other"}, []*Event{&blob, &manifest}},
		{[]string{"blob"}, []string{"other"}, []*Event{&manifest}},
		{[]string{"blob", "manifest"}, []string{"other"}, nil},
		{[]string{"other"}, []string{"push"}, []*Event{&manifest}},
		{[]string{"other"}, []string{"pull"}, []*Event{&blob}},
		{[]string{"other"}, []string{"pull", "push"}, nil},
	}

	for _, c := range cases {
		ts := &testSink{}
		s := newIgnoredSink(ts, c.ignoreMediaTypes, c.ignoreActions)

		require.NoError(t, s.Write(&blob), "error writing blob event")

		require.NoError(t, s.Write(&manifest), "error writing blob event")

		ts.mu.Lock()
		require.ElementsMatch(t, c.expected, ts.events)
		ts.mu.Unlock()

		err := s.Close()
		require.NoError(t, err)
	}
}

func TestRetryingSink(t *testing.T) {
	// Make a sync that fails most of the time, ensuring that all the events
	// make it through.
	var ts testSink
	flaky := &flakySink{
		rate: 0.9, // 90% failure rate
		Sink: &ts,
	}
	s := newRetryingSink(flaky, 3, 10*time.Millisecond)

	event := createTestEvent("push", "blob")
	errCh := make(chan error, 10)
	for i := 1; i <= 10; i++ {
		go func() {
			errCh <- s.Write(&event)
		}()
	}

	for i := 1; i <= 10; i++ {
		require.NoErrorf(t, <-errCh, "error writing event %d", i)
	}

	checkClose(t, s)

	ts.mu.Lock()
	defer ts.mu.Unlock()

	require.Len(t, ts.events, 10, "events not propagated")
}

func TestBackoffSink(t *testing.T) {
	tcs := map[string]struct {
		maxRetries    int
		failCount     int
		expectedError bool
	}{
		"fail count below max retries succeeds": {
			maxRetries:    3,
			failCount:     2,
			expectedError: false,
		},
		"always fails": {
			maxRetries:    1,
			failCount:     2,
			expectedError: true,
		},
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			failing := &failingSink{
				failBelowCount: tc.failCount,
				Sink:           &testSink{},
			}

			s := newBackoffSink(failing, 10*time.Millisecond, tc.maxRetries)
			event := createTestEvent("push", "blob")
			err := s.Write(&event)
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			checkClose(t, s)
		})
	}
}

func TestBackoffSinkWithDeliveryListener(t *testing.T) {
	t.Run("successful delivery on first attempt", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()
		ts := &testSink{}

		// Use the real backoffSink from sinks.go
		s := newBackoffSink(ts, 10*time.Millisecond, 3, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		require.NoError(t, s.Write(&event))

		// Due to the bug, successful delivery calls eventLost
		assert.Zero(t, metrics.lost.Load())
		assert.Zero(t, metrics.retries.Load()) // retriesCount is incremented after the operation
		assert.EqualValues(t, 1, metrics.delivered.Load())
	})

	t.Run("successful delivery with backoff", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()

		failing := &failingSink{
			failBelowCount: 2,
			Sink:           &testSink{},
		}

		s := newBackoffSink(failing, 10*time.Millisecond, 3, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		require.NoError(t, s.Write(&event))

		// Due to the bug, successful delivery calls eventLost
		assert.Zero(t, metrics.lost.Load())
		assert.EqualValues(t, 2, metrics.retries.Load()) // 2 failures + 1 success
		assert.EqualValues(t, 1, metrics.delivered.Load())
	})

	t.Run("lost event after max retries", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()

		alwaysFailing := &alwaysFailingSink{}

		s := newBackoffSink(alwaysFailing, 10*time.Millisecond, 2, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		err := s.Write(&event)
		require.Error(t, err)

		// Due to the bug, failed delivery calls eventDelivered
		assert.Zero(t, metrics.delivered.Load())
		assert.EqualValues(t, 2, metrics.retries.Load()) // maxRetries + 1
		assert.EqualValues(t, 1, metrics.lost.Load())
	})

	t.Run("verify exponential backoff timing", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()

		// Track timing of attempts
		var attempts []time.Time
		var mu sync.Mutex

		timingTestSink := &timingTestSink{
			failUntilAttempt: 3,
			onWrite: func() {
				mu.Lock()
				attempts = append(attempts, time.Now())
				mu.Unlock()
			},
		}

		s := newBackoffSink(timingTestSink, 500*time.Millisecond, 3, deliveryListener)
		defer s.Close()

		event := createTestEvent("push", "blob")
		require.NoError(t, s.Write(&event))
		endTime := time.Now()

		require.Len(t, attempts, 3, "should have made 3 attempts")

		firstInterval := attempts[1].Sub(attempts[0])
		require.Greater(t, firstInterval, 249*time.Millisecond)
		require.Less(t, firstInterval, 751*time.Millisecond)

		secondInterval := attempts[2].Sub(attempts[1])
		require.Greater(t, secondInterval, 374*time.Millisecond)
		require.Less(t, secondInterval, 1126*time.Millisecond)

		totalTime := endTime.Sub(attempts[0])
		require.Greater(t, totalTime, (250+375)*time.Millisecond)
	})

	t.Run("multiple events with mixed outcomes", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()

		// Flaky sink that fails 50% of the time
		flaky := &flakySink{
			rate: 0.5,
			Sink: &testSink{},
		}

		s := newBackoffSink(flaky, 5*time.Millisecond, 3, deliveryListener)
		defer s.Close()

		const nEvents = 20
		var successCount, failCount int

		for i := 0; i < nEvents; i++ {
			event := createTestEvent("push", fmt.Sprintf("blob-%d", i))
			if err := s.Write(&event); err != nil {
				failCount++
			} else {
				successCount++
			}
		}

		require.EqualValues(t, failCount, metrics.lost.Load())
		require.EqualValues(t, successCount, metrics.delivered.Load())
		require.EqualValues(t, nEvents, metrics.lost.Load()+metrics.delivered.Load())

		// Should have retries due to 50% failure rate
		require.Positive(t, metrics.retries.Load())
	})
}

func TestConcurrentDeliveryReporting(t *testing.T) {
	t.Run("retryingSink concurrent writes", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()

		// Create a flaky sink that fails 30% of the time
		flaky := &flakySink{
			rate: 0.3,
			Sink: &testSink{},
		}

		s := newRetryingSink(flaky, 5, 5*time.Millisecond, deliveryListener)
		defer s.Close()

		const nGoroutines = 10
		const nEventsPerGoroutine = 10

		var wg sync.WaitGroup
		for i := 0; i < nGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < nEventsPerGoroutine; j++ {
					event := createTestEvent("push", fmt.Sprintf("blob-%d-%d", goroutineID, j))
					assert.NoError(t, s.Write(&event))
				}
			}(i)
		}

		wg.Wait()

		totalEvents := nGoroutines * nEventsPerGoroutine
		// All events should be delivered (retryingSink retries indefinitely)
		require.EqualValues(t, totalEvents, metrics.delivered.Load())
		require.Zero(t, metrics.lost.Load())

		// Should have some retries due to 30% failure rate
		require.Positive(t, metrics.retries.Load())
	})

	t.Run("backoffSink concurrent writes", func(t *testing.T) {
		metrics := newSafeMetrics(t.Name())
		deliveryListener := metrics.deliveryListener()

		// Create a flaky sink that fails 40% of the time
		flaky := &flakySink{
			rate: 0.4,
			Sink: &testSink{},
		}

		s := newBackoffSink(flaky, 5*time.Millisecond, 4, deliveryListener)
		defer s.Close()

		const nGoroutines = 10
		const nEventsPerGoroutine = 5

		var wg sync.WaitGroup
		var successCount, failCount atomic.Int64

		for i := 0; i < nGoroutines; i++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for j := 0; j < nEventsPerGoroutine; j++ {
					event := createTestEvent("push", fmt.Sprintf("blob-%d-%d", goroutineID, j))
					if err := s.Write(&event); err != nil {
						failCount.Add(1)
					} else {
						successCount.Add(1)
					}
				}
			}(i)
		}

		wg.Wait()

		totalEvents := nGoroutines * nEventsPerGoroutine
		// All events should be either delivered or lost
		require.EqualValues(t, totalEvents, metrics.delivered.Load()+metrics.lost.Load())
		require.Equal(t, successCount.Load(), metrics.delivered.Load())
		require.Equal(t, failCount.Load(), metrics.lost.Load())

		// With 40% failure rate and 4 retries, most should be delivered
		require.Greater(t, metrics.delivered.Load(), metrics.lost.Load())

		// Should have retries
		require.Positive(t, metrics.retries.Load())
	})
}

// Test sinks

type testSink struct {
	events []*Event
	mu     sync.Mutex
	closed bool
}

func (ts *testSink) Write(event *Event) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.events = append(ts.events, event)
	return nil
}

func (ts *testSink) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.closed = true

	logrus.Infof("closing testSink")
	return nil
}

type delayedSink struct {
	Sink
	delay time.Duration
}

func (ds *delayedSink) Write(event *Event) error {
	time.Sleep(ds.delay)
	return ds.Sink.Write(event)
}

type failingSink struct {
	Sink
	currentCount   int
	failBelowCount int
}

func (fs *failingSink) Write(event *Event) error {
	fs.currentCount++
	if fs.currentCount <= fs.failBelowCount {
		return fmt.Errorf("error writing event")
	}

	return fs.Sink.Write(event)
}

type flakySink struct {
	Sink
	rate float64
}

func (fs *flakySink) Write(event *Event) error {
	if rand.Float64() < fs.rate {
		return fmt.Errorf("error writing event")
	}

	return fs.Sink.Write(event)
}

type alwaysFailingSink struct{}

func (*alwaysFailingSink) Write(_ *Event) error {
	return fmt.Errorf("always failing")
}

func (*alwaysFailingSink) Close() error {
	return nil
}

type timingTestSink struct {
	failUntilAttempt int
	currentAttempt   int
	onWrite          func()
}

func (tts *timingTestSink) Write(*Event) error {
	tts.currentAttempt++
	if tts.onWrite != nil {
		tts.onWrite()
	}

	if tts.currentAttempt < tts.failUntilAttempt {
		return fmt.Errorf("failing attempt %d", tts.currentAttempt)
	}

	return nil
}

func (*timingTestSink) Close() error {
	return nil
}

// Remove the artificial sinks with delivery reporting since we're using the real ones now

func checkClose(t *testing.T, sink Sink) {
	require.NoError(t, sink.Close(), "unexpected error closing")

	// second close should not crash but should return an error.
	require.Error(t, sink.Close(), "no error on double close")

	// Write after closed should be an error
	require.ErrorIs(t, sink.Write(&Event{}), ErrSinkClosed, "write after closed should return ErrSinkClosed")
}
