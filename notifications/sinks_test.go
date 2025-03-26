package notifications

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
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
	metrics.Lock()
	defer metrics.Unlock()

	require.Len(t, ts.events, nEvents, "events did not make it to the sink")

	require.True(t, ts.closed, "sink should have been closed")

	require.Equal(t, nEvents, metrics.Events, "unexpected ingress count")

	require.Equal(t, 0, metrics.Pending, "unexpected egress count")
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

func checkClose(t *testing.T, sink Sink) {
	require.NoError(t, sink.Close(), "unexpected error closing")

	// second close should not crash but should return an error.
	require.Error(t, sink.Close(), "no error on double close")

	// Write after closed should be an error
	require.ErrorIs(t, sink.Write(&Event{}), ErrSinkClosed, "write after closed should return ErrSinkClosed")
}
