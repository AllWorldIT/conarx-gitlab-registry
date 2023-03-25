package notifications

import (
	"fmt"
	"math/rand"
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

	b := NewBroadcaster(sinks...)

	event := createTestEvent("push", "library/test", "blob")
	for i := 0; i <= nEvents-1; i++ {
		if err := b.Write(&event); err != nil {
			t.Errorf("error writing event: %v", err)
		}
	}

	checkClose(t, b)

	// Iterate through the sinks and check that they all have the expected length.
	for _, sink := range sinks {
		ts := sink.(*testSink)
		ts.mu.Lock()
		defer ts.mu.Unlock()

		if len(ts.events) != nEvents {
			t.Fatalf("not all events ended up in testsink: len(testSink) == %d, not %d", len(ts.events), nEvents)
		}

		if !ts.closed {
			t.Fatalf("sink should have been closed")
		}
	}
}

func TestEventQueue(t *testing.T) {
	const nEvents = 1000
	var ts testSink
	metrics := newSafeMetrics()
	eq := newEventQueue(
		// delayed sync simulates destination slower than channel comms
		&delayedSink{
			Sink:  &ts,
			delay: time.Millisecond * 1,
		}, metrics.eventQueueListener())

	event := createTestEvent("push", "library/test", "blob")
	for i := 0; i <= nEvents-1; i++ {
		if err := eq.Write(&event); err != nil {
			t.Errorf("error writing event: %v", err)
		}
	}

	checkClose(t, eq)

	ts.mu.Lock()
	defer ts.mu.Unlock()
	metrics.Lock()
	defer metrics.Unlock()

	if len(ts.events) != nEvents {
		t.Fatalf("events did not make it to the sink: %d != %d", len(ts.events), 1000)
	}

	if !ts.closed {
		t.Fatalf("sink should have been closed")
	}

	if metrics.Events != nEvents {
		t.Fatalf("unexpected ingress count: %d != %d", metrics.Events, nEvents)
	}

	if metrics.Pending != 0 {
		t.Fatalf("unexpected egress count: %d != %d", metrics.Pending, 0)
	}
}

func TestIgnoredSink(t *testing.T) {
	blob := createTestEvent("push", "library/test", "blob")
	manifest := createTestEvent("pull", "library/test", "manifest")

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

		if err := s.Write(&blob); err != nil {
			t.Fatalf("error writing blob event: %v", err)
		}

		if err := s.Write(&manifest); err != nil {
			t.Fatalf("error writing blob event: %v", err)
		}

		ts.mu.Lock()
		require.ElementsMatch(t, ts.events, c.expected)
		ts.mu.Unlock()

		s.Close()
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

	var wg sync.WaitGroup
	event := createTestEvent("push", "library/test", "blob")
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Write(&event); err != nil {
				t.Errorf("error writing event block: %v", err)
			}
		}()
	}

	wg.Wait()
	if t.Failed() {
		t.FailNow()
	}
	checkClose(t, s)

	ts.mu.Lock()
	defer ts.mu.Unlock()

	if len(ts.events) != 10 {
		t.Fatalf("events not propagated: %d != %d", len(ts.events), 10)
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

func checkClose(t *testing.T, sink Sink) {
	if err := sink.Close(); err != nil {
		t.Fatalf("unexpected error closing: %v", err)
	}

	// second close should not crash but should return an error.
	if err := sink.Close(); err == nil {
		t.Fatalf("no error on double close")
	}

	// Write after closed should be an error
	if err := sink.Write(&Event{}); err == nil {
		t.Fatalf("write after closed did not have an error")
	} else if err != ErrSinkClosed {
		t.Fatalf("error should be ErrSinkClosed")
	}
}
