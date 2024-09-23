package notifications

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	log "github.com/sirupsen/logrus"
)

// NOTE(stevvooe): This file contains definitions for several utility sinks.
// Typically, the broadcaster is the only sink that should be required
// externally, but others are suitable for export if the need arises. Albeit,
// the tight integration with endpoint metrics should be removed.

// Broadcaster sends events to multiple, reliable Sinks. The goal of this
// component is to dispatch events to configured endpoints. Reliability can be
// provided by wrapping incoming sinks.
type Broadcaster struct {
	sinks  []Sink
	events chan *Event
	closed chan chan struct{}
}

// NewBroadcaster ...
// Add appends one or more sinks to the list of sinks. The broadcaster
// behavior will be affected by the properties of the sink. Generally, the
// sink should accept all messages and deal with reliability on its own. Use
// of EventQueue and RetryingSink should be used here.
func NewBroadcaster(sinks ...Sink) *Broadcaster {
	b := Broadcaster{
		sinks:  sinks,
		events: make(chan *Event),
		closed: make(chan chan struct{}),
	}

	// Start the broadcaster
	go b.run()

	return &b
}

// Write accepts an event to be dispatched to all sinks. This method
// will never fail and should never block (hopefully!). The caller cedes the
// slice memory to the broadcaster and should not modify it after calling
// write.
func (b *Broadcaster) Write(event *Event) error {
	select {
	case b.events <- event:
	case <-b.closed:
		return ErrSinkClosed
	}
	return nil
}

// Close the broadcaster, ensuring that all messages are flushed to the
// underlying sink before returning.
func (b *Broadcaster) Close() error {
	log.Infof("broadcaster: closing")
	select {
	case <-b.closed:
		// already closed
		return fmt.Errorf("broadcaster: already closed")
	default:
		// do a little chan handoff dance to synchronize closing
		closed := make(chan struct{})
		b.closed <- closed
		close(b.closed)
		<-closed
		return nil
	}
}

// run is the main broadcast loop, started when the broadcaster is created.
// Under normal conditions, it waits for events on the event channel. After
// Close is called, this goroutine will exit.
func (b *Broadcaster) run() {
	for {
		select {
		case block := <-b.events:
			for _, sink := range b.sinks {
				if err := sink.Write(block); err != nil {
					log.Errorf("broadcaster: error writing events to %v, these events will be lost: %v", sink, err)
				}
			}
		case closing := <-b.closed:

			// close all the underlying sinks
			for _, sink := range b.sinks {
				if err := sink.Close(); err != nil {
					log.Errorf("broadcaster: error closing sink %v: %v", sink, err)
				}
			}
			closing <- struct{}{}

			log.Debugf("broadcaster: closed")
			return
		}
	}
}

// eventQueue accepts all messages into a queue for asynchronous consumption
// by a sink. It is unbounded and thread safe but the sink must be reliable or
// events will be dropped.
type eventQueue struct {
	sink      Sink
	events    *list.List
	listeners []eventQueueListener
	cond      *sync.Cond
	mu        sync.Mutex
	closed    bool
}

// eventQueueListener is called when various events happen on the queue.
type eventQueueListener interface {
	ingress(events *Event)
	egress(events *Event)
}

// newEventQueue returns a queue to the provided sink. If the updater is non-
// nil, it will be called to update pending metrics on ingress and egress.
func newEventQueue(sink Sink, listeners ...eventQueueListener) *eventQueue {
	eq := eventQueue{
		sink:      sink,
		events:    list.New(),
		listeners: listeners,
	}

	eq.cond = sync.NewCond(&eq.mu)
	go eq.run()
	return &eq
}

// Write accepts an event into the queue, only failing if the queue has
// beend closed.
func (eq *eventQueue) Write(event *Event) error {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	if eq.closed {
		return ErrSinkClosed
	}

	for _, listener := range eq.listeners {
		listener.ingress(event)
	}
	eq.events.PushBack(event)
	eq.cond.Signal() // signal waiters

	return nil
}

// Close shuts down the event queue, flushing
func (eq *eventQueue) Close() error {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	if eq.closed {
		return fmt.Errorf("eventqueue: already closed")
	}

	// set closed flag
	eq.closed = true
	eq.cond.Signal() // signal flushes queue
	eq.cond.Wait()   // wait for signal from last flush

	return eq.sink.Close()
}

// run is the main goroutine to flush events to the target sink.
func (eq *eventQueue) run() {
	for {
		block := eq.next()

		if block == nil {
			return // nil block means event queue is closed.
		}

		if err := eq.sink.Write(block); err != nil {
			log.WithError(err).Warnf("eventqueue: event lost: %v", block)
		}

		for _, listener := range eq.listeners {
			listener.egress(block)
		}
	}
}

// next encompasses the critical section of the run loop. When the queue is
// empty, it will block on the condition. If new data arrives, it will wake
// and return a block. When closed, a nil slice will be returned.
func (eq *eventQueue) next() *Event {
	eq.mu.Lock()
	defer eq.mu.Unlock()

	for eq.events.Len() < 1 {
		if eq.closed {
			eq.cond.Broadcast()
			return nil
		}

		eq.cond.Wait()
	}
	front := eq.events.Front()
	block := front.Value.(*Event)
	eq.events.Remove(front)

	return block
}

// ignoredSink discards events with ignored target media types and actions.
// passes the rest along.
type ignoredSink struct {
	Sink
	ignoreMediaTypes map[string]bool
	ignoreActions    map[string]bool
}

func newIgnoredSink(sink Sink, ignored, ignoreActions []string) Sink {
	if len(ignored) == 0 {
		return sink
	}

	ignoredMap := make(map[string]bool)
	for _, mediaType := range ignored {
		ignoredMap[mediaType] = true
	}

	ignoredActionsMap := make(map[string]bool)
	for _, action := range ignoreActions {
		ignoredActionsMap[action] = true
	}

	return &ignoredSink{
		Sink:             sink,
		ignoreMediaTypes: ignoredMap,
		ignoreActions:    ignoredActionsMap,
	}
}

// Write discards an event with ignored target media types or passes the event
// along.
func (imts *ignoredSink) Write(event *Event) error {
	if event == nil {
		return nil
	}
	if imts.ignoreMediaTypes[event.Target.MediaType] {
		return nil
	}
	if imts.ignoreActions[event.Action] {
		return nil
	}

	return imts.Sink.Write(event)
}

// retryingSink retries the write until success or an ErrSinkClosed is
// returned. Underlying sink must have p > 0 of succeeding or the sink will
// block. Internally, it is a circuit breaker retries to manage reset.
// Concurrent calls to a retrying sink are serialized through the sink,
// meaning that if one is in-flight, another will not proceed.
type retryingSink struct {
	sink Sink

	doneCh   chan struct{}
	eventsCh chan *Event
	errCh    chan error

	wg *sync.WaitGroup

	// circuit breaker heuristics
	failures struct {
		threshold int
		backoff   time.Duration // time after which we retry after failure.
	}
}

// newRetryingSink returns a sink that will retry writes to a sink, backing
// off on failure. Parameters threshold and backoff adjust the behavior of the
// circuit breaker.
func newRetryingSink(sink Sink, threshold int, backoff time.Duration) *retryingSink {
	rs := &retryingSink{
		sink: sink,

		doneCh:   make(chan struct{}),
		eventsCh: make(chan *Event),
		errCh:    make(chan error),

		wg: new(sync.WaitGroup),
	}
	rs.failures.threshold = threshold
	rs.failures.backoff = backoff

	rs.wg.Add(1)
	go rs.run()

	return rs
}

func (rs *retryingSink) run() {
	defer rs.wg.Done()

main:
	for {
		select {
		case <-rs.doneCh:
			return
		case event := <-rs.eventsCh:
			for failuresCount := 0; failuresCount < rs.failures.threshold; failuresCount++ {
				select {
				case <-rs.doneCh:
					rs.errCh <- ErrSinkClosed
					return
				default:
				}

				err := rs.sink.Write(event)

				// Event sent sucessfully, fetch next event from channel:
				if err == nil {
					rs.errCh <- nil
					continue main
				}

				// Underlying sink is closed, let's wrap up:
				if errors.Is(err, ErrSinkClosed) {
					rs.errCh <- ErrSinkClosed
					return
				}

				log.WithError(err).
					WithField("railure_count", failuresCount).
					Error("retryingsink: error writing event, retrying")
			}

			log.WithField("sink", rs.sink).
				Warnf("encountered too many errors when writing to sink, enabling backoff")

			for {
				// NOTE(prozlach): We can't use Ticker here as the write()
				// operation may take longer than the backoff period and this
				// would result in triggering new write imediatelly after the
				// previous one.
				timer := time.NewTimer(rs.failures.backoff)

				select {
				case <-rs.doneCh:
					rs.errCh <- ErrSinkClosed
					timer.Stop()
					return
				case lastFailureTime := <-timer.C:
					err := rs.sink.Write(event)

					// Event sent successfully, fetch next event from channel:
					if err == nil {
						rs.errCh <- nil
						continue main
					}

					// Underlying sink is closed, let's wrap up:
					if errors.Is(err, ErrSinkClosed) {
						rs.errCh <- ErrSinkClosed
						return
					}

					log.WithError(err).
						WithField("next_retry_time", lastFailureTime.Add(rs.failures.backoff).String()).
						Error("retryingsink: error writing event, backing off")
				}
			}
		}
	}
}

// Write attempts to flush the event to the downstream sink until it succeeds
// or the sink is closed.
func (rs *retryingSink) Write(event *Event) error {
	// NOTE(prozlach): avoid a racy situation when both channels are "ready",
	// and make sure that closing the Sink takes priority:
	select {
	case <-rs.doneCh:
		return ErrSinkClosed
	default:
		select {
		case rs.eventsCh <- event:
			return <-rs.errCh
		case <-rs.doneCh:
			return ErrSinkClosed
		}
	}
}

// Close closes the sink and the underlying sink.
func (rs *retryingSink) Close() error {
	log.Infof("retryingSink: closing")
	select {
	case <-rs.doneCh:
		return fmt.Errorf("retryingSink: already closed")
	default:
		close(rs.doneCh)
	}

	// NOTE(prozlach): the order of things is very important here as we need to
	// first make sure that no new events will be accepted by this sink and
	// then cancel the underlying sink so that we can unblock this sink so that
	// it notices that the termination signal came.
	err := rs.sink.Close()
	rs.wg.Wait()

	// NOTE(prozlach): not stricly necessary, just a basic hygiene
	close(rs.eventsCh)
	close(rs.errCh)

	log.Debugf("retryingSink: closed")

	return err
}

// backoffSink attempts to write an event to the given sink.
// It will retry up to a number of maxretries as defined in the configuration
// and will drop the event after it reaches the number of retries.
type backoffSink struct {
	doneCh  chan struct{}
	sink    Sink
	backoff backoff.BackOff
}

func newBackoffSink(sink Sink, initialInterval time.Duration, maxRetries int) *backoffSink {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initialInterval

	return &backoffSink{
		doneCh:  make(chan struct{}),
		sink:    sink,
		backoff: backoff.WithMaxRetries(b, uint64(maxRetries)),
	}
}

// Write attempts to flush the event to the downstream sink using an
// exponential backoff strategy. If the max number of retries is
// reached, an error is returned and the event is dropped.
// It returns early if the sink is closed.
func (bs *backoffSink) Write(event *Event) error {
	op := func() error {
		select {
		case <-bs.doneCh:
			return backoff.Permanent(ErrSinkClosed)
		default:
		}

		if err := bs.sink.Write(event); err != nil {
			log.WithError(err).Error("backoffSink: error writing event")
			return err
		}

		return nil
	}

	return backoff.Retry(op, bs.backoff)
}

// Close closes the sink and the underlying sink.
func (bs *backoffSink) Close() error {
	log.Infof("backoffSink: closing")
	select {
	case <-bs.doneCh:
		// already closed
		return fmt.Errorf("backoffSink: already closed")
	default:
		// NOTE(prozlach): not stricly necessary, just a basic hygiene
		close(bs.doneCh)
	}

	return bs.sink.Close()
}
