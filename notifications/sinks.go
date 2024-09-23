package notifications

import (
	"container/list"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/go-multierror"

	log "github.com/sirupsen/logrus"
)

const DefaultBroadcasterFanoutTimeout = 15 * time.Second

// NOTE(stevvooe): This file contains definitions for several utility sinks.
// Typically, the broadcaster is the only sink that should be required
// externally, but others are suitable for export if the need arises. Albeit,
// the tight integration with endpoint metrics should be removed.

// Broadcaster sends events to multiple, reliable Sinks. The goal of this
// component is to dispatch events to configured endpoints. Reliability can be
// provided by wrapping incoming sinks.
type Broadcaster struct {
	sinks []Sink

	eventsCh chan *Event
	doneCh   chan struct{}

	fanoutTimeout time.Duration

	wg *sync.WaitGroup
}

// NewBroadcaster ...
// Add appends one or more sinks to the list of sinks. The broadcaster
// behavior will be affected by the properties of the sink. Generally, the
// sink should accept all messages and deal with reliability on its own. Use
// of EventQueue and RetryingSink should be used here.
func NewBroadcaster(fanoutTimeout time.Duration, sinks ...Sink) *Broadcaster {
	if fanoutTimeout == 0 {
		fanoutTimeout = DefaultBroadcasterFanoutTimeout
	}
	b := Broadcaster{
		sinks: sinks,

		eventsCh: make(chan *Event),
		doneCh:   make(chan struct{}),

		fanoutTimeout: fanoutTimeout,

		wg: new(sync.WaitGroup),
	}

	// Start the broadcaster
	b.wg.Add(1)
	go b.run()

	return &b
}

// Write accepts an event to be dispatched to all sinks. This method
// will never fail and should never block (hopefully!). The caller cedes the
// slice memory to the broadcaster and should not modify it after calling
// write.
func (b *Broadcaster) Write(event *Event) error {
	// NOTE(prozlach): avoid a racy situation when both channels are "ready",
	// and make sure that closing the Sink takes priority:
	select {
	case <-b.doneCh:
		return ErrSinkClosed
	default:
		select {
		case b.eventsCh <- event:
		case <-b.doneCh:
			return ErrSinkClosed
		}
	}

	return nil
}

// Close the broadcaster, ensuring that all messages are flushed to the
// underlying sink before returning.
func (b *Broadcaster) Close() error {
	log.Infof("broadcaster: closing")
	select {
	case <-b.doneCh:
		// already closed
		return fmt.Errorf("broadcaster: already closed")
	default:
		close(b.doneCh)
	}

	b.wg.Wait()

	errs := new(multierror.Error)
	for _, sink := range b.sinks {
		if err := sink.Close(); err != nil {
			errs = multierror.Append(errs, err)
			log.WithError(err).Errorf("broadcaster: error closing sink %v", sink)
		}
	}

	// NOTE(prozlach): not stricly necessary, just a basic hygiene
	close(b.eventsCh)

	log.Debugf("broadcaster: closed")
	return errs.ErrorOrNil()
}

// run is the main broadcast loop, started when the broadcaster is created.
// Under normal conditions, it waits for events on the event channel. After
// Close is called, this goroutine will exit.
func (b *Broadcaster) run() {
	defer b.wg.Done()

loop:
	for {
		select {
		case event := <-b.eventsCh:
			sinksCount := len(b.sinks)

			// NOTE(prozlach): we would only have a sink in the broadcaster if
			// there are any endpoints configured. Ideally the broadcaster
			// should not exist if there are no endpoints configured, but this
			// would require a bigger refactoring.
			if sinksCount == 0 {
				log.Debugf("broadcaster: there are no sinks configured, dropping event %v", event)
				continue loop
			}

			// NOTE(prozlach): The approach here is a compromise between the
			// existing behaviour of Broadcaster (__attempt__ to reliably
			// deliver to all dependant sinks) and making Broadcaster
			// interruptable so that gracefull shutdown of container registry
			// is possible.
			// The idea is to do Write() calls in goroutine (they are blocking)
			// and if the termination signal is received, wait up to
			// fanouttimeout and then terminate `run()` goroutine, which in
			// turn unblocks `Close()` call to close all sinks which terminates
			// the gouroutines which do `Write()` calls as an efect..
			finishedCount := 0
			finishedCh := make(chan struct{}, sinksCount)

			for i := 0; i < sinksCount; i++ {
				go func(i int) {
					if err := b.sinks[i].Write(event); err != nil {
						log.WithError(err).
							Errorf("broadcaster: error writing events to %v, these events will be lost", b.sinks[i])
					}

					finishedCh <- struct{}{}
				}(i)
			}

		inner:
			for {
				select {
				case <-b.doneCh:
					timer := time.NewTimer(b.fanoutTimeout)

					log.WithField("sinks_remaining", sinksCount-finishedCount).
						Warnf("broadcaster: received termination signal")

					select {
					case <-timer.C:
						log.WithField("sinks_remaining", sinksCount-finishedCount).
							Warnf("broadcaster: queue purge timeout reached, sink broadcasts dropped")
						return
					case <-finishedCh:
						finishedCount += 1
						if finishedCount == sinksCount {
							// All notifications were sent before the timeout
							// was reached. We are done here.
							return
						}
					}
				case <-finishedCh:
					finishedCount += 1
					if finishedCount == sinksCount {
						// All done!
						break inner
					}
				}
			}
		case <-b.doneCh:
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
	mu     sync.Mutex
	sink   Sink
	closed bool

	// circuit breaker heuristics
	failures struct {
		threshold int
		recent    int
		last      time.Time
		backoff   time.Duration // time after which we retry after failure.
	}
}

// newRetryingSink returns a sink that will retry writes to a sink, backing
// off on failure. Parameters threshold and backoff adjust the behavior of the
// circuit breaker.
func newRetryingSink(sink Sink, threshold int, backoff time.Duration) *retryingSink {
	rs := &retryingSink{
		sink: sink,
	}
	rs.failures.threshold = threshold
	rs.failures.backoff = backoff

	return rs
}

// Write attempts to flush the event to the downstream sink until it succeeds
// or the sink is closed.
func (rs *retryingSink) Write(event *Event) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

retry:

	if rs.closed {
		return ErrSinkClosed
	}

	if !rs.proceed() {
		log.Warnf("%v encountered too many errors, backing off", rs.sink)
		rs.wait(rs.failures.backoff)
		goto retry
	}

	if err := rs.write(event); err != nil {
		if errors.Is(err, ErrSinkClosed) {
			// terminal!
			return err
		}

		log.Errorf("retryingsink: error writing events: %v, retrying", err)
		goto retry
	}

	return nil
}

// Close closes the sink and the underlying sink.
func (rs *retryingSink) Close() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.closed {
		return fmt.Errorf("retryingsink: already closed")
	}

	rs.closed = true
	return rs.sink.Close()
}

// write provides a helper that dispatches failure and success properly. Used
// by write as the single-flight write call.
func (rs *retryingSink) write(event *Event) error {
	if err := rs.sink.Write(event); err != nil {
		rs.failure()
		return err
	}

	rs.reset()
	return nil
}

// wait backoff time against the sink, unlocking so others can proceed. Should
// only be called by methods that currently have the mutex.
func (rs *retryingSink) wait(backoff time.Duration) {
	rs.mu.Unlock()
	defer rs.mu.Lock()

	// backoff here
	time.Sleep(backoff)
}

// reset marks a successful call.
func (rs *retryingSink) reset() {
	rs.failures.recent = 0
	rs.failures.last = time.Time{}
}

// failure records a failure.
func (rs *retryingSink) failure() {
	rs.failures.recent++
	rs.failures.last = time.Now().UTC()
}

// proceed returns true if the call should proceed based on circuit breaker
// heuristics.
func (rs *retryingSink) proceed() bool {
	return rs.failures.recent < rs.failures.threshold ||
		time.Now().UTC().After(rs.failures.last.Add(rs.failures.backoff))
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
