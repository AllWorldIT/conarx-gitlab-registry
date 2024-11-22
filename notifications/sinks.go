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
			// existing behavior of Broadcaster (__attempt__ to reliably
			// deliver to all dependant sinks) and making Broadcaster
			// interruptable so that graceful shutdown of container registry
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
	listeners []eventQueueListener

	doneCh chan struct{}

	bufferInCh  chan *Event
	bufferOutCh chan *Event

	queuePurgeTimeout time.Duration

	wgBufferer *sync.WaitGroup
	wgSender   *sync.WaitGroup
}

// eventQueueListener is called when various events happen on the queue.
type eventQueueListener interface {
	ingress(events *Event)
	egress(events *Event)
}

// newEventQueue returns a queue to the provided sink. If the updater is non-
// nil, it will be called to update pending metrics on ingress and egress.
func newEventQueue(sink Sink, queuePurgeTimeout time.Duration, listeners ...eventQueueListener) *eventQueue {
	eq := eventQueue{
		sink:      sink,
		listeners: listeners,

		doneCh: make(chan struct{}),

		bufferInCh:  make(chan *Event),
		bufferOutCh: make(chan *Event),

		queuePurgeTimeout: queuePurgeTimeout,

		wgBufferer: new(sync.WaitGroup),
		wgSender:   new(sync.WaitGroup),
	}

	eq.wgSender.Add(1)
	eq.wgBufferer.Add(1)

	go eq.sender()
	go eq.bufferer()
	return &eq
}

// Write accepts an event into the queue, only failing if the queue has
// been closed.
func (eq *eventQueue) Write(event *Event) error {
	// NOTE(prozlach): avoid a racy situation when both channels are "ready",
	// and make sure that closing the Sink takes priority:
	select {
	case <-eq.doneCh:
		return ErrSinkClosed
	default:
		select {
		case eq.bufferInCh <- event:
		case <-eq.doneCh:
			return ErrSinkClosed
		}
	}

	return nil
}

func (eq *eventQueue) bufferer() {
	defer eq.wgBufferer.Done()
	defer log.Debugf("eventQueue bufferer: closed")

	events := list.New()

	// Main loop is executed during normal operation. Depending on whether there
	// are any events in the buffer or not, we include in select wait on write
	// to the sender goroutine or not respectively.
main:
	for {
		if events.Len() < 1 {
			// List is empty, wait for an event
			select {
			case event := <-eq.bufferInCh:
				for _, listener := range eq.listeners {
					listener.ingress(event)
				}
				events.PushBack(event)
			case <-eq.doneCh:
				break main
			}
		} else {
			front := events.Front()
			select {
			case event := <-eq.bufferInCh:
				for _, listener := range eq.listeners {
					listener.ingress(event)
				}
				events.PushBack(event)
			case eq.bufferOutCh <- front.Value.(*Event):
				events.Remove(front)
			case <-eq.doneCh:
				break main
			default:
			}
		}
	}

	timer := time.NewTimer(eq.queuePurgeTimeout)
	log.WithField("remaining_events", events.Len()).
		Warnf("eventqueue: received termination signal")

		// This loop is executed only during the termination phase. It's purpose is to
		// try to send all unsend notifications in the given time window.
loop:
	for events.Len() > 0 {
		front := events.Front()
		select {
		case eq.bufferOutCh <- front.Value.(*Event):
			events.Remove(front)
		case <-timer.C:
			break loop
		default:
		}
	}

	// NOTE(prozlach): We are done, tell sender to wrap it up too.
	close(eq.bufferOutCh)

	// NOTE(prozlach): queue is terminating, if there are still events in the
	// buffer, let the operator know they were lost:
	for events.Len() > 0 {
		front := events.Front()
		event := front.Value.(*Event)
		log.Warnf("eventqueue: event lost: %v", event)
		events.Remove(front)
	}
}

func (eq *eventQueue) sender() {
	defer eq.wgSender.Done()
	defer log.Debugf("eventQueue sender: closed")

	for {
		event, isOpen := <-eq.bufferOutCh
		if !isOpen {
			break
		}

		if err := eq.sink.Write(event); err != nil {
			log.WithError(err).
				WithField("event", event).
				Warnf("eventqueue: event lost")
		}

		for _, listener := range eq.listeners {
			listener.egress(event)
		}
	}
}

// Close shuts down the event queue, flushing
func (eq *eventQueue) Close() error {
	log.Infof("event queue: closing")
	select {
	case <-eq.doneCh:
		// already closed
		return fmt.Errorf("eventqueue: already closed")
	default:
		close(eq.doneCh)
	}

	// NOTE(prozlach): the order of things is very important here as we need to
	// first make sure that no new events will be accepted by this sink and
	// then cancel the underlying sink so that we can unblock this sink so that
	// it notices that the termination signal came.
	eq.wgBufferer.Wait()
	err := eq.sink.Close()
	eq.wgSender.Wait()

	// NOTE(prozlach): not stricly necessary, just a basic hygiene
	// NOTE(prozalch): we MUST not close eq.bufferInCh channel before waiting
	// gouroutines had a chance to err out or send event, otherwise we will
	// cause panics
	close(eq.bufferInCh)

	return err
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

	rs.wg.Wait()
	err := rs.sink.Close()

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
		doneCh: make(chan struct{}),
		sink:   sink,
		//nolint: gosec
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
