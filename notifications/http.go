package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// httpSink implements a single-flight, http notification endpoint. This is
// very lightweight in that it only makes an attempt at an http request.
// Reliability should be provided by the caller.
type httpSink struct {
	url string

	ctx     context.Context
	cancelF context.CancelFunc

	eventsCh chan *Event
	errCh    chan error

	wg *sync.WaitGroup

	client    *http.Client
	listeners []httpStatusListener
}

// newHTTPSink returns an unreliable, single-flight http sink. Wrap in other
// sinks for increased reliability.
func newHTTPSink(u string, timeout time.Duration, headers http.Header, transport *http.Transport, listeners ...httpStatusListener) *httpSink {
	if transport == nil {
		transport = http.DefaultTransport.(*http.Transport)
	}

	ctx, cancelF := context.WithCancel(context.Background())

	hs := &httpSink{
		url:       u,
		listeners: listeners,

		ctx:     ctx,
		cancelF: cancelF,

		eventsCh: make(chan *Event),
		errCh:    make(chan error),

		wg: new(sync.WaitGroup),

		client: &http.Client{
			Transport: &headerRoundTripper{
				Transport: transport,
				headers:   headers,
			},
			Timeout: timeout,
		},
	}

	hs.wg.Add(1)
	go hs.run()

	return hs
}

// httpStatusListener is called on various outcomes of sending notifications.
type httpStatusListener interface {
	success(status int, event *Event)
	failure(status int, event *Event)
	err(events *Event)
}

// Accept makes an attempt to notify the endpoint, returning an error if it
// fails. It is the caller's responsibility to retry on error. The events are
// accepted or rejected as a group.
func (hs *httpSink) Write(event *Event) error {
	select {
	case <-hs.ctx.Done():
		return ErrSinkClosed
	default:
		select {
		case hs.eventsCh <- event:
			return <-hs.errCh
		case <-hs.ctx.Done():
			return ErrSinkClosed
		}
	}
}

func (hs *httpSink) run() {
	defer hs.wg.Done()
	defer hs.client.Transport.(*headerRoundTripper).CloseIdleConnections()

	for {
		select {
		case <-hs.ctx.Done():
			return
		case event := <-hs.eventsCh:
			envelope := Envelope{
				Events: []Event{*event},
			}

			p, err := json.MarshalIndent(envelope, "", "   ")
			if err != nil {
				for _, listener := range hs.listeners {
					listener.err(event)
				}
				hs.errCh <- fmt.Errorf("%v: error marshaling event envelope: %v", hs, err)
				continue
			}

			body := bytes.NewReader(p)
			req, err := http.NewRequestWithContext(hs.ctx, "POST", hs.url, body)
			if err != nil {
				for _, listener := range hs.listeners {
					listener.err(event)
				}

				hs.errCh <- fmt.Errorf("%v: error creating request: %v", hs, err)
				continue
			}
			req.Header.Set("Content-Type", EventsMediaType)
			resp, err := hs.client.Do(req)
			if err != nil {
				for _, listener := range hs.listeners {
					listener.err(event)
				}

				hs.errCh <- fmt.Errorf("%v: error posting: %v", hs, err)
				continue
			}

			// The notifier will treat any 2xx or 3xx response as accepted by the
			// endpoint.
			switch {
			case resp.StatusCode >= 200 && resp.StatusCode < 400:
				for _, listener := range hs.listeners {
					listener.success(resp.StatusCode, event)
				}
				err = nil
			default:
				for _, listener := range hs.listeners {
					listener.failure(resp.StatusCode, event)
				}
				err = fmt.Errorf("%v: response status %v unaccepted", hs, resp.Status)
			}
			_ = resp.Body.Close()
			hs.errCh <- err
		}
	}
}

// Close the endpoint
func (hs *httpSink) Close() error {
	log.Infof("httpsink: closing")
	select {
	case <-hs.ctx.Done():
		return fmt.Errorf("httpsink: already closed")
	default:
	}

	hs.cancelF()
	hs.wg.Wait()
	close(hs.eventsCh)
	close(hs.errCh)
	log.Debugf("httpSink: closed")

	return nil
}

func (hs *httpSink) String() string {
	return fmt.Sprintf("httpSink{%s}", hs.url)
}

type headerRoundTripper struct {
	*http.Transport // must be transport to support CancelRequest
	headers         http.Header
}

func (hrt *headerRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	nreq := req.Clone(req.Context())
	nreq.Header = make(http.Header)

	merge := func(headers http.Header) {
		for k, v := range headers {
			nreq.Header[k] = append(nreq.Header[k], v...)
		}
	}

	merge(req.Header)
	merge(hrt.headers)

	return hrt.Transport.RoundTrip(nreq)
}
