package testutil

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
)

const EnvMITMProxyURL = "ENV_MITM_PROXY_URL"

// RequestMatcher defines criteria for matching HTTP requests
type RequestMatcher func(*http.Request) bool

// RequestModifier is a function type that can modify an HTTP request
type RequestModifier func(*http.Request) (*http.Request, bool)

// ResponseModifier is a function type that can modify an HTTP response
type ResponseModifier func(*http.Response) (*http.Response, bool)

// Hook represents a modification hook with its matching criteria
type Hook struct {
	matcher RequestMatcher

	reqMod  RequestModifier
	respMod ResponseModifier
}

// Interceptor implements an HTTP interceptor with modifiable hooks
type Interceptor struct {
	core http.RoundTripper

	requestHooksApplied  int
	responseHooksApplied int

	mu    sync.RWMutex
	hooks []Hook
}

type InterceptorConfig struct {
	Matcher                            func(*testing.T) RequestMatcher
	RequestModifier                    func(*testing.T) RequestModifier
	ExpectedRequestModificationsCount  int
	ResponseModifier                   func(*testing.T) ResponseModifier
	ExpectedResponseModificationsCount int
}

// NewInterceptor creates a new Interceptor with the given RoundTripper
func NewInterceptor(transport *http.Transport) (*Interceptor, error) {
	// Make sure that we enable inspecting http traffic using tools like
	// Mitmproxy.
	transport.Proxy = http.ProxyFromEnvironment

	// Required in case when proxy is listening on localhost, as in this case
	// Golang ignores HTTP(S)_PROXY settings as a setting measure.
	if v := os.Getenv(EnvMITMProxyURL); v != "" {
		proxyURL, err := url.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("parsing mitmproxy url %q: %w", v, err)
		}

		// Create custom transport with proxy
		transport.Proxy = http.ProxyURL(proxyURL)
	}

	return &Interceptor{
		core:  transport,
		hooks: make([]Hook, 0),
	}, nil
}

// AddRequestHook adds a new request modification hook
func (i *Interceptor) AddRequestHook(matcher RequestMatcher, modifier RequestModifier) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.hooks = append(i.hooks, Hook{
		matcher: matcher,
		reqMod:  modifier,
	})
}

// AddResponseHook adds a new response modification hook
func (i *Interceptor) AddResponseHook(matcher RequestMatcher, modifier ResponseModifier) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.hooks = append(i.hooks, Hook{
		matcher: matcher,
		respMod: modifier,
	})
}

// applyRequestHooks applies all matching request hooks
func (i *Interceptor) applyRequestHooks(req, reqClone *http.Request) *http.Request {
	i.mu.RLock()
	defer i.mu.RUnlock()

	for _, hook := range i.hooks {
		if hook.reqMod != nil && hook.matcher(req) {
			applied := false
			reqClone, applied = hook.reqMod(reqClone)
			if applied {
				i.requestHooksApplied++
			}
		}
	}
	return reqClone
}

// applyResponseHooks applies all matching response hooks
func (i *Interceptor) applyResponseHooks(req *http.Request, resp *http.Response) *http.Response {
	i.mu.RLock()
	defer i.mu.RUnlock()

	for _, hook := range i.hooks {
		if hook.respMod != nil && hook.matcher(req) {
			applied := false
			resp, applied = hook.respMod(resp)
			if applied {
				i.responseHooksApplied++
			}
		}
	}
	return resp
}

// RoundTrip implements the http.RoundTripper interface
func (i *Interceptor) RoundTrip(req *http.Request) (*http.Response, error) {
	// Clone the request to avoid modifying the original
	reqClone := req.Clone(context.Background())

	// Apply request hooks
	reqClone = i.applyRequestHooks(req, reqClone)

	// Send the request
	resp, err := i.core.RoundTrip(reqClone)
	if err != nil {
		return nil, fmt.Errorf("round-trip after applying request hooks: %w", err)
	}

	// Apply response hooks
	return i.applyResponseHooks(req, resp), nil
}

func (i *Interceptor) GetRequestHooksMatchedCount() int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.requestHooksApplied
}

func (i *Interceptor) GetResponseHooksMatchedCount() int {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.responseHooksApplied
}

// RandomizeTail returns a new io.ReadCloser that reads from the input reader
// but replaces the last 10 bytes with random data. It is meant to simulate a
// malformed upload that got corrupted at the very end. To make things nastier,
// we randomize last ten bytes, but do not truncate the contents in general to
// make corruption detection non-trivial.
func RandomizeTail(input io.ReadCloser) (io.ReadCloser, error) {
	// Read all data from input
	data, err := io.ReadAll(input)
	if err != nil {
		return nil, err
	}
	defer input.Close()

	// If data is less than 10 bytes, just return random data of the same length
	if len(data) < 10 {
		randomData := make([]byte, len(data))
		for i := range randomData {
			randomData[i] = byte(rand.IntN(256)) // nolint: gosec
		}
		return io.NopCloser(bytes.NewReader(randomData)), nil
	}

	// Generate 10 random bytes
	randomTail := make([]byte, 10)
	for i := range randomTail {
		randomTail[i] = byte(rand.IntN(256)) // nolint: gosec
	}

	// Replace last 10 bytes with random data
	copy(data[len(data)-10:], randomTail)

	// Return a new ReadCloser with modified data
	return io.NopCloser(bytes.NewReader(data)), nil
}
