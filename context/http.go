package context

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/docker/distribution/uuid"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/correlation"
)

const (
	methodKey        = "method"
	hostKey          = "host"
	uriKey           = "uri"
	refererKey       = "referer"
	userAgentKey     = "user_agent"
	remoteAddressKey = "remote_addr"
	contentTypeKey   = "content_type"
)

// LogRequestFields is a map of log fields to their associated context keys
var LogRequestFields = map[string]string{
	methodKey:        "http.request.method",
	hostKey:          "http.request.host",
	uriKey:           "http.request.uri",
	refererKey:       "http.request.referer",
	userAgentKey:     "http.request.useragent",
	remoteAddressKey: "http.request.remoteaddr",
	contentTypeKey:   "http.request.contenttype",
}

// Common errors used with this package.
var (
	ErrNoRequestContext        = errors.New("no http request in context")
	ErrNoResponseWriterContext = errors.New("no http response in context")
)

func parseIP(ipStr string) net.IP {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		log.Warnf("invalid remote IP address: %q", ipStr)
	}
	return ip
}

// RemoteAddr extracts the remote address of the request, taking into
// account proxy headers.
func RemoteAddr(r *http.Request) string {
	if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
		proxies := strings.Split(prior, ",")
		if len(proxies) > 0 {
			remoteAddr := strings.Trim(proxies[0], " ")
			if parseIP(remoteAddr) != nil {
				return remoteAddr
			}
		}
	}
	// X-Real-Ip is less supported, but worth checking in the
	// absence of X-Forwarded-For
	if realIP := r.Header.Get("X-Real-Ip"); realIP != "" {
		if parseIP(realIP) != nil {
			return realIP
		}
	}

	return r.RemoteAddr
}

// RemoteIP extracts the remote IP of the request, taking into
// account proxy headers.
func RemoteIP(r *http.Request) string {
	addr := RemoteAddr(r)

	// Try parsing it as "IP:port"
	if ip, _, err := net.SplitHostPort(addr); err == nil {
		return ip
	}

	return addr
}

// WithRequest places the request on the context. The context of the request
// is assigned a unique id, available at "http.request.id". The request itself
// is available at "http.request". Other common attributes are available under
// the prefix "http.request.". If a request is already present on the context,
// this method will panic.
func WithRequest(ctx context.Context, r *http.Request) context.Context {
	if ctx.Value("http.request") != nil {
		// NOTE(stevvooe): This needs to be considered a programming error. It
		// is unlikely that we'd want to have more than one request in
		// context.
		panic("only one request per context")
	}

	return &httpMappedRequestContext{
		Context: &httpRequestContext{
			Context:   ctx,
			startedAt: time.Now(),
			id:        uuid.Generate().String(),
			r:         r,
		},
	}
}

// GetRequest returns the http request in the given context. Returns
// ErrNoRequestContext if the context does not have an http request associated
// with it.
func GetRequest(ctx context.Context) (*http.Request, error) {
	if r, ok := ctx.Value("http.request").(*http.Request); r != nil && ok {
		return r, nil
	}
	return nil, ErrNoRequestContext
}

// GetRequestID attempts to resolve the current request id, if possible. An
// error is return if it is not available on the context.
func GetRequestID(ctx context.Context) string {
	return GetStringValue(ctx, "http.request.id")
}

// GetRequestCorrelationID attempts to resolve the current request correlation id, if any.
func GetRequestCorrelationID(ctx context.Context) string {
	return correlation.ExtractFromContext(ctx)
}

// WithResponseWriter returns a new context and response writer that makes
// interesting response statistics available within the context.
func WithResponseWriter(ctx context.Context, w http.ResponseWriter) (context.Context, http.ResponseWriter) {
	irw := instrumentedResponseWriter{
		ResponseWriter: w,
		Context:        ctx,
	}
	return &irw, &irw
}

// GetResponseWriter returns the http.ResponseWriter from the provided
// context. If not present, ErrNoResponseWriterContext is returned. The
// returned instance provides instrumentation in the context.
func GetResponseWriter(ctx context.Context) (http.ResponseWriter, error) {
	v := ctx.Value("http.response")

	rw, ok := v.(http.ResponseWriter)
	if !ok || rw == nil {
		return nil, ErrNoResponseWriterContext
	}

	return rw, nil
}

// getVarsFromRequest let's us change request vars implementation for testing
// and maybe future changes.
var getVarsFromRequest = mux.Vars

// WithVars extracts gorilla/mux vars and makes them available on the returned
// context. Variables are available as keys with the prefix "vars.". For
// example, if looking for the variable "name", it can be accessed as
// "vars.name". Implementations that are accessing values need not know that
// the underlying context is implemented with gorilla/mux vars.
func WithVars(ctx context.Context, r *http.Request) context.Context {
	return &muxVarsContext{
		Context: ctx,
		vars:    getVarsFromRequest(r),
	}
}

type CFRayIDKey string

const (
	CFRayIDHeader = "CF-ray"
	CFRayIDLogKey = CFRayIDKey("CF-RAY")
)

// WithCFRayID extracts a `CF-ray` ID (https://developers.cloudflare.com/fundamentals/get-started/reference/http-request-headers/#cf-ray)
// request header (if present) from the request (r) and sets the value extracted in the returned context (ctx) with a key of CFRayIDLogKey.
// In the event that the `CF-ray` ID Header is not present the CFRayIDLogKey is not set in the context, if the header is present but empty
// the CFRayIDLogKey is set with an empty value.
func WithCFRayID(ctx context.Context, r *http.Request) context.Context {
	if rv, ok := r.Header[http.CanonicalHeaderKey(CFRayIDHeader)]; ok && rv != nil {
		ctx = context.WithValue(ctx, CFRayIDLogKey, r.Header.Get(CFRayIDHeader))
	}
	return ctx
}

// GetMappedRequestLogger returns a logger that contains specific keyed fields from the request in the current context.
// The fields displayed by MappedRequestLogger relies on the presence of a http request in the chained context, this is
// typically satisfied by calling `WithRequest` on the context (or one of the context inhereted from the existing context).
// If the request is not available in the context chain, no fields will display. Request loggers can safely be pushed onto the context.
func GetMappedRequestLogger(ctx context.Context) Logger {
	return GetLogger(ctx,
		methodKey,
		hostKey,
		uriKey,
		refererKey,
		userAgentKey,
		remoteAddressKey,
		contentTypeKey)
}

// GetResponseLogger reads the current response stats and builds a logger.
// Because the values are read at call time, pushing a logger returned from
// this function on the context will lead to missing or invalid data. Only
// call this at the end of a request, after the response has been written.
func GetResponseLogger(ctx context.Context) Logger {
	l := getLogrusLogger(ctx,
		"http.response.written",
		"http.response.status",
		"http.response.contenttype")

	duration := Since(ctx, "http.request.startedat")

	if duration > 0 {
		l = l.WithField("http_response_duration", duration.String())
	}

	return l
}

// httpRequestContext makes information about a request available to context.
type httpRequestContext struct {
	context.Context

	startedAt time.Time
	id        string
	r         *http.Request
}

// Value returns a keyed element of the request for use in the context. To get
// the request itself, query "request". For other components, access them as
// "request.<component>". For example, r.RequestURI
func (ctx *httpRequestContext) Value(key interface{}) interface{} {
	if keyStr, ok := key.(string); ok {
		if keyStr == "http.request" {
			return ctx.r
		}

		if !strings.HasPrefix(keyStr, "http.request.") {
			goto fallback
		}

		parts := strings.Split(keyStr, ".")

		if len(parts) != 3 {
			goto fallback
		}

		switch parts[2] {
		case "uri":
			return ctx.r.RequestURI
		case "remoteaddr":
			return RemoteAddr(ctx.r)
		case "method":
			return ctx.r.Method
		case "host":
			return ctx.r.Host
		case "referer":
			referer := ctx.r.Referer()
			if referer != "" {
				return referer
			}
		case "useragent":
			return ctx.r.UserAgent()
		case "id":
			return ctx.id
		case "startedat":
			return ctx.startedAt
		case "contenttype":
			ct := ctx.r.Header.Get("Content-Type")
			if ct != "" {
				return ct
			}
		}
	}

fallback:
	return ctx.Context.Value(key)
}

// httpMappedRequestContext makes information about a request available to context through `httpRequestContext`.
// keys requested from this context that match `MappedLogRequestFields` keys are converted to their `httpRequestContext` counterparts.
type httpMappedRequestContext struct {
	context.Context
}

// Value returns a keyed element of the request for use in the context. To get
// the request itself, query "request". For other components, access them as
// "request.<component>". For example, r.RequestURI
func (ctx *httpMappedRequestContext) Value(key interface{}) interface{} {
	if keyStr, ok := key.(string); ok {
		// retrieve the actual http request key that corresponds to the request field
		if requestFieldKey, ok := LogRequestFields[keyStr]; ok {
			return ctx.Context.Value(requestFieldKey)
		}
	}
	return ctx.Context.Value(key)
}

type muxVarsContext struct {
	context.Context
	vars map[string]string
}

func (ctx *muxVarsContext) Value(key interface{}) interface{} {
	if keyStr, ok := key.(string); ok {
		if keyStr == "vars" {
			return ctx.vars
		}

		if strings.HasPrefix(keyStr, "vars.") {
			keyStr = strings.TrimPrefix(keyStr, "vars.")
		}

		if v, ok := ctx.vars[keyStr]; ok {
			return v
		}
	}

	return ctx.Context.Value(key)
}

// instrumentedResponseWriter provides response writer information in a
// context. This variant is only used in the case where CloseNotifier is not
// implemented by the parent ResponseWriter.
type instrumentedResponseWriter struct {
	http.ResponseWriter
	context.Context

	mu      sync.Mutex
	status  int
	written int64
}

func (irw *instrumentedResponseWriter) Write(p []byte) (n int, err error) {
	n, err = irw.ResponseWriter.Write(p)

	irw.mu.Lock()
	irw.written += int64(n)

	// Guess the likely status if not set.
	if irw.status == 0 {
		irw.status = http.StatusOK
	}

	irw.mu.Unlock()

	return
}

func (irw *instrumentedResponseWriter) WriteHeader(status int) {
	irw.ResponseWriter.WriteHeader(status)

	irw.mu.Lock()
	irw.status = status
	irw.mu.Unlock()
}

func (irw *instrumentedResponseWriter) Flush() {
	if flusher, ok := irw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (irw *instrumentedResponseWriter) Value(key interface{}) interface{} {
	if keyStr, ok := key.(string); ok {
		if keyStr == "http.response" {
			return irw
		}

		if !strings.HasPrefix(keyStr, "http.response.") {
			goto fallback
		}

		parts := strings.Split(keyStr, ".")

		if len(parts) != 3 {
			goto fallback
		}

		irw.mu.Lock()
		defer irw.mu.Unlock()

		switch parts[2] {
		case "written":
			return irw.written
		case "status":
			return irw.status
		case "contenttype":
			contentType := irw.Header().Get("Content-Type")
			if contentType != "" {
				return contentType
			}
		}
	}

fallback:
	return irw.Context.Value(key)
}
