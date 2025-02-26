package context

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithRequest(t *testing.T) {
	var req http.Request

	start := time.Now()
	req.Method = http.MethodGet
	req.Host = "example.com"
	req.RequestURI = "/test-test"
	req.Header = make(http.Header)
	req.Header.Set("Referer", "foo.com/referer")
	req.Header.Set("User-Agent", "test/0.1")

	ctx := WithRequest(Background(), &req)
	for _, testcase := range []struct {
		key      string
		expected any
	}{
		{
			key:      "http.request",
			expected: &req,
		},
		{
			key: "http.request.id",
		},
		{
			key:      "http.request.method",
			expected: req.Method,
		},
		{
			key:      "http.request.host",
			expected: req.Host,
		},
		{
			key:      "http.request.uri",
			expected: req.RequestURI,
		},
		{
			key:      "http.request.referer",
			expected: req.Referer(),
		},
		{
			key:      "http.request.useragent",
			expected: req.UserAgent(),
		},
		{
			key:      "http.request.remoteaddr",
			expected: req.RemoteAddr,
		},
		{
			key: "http.request.startedat",
		},
	} {
		v := ctx.Value(testcase.key)

		require.NotNilf(t, v, "value not found for %q", testcase.key)

		if testcase.expected != nil {
			require.Equalf(t, testcase.expected, v, "%s", testcase.key)
		}

		// Key specific checks!
		switch testcase.key {
		case "http.request.id":
			assert.IsType(t, "", v)
		case "http.request.startedat":
			vt, ok := v.(time.Time)
			require.True(t, ok, "v is not of type time.Time")

			now := time.Now()
			assert.False(t, vt.After(now), "time generated too late")
			assert.False(t, vt.Before(start), "time generated too early")
		}
	}
}

func TestWithRequest_MappedKeys(t *testing.T) {
	var req http.Request

	req.Method = http.MethodGet
	req.Host = "example.com"
	req.RequestURI = "/test-test"
	req.Header = make(http.Header)
	req.Header.Set("Referer", "foo.com/referer")
	req.Header.Set("User-Agent", "test/0.1")

	ctx := WithRequest(Background(), &req)
	for _, testcase := range []struct {
		key      string
		expected any
	}{
		{
			key:      "method",
			expected: req.Method,
		},
		{
			key:      "host",
			expected: req.Host,
		},
		{
			key:      "uri",
			expected: req.RequestURI,
		},
		{
			key:      "referer",
			expected: req.Referer(),
		},
		{
			key:      "user_agent",
			expected: req.UserAgent(),
		},
		{
			key:      "remote_addr",
			expected: req.RemoteAddr,
		},
	} {
		v := ctx.Value(testcase.key)

		require.NotNil(t, v, "value not found for %q", testcase.key)

		if testcase.expected != nil {
			assert.Equal(t, testcase.expected, v, "%s", testcase.key)
		}
	}
}

type testResponseWriter struct {
	flushed bool
	status  int
	written int64
	header  http.Header
}

func (trw *testResponseWriter) Header() http.Header {
	if trw.header == nil {
		trw.header = make(http.Header)
	}

	return trw.header
}

func (trw *testResponseWriter) Write(p []byte) (n int, err error) {
	if trw.status == 0 {
		trw.status = http.StatusOK
	}

	n = len(p)
	trw.written += int64(n)
	return n, err
}

func (trw *testResponseWriter) WriteHeader(status int) {
	trw.status = status
}

func (trw *testResponseWriter) Flush() {
	trw.flushed = true
}

func TestWithResponseWriter(t *testing.T) {
	trw := testResponseWriter{}
	ctx, rw := WithResponseWriter(Background(), &trw)

	require.Equal(t, rw, ctx.Value("http.response"), "response not available in context")

	grw, err := GetResponseWriter(ctx)
	require.NoError(t, err, "error getting response writer")

	require.Equal(t, rw, grw, "unexpected response writer returned")

	require.Zero(t, ctx.Value("http.response.status"), "response status should always be a number and should be zero here")

	n, err := rw.Write(make([]byte, 1024))
	require.NoError(t, err, "unexpected error writing")
	require.Equal(t, 1024, n, "unexpected number of bytes written")

	require.Equal(t, http.StatusOK, ctx.Value("http.response.status"), "unexpected response status in context")

	require.EqualValues(t, 1024, ctx.Value("http.response.written"), "unexpected number reported bytes written")

	// Make sure flush propagates
	rw.(http.Flusher).Flush()

	require.True(t, trw.flushed, "response writer not flushed")

	// Write another status and make sure context is correct. This normally
	// wouldn't work except for in this contrived testcase.
	rw.WriteHeader(http.StatusBadRequest)

	require.Equal(t, http.StatusBadRequest, ctx.Value("http.response.status"), "unexpected response status in context")
}

func TestWithVars(t *testing.T) {
	var req http.Request
	vars := map[string]string{
		"foo": "asdf",
		"bar": "qwer",
	}

	getVarsFromRequest = func(r *http.Request) map[string]string {
		require.Equal(t, &req, r, "unexpected request")

		return vars
	}

	ctx := WithVars(Background(), &req)
	for _, testcase := range []struct {
		key      string
		expected any
	}{
		{
			key:      "vars",
			expected: vars,
		},
		{
			key:      "vars.foo",
			expected: "asdf",
		},
		{
			key:      "vars.bar",
			expected: "qwer",
		},
	} {
		v := ctx.Value(testcase.key)

		require.Equalf(t, testcase.expected, v, "%q", testcase.key)
	}
}

// SingleHostReverseProxy will insert an X-Forwarded-For header, and can be used to test
// RemoteAddr().  A fake RemoteAddr cannot be set on the HTTP request - it is overwritten
// at the transport layer to 127.0.0.1:<port> .  However, as the X-Forwarded-For header
// just contains the IP address, it is different enough for testing.
func TestRemoteAddr(t *testing.T) {
	var expectedRemote string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		assert.NotEqual(t, expectedRemote, r.RemoteAddr, "unexpected matching remote addresses")

		actualRemote := RemoteAddr(r)
		assert.Equal(t, expectedRemote, actualRemote, "mismatching remote hosts")

		w.WriteHeader(200)
	}))

	defer backend.Close()
	backendURL, err := url.Parse(backend.URL)
	require.NoError(t, err)

	proxy := httputil.NewSingleHostReverseProxy(backendURL)
	frontend := httptest.NewServer(proxy)
	defer frontend.Close()

	// X-Forwarded-For set by proxy
	expectedRemote = "127.0.0.1"
	proxyReq, err := http.NewRequest(http.MethodGet, frontend.URL, nil)
	require.NoError(t, err)

	rsp, err := http.DefaultClient.Do(proxyReq)
	require.NoError(t, err)

	err = rsp.Body.Close()
	require.NoError(t, err)

	// RemoteAddr in X-Real-Ip
	getReq, err := http.NewRequest(http.MethodGet, backend.URL, nil)
	require.NoError(t, err)

	expectedRemote = "1.2.3.4"
	getReq.Header["X-Real-ip"] = []string{expectedRemote}
	rsp, err = http.DefaultClient.Do(getReq)
	require.NoError(t, err)

	err = rsp.Body.Close()
	require.NoError(t, err)

	// Valid X-Real-Ip and invalid X-Forwarded-For
	getReq.Header["X-forwarded-for"] = []string{"1.2.3"}
	rsp, err = http.DefaultClient.Do(getReq)
	require.NoError(t, err)

	err = rsp.Body.Close()
	require.NoError(t, err)
}

func TestWithCFRayID(t *testing.T) {
	testcases := []struct {
		name                 string
		requestHeaders       map[string]string
		expectedContextValue any
		expectedCFRayIDKey   CFRayIDKey
	}{
		{
			name:                 "a request with a CF-ray header",
			requestHeaders:       map[string]string{"CF-ray": "value"},
			expectedContextValue: "value",
		},
		{
			name:                 "a request with a CF-ray header that has an empty value",
			requestHeaders:       map[string]string{"CF-ray": ""},
			expectedContextValue: "",
		},
		{
			name:                 "a request without a CF-ray header",
			requestHeaders:       make(map[string]string),
			expectedContextValue: nil,
		},
	}
	t.Logf("Running Test %s", t.Name())
	for _, test := range testcases {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.TODO()
			req := generateRequestWithHeaders(test.requestHeaders)
			actualContext := WithCFRayID(ctx, req)
			require.Equal(t, test.expectedContextValue, actualContext.Value(CFRayIDLogKey))
		})
	}
}

func generateRequestWithHeaders(headers map[string]string) *http.Request {
	var header http.Header = make(map[string][]string)
	for key, val := range headers {
		header.Add(key, val)
	}
	return &http.Request{
		Header: header,
	}
}
