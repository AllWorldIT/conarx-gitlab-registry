package auth

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/docker/distribution/registry/client/auth/challenge"
	"github.com/docker/distribution/registry/client/transport"
	"github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An implementation of clock for providing fake time data.
type fakeClock struct {
	current time.Time
}

// Now implements clock
func (fc *fakeClock) Now() time.Time { return fc.current }

func testServer(rrm testutil.RequestResponseMap) (string, func()) {
	h := testutil.NewHandler(rrm)
	s := httptest.NewServer(h)
	return s.URL, s.Close
}

type testAuthenticationWrapper struct {
	headers   http.Header
	authCheck func(string) bool
	next      http.Handler
}

func (w *testAuthenticationWrapper) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	if auth == "" || !w.authCheck(auth) {
		h := rw.Header()
		for k, values := range w.headers {
			h[k] = values
		}
		rw.WriteHeader(http.StatusUnauthorized)
		return
	}
	w.next.ServeHTTP(rw, r)
}

func testServerWithAuth(rrm testutil.RequestResponseMap, authenticate string, authCheck func(string) bool) (string, func()) {
	h := testutil.NewHandler(rrm)
	wrapper := &testAuthenticationWrapper{
		headers: http.Header(map[string][]string{
			"X-API-Version":       {"registry/2.0"},
			"X-Multi-API-Version": {"registry/2.0", "registry/2.1", "trust/1.0"},
			"WWW-Authenticate":    {authenticate},
		}),
		authCheck: authCheck,
		next:      h,
	}

	s := httptest.NewServer(wrapper)
	return s.URL, s.Close
}

// ping pings the provided endpoint to determine its required authorization challenges.
// If a version header is provided, the versions will be returned.
func ping(manager challenge.Manager, endpoint, versionHeader string) ([]APIVersion, error) {
	resp, err := http.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := manager.AddResponse(resp); err != nil {
		return nil, err
	}

	return APIVersions(resp, versionHeader), err
}

type testCredentialStore struct {
	username      string
	password      string
	refreshTokens map[string]string
}

func (tcs *testCredentialStore) Basic(*url.URL) (string, string) {
	return tcs.username, tcs.password
}

func (tcs *testCredentialStore) RefreshToken(_ *url.URL, service string) string {
	return tcs.refreshTokens[service]
}

func (tcs *testCredentialStore) SetRefreshToken(_ *url.URL, service, token string) {
	if tcs.refreshTokens != nil {
		tcs.refreshTokens[service] = token
	}
}

func TestEndpointAuthorizeToken(t *testing.T) {
	service := "localhost.localdomain"
	repo1 := "some/registry"
	repo2 := "other/registry"
	scope1 := fmt.Sprintf("repository:%s:pull,push", repo1)
	scope2 := fmt.Sprintf("repository:%s:pull,push", repo2)
	tokenMap := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?scope=%s&service=%s", url.QueryEscape(scope1), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"token":"statictoken"}`),
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?scope=%s&service=%s", url.QueryEscape(scope2), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"token":"badtoken"}`),
			},
		},
	})
	te, tc := testServer(tokenMap)
	defer tc()

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	authenicate := fmt.Sprintf("Bearer realm=%q,service=%q", te+"/token", service)
	validCheck := func(a string) bool {
		return a == "Bearer statictoken"
	}
	e, c := testServerWithAuth(m, authenicate, validCheck)
	defer c()

	challengeManager1 := challenge.NewSimpleManager()
	versions, err := ping(challengeManager1, e+"/v2/", "x-api-version")
	require.NoError(t, err)
	require.Len(t, versions, 1, "unexpected version count")
	require.Equal(t, APIVersion{Type: "registry", Version: "2.0"}, versions[0], "unexpected api version")

	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager1, NewTokenHandler(nil, nil, repo1, "pull", "push")))
	client := &http.Client{Transport: transport1}

	req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
	resp, err := client.Do(req)
	require.NoError(t, err, "error sending get request")

	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode, "unexpected status code")

	e2, c2 := testServerWithAuth(m, authenicate, validCheck)
	defer c2()

	challengeManager2 := challenge.NewSimpleManager()
	versions, err = ping(challengeManager2, e2+"/v2/", "x-multi-api-version")
	require.NoError(t, err)
	require.Len(t, versions, 3, "unexpected version count")
	require.Equal(t, APIVersion{Type: "registry", Version: "2.0"}, versions[0], "unexpected api version")
	require.Equal(t, APIVersion{Type: "registry", Version: "2.1"}, versions[1], "unexpected api version")
	require.Equal(t, APIVersion{Type: "trust", Version: "1.0"}, versions[2], "unexpected api version")

	transport2 := transport.NewTransport(nil, NewAuthorizer(challengeManager2, NewTokenHandler(nil, nil, repo2, "pull", "push")))
	client2 := &http.Client{Transport: transport2}

	req, _ = http.NewRequest(http.MethodGet, e2+"/v2/hello", nil)
	resp, err = client2.Do(req)
	require.NoError(t, err, "error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "unexpected status code")
}

func TestEndpointAuthorizeRefreshToken(t *testing.T) {
	service := "localhost.localdomain"
	repo1 := "some/registry"
	repo2 := "other/registry"
	scope1 := fmt.Sprintf("repository:%s:pull,push", repo1)
	scope2 := fmt.Sprintf("repository:%s:pull,push", repo2)
	refreshToken1 := "0123456790abcdef"
	refreshToken2 := "0123456790fedcba"
	tokenMap := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodPost,
				Route:  "/token",
				Body:   []byte(fmt.Sprintf("client_id=registry-client&grant_type=refresh_token&refresh_token=%s&scope=%s&service=%s", refreshToken1, url.QueryEscape(scope1), service)),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(fmt.Sprintf(`{"access_token":"statictoken","refresh_token":"%s"}`, refreshToken1)),
			},
		},
		{
			// In the future this test may fail and require using basic auth to get a different refresh token
			Request: testutil.Request{
				Method: http.MethodPost,
				Route:  "/token",
				Body:   []byte(fmt.Sprintf("client_id=registry-client&grant_type=refresh_token&refresh_token=%s&scope=%s&service=%s", refreshToken1, url.QueryEscape(scope2), service)),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(fmt.Sprintf(`{"access_token":"statictoken","refresh_token":"%s"}`, refreshToken2)),
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodPost,
				Route:  "/token",
				Body:   []byte(fmt.Sprintf("client_id=registry-client&grant_type=refresh_token&refresh_token=%s&scope=%s&service=%s", refreshToken2, url.QueryEscape(scope2), service)),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"access_token":"badtoken","refresh_token":"%s"}`),
			},
		},
	})
	te, tc := testServer(tokenMap)
	defer tc()

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	authenicate := fmt.Sprintf("Bearer realm=%q,service=%q", te+"/token", service)
	validCheck := func(a string) bool {
		return a == "Bearer statictoken"
	}
	e, c := testServerWithAuth(m, authenicate, validCheck)
	defer c()

	challengeManager1 := challenge.NewSimpleManager()
	versions, err := ping(challengeManager1, e+"/v2/", "x-api-version")
	require.NoError(t, err)
	require.Len(t, versions, 1, "unexpected version count")
	require.Equal(t, APIVersion{Type: "registry", Version: "2.0"}, versions[0], "unexpected api version")

	creds := &testCredentialStore{
		refreshTokens: map[string]string{
			service: refreshToken1,
		},
	}
	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager1, NewTokenHandler(nil, creds, repo1, "pull", "push")))
	client := &http.Client{Transport: transport1}

	req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
	resp, err := client.Do(req)
	require.NoError(t, err, "error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, resp.StatusCode, "unexpected status code")

	// Try with refresh token setting
	e2, c2 := testServerWithAuth(m, authenicate, validCheck)
	defer c2()

	challengeManager2 := challenge.NewSimpleManager()
	versions, err = ping(challengeManager2, e2+"/v2/", "x-api-version")
	require.NoError(t, err)
	require.Len(t, versions, 1, "unexpected version count")
	require.Equal(t, APIVersion{Type: "registry", Version: "2.0"}, versions[0], "unexpected api version")

	transport2 := transport.NewTransport(nil, NewAuthorizer(challengeManager2, NewTokenHandler(nil, creds, repo2, "pull", "push")))
	client2 := &http.Client{Transport: transport2}

	req, _ = http.NewRequest(http.MethodGet, e2+"/v2/hello", nil)
	resp, err = client2.Do(req)
	require.NoError(t, err, "error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, resp.StatusCode, "unexpected status code")

	require.Equal(t, refreshToken2, creds.refreshTokens[service], "refresh token not set after change")

	// Try with bad token
	e3, c3 := testServerWithAuth(m, authenicate, validCheck)
	defer c3()

	challengeManager3 := challenge.NewSimpleManager()
	versions, err = ping(challengeManager3, e3+"/v2/", "x-api-version")
	require.NoError(t, err)
	require.Equal(t, APIVersion{Type: "registry", Version: "2.0"}, versions[0], "unexpected api version")

	transport3 := transport.NewTransport(nil, NewAuthorizer(challengeManager3, NewTokenHandler(nil, creds, repo2, "pull", "push")))
	client3 := &http.Client{Transport: transport3}

	req, _ = http.NewRequest(http.MethodGet, e3+"/v2/hello", nil)
	resp, err = client3.Do(req)
	require.NoError(t, err, "error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "unexpected status code")
}

func TestEndpointAuthorizeV2RefreshToken(t *testing.T) {
	service := "localhost.localdomain"
	scope1 := "registry:catalog:search"
	refreshToken1 := "0123456790abcdef"
	tokenMap := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodPost,
				Route:  "/token",
				Body:   []byte(fmt.Sprintf("client_id=registry-client&grant_type=refresh_token&refresh_token=%s&scope=%s&service=%s", refreshToken1, url.QueryEscape(scope1), service)),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(fmt.Sprintf(`{"access_token":"statictoken","refresh_token":"%s"}`, refreshToken1)),
			},
		},
	})
	te, tc := testServer(tokenMap)
	defer tc()

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v1/search",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	authenicate := fmt.Sprintf("Bearer realm=%q,service=%q", te+"/token", service)
	validCheck := func(a string) bool {
		return a == "Bearer statictoken"
	}
	e, c := testServerWithAuth(m, authenicate, validCheck)
	defer c()

	challengeManager1 := challenge.NewSimpleManager()
	versions, err := ping(challengeManager1, e+"/v2/", "x-api-version")
	require.NoError(t, err)
	require.Len(t, versions, 1, "Unexpected version count")
	require.Equal(t, APIVersion{Type: "registry", Version: "2.0"}, versions[0])
	tho := TokenHandlerOptions{
		Credentials: &testCredentialStore{
			refreshTokens: map[string]string{
				service: refreshToken1,
			},
		},
		Scopes: []Scope{
			RegistryScope{
				Name:    "catalog",
				Actions: []string{"search"},
			},
		},
	}

	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager1, NewTokenHandlerWithOptions(tho)))
	client := &http.Client{Transport: transport1}

	req, _ := http.NewRequest(http.MethodGet, e+"/v1/search", nil)
	resp, err := client.Do(req)
	require.NoError(t, err, "Error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}

func TestEndpointAuthorizeTokenBasic(t *testing.T) {
	service := "localhost.localdomain"
	repo := "some/fun/registry"
	scope := fmt.Sprintf("repository:%s:pull,push", repo)
	username := "tokenuser"
	password := "superSecretPa$$word"

	tokenMap := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?account=%s&scope=%s&service=%s", username, url.QueryEscape(scope), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"access_token":"statictoken"}`),
			},
		},
	})

	authenicate1 := "Basic realm=localhost"
	basicCheck := func(a string) bool {
		return a == fmt.Sprintf("Basic %s", basicAuth(username, password))
	}
	te, tc := testServerWithAuth(tokenMap, authenicate1, basicCheck)
	defer tc()

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	authenicate2 := fmt.Sprintf("Bearer realm=%q,service=%q", te+"/token", service)
	bearerCheck := func(a string) bool {
		return a == "Bearer statictoken"
	}
	e, c := testServerWithAuth(m, authenicate2, bearerCheck)
	defer c()

	creds := &testCredentialStore{
		username: username,
		password: password,
	}

	challengeManager := challenge.NewSimpleManager()
	_, err := ping(challengeManager, e+"/v2/", "")
	require.NoError(t, err)
	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager, NewTokenHandler(nil, creds, repo, "pull", "push"), NewBasicHandler(creds)))
	client := &http.Client{Transport: transport1}

	req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
	resp, err := client.Do(req)
	require.NoError(t, err, "Error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestEndpointAuthorizeTokenBasicWithExpiresIn(t *testing.T) {
	service := "localhost.localdomain"
	repo := "some/fun/registry"
	scope := fmt.Sprintf("repository:%s:pull,push", repo)
	username := "tokenuser"
	password := "superSecretPa$$word"

	tokenMap := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?account=%s&scope=%s&service=%s", username, url.QueryEscape(scope), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"token":"statictoken", "expires_in": 3001}`),
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?account=%s&scope=%s&service=%s", username, url.QueryEscape(scope), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"access_token":"statictoken", "expires_in": 3001}`),
			},
		},
	})

	authenicate1 := "Basic realm=localhost"
	tokenExchanges := 0
	basicCheck := func(a string) bool {
		tokenExchanges++
		return a == fmt.Sprintf("Basic %s", basicAuth(username, password))
	}
	te, tc := testServerWithAuth(tokenMap, authenicate1, basicCheck)
	defer tc()

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	authenicate2 := fmt.Sprintf("Bearer realm=%q,service=%q", te+"/token", service)
	bearerCheck := func(a string) bool {
		return a == "Bearer statictoken"
	}
	e, c := testServerWithAuth(m, authenicate2, bearerCheck)
	defer c()

	creds := &testCredentialStore{
		username: username,
		password: password,
	}

	challengeManager := challenge.NewSimpleManager()
	_, err := ping(challengeManager, e+"/v2/", "")
	require.NoError(t, err)
	clock := &fakeClock{current: time.Now()}
	options := TokenHandlerOptions{
		Transport:   nil,
		Credentials: creds,
		Scopes: []Scope{
			RepositoryScope{
				Repository: repo,
				Actions:    []string{"pull", "push"},
			},
		},
	}
	tHandler := NewTokenHandlerWithOptions(options)
	tHandler.(*tokenHandler).clock = clock
	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager, tHandler, NewBasicHandler(creds)))
	client := &http.Client{Transport: transport1}

	// First call should result in a token exchange
	// Subsequent calls should recycle the token from the first request, until the expiration has lapsed.
	timeIncrement := 1000 * time.Second
	for i := 0; i < 4; i++ {
		req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
		resp, err := client.Do(req)
		require.NoError(t, err, "Error sending get request")
		// nolint: revive // defer
		defer resp.Body.Close()

		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		require.Equal(t, 1, tokenExchanges, "Unexpected number of token exchanges")
		clock.current = clock.current.Add(timeIncrement)
	}

	// After we've exceeded the expiration, we should see a second token exchange.
	req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
	resp, err := client.Do(req)
	require.NoError(t, err, "Error sending get request")
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	require.Equal(t, 2, tokenExchanges)
}

func TestEndpointAuthorizeTokenBasicWithExpiresInAndIssuedAt(t *testing.T) {
	service := "localhost.localdomain"
	repo := "some/fun/registry"
	scope := fmt.Sprintf("repository:%s:pull,push", repo)
	username := "tokenuser"
	password := "superSecretPa$$word"

	// This test sets things up such that the token was issued one increment
	// earlier than its sibling in TestEndpointAuthorizeTokenBasicWithExpiresIn.
	// This will mean that the token expires after 3 increments instead of 4.
	clock := &fakeClock{current: time.Now()}
	timeIncrement := 1000 * time.Second
	firstIssuedAt := clock.Now()
	clock.current = clock.current.Add(timeIncrement)
	secondIssuedAt := clock.current.Add(2 * timeIncrement)
	tokenMap := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?account=%s&scope=%s&service=%s", username, url.QueryEscape(scope), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"token":"statictoken", "issued_at": "` + firstIssuedAt.Format(time.RFC3339Nano) + `", "expires_in": 3001}`),
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  fmt.Sprintf("/token?account=%s&scope=%s&service=%s", username, url.QueryEscape(scope), service),
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Body:       []byte(`{"access_token":"statictoken", "issued_at": "` + secondIssuedAt.Format(time.RFC3339Nano) + `", "expires_in": 3001}`),
			},
		},
	})

	authenicate1 := "Basic realm=localhost"
	tokenExchanges := 0
	basicCheck := func(a string) bool {
		tokenExchanges++
		return a == fmt.Sprintf("Basic %s", basicAuth(username, password))
	}
	te, tc := testServerWithAuth(tokenMap, authenicate1, basicCheck)
	defer tc()

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	authenicate2 := fmt.Sprintf("Bearer realm=%q,service=%q", te+"/token", service)
	bearerCheck := func(a string) bool {
		return a == "Bearer statictoken"
	}
	e, c := testServerWithAuth(m, authenicate2, bearerCheck)
	defer c()

	creds := &testCredentialStore{
		username: username,
		password: password,
	}

	challengeManager := challenge.NewSimpleManager()
	_, err := ping(challengeManager, e+"/v2/", "")
	require.NoError(t, err)

	options := TokenHandlerOptions{
		Transport:   nil,
		Credentials: creds,
		Scopes: []Scope{
			RepositoryScope{
				Repository: repo,
				Actions:    []string{"pull", "push"},
			},
		},
	}
	tHandler := NewTokenHandlerWithOptions(options)
	tHandler.(*tokenHandler).clock = clock
	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager, tHandler, NewBasicHandler(creds)))
	client := &http.Client{Transport: transport1}

	// First call should result in a token exchange
	// Subsequent calls should recycle the token from the first request, until the expiration has lapsed.
	// We shaved one increment off of the equivalent logic in TestEndpointAuthorizeTokenBasicWithExpiresIn
	// so this loop should have one fewer iteration.
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
		resp, err := client.Do(req)
		require.NoError(t, err)
		// nolint: revive // defer
		defer resp.Body.Close()

		require.Equal(t, http.StatusAccepted, resp.StatusCode)
		require.Equal(t, 1, tokenExchanges)
		clock.current = clock.current.Add(timeIncrement)
	}

	// After we've exceeded the expiration, we should see a second token exchange.
	req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
	resp, err := client.Do(req)
	require.NoError(t, err)
	err = resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	require.Equal(t, 2, tokenExchanges)
}

func TestEndpointAuthorizeBasic(t *testing.T) {
	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/hello",
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
			},
		},
	})

	username := "user1"
	password := "funSecretPa$$word"
	authenicate := "Basic realm=localhost"
	validCheck := func(a string) bool {
		return a == fmt.Sprintf("Basic %s", basicAuth(username, password))
	}
	e, c := testServerWithAuth(m, authenicate, validCheck)
	defer c()
	creds := &testCredentialStore{
		username: username,
		password: password,
	}

	challengeManager := challenge.NewSimpleManager()
	_, err := ping(challengeManager, e+"/v2/", "")
	require.NoError(t, err)
	transport1 := transport.NewTransport(nil, NewAuthorizer(challengeManager, NewBasicHandler(creds)))
	client := &http.Client{Transport: transport1}

	req, _ := http.NewRequest(http.MethodGet, e+"/v2/hello", nil)
	resp, err := client.Do(req)
	require.NoError(t, err, "Error sending get request")

	err = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}
