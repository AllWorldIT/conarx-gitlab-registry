package challenge

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthChallengeParse(t *testing.T) {
	header := http.Header{}
	header.Add("WWW-Authenticate", `Bearer realm="https://auth.example.com/token",service="registry.example.com",other=fun,slashed="he\"\l\lo"`)

	challenges := parseAuthHeader(header)
	require.Len(t, challenges, 1, "unexpected number of auth challenges")
	challenge := challenges[0]

	assert.Equal(t, "bearer", challenge.Scheme, "unexpected scheme")

	assert.Equal(t, "https://auth.example.com/token", challenge.Parameters["realm"], "unexpected param")

	assert.Equal(t, "registry.example.com", challenge.Parameters["service"], "unexpected param")

	assert.Equal(t, "fun", challenge.Parameters["other"], "unexpected param")

	assert.Equal(t, "he\"llo", challenge.Parameters["slashed"], "unexpected param")
}

func TestAuthChallengeNormalization(t *testing.T) {
	testAuthChallengeNormalizationImpl(t, "reg.EXAMPLE.com")
	testAuthChallengeNormalizationImpl(t, "bɿɒʜɔiɿ-ɿɘƚƨim-ƚol-ɒ-ƨʞnɒʜƚ.com")
	testAuthChallengeNormalizationImpl(t, "reg.example.com:80")
	testAuthChallengeConcurrent(t, "reg.EXAMPLE.com")
}

func testAuthChallengeNormalizationImpl(t *testing.T, host string) {
	scm := NewSimpleManager()

	registryURL, err := url.Parse(fmt.Sprintf("http://%s/v2/", host))
	require.NoError(t, err)

	resp := &http.Response{
		Request: &http.Request{
			URL: registryURL,
		},
		Header:     make(http.Header),
		StatusCode: http.StatusUnauthorized,
	}
	resp.Header.Add("WWW-Authenticate", fmt.Sprintf("Bearer realm=\"https://%s/token\",service=\"registry.example.com\"", host))

	require.NoError(t, scm.AddResponse(resp))

	lowered := *registryURL
	lowered.Host = strings.ToLower(lowered.Host)
	lowered.Host = canonicalAddr(&lowered)
	c, err := scm.GetChallenges(lowered)
	require.NoError(t, err)

	assert.NotEmpty(t, c, "expected challenge for lower-cased-host URL")
}

func testAuthChallengeConcurrent(t *testing.T, host string) {
	scm := NewSimpleManager()

	registryURL, err := url.Parse(fmt.Sprintf("http://%s/v2/", host))
	require.NoError(t, err)

	resp := &http.Response{
		Request: &http.Request{
			URL: registryURL,
		},
		Header:     make(http.Header),
		StatusCode: http.StatusUnauthorized,
	}
	resp.Header.Add("WWW-Authenticate", fmt.Sprintf("Bearer realm=\"https://%s/token\",service=\"registry.example.com\"", host))
	var s sync.WaitGroup
	s.Add(2)
	go func() {
		defer s.Done()
		for i := 0; i < 200; i++ {
			assert.NoError(t, scm.AddResponse(resp))
		}
	}()
	go func() {
		defer s.Done()
		lowered := *registryURL
		lowered.Host = strings.ToLower(lowered.Host)
		for k := 0; k < 200; k++ {
			_, err := scm.GetChallenges(lowered)
			assert.NoError(t, err)
		}
	}()
	s.Wait()
}
