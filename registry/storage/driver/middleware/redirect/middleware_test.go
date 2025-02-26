package middleware

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestRedirectMiddlewareSuite(t *testing.T) {
	suite.Run(t, new(RedirectMiddlewareSuite))
}

type RedirectMiddlewareSuite struct {
	suite.Suite
}

func (s *RedirectMiddlewareSuite) TestNoConfig() {
	options := make(map[string]any)
	_, err := newRedirectStorageMiddleware(nil, options)
	require.ErrorContains(s.T(), err, "no baseurl provided")
}

func (s *RedirectMiddlewareSuite) TestMissingScheme() {
	options := make(map[string]any)
	options["baseurl"] = "example.com"
	_, err := newRedirectStorageMiddleware(nil, options)
	require.ErrorContains(s.T(), err, "no scheme specified for redirect baseurl")
}

func (s *RedirectMiddlewareSuite) TestHttpsPort() {
	options := make(map[string]any)
	options["baseurl"] = "https://example.com:5443"
	middleware, err := newRedirectStorageMiddleware(nil, options)
	require.NoError(s.T(), err)

	m, ok := middleware.(*redirectStorageMiddleware)
	require.True(s.T(), ok)
	require.Equal(s.T(), "https", m.scheme)
	require.Equal(s.T(), "example.com:5443", m.host)

	url, err := middleware.URLFor(context.TODO(), "/rick/data", nil)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "https://example.com:5443/rick/data", url)
}

func (s *RedirectMiddlewareSuite) TestHTTP() {
	options := make(map[string]any)
	options["baseurl"] = "http://example.com"
	middleware, err := newRedirectStorageMiddleware(nil, options)
	require.NoError(s.T(), err)

	m, ok := middleware.(*redirectStorageMiddleware)
	require.True(s.T(), ok)
	require.Equal(s.T(), "http", m.scheme)
	require.Equal(s.T(), "example.com", m.host)

	url, err := middleware.URLFor(context.TODO(), "morty/data", nil)
	require.NoError(s.T(), err)
	require.Equal(s.T(), "http://example.com/morty/data", url)
}
