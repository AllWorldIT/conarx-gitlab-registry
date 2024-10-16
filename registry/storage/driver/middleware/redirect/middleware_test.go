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

func (suite *RedirectMiddlewareSuite) TestNoConfig() {
	options := make(map[string]interface{})
	_, err := newRedirectStorageMiddleware(nil, options)
	require.ErrorContains(suite.T(), err, "no baseurl provided")
}

func (suite *RedirectMiddlewareSuite) TestMissingScheme() {
	options := make(map[string]interface{})
	options["baseurl"] = "example.com"
	_, err := newRedirectStorageMiddleware(nil, options)
	require.ErrorContains(suite.T(), err, "no scheme specified for redirect baseurl")
}

func (suite *RedirectMiddlewareSuite) TestHttpsPort() {
	options := make(map[string]interface{})
	options["baseurl"] = "https://example.com:5443"
	middleware, err := newRedirectStorageMiddleware(nil, options)
	require.NoError(suite.T(), err)

	m, ok := middleware.(*redirectStorageMiddleware)
	require.True(suite.T(), ok)
	require.Equal(suite.T(), "https", m.scheme)
	require.Equal(suite.T(), "example.com:5443", m.host)

	url, err := middleware.URLFor(context.TODO(), "/rick/data", nil)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), "https://example.com:5443/rick/data", url)
}

func (suite *RedirectMiddlewareSuite) TestHTTP() {
	options := make(map[string]interface{})
	options["baseurl"] = "http://example.com"
	middleware, err := newRedirectStorageMiddleware(nil, options)
	require.NoError(suite.T(), err)

	m, ok := middleware.(*redirectStorageMiddleware)
	require.True(suite.T(), ok)
	require.Equal(suite.T(), "http", m.scheme)
	require.Equal(suite.T(), "example.com", m.host)

	url, err := middleware.URLFor(context.TODO(), "morty/data", nil)
	require.NoError(suite.T(), err)
	require.Equal(suite.T(), "http://example.com/morty/data", url)
}
