package silly

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSillyAccessController(t *testing.T) {
	ac := &accessController{
		realm:   "test-realm",
		service: "test-service",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithRequest(context.Background(), r)
		authCtx, err := ac.Authorized(ctx)
		if err != nil {
			switch err := err.(type) {
			case auth.Challenge:
				err.SetHeaders(r, w)
				w.WriteHeader(http.StatusUnauthorized)
				return
			default:
				assert.NoError(t, err, "unexpected error authorizing request")
			}
		}

		userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
		assert.True(t, ok, "silly accessController did not set auth.user context")

		assert.Equal(t, "silly", userInfo.Name)
		assert.Equal(t, "silly-type", userInfo.Type)

		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	resp, err := http.Get(server.URL)
	require.NoError(t, err, "unexpected error during GET")
	defer resp.Body.Close()

	// Request should not be authorized
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode, "unexpected response status")

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	require.NoError(t, err, "unexpected error creating new request")
	req.Header.Set("Authorization", "seriously, anything")

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err, "unexpected error during GET")
	defer resp.Body.Close()

	// Request should not be authorized
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "unexpected response status")
}
