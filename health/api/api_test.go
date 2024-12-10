package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/docker/distribution/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGETDownHandlerDoesNotChangeStatus ensures that calling the endpoint
// /debug/health/down with METHOD GET returns a 404
func TestGETDownHandlerDoesNotChangeStatus(t *testing.T) {
	recorder := httptest.NewRecorder()

	req, err := http.NewRequest(http.MethodGet, "https://fakeurl.com/debug/health/down", nil)
	require.NoError(t, err, "failed to create request")

	DownHandler(recorder, req)

	assert.Equal(t, 404, recorder.Code, "did not get a 404")
}

// TestGETUpHandlerDoesNotChangeStatus ensures that calling the endpoint
// /debug/health/down with METHOD GET returns a 404
func TestGETUpHandlerDoesNotChangeStatus(t *testing.T) {
	recorder := httptest.NewRecorder()

	req, err := http.NewRequest(http.MethodGet, "https://fakeurl.com/debug/health/up", nil)
	require.NoError(t, err, "failed to create request")

	DownHandler(recorder, req)

	assert.Equal(t, 404, recorder.Code, "did not get a 404")
}

// TestPOSTDownHandlerChangeStatus ensures the endpoint /debug/health/down changes
// the status code of the response to 503
// This test is order dependent, and should come before TestPOSTUpHandlerChangeStatus
func TestPOSTDownHandlerChangeStatus(t *testing.T) {
	recorder := httptest.NewRecorder()

	req, err := http.NewRequest(http.MethodPost, "https://fakeurl.com/debug/health/down", nil)
	require.NoError(t, err, "failed to create request")

	DownHandler(recorder, req)

	assert.Equal(t, 200, recorder.Code, "did not get a 200")
	assert.Len(t, health.CheckStatus(), 1, "DownHandler didn't add an error check")
}

// TestPOSTUpHandlerChangeStatus ensures the endpoint /debug/health/up changes
// the status code of the response to 200
func TestPOSTUpHandlerChangeStatus(t *testing.T) {
	recorder := httptest.NewRecorder()

	req, err := http.NewRequest(http.MethodPost, "https://fakeurl.com/debug/health/up", nil)
	require.NoError(t, err, "failed to create request")

	UpHandler(recorder, req)

	assert.Equal(t, 200, recorder.Code, "did not get a 200")
	assert.Empty(t, health.CheckStatus(), "UpHandler didn't remove the error check")
}
