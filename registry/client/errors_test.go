package client

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error { return nil }

func TestHandleErrorResponse401ValidBody(t *testing.T) {
	json := "{\"errors\":[{\"code\":\"UNAUTHORIZED\",\"message\":\"action requires authentication\"}]}"
	response := &http.Response{
		Status:     "401 Unauthorized",
		StatusCode: 401,
		Body:       nopCloser{bytes.NewBufferString(json)},
	}
	err := HandleErrorResponse(response)

	expectedMsg := "unauthorized: action requires authentication"
	assert.ErrorContains(t, err, expectedMsg)
}

func TestHandleErrorResponse401WithInvalidBody(t *testing.T) {
	json := "{invalid json}"
	response := &http.Response{
		Status:     "401 Unauthorized",
		StatusCode: 401,
		Body:       nopCloser{bytes.NewBufferString(json)},
	}
	err := HandleErrorResponse(response)

	expectedMsg := "unauthorized: authentication required"
	assert.ErrorContains(t, err, expectedMsg)
}

func TestHandleErrorResponseExpectedStatusCode400ValidBody(t *testing.T) {
	json := "{\"errors\":[{\"code\":\"DIGEST_INVALID\",\"message\":\"provided digest does not match\"}]}"
	response := &http.Response{
		Status:     "400 Bad Request",
		StatusCode: 400,
		Body:       nopCloser{bytes.NewBufferString(json)},
	}
	err := HandleErrorResponse(response)

	expectedMsg := "digest invalid: provided digest does not match"
	assert.ErrorContains(t, err, expectedMsg)
}

func TestHandleErrorResponseExpectedStatusCode404EmptyErrorSlice(t *testing.T) {
	json := `{"randomkey": "randomvalue"}`
	response := &http.Response{
		Status:     "404 Not Found",
		StatusCode: 404,
		Body:       nopCloser{bytes.NewBufferString(json)},
	}
	err := HandleErrorResponse(response)

	expectedMsg := `error parsing HTTP 404 response body: no error details found in HTTP response body: "{\"randomkey\": \"randomvalue\"}"`
	assert.ErrorContains(t, err, expectedMsg)
}

func TestHandleErrorResponseExpectedStatusCode404InvalidBody(t *testing.T) {
	json := "{invalid json}"
	response := &http.Response{
		Status:     "404 Not Found",
		StatusCode: 404,
		Body:       nopCloser{bytes.NewBufferString(json)},
	}
	err := HandleErrorResponse(response)

	expectedMsg := "error parsing HTTP 404 response body: invalid character 'i' looking for beginning of object key string: \"{invalid json}\""
	assert.ErrorContains(t, err, expectedMsg)
}

func TestHandleErrorResponseUnexpectedStatusCode501(t *testing.T) {
	response := &http.Response{
		Status:     "501 Not Implemented",
		StatusCode: 501,
		Body:       nopCloser{bytes.NewBufferString("{\"Error Encountered\" : \"Function not implemented.\"}")},
	}
	err := HandleErrorResponse(response)

	expectedMsg := "received unexpected HTTP status: 501 Not Implemented"
	assert.ErrorContains(t, err, expectedMsg)
}
