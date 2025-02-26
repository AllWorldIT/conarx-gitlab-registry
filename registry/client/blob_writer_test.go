package client

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test implements distribution.BlobWriter
var _ distribution.BlobWriter = &httpBlobUpload{}

func TestUploadReadFrom(t *testing.T) {
	_, b := newRandomBlob(64)
	repo := "test/upload/readfrom"
	locationPath := fmt.Sprintf("/v2/%s/uploads/testid", repo)

	m := testutil.RequestResponseMap([]testutil.RequestResponseMapping{
		{
			Request: testutil.Request{
				Method: http.MethodGet,
				Route:  "/v2/",
			},
			Response: testutil.Response{
				StatusCode: http.StatusOK,
				Headers: http.Header(map[string][]string{
					"Docker-Distribution-API-Version": {"registry/2.0"},
				}),
			},
		},
		// Test Valid case
		{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  locationPath,
				Body:   b,
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
				Headers: http.Header(map[string][]string{
					"Docker-Upload-UUID": {"46603072-7a1b-4b41-98f9-fd8a7da89f9b"},
					"Location":           {locationPath},
					"Range":              {"0-63"},
				}),
			},
		},
		// Test invalid range
		{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  locationPath,
				Body:   b,
			},
			Response: testutil.Response{
				StatusCode: http.StatusAccepted,
				Headers: http.Header(map[string][]string{
					"Docker-Upload-UUID": {"46603072-7a1b-4b41-98f9-fd8a7da89f9b"},
					"Location":           {locationPath},
					"Range":              {""},
				}),
			},
		},
		// Test 404
		{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  locationPath,
				Body:   b,
			},
			Response: testutil.Response{
				StatusCode: http.StatusNotFound,
			},
		},
		// Test 400 valid json
		{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  locationPath,
				Body:   b,
			},
			Response: testutil.Response{
				StatusCode: http.StatusBadRequest,
				Body: []byte(`
					{ "errors":
						[
							{
								"code": "BLOB_UPLOAD_INVALID",
								"message": "blob upload invalid",
								"detail": "more detail"
							}
						]
					} `),
			},
		},
		// Test 400 invalid json
		{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  locationPath,
				Body:   b,
			},
			Response: testutil.Response{
				StatusCode: http.StatusBadRequest,
				Body:       []byte("something bad happened"),
			},
		},
		// Test 500
		{
			Request: testutil.Request{
				Method: http.MethodPatch,
				Route:  locationPath,
				Body:   b,
			},
			Response: testutil.Response{
				StatusCode: http.StatusInternalServerError,
			},
		},
	})

	e, c := testServer(m)
	defer c()

	blobUpload := &httpBlobUpload{
		client: &http.Client{},
	}

	// Valid case
	blobUpload.location = e + locationPath
	n, err := blobUpload.ReadFrom(bytes.NewReader(b))
	require.NoError(t, err, "error calling ReadFrom")
	require.Equal(t, int64(64), n, "wrong length returned from ReadFrom")

	// Bad range
	blobUpload.location = e + locationPath
	_, err = blobUpload.ReadFrom(bytes.NewReader(b))
	require.Error(t, err, "expected error when bad range received")

	// 404
	blobUpload.location = e + locationPath
	_, err = blobUpload.ReadFrom(bytes.NewReader(b))
	require.Error(t, err, "expected error when not found")
	require.ErrorIs(t, err, distribution.ErrBlobUploadUnknown, "wrong error thrown")

	// 400 valid json
	blobUpload.location = e + locationPath
	_, err = blobUpload.ReadFrom(bytes.NewReader(b))
	require.Error(t, err, "expected error when not found")
	var uploadErr errcode.Errors
	require.ErrorAs(t, err, &uploadErr, "wrong error type %T", err)
	assert.Len(t, uploadErr, 1, "unexpected number of errors")
	var v2Err errcode.Error
	require.ErrorAs(t, uploadErr[0], &v2Err)
	assert.Equal(t, v2.ErrorCodeBlobUploadInvalid, v2Err.Code, "unexpected error code")
	assert.Equal(t, "blob upload invalid", v2Err.Message, "unexpected error message")
	assert.Equal(t, "more detail", v2Err.Detail.(string), "unexpected error detail")

	// 400 invalid json
	blobUpload.location = e + locationPath
	_, err = blobUpload.ReadFrom(bytes.NewReader(b))
	require.Error(t, err, "expected error when not found")
	uploadErr2 := new(UnexpectedHTTPResponseError)
	require.ErrorAs(t, err, &uploadErr2)
	assert.Equal(t, "something bad happened", string(uploadErr2.Response), "unexpected response string")

	// 500
	blobUpload.location = e + locationPath
	_, err = blobUpload.ReadFrom(bytes.NewReader(b))
	require.Error(t, err, "expected error when not found")
	uploadErr3 := new(UnexpectedHTTPStatusError)
	require.ErrorAs(t, err, &uploadErr3)
	assert.Equal(t, "500 "+http.StatusText(http.StatusInternalServerError), uploadErr3.Status, "unexpected response status")
}
