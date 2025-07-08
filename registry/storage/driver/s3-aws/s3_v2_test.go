package s3

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/smithy-go"
	"github.com/docker/distribution/log"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	btestutil "github.com/docker/distribution/testutil"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3DriverRetriesAndErrorHandling(t *testing.T) {
	if skipMsg := skipCheck(true, common.V2DriverName); skipMsg != "" {
		t.Skip(skipMsg)
	}

	ctx := context.Background()
	rootDirectory := t.TempDir()
	blobber := btestutil.NewBlober(btestutil.RandomBlob(t, common.DefaultChunkSize*2+1))

	testFuncWriterAllOK := func(
		tt *testing.T,
		sDriver storagedriver.StorageDriver,
		filename string,
		isTestValidf func() bool,
	) bool {
		writer, err := sDriver.Writer(ctx, filename, false)
		require.NoError(tt, err)

		nn, err := io.Copy(writer, blobber.GetReader())
		if !isTestValidf() {
			return false
		}
		require.NoError(tt, err)
		require.EqualValues(tt, blobber.Size(), nn)

		err = writer.Commit()
		require.NoError(tt, err)
		err = writer.Close()
		require.NoError(tt, err)
		require.EqualValues(tt, blobber.Size(), writer.Size())

		reader, err := sDriver.Reader(ctx, filename, 0)
		require.NoError(tt, err)
		defer reader.Close()

		blobber.RequireStreamEqual(tt, reader, 0, fmt.Sprintf("filename: %s", filename))
		return true
	}

	// testFuncDeleteAllFail simulates deleting 2*common.DeleteMax files. The
	// driver should run two iterations even if the first errors out.
	testFuncDeleteAllFail := func(
		tt *testing.T,
		sDriver storagedriver.StorageDriver,
		prefix string,
		isTestValidf func() bool,
	) bool {
		testFiles := dtestutil.BuildFiles(
			ctx,
			tt,
			sDriver,
			prefix,
			2*common.DeleteMax,
			// We do not need big tests files here and uploading them would
			// take tool long.
			btestutil.NewBlober(btestutil.RandomBlob(tt, 512)),
		)

		count, err := sDriver.DeleteFiles(ctx, testFiles)
		if !isTestValidf() {
			return false
		}
		require.Zero(tt, count)
		errs := new(multierror.Error)
		require.ErrorAs(tt, err, &errs)
		require.Len(tt, errs.WrappedErrors(), 2)

		for _, e := range errs.WrappedErrors() {
			opErr := new(smithy.OperationError)
			assert.ErrorAs(tt, e, &opErr)
		}

		return true
	}

	testFuncWriterErrorContains := func(errMsg string) func(*testing.T, storagedriver.StorageDriver, string, func() bool) bool {
		return func(
			tt *testing.T,
			sDriver storagedriver.StorageDriver,
			filename string,
			isTestValidf func() bool,
		) bool {
			writer, err := sDriver.Writer(ctx, filename, false)
			require.NoError(tt, err)

			_, err = io.Copy(writer, blobber.GetReader())
			if !isTestValidf() {
				return false
			}
			assert.ErrorContains(tt, err, errMsg)
			return true
		}
	}

	matchWriterTwoUploadRequests := func(*testing.T) dtestutil.RequestMatcher {
		reqNumber := 0

		return func(req *http.Request) bool {
			if req.Method != http.MethodPut {
				return false
			}

			reqQP := req.URL.Query()
			xID := reqQP.Get("x-id")
			if xID != "UploadPart" {
				return false
			}
			pn := reqQP.Get("partNumber")
			if pn != "2" {
				return false
			}

			// In case of a retry there are going to be more then one such
			// request
			reqNumber++
			return reqNumber == 1 || reqNumber == 2
		}
	}

	matchDeleteRequests := func(*testing.T) dtestutil.RequestMatcher {
		return func(req *http.Request) bool {
			if req.Method != http.MethodPost {
				return false
			}

			reqQP := req.URL.Query()
			return reqQP.Has("delete")
		}
	}

	matchAlways := func(*testing.T) dtestutil.RequestMatcher {
		return func(*http.Request) bool { return true }
	}

	matchUserAgent := func(tt *testing.T) dtestutil.RequestModifier {
		return func(req *http.Request) (*http.Request, bool) {
			ua := req.Header.Get("User-Agent")
			assert.Contains(tt, ua, "docker-distribution")
			return req, true
		}
	}

	makeResponseOperationTimeout := func(tt *testing.T) dtestutil.ResponseModifier {
		return func(resp *http.Response) (*http.Response, bool) {
			if resp.StatusCode != http.StatusOK {
				tt.Logf("makeResponseOperationTimeout, response not matched: %d", resp.StatusCode)
				return resp, false
			}

			resp.StatusCode = http.StatusInternalServerError
			resp.Status = "Internal server error occurred"
			tt.Log("response marked as timed out")

			return resp, true
		}
	}

	makeResponseNonretryableError := func(tt *testing.T) dtestutil.ResponseModifier {
		return func(resp *http.Response) (*http.Response, bool) {
			if resp.StatusCode != http.StatusOK {
				tt.Logf("makeResponseNonretryableError, response not matched: %d", resp.StatusCode)
				return resp, false
			}

			// Make sure this is not a retryable error for S3:
			resp.StatusCode = http.StatusBadRequest
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			newBodyContent := `
<Error>
  <Code>TooManyParts</Code>
  <Message>Too many parts in this multipart upload. The maximum number of parts allowed is 10000.</Message>
  <MaxParts>10000</MaxParts>
  <RequestId>4442587FB7D0A2F9</RequestId>
  <HostId>oaXn1PJF+lcCHXdjyFCjfKW9NFYeJXwWNTFSJj3/Ck9LRvYTK9lGaJ+gtL9+UAT8</HostId>
</Error>`

			resp.Body = io.NopCloser(strings.NewReader(newBodyContent))
			resp.ContentLength = int64(len(newBodyContent))
			tt.Log("response marked as non-retryable error")

			return resp, true
		}
	}

	tests := []struct {
		name               string
		interceptorConfigs []dtestutil.InterceptorConfig
		testFunc           func(*testing.T, storagedriver.StorageDriver, string, func() bool) bool
		maxRetries         int64
	}{
		{
			name: "writer happy path - no injected failure",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchAlways,
					RequestModifier:                    matchUserAgent,
					ResponseModifier:                   nil,
					ExpectedRequestModificationsCount:  6,
					ExpectedResponseModificationsCount: 0,
				},
			},
			testFunc:   testFuncWriterAllOK,
			maxRetries: 3,
		},
		{
			name: "unrecoverable error from server",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterTwoUploadRequests,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseNonretryableError,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc:   testFuncWriterErrorContains("Too many parts in this multipart upload"),
			maxRetries: 3,
		},
		{
			name: "recovered retry",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterTwoUploadRequests,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 2,
				},
			},
			testFunc:   testFuncWriterAllOK,
			maxRetries: 3,
		},
		{
			name: "number of retries exceeded",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterTwoUploadRequests,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 2,
				},
			},
			testFunc:   testFuncWriterErrorContains("api error InternalServerError: Internal Server Error"),
			maxRetries: 2,
		},
		{
			name: "retries disabled",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:                            matchWriterTwoUploadRequests,
					RequestModifier:                    nil,
					ResponseModifier:                   makeResponseOperationTimeout,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 1,
				},
			},
			testFunc:   testFuncWriterErrorContains("api error InternalServerError: Internal Server Error"),
			maxRetries: 0,
		},
		{
			name: "delete files all failed",
			interceptorConfigs: []dtestutil.InterceptorConfig{
				{
					Matcher:         matchDeleteRequests,
					RequestModifier: nil,
					// The kind of error returned does not exactly belong to
					// the errors returned in response to Delete call, but this
					// is OK as long as the SDK does not retry.
					ResponseModifier:                   makeResponseNonretryableError,
					ExpectedRequestModificationsCount:  0,
					ExpectedResponseModificationsCount: 2,
				},
			},
			testFunc:   testFuncDeleteAllFail,
			maxRetries: 3,
		},
	}

	fetchDriver := func(
		tt *testing.T,
		interceptorConfigs []dtestutil.InterceptorConfig,
		maxRetries int64,
	) (storagedriver.StorageDriver, func() bool) {
		parsedParams, err := fetchDriverConfig(
			rootDirectory,
			s3.StorageClassStandard,
			log.GetLogger(log.WithTestingTB(tt)),
		)
		require.NoError(tt, err)
		parsedParams.MaxRetries = maxRetries

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConnsPerHost = 10
		// nolint: gosec
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: parsedParams.SkipVerify,
		}
		interceptor, err := dtestutil.NewInterceptor(transport)
		require.NoError(tt, err)

		expectedRequestModificationsCount := 0
		expectedResponseModificationsCount := 0
		for _, ic := range interceptorConfigs {
			if ic.RequestModifier != nil {
				interceptor.AddRequestHook(ic.Matcher(tt), ic.RequestModifier(tt))
				expectedRequestModificationsCount += ic.ExpectedRequestModificationsCount
			}
			if ic.ResponseModifier != nil {
				interceptor.AddResponseHook(ic.Matcher(tt), ic.ResponseModifier(tt))
				expectedResponseModificationsCount += ic.ExpectedResponseModificationsCount
			}
		}

		parsedParams.Transport = interceptor
		sDriver, err := newDriverFn(parsedParams)
		require.NoError(tt, err)

		isTestValidf := func() bool {
			if expectedRequestModificationsCount != interceptor.GetRequestHooksMatchedCount() {
				tt.Logf("expected vs. got request modifications: %d != %d", expectedRequestModificationsCount, interceptor.GetRequestHooksMatchedCount())
				return false
			}
			if expectedResponseModificationsCount != interceptor.GetResponseHooksMatchedCount() {
				tt.Logf("expected vs. got response modifications: %d != %d", expectedResponseModificationsCount, interceptor.GetResponseHooksMatchedCount())
				return false
			}
			return true
		}

		return sDriver, isTestValidf
	}

	maxAttempts := 3
	for _, test := range tests {
		testIsValid := false
		for i := 0; i < maxAttempts && !testIsValid; i++ {
			testName := fmt.Sprintf("%s_attempt_%d", test.name, i)
			t.Run(testName, func(tt *testing.T) {
				filename := dtestutil.RandomPath(4, 32)
				tt.Logf("blob path used for testing: %s", filename)
				// We can't use `sDriver` below as it has active interceptro
				// config that may interfere with cleanup/deletion
				vanillaSDriverConfig, err := fetchDriverConfig(
					rootDirectory,
					s3.StorageClassStandard,
					log.GetLogger(log.WithTestingTB(tt)),
				)
				require.NoError(tt, err)
				vanillaSDriver, err := newDriverFn(vanillaSDriverConfig)
				require.NoError(tt, err)
				defer dtestutil.EnsurePathDeleted(ctx, tt, vanillaSDriver, filename)

				sDriver, isTestValidf := fetchDriver(tt, test.interceptorConfigs, test.maxRetries)

				testIsValid = test.testFunc(tt, sDriver, filename, isTestValidf)
				if !testIsValid {
					if i+1 == maxAttempts { // last iteration
						require.FailNowf(tt, "number of attempts exceeded", "despite %d attempts, preconditions for this test were not reached", maxAttempts)
					} else {
						tt.Skipf("preconditions required to continue were not met, skipping this attempt")
					}
				}
			})
		}
	}
}
