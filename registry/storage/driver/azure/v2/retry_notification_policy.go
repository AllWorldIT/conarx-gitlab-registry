package v2

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
)

// Inspired by/credit goes to https://github.com/Azure/azure-storage-azcopy/blob/97ab7b92e766ad48965ac2933495dff1b04fb2a7/ste/xferRetryNotificationPolicy.go

type contextKey struct {
	name string
}

var timeoutNotifyContextKey = contextKey{"timeoutNotify"}

// withTimeoutNotification returns a context that contains indication of a
// timeout. The retryNotificationPolicy will then set the timeout flag when a
// timeout happens
func withTimeoutNotification(ctx context.Context, timeout *bool) context.Context { // nolint: unused
	return context.WithValue(ctx, timeoutNotifyContextKey, timeout)
}

// PolicyFunc is a type that implements the Policy interface.
// Use this type when implementing a stateless policy as a first-class function.
type PolicyFunc func(*policy.Request) (*http.Response, error)

// Do implements the Policy interface on policyFunc.
func (pf PolicyFunc) Do(req *policy.Request) (*http.Response, error) {
	return pf(req)
}

func newRetryNotificationPolicy() policy.Policy {
	getErrorCode := func(resp *http.Response) string {
		// NOTE(prozlach): This is a hacky way to handle all possible cases of
		// emitting error by the Azure backend.
		// In theory we could look just at `x-ms-error-code` HTTP header, but
		// in practice Azure SDK also looks at the body and decodes it as JSON
		// or XML in case when the header is absent.
		// So the idea is to piggy-back on the runtime.NewResponseError that
		// will do the proper decoding for us and just return the ErrorCode
		// field instead.
		err := runtime.NewResponseError(resp)
		if err == nil {
			return ""
		}
		errTyped, ok := err.(*azcore.ResponseError)
		if ok {
			return errTyped.ErrorCode
		}
		return fmt.Sprintf("non standard error: %s", err.Error())
	}

	return PolicyFunc(func(req *policy.Request) (*http.Response, error) {
		response, err := req.Next() // Make the request

		if response == nil {
			return nil, err
		}

		if response.StatusCode != http.StatusInternalServerError {
			return response, err
		}

		errorCodeHeader := getErrorCode(response)
		if bloberror.Code(errorCodeHeader) != bloberror.OperationTimedOut {
			return response, err
		}

		if timeout, ok := req.Raw().Context().Value(timeoutNotifyContextKey).(*bool); ok {
			*timeout = true
		}

		return response, err
	})
}
