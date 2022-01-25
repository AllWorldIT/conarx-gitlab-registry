package migration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	tcs := map[string]struct {
		endpoint       string
		secret         string
		expectedErrMsg string
	}{
		"valid_configuration": {endpoint: "https://gitlab.com", secret: "secret"},
		"invalid_endpoint":    {endpoint: "%", expectedErrMsg: "parsing endpoint:"},
		"empty_endpoint":      {endpoint: "", expectedErrMsg: errMissingURL.Error()},
		"missing_secret":      {endpoint: "https://gitlab.com", expectedErrMsg: errMissingAPISecret.Error()},
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			got, err := New(tc.endpoint, tc.secret, time.Second)
			if tc.expectedErrMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrMsg)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
		})
	}
}

func TestNotify(t *testing.T) {
	notification := &Notification{
		Name:   "name",
		Path:   "path",
		Status: "success",
		Detail: "import completed successfully",
	}

	delay := 50 * time.Millisecond
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "secret" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		time.Sleep(delay)

		w.WriteHeader(http.StatusOK)
	}))

	tcs := map[string]struct {
		ctx            context.Context
		timeout        time.Duration
		secret         string
		expectedErrMsg string
	}{
		"success": {
			ctx:     context.Background(),
			timeout: 2 * delay,
			secret:  "secret",
		},
		"invalid_secret": {
			ctx:            context.Background(),
			timeout:        2 * delay,
			secret:         "bad secret",
			expectedErrMsg: fmt.Sprintf("import notifier received response: %d", http.StatusUnauthorized),
		},
		"client_timeout_waiting_for_response": {
			ctx:            context.Background(),
			timeout:        delay / 2,
			secret:         "secret",
			expectedErrMsg: fmt.Sprintf("Client.Timeout"),
		},
		"context_is_canceled": {
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				return ctx
			}(),
			timeout:        2 * delay,
			secret:         "secret",
			expectedErrMsg: context.Canceled.Error(),
		},
		"context_timed_out": {
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), delay/2)
				t.Cleanup(cancel)

				return ctx
			}(),
			timeout:        2 * delay,
			secret:         "secret",
			expectedErrMsg: context.DeadlineExceeded.Error(),
		},
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			n, err := New(s.URL, tc.secret, tc.timeout)
			require.NoError(t, err)

			err = n.Notify(tc.ctx, notification)
			if tc.expectedErrMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrMsg)
				return
			}

			require.NoError(t, err)
		})
	}

}