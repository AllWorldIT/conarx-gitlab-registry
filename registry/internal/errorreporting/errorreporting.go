package errorreporting

import (
	"context"
	"errors"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

func isTransientError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.QueryCanceled {
		return true
	}

	return false
}

// Capture reports an error to Sentry, automatically adding context and stack trace.
// Transient errors (context cancellation, deadline exceeded, DB statement timeouts)
// are filtered out and not reported.
//
// Additional options (e.g., errortracking.WithRequest) can be passed as extra arguments.
func Capture(ctx context.Context, err error, opts ...errortracking.CaptureOption) {
	if err == nil {
		return
	}
	if isTransientError(err) {
		return
	}
	baseOpts := []errortracking.CaptureOption{
		errortracking.WithContext(ctx),
		errortracking.WithStackTrace(),
	}
	errortracking.Capture(err, append(baseOpts, opts...)...)
}
