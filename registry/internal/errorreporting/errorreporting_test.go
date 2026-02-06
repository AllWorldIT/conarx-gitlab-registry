package errorreporting

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func Test_isTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "context deadline exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "context canceled",
			err:  context.Canceled,
			want: true,
		},
		{
			name: "wrapped context deadline exceeded",
			err:  errors.Join(errors.New("operation failed"), context.DeadlineExceeded),
			want: true,
		},
		{
			name: "wrapped context canceled",
			err:  errors.Join(errors.New("operation failed"), context.Canceled),
			want: true,
		},
		{
			name: "postgres query canceled (SQLSTATE 57014)",
			err:  &pgconn.PgError{Code: pgerrcode.QueryCanceled},
			want: true,
		},
		{
			name: "wrapped postgres query canceled",
			err:  errors.Join(errors.New("db error"), &pgconn.PgError{Code: pgerrcode.QueryCanceled}),
			want: true,
		},
		{
			name: "postgres connection failure",
			err:  &pgconn.PgError{Code: pgerrcode.ConnectionFailure},
			want: false,
		},
		{
			name: "postgres syntax error",
			err:  &pgconn.PgError{Code: pgerrcode.SyntaxError},
			want: false,
		},
		{
			name: "generic error",
			err:  errors.New("something went wrong"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isTransientError(tt.err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestCapture(t *testing.T) {
	ctx := context.Background()

	require.NotPanics(t, func() {
		Capture(ctx, nil)
	}, "nil errors should be ignored")

	require.NotPanics(t, func() {
		Capture(ctx, context.DeadlineExceeded)
	}, "transient errors should be filtered")
}
