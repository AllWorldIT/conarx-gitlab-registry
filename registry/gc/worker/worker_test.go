package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	dbmock "github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func stubClock(tb testing.TB, t time.Time) clock.Clock {
	tb.Helper()

	mock := clock.NewMock()
	mock.Set(t)
	testutil.StubClock(tb, &systemClock, mock)

	return mock
}

type isDuration struct {
	d time.Duration
}

// Matches implements gomock.Matcher.
func (m isDuration) Matches(x any) bool {
	d, ok := x.(time.Duration)
	if !ok {
		return false
	}
	return d == m.d
}

// String implements gomock.Matcher.
func (m isDuration) String() string {
	return fmt.Sprintf("is duration of %q", m.d)
}

var (
	errFakeA = errors.New("error A") // nolint: stylecheck
	errFakeB = errors.New("error B") // nolint: stylecheck
)

func Test_baseWorker_Name(t *testing.T) {
	w := &baseWorker{name: "foo"}
	require.Equal(t, "foo", w.Name())
}

func Test_baseWorker_rollbackOnExit_PanicRecover(t *testing.T) {
	ctrl := gomock.NewController(t)
	txMock := dbmock.NewMockTransactor(ctrl)

	txMock.EXPECT().Rollback().Times(1)

	w := &baseWorker{}
	err := errors.New("foo")
	f := func() {
		defer w.rollbackOnExit(context.Background(), txMock)
		panic(err)
	}

	require.PanicsWithError(t, err.Error(), f)
}

func Test_exponentialBackoff(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  time.Duration
	}{
		{"negative", -1, 5 * time.Minute},
		{"0", 0, 5 * time.Minute},
		{"1", 1, 10 * time.Minute},
		{"2", 2, 20 * time.Minute},
		{"3", 3, 40 * time.Minute},
		{"4", 4, 1*time.Hour + 20*time.Minute},
		{"5", 5, 2*time.Hour + 40*time.Minute},
		{"6", 6, 5*time.Hour + 20*time.Minute},
		{"7", 7, 10*time.Hour + 40*time.Minute},
		{"8", 8, 21*time.Hour + 20*time.Minute},
		{"beyond max", 9, 24 * time.Hour},
		{"int64 overflow", 31, 24 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := exponentialBackoff(tt.input)
			require.Equal(t, tt.want, got)
		})
	}

	// Test a wide range of input values to ensure base and max values are not violated.
	base := 5 * time.Minute
	maximum := 24 * time.Hour
	durations := make([]time.Duration, 0)

	for i := -12; i < 144; i++ {
		d := exponentialBackoff(i)

		// Ensure values never exceed base or max.
		require.GreaterOrEqual(t, d, base)
		require.LessOrEqual(t, d, maximum)

		durations = append(durations, d)
	}

	// Ensure values are monotonic.
	require.IsNonDecreasing(t, durations)
}
