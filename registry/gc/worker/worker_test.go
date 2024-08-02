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
func (m isDuration) Matches(x interface{}) bool {
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
	fakeErrorA = errors.New("error A") //nolint: stylecheck
	fakeErrorB = errors.New("error B") //nolint: stylecheck
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
			if got != tt.want {
				t.Errorf("want %v, got %v", tt.want, got)
			}
		})
	}
}
