package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	dbmock "github.com/docker/distribution/registry/datastore/mocks"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func stubClock(tb testing.TB, t time.Time) {
	tb.Helper()

	bkp := timeNow
	timeNow = func() time.Time { return t }

	tb.Cleanup(func() { timeNow = bkp })
}

type isContextWithDeadline struct {
	deadline time.Time
}

// Matches implements gomock.Matcher.
func (m isContextWithDeadline) Matches(x interface{}) bool {
	ctx, ok := x.(context.Context)
	if !ok {
		return false
	}
	d, ok := ctx.Deadline()
	if !ok {
		return false
	}

	return d == m.deadline
}

// String implements gomock.Matcher.
func (m isContextWithDeadline) String() string {
	return fmt.Sprintf("is context.Context with a deadline of %q", m.deadline)
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
	fakeErrorA = errors.New("error A")
	fakeErrorB = errors.New("error B")
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
