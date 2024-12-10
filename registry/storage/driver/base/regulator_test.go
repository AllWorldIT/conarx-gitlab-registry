package base

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRegulatorEnterExit(t *testing.T) {
	const limit = 500

	// nolint: revive // unchecked-type-assertion
	r := NewRegulator(nil, limit).(*Regulator)

	for try := 0; try < 50; try++ {
		run := make(chan struct{})

		var firstGroupReady sync.WaitGroup
		var firstGroupDone sync.WaitGroup
		firstGroupReady.Add(limit)
		firstGroupDone.Add(limit)
		for i := 0; i < limit; i++ {
			go func() {
				r.enter()
				firstGroupReady.Done()
				<-run
				r.exit()
				firstGroupDone.Done()
			}()
		}
		firstGroupReady.Wait()

		// now we exhausted all the limit, let's run a little bit more
		var secondGroupReady sync.WaitGroup
		var secondGroupDone sync.WaitGroup
		for i := 0; i < 50; i++ {
			secondGroupReady.Add(1)
			secondGroupDone.Add(1)
			go func() {
				secondGroupReady.Done()
				r.enter()
				r.exit()
				secondGroupDone.Done()
			}()
		}
		secondGroupReady.Wait()

		// allow the first group to return resources
		close(run)

		done := make(chan struct{})
		go func() {
			secondGroupDone.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			require.Fail(t, "some r.enter() are still locked")
		}

		firstGroupDone.Wait()

		require.Equal(t, limit, r.available, "r.available")
	}
}

func TestGetLimitFromParameter(t *testing.T) {
	tests := []struct {
		Input    any
		Expected uint64
		Min      uint64
		Default  uint64
		Err      error
	}{
		{"foo", 0, 5, 5, fmt.Errorf("parameter must be an integer, 'foo' invalid")},
		{"50", 50, 5, 5, nil},
		{"5", 25, 25, 50, nil}, // lower than Min returns Min
		{nil, 50, 25, 50, nil}, // nil returns default
		{812, 812, 25, 50, nil},
	}

	for _, item := range tests {
		t.Run(fmt.Sprint(item.Input), func(t *testing.T) {
			actual, err := GetLimitFromParameter(item.Input, item.Min, item.Default)

			if item.Err != nil {
				require.EqualError(t, err, item.Err.Error())
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, item.Expected, actual)
		})
	}
}
