package testutil

import (
	"testing"

	"github.com/docker/distribution/registry/internal"
)

// StubClock stubs a given clock.Clock with a mock clock.Clock. The original clock.Clock value is automatically
// restored after tb completes.
func StubClock(tb testing.TB, original *internal.Clock, mock internal.Clock) {
	tb.Helper()

	bkp := original
	*original = mock
	tb.Cleanup(func() { original = bkp })
}
