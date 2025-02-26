package context

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionContext(t *testing.T) {
	ctx := Background()

	require.Empty(t, GetVersion(ctx), "context should not yet have a version")

	expected := "2.1-whatever"
	ctx = WithVersion(ctx, expected)
	version := GetVersion(ctx)

	require.Equal(t, expected, version, "version was not set")
}
