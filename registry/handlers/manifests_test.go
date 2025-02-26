package handlers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fixed set of valid and overly complex regex patterns within the protectedTagPatternMaxLen limit. A fixed set of
// patterns provides reproducibility for benchmarking.
var fixedPatterns = []string{
	"^[a-zA-Z0-9]{1,10}-[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{1,2}-(alpha|beta|rc)-[a-zA-Z0-9]{1,10}$",    // Matches: "release-1.0.0-alpha-v1", "v10-1.2.3-beta-x"
	"^[a-zA-Z0-9]{1,15}\\.v[0-9]{1,2}\\.stable\\.[0-9]{1,3}-[a-zA-Z0-9]{1,10}-[0-9]{1,2}$",          // Matches: "app1.v1.stable.001-beta-10"
	"^release-[a-zA-Z0-9]{3,20}-(candidate|preview|final)-v[0-9]{1,2}\\.[0-9]{1,2}$",                // Matches: "release-abcdef-candidate-v1.2"
	"^[a-zA-Z0-9_]{10,30}\\.[a-zA-Z0-9]{5,10}-[a-zA-Z]{3,5}\\.[a-zA-Z0-9]{1,5}-[0-9]{1,2}$",         // Matches: "alpha_release.v1.app-beta.1"
	"^stable-[a-zA-Z0-9]{5,20}-v[0-9]{1,2}\\.[0-9]{1,2}\\.[0-9]{1,2}-[a-zA-Z0-9]{5,10}-[0-9]{1,2}$", // Matches: "stable-xyz123-v1.0.0-beta-20"
}

// BenchmarkValidateTagProtection benchmarks the validateTagProtection function using fixed-length patterns.
func BenchmarkValidateTagProtection(b *testing.B) {
	ctx := context.Background()
	tagName := "v1.0.0"

	// Use the maximum allowed number of patterns
	patterns := fixedPatterns[:protectedTagPatternMaxCount]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := validateTagProtection(ctx, tagName, patterns)
		require.NoError(b, err, "Expected no error during tag protection validation")
	}
}
