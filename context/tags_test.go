package context

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_WithTagDenyAccessPatterns(t *testing.T) {
	// Define patterns with push and delete actions
	patterns := map[string]TagActionPatterns{
		"repo1": {Push: []string{"^v1.*"}, Delete: []string{"^delete.*"}},
		"repo2": {Push: []string{"^stable.*"}, Delete: []string{"^v2.*"}},
	}

	// Create a context with the tag deny access patterns added
	ctx := WithTagDenyAccessPatterns(context.Background(), patterns)

	// Verify that the patterns were stored in the context
	retrievedPatterns := ctx.Value(tagDenyAccessPatternsKey)
	require.NotNil(t, retrievedPatterns)
	require.Equal(t, patterns, retrievedPatterns)
}

func Test_tagDenyAccessPatternsContext_TagPushDenyAccessPatterns(t *testing.T) {
	// Define separate push and delete patterns
	patterns := map[string]TagActionPatterns{
		"repo1": {Push: []string{"^v1.*", "^release.*"}, Delete: []string{"^delete.*"}},
		"repo2": {Push: []string{"^stable.*"}, Delete: []string{"^v2.*"}},
	}
	ctx := WithTagDenyAccessPatterns(context.Background(), patterns)

	// Test retrieving push patterns for repo1
	retrievedPatterns, exists := TagPushDenyAccessPatterns(ctx, "repo1")
	require.True(t, exists)
	require.Equal(t, patterns["repo1"].Push, retrievedPatterns)

	// Test retrieving push patterns for repo2
	retrievedPatterns, exists = TagPushDenyAccessPatterns(ctx, "repo2")
	require.True(t, exists)
	require.Equal(t, patterns["repo2"].Push, retrievedPatterns)

	// Test non-existent repository for push patterns
	retrievedPatterns, exists = TagPushDenyAccessPatterns(ctx, "foo")
	require.False(t, exists)
	require.Nil(t, retrievedPatterns)
}

func Test_tagDenyAccessPatternsContext_TagDeleteDenyAccessPatterns(t *testing.T) {
	// Define separate push and delete patterns
	patterns := map[string]TagActionPatterns{
		"repo1": {Push: []string{"^v1.*", "^release.*"}, Delete: []string{"^delete.*"}},
		"repo2": {Push: []string{"^stable.*"}, Delete: []string{"^v2.*"}},
	}
	ctx := WithTagDenyAccessPatterns(context.Background(), patterns)

	// Test retrieving delete patterns for repo1
	retrievedPatterns, exists := TagDeleteDenyAccessPatterns(ctx, "repo1")
	require.True(t, exists)
	require.Equal(t, patterns["repo1"].Delete, retrievedPatterns)

	// Test retrieving delete patterns for repo2
	retrievedPatterns, exists = TagDeleteDenyAccessPatterns(ctx, "repo2")
	require.True(t, exists)
	require.Equal(t, patterns["repo2"].Delete, retrievedPatterns)

	// Test non-existent repository for delete patterns
	retrievedPatterns, exists = TagDeleteDenyAccessPatterns(ctx, "foo")
	require.False(t, exists)
	require.Nil(t, retrievedPatterns)
}

func Test_tagDenyAccessPatternsContext_Value(t *testing.T) {
	// Define patterns with push and delete actions
	patterns := map[string]TagActionPatterns{
		"repo1": {Push: []string{"^v1.*"}, Delete: []string{"^delete.*"}},
	}

	// Base context with an unrelated key-value pair
	baseCtx := context.WithValue(context.Background(), "unrelatedKey", "unrelatedValue")

	// Create context with tag deny access patterns added
	ctxWithPatterns := WithTagDenyAccessPatterns(baseCtx, patterns)

	// Verify the unrelated key-value pair is still accessible
	unrelatedValue := ctxWithPatterns.Value("unrelatedKey")
	require.NotNil(t, unrelatedValue)
	require.Equal(t, "unrelatedValue", unrelatedValue)

	// Verify the correct tag deny access patterns are accessible
	retrieved := ctxWithPatterns.Value(tagDenyAccessPatternsKey)
	require.NotNil(t, retrieved)
	retrievedPatterns, ok := retrieved.(map[string]TagActionPatterns)
	require.True(t, ok)
	require.Equal(t, patterns, retrievedPatterns)
}
