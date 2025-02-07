package context

import "context"

const (
	tagDenyAccessPatternsKey = "tagdenyaccesspatterns"
	actionPush               = "push"
	actionDelete             = "delete"
	tagImmutablePatternsKey  = "tagimmutablepatterns"
)

// TagActionPatterns represents the name matching patterns used to restrict specific tag operations. The `Push` field
// holds patterns that prevent tags from being created or updated, while the `Delete` field holds patterns that prevent
// tags from being deleted.
type TagActionPatterns struct {
	Push   []string
	Delete []string
}

// tagDenyAccessPatternsContext is the context key used to store tag deny access patterns for specific repository paths.
type tagDenyAccessPatternsContext struct {
	context.Context
	patterns map[string]TagActionPatterns
}

// Value implements context.Context.
func (c tagDenyAccessPatternsContext) Value(key any) any {
	switch key {
	case tagDenyAccessPatternsKey:
		return c.patterns
	default:
		return c.Context.Value(key)
	}
}

// WithTagDenyAccessPatterns stores tag deny access patterns in the context.
func WithTagDenyAccessPatterns(ctx context.Context, patterns map[string]TagActionPatterns) context.Context {
	return tagDenyAccessPatternsContext{
		Context:  ctx,
		patterns: patterns,
	}
}

// TagPushDenyAccessPatterns retrieves the tag deny access patterns for push actions for a specific repository path.
func TagPushDenyAccessPatterns(ctx context.Context, repoPath string) ([]string, bool) {
	return tagDenyAccessPatterns(ctx, repoPath, actionPush)
}

// TagDeleteDenyAccessPatterns retrieves the tag deny access patterns for delete actions for a specific repository path.
func TagDeleteDenyAccessPatterns(ctx context.Context, repoPath string) ([]string, bool) {
	return tagDenyAccessPatterns(ctx, repoPath, actionDelete)
}

// tagDenyAccessPatterns is an unexported function that retrieves the patterns for a given repo path and action type.
func tagDenyAccessPatterns(ctx context.Context, repoPath, action string) ([]string, bool) {
	if patterns, ok := ctx.Value(tagDenyAccessPatternsKey).(map[string]TagActionPatterns); ok {
		if repoPatterns, exists := patterns[repoPath]; exists {
			switch action {
			case actionPush:
				return repoPatterns.Push, true
			case actionDelete:
				return repoPatterns.Delete, true
			}
		}
	}
	return nil, false
}

// tagImmutablePatternsContext is the context key used to store tag immutability patterns for specific repository paths.
type tagImmutablePatternsContext struct {
	context.Context
	patterns map[string][]string
}

// Value implements context.Context.
func (c tagImmutablePatternsContext) Value(key any) any {
	switch key {
	case tagImmutablePatternsKey:
		return c.patterns
	default:
		return c.Context.Value(key)
	}
}

// WithTagImmutablePatterns stores tag immutability patterns in the context.
func WithTagImmutablePatterns(ctx context.Context, patterns map[string][]string) context.Context {
	return tagImmutablePatternsContext{
		Context:  ctx,
		patterns: patterns,
	}
}

// TagImmutablePatterns retrieves the tag immutability patterns for a given repo path.
func TagImmutablePatterns(ctx context.Context, repoPath string) ([]string, bool) {
	if patterns, ok := ctx.Value(tagImmutablePatternsKey).(map[string][]string); ok {
		if repoPatterns, exists := patterns[repoPath]; exists {
			return repoPatterns, true
		}
	}
	return nil, false
}
