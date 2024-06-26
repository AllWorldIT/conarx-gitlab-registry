package context

import (
	"context"
	"time"
)

// Since looks up key, which should be a time.Time, and returns the duration
// since that time. If the key is not found, the value returned will be zero.
// This is helpful when inferring metrics related to context execution times.
func Since(ctx context.Context, key any) time.Duration {
	if startedAt, ok := ctx.Value(key).(time.Time); ok {
		return time.Since(startedAt)
	}
	return 0
}

// GetStringValue returns a string value from the context. The empty string
// will be returned if not found.
func GetStringValue(ctx context.Context, key any) (value string) {
	if v, ok := ctx.Value(key).(string); ok {
		value = v
	}
	return value
}

// GetInt64Value returns an int64 value from the context.
func GetInt64Value(ctx context.Context, key any) (value int64) {
	if v, ok := ctx.Value(key).(int64); ok {
		value = v
	}
	return value
}
