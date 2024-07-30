package storage

import (
	"context"
	"testing"

	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/auth/token"
	"github.com/stretchr/testify/require"
)

func TestInjectCustomKeyOpts(t *testing.T) {
	tests := []struct {
		name        string
		extraOptMap map[string]any
		opt         map[string]any
		expectedOpt map[string]any
		ctx         func() context.Context
	}{
		{
			name:        "custom keys in context",
			opt:         map[string]any{},
			extraOptMap: map[string]any{SizeBytesKey: int64(123)},
			expectedOpt: map[string]any{
				ProjectIdKey:   int64(123),
				NamespaceIdKey: int64(456),
				AuthTypeKey:    "pat",
				SizeBytesKey:   int64(123),
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(
						context.WithValue(context.Background(),
							token.EgressNamespaceIdKey, int64(456)),
						token.EgressProjectIdKey, int64(123)),
					auth.UserTypeKey, "pat")
			},
		},
		{
			name: "extra options",
			opt:  map[string]any{},
			extraOptMap: map[string]any{
				SizeBytesKey: int64(456),
				"custom":     "custom",
			},
			expectedOpt: map[string]any{
				SizeBytesKey: int64(456),
				"custom":     "custom",
			},
			ctx: func() context.Context {
				return context.Background()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			injectCustomKeyOpts(test.ctx(), test.opt, test.extraOptMap)
			require.Equal(t, test.expectedOpt, test.opt)
		})
	}
}
