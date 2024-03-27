package storage

import (
	"context"
	"strconv"
	"testing"

	"github.com/docker/distribution/registry/auth"
	"github.com/stretchr/testify/require"
)

func TestInjectCustomKeyOpts(t *testing.T) {

	var tests = []struct {
		name        string
		extraOptMap map[string]string
		opt         map[string]any
		expectedOpt map[string]any
		ctx         func() context.Context
	}{
		{
			name:        "custom keys in context",
			opt:         map[string]any{},
			extraOptMap: map[string]string{SizeBytesKey: "123"},
			expectedOpt: map[string]any{
				ProjectPathKey: "gilab-org/container-registry",
				NamespaceKey:   "gilab-org",
				AuthTypeKey:    "pat",
				SizeBytesKey:   "123",
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(
						context.WithValue(context.Background(),
							auth.ResourceProjectPathsKey, []string{"gilab-org/container-registry"}),
						repositoryNameContextKey, "gilab-org/container-registry/test"),
					auth.UserTypeKey, "pat")
			},
		},
		{
			name: "multiple project paths",
			opt:  map[string]any{},
			expectedOpt: map[string]any{
				ProjectPathKey: "gilab-org/container-registry",
				NamespaceKey:   "gilab-org",
				AuthTypeKey:    "pat",
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(
						context.WithValue(context.Background(),
							auth.ResourceProjectPathsKey, []string{"gilab-org/container-registry", "gilab-org/container-registry-2"}),
						repositoryNameContextKey, "gilab-org/container-registry/test"),
					auth.UserTypeKey, "pat")
			},
		},
		{
			name: "project paths do not match repo",
			opt:  map[string]any{},
			expectedOpt: map[string]any{
				AuthTypeKey: "pat",
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(
						context.WithValue(context.Background(),
							auth.ResourceProjectPathsKey, []string{"gilab-org/container-registry", "gilab-org/container-registry-2"}),
						repositoryNameContextKey, "gilab-org/not-container-registry/test"),
					auth.UserTypeKey, "pat")
			},
		},
		{
			name: "missing auth type key",
			opt:  map[string]any{},
			expectedOpt: map[string]any{
				ProjectPathKey: "gilab-org/container-registry",
				NamespaceKey:   "gilab-org",
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(context.Background(),
						auth.ResourceProjectPathsKey, []string{"gilab-org/container-registry"}),
					repositoryNameContextKey, "gilab-org/container-registry/test")
			},
		},
		{
			name: "missing project paths",
			opt:  map[string]any{},
			expectedOpt: map[string]any{
				AuthTypeKey: "pat",
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(
						context.Background(), repositoryNameContextKey, "gilab-org/not-container-registry/test"),
					auth.UserTypeKey, "pat")
			},
		},
		{
			name: "extra options",
			opt:  map[string]any{},
			extraOptMap: map[string]string{
				SizeBytesKey: strconv.FormatInt(int64(456), 10),
				"custom":     "custom",
			},
			expectedOpt: map[string]any{
				SizeBytesKey: "456",
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
			require.Equal(t, test.opt, test.expectedOpt)
		})
	}
}
