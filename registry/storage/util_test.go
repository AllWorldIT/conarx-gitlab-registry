package storage

import (
	"context"
	"testing"

	"github.com/docker/distribution/registry/auth"
	"github.com/stretchr/testify/require"
)

func TestInjectCustomKeyOpts(t *testing.T) {

	var tests = []struct {
		name        string
		opt         map[string]interface{}
		expectedOpt map[string]interface{}
		ctx         func() context.Context
	}{
		{
			name: "custom keys in context",
			opt:  map[string]interface{}{},
			expectedOpt: map[string]interface{}{
				ProjectPathKey: "gilab-org/container-registry",
				NamespaceKey:   "gilab-org",
				AuthTypeKey:    "pat",
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
			opt:  map[string]interface{}{},
			expectedOpt: map[string]interface{}{
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
			opt:  map[string]interface{}{},
			expectedOpt: map[string]interface{}{
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
			opt:  map[string]interface{}{},
			expectedOpt: map[string]interface{}{
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
			opt:  map[string]interface{}{},
			expectedOpt: map[string]interface{}{
				AuthTypeKey: "pat",
			},
			ctx: func() context.Context {
				return context.WithValue(
					context.WithValue(
						context.Background(), repositoryNameContextKey, "gilab-org/not-container-registry/test"),
					auth.UserTypeKey, "pat")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			injectCustomKeyOpts(test.ctx(), test.opt)
			require.Equal(t, test.opt, test.expectedOpt)
		})
	}
}
