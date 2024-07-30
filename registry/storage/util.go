package storage

import (
	"context"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/auth/token"
	"github.com/docker/distribution/registry/storage/driver"
)

const (
	// AuthTypeKey is the key used to reference a request's authentication type in the options map passed to a driver's `URLFor` method
	AuthTypeKey = "auth_type"
	// ProjectIdKey is the key used to reference a request's GitLab project ID in the options map passed to a driver's `URLFor` method.
	ProjectIdKey = "project_id"
	// NamespaceIdKey is the key used to reference a request's GitLab namespace ID in the options map passed to a driver's `URLFor` method.
	NamespaceIdKey = "namespace_id"
	// SizeBytesKey is the key used to reference the size of an object to be downloaded in the options map passed to a driver's `URLFor` method.
	SizeBytesKey = "size_bytes"

	// repositoryNameContextKey is the context key used to reference the targeted repository of a request.
	// The key is set in the handlers when processing a request.
	repositoryNameContextKey = "vars.name"
)

// Exists provides a utility method to test whether a path exists in
// the given driver.
func exists(ctx context.Context, drv driver.StorageDriver, path string) (bool, error) {
	if _, err := drv.Stat(ctx, path); err != nil {
		switch err := err.(type) {
		case driver.PathNotFoundError:
			return false, nil
		default:
			return false, err
		}
	}

	return true, nil
}

// injectCustomKeyOpts injects GitLab metadata in the extraOpts into opts parameter.
func injectCustomKeyOpts(ctx context.Context, opts map[string]any, extraOpts map[string]any) {
	if opts == nil {
		return
	}

	// add extraOpts
	for k, v := range extraOpts {
		opts[k] = v
	}

	if nid := dcontext.GetInt64Value(ctx, token.EgressNamespaceIdKey); nid > 0 {
		opts[NamespaceIdKey] = nid
	}

	if pid := dcontext.GetInt64Value(ctx, token.EgressProjectIdKey); pid > 0 {
		opts[ProjectIdKey] = pid
	}

	if authType := dcontext.GetStringValue(ctx, auth.UserTypeKey); authType != "" {
		opts[AuthTypeKey] = dcontext.GetStringValue(ctx, auth.UserTypeKey)
	}
}
