package storage

import (
	"context"
	"strings"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/storage/driver"
)

const (
	// AuthTypeKey is the key used to reference a request's authentication type in the options map passed to a driver's `URLFor` method
	AuthTypeKey = "auth_type"
	// ProjectPathKey is the key used to reference a request's GitLab project in the options map passed to a driver's `URLFor` method.
	ProjectPathKey = "project_path"
	// NamespaceKey is the key used to reference a request's GitLab project namespace in the options map passed to a driver's `URLFor` method.
	NamespaceKey = "namespace"

	// repositoryNameContextKey is the context key used to reference the targeted repository of a request.
	// The key is set in the handlers when processing a request.
	repositoryNameContextKey = "vars.name"
)

// Exists provides a utility method to test whether or not a path exists in
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

// injectCustomKeyOpts injects the `namespace`, `project_path` and `auth_type` keys into the opts parameter
func injectCustomKeyOpts(ctx context.Context, opts map[string]any) {
	if opts == nil {
		return
	}

	authProjectPaths := dcontext.GetStringSliceValue(ctx, auth.ResourceProjectPathsKey)
	repositoryPath := dcontext.GetStringValue(ctx, repositoryNameContextKey)

	// find the project path that corresponds to the repository path in the request
	for _, authProjectPath := range authProjectPaths {
		if strings.HasPrefix(repositoryPath, authProjectPath) {
			opts[ProjectPathKey] = authProjectPath
			opts[NamespaceKey] = strings.Split(repositoryPath, "/")[0]
		}
	}

	if authType := dcontext.GetStringValue(ctx, auth.UserTypeKey); authType != "" {
		opts[AuthTypeKey] = dcontext.GetStringValue(ctx, auth.UserTypeKey)
	}
}
