package common_test

import (
	"strings"
	"testing"

	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/stretchr/testify/require"
)

func TestAzureDriverPathToKey(t *testing.T) {
	tests := []struct {
		name          string
		rootDirectory string
		providedPath  string
		expectedPath  string
		legacyPath    bool
	}{
		{
			name:          "legacy leading slash empty root directory",
			rootDirectory: "",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "legacy leading slash single slash root directory",
			rootDirectory: "/",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "empty root directory results in expected path",
			rootDirectory: "",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "docker/registry/v2",
		},
		{
			name:          "legacy empty root directory results in expected path",
			rootDirectory: "",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "root directory no slashes prefixed to path with slash between root and path",
			rootDirectory: "opt",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "opt/docker/registry/v2",
		},
		{
			name:          "legacy root directory no slashes prefixed to path with slash between root and path",
			rootDirectory: "opt",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "/opt/docker/registry/v2",
			legacyPath:    true,
		},
		{
			name:          "root directory with slashes prefixed to path no leading slash",
			rootDirectory: "/opt/",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "opt/docker/registry/v2",
		},
		{
			name:          "dirty root directory prefixed to path cleanly",
			rootDirectory: "/opt////",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "opt/docker/registry/v2",
		},
		{
			name:          "nested custom root directory prefixed to path",
			rootDirectory: "a/b/c/d/",
			providedPath:  "/docker/registry/v2/",
			expectedPath:  "a/b/c/d/docker/registry/v2",
		},
		{
			name:          "legacy root directory results in expected root path",
			rootDirectory: "",
			providedPath:  "/",
			expectedPath:  "/",
			legacyPath:    true,
		},
		{
			name:          "root directory results in expected root path",
			rootDirectory: "",
			providedPath:  "/",
			expectedPath:  "",
		},
		{
			name:          "legacy root directory no slashes results in expected root path",
			rootDirectory: "opt",
			providedPath:  "/",
			expectedPath:  "/opt",
			legacyPath:    true,
		},
		{
			name:          "root directory no slashes results in expected root path",
			rootDirectory: "opt",
			providedPath:  "/",
			expectedPath:  "opt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rootDirectory := strings.Trim(tt.rootDirectory, "/")
			if rootDirectory != "" {
				rootDirectory += "/"
			}
			d := common.NewPather(rootDirectory, tt.legacyPath)
			require.Equal(t, tt.expectedPath, d.PathToKey(tt.providedPath))
		})
	}
}
