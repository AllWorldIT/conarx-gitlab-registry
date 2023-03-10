package version

// Package is the overall, canonical project import path under which the
// package was built.
var Package = "github.com/docker/distribution"

// Version is filled with the version in git used to build the program at
// linking time. The value here will be used if the registry is run after a go
// get based install.
var Version = "unknown"

// Revision is filled with the VCS (e.g. git) revision being used to build
// the program at linking time.
var Revision = ""

// BuildTime is filled with the UTC datetime when the binary was built.
var BuildTime = ""

// ExtFeatures is a comma separated list of extensions/features supported by the GitLab Container Registry that are
// not part of the Docker Distribution spec.
const ExtFeatures = "tag_delete"
