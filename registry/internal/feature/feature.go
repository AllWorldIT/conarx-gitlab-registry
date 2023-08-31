package feature

import "os"

// Feature defines an application feature toggled by a specific environment variable.
type Feature struct {
	// EnvVariable defines the name of the corresponding environment variable.
	EnvVariable    string
	defaultEnabled bool
}

// Enabled reads the environment variable responsible for the feature flag. If FF is disabled by default, the
// environment variable needs to be `true` to explicitly enable it. If FF is enabled by default, variable needs to be
// `false` to explicitly disable it.
func (f Feature) Enabled() bool {
	env := os.Getenv(f.EnvVariable)

	if f.defaultEnabled {
		return env != "false"
	}

	return env == "true"
}

// OngoingRenameCheck is used to check redis for any GitLab projects (i.e base repositories - if exists) that are undergoing a rename.
// The record to signal that a GitLab project is undergoing a rename is created in redis on a call to `PATCH /gitlab/v1/repositories/<path>/`.
// This "check" feature is triggered on a "per-repository-write" basis, meaning the number of (read) requests to redis will be at least
// the number of registry repository write requests received, with this feature enabled. Proceed with caution.
var OngoingRenameCheck = Feature{
	EnvVariable: "REGISTRY_FF_ONGOING_RENAME_CHECK",
	// defaults to false
}

// AccurateLayerMediaTypes is used to store the layer media type specified by
// the manifest json in the layers table. Without this feature enabled, layers
// are stored with the generic blob media type "application/octet-stream".
var AccurateLayerMediaTypes = Feature{
	EnvVariable:    "REGISTRY_FF_ACCURATE_LAYER_MEDIA_TYPES",
	defaultEnabled: false,
}

// FailOnUnknownLayerMediaTypes is used to toggle failure on manifest PUTs when
// a layer has an unknown layer media type when AccurateLayerMediaTypes is
// enabled. With this feature disabled, layers with an unknown media type are
// stored with the generic blob media type and the PUT allowed to succeed.
var FailOnUnknownLayerMediaTypes = Feature{
	EnvVariable:    "REGISTRY_FF_FAIL_ON_UNKNOWN_LAYER_MEDIA_TYPES",
	defaultEnabled: false,
}
