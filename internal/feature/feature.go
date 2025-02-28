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
}

// DynamicMediaTypes is used to allow the creation of new media types during
// runtime. These new media types will be persisted in the database, so if this
// feature is disabled after creation of new media types, those new media types
// will still be accessible. By default, if an unknown media type encountered,
// the registry, and associated tools, will emit an error.
var DynamicMediaTypes = Feature{
	EnvVariable: "REGISTRY_FF_DYNAMIC_MEDIA_TYPES",
}

// BBMProcess is used to decide if the registry should (or should not) start the asynchronous batched backround migration process when serving traffic.
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-background-migrations.md?ref_type=heads#asynchronous-execution-when-serving-requests-on-the-registry
var BBMProcess = Feature{
	EnvVariable: "REGISTRY_FF_BBM",
}

// EnforceLockfiles is used to enable lock file checking for the `database-in-use` and `filesystem-in-use` checks.
// Enabling this feature will stop the registry in an unsupported mode. See
// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/lockfiles.md
// for more information.
var EnforceLockfiles = Feature{
	defaultEnabled: false,
	EnvVariable:    "REGISTRY_FF_ENFORCE_LOCKFILES",
}

// testFeature is used for testing purposes only
var testFeature = Feature{
	EnvVariable: "REGISTRY_FF_TEST",
}

var all = []Feature{
	testFeature,
	OngoingRenameCheck,
	DynamicMediaTypes,
	BBMProcess,
}

// KnownEnvVar evaluates whether the input string matches the name of one of the known feature flag env vars.
func KnownEnvVar(name string) bool {
	for _, f := range all {
		if f.EnvVariable == name {
			return true
		}
	}

	return false
}
