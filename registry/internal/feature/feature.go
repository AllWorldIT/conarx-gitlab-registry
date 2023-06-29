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
