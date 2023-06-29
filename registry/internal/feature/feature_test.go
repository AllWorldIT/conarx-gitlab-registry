package feature

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnabled(t *testing.T) {
	cases := map[string]struct {
		envVal         string
		defaultEnabled bool
		expected       bool
	}{
		"disabled_by_default": {
			expected: false,
		},
		"enabled_by_env_variable": {
			envVal:   "true",
			expected: true,
		},
		"disabled_by_env_variable": {
			envVal:   "false",
			expected: false,
		},
		"enabled_by_default": {
			defaultEnabled: true,
			expected:       true,
		},
		"enabled_by_default_but_disabled_by_env_variable": {
			envVal:         "false",
			defaultEnabled: true,
		},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			feature := Feature{
				EnvVariable:    "testFeatureFlag",
				defaultEnabled: tt.defaultEnabled,
			}
			t.Setenv(feature.EnvVariable, tt.envVal)
			require.Equal(t, tt.expected, feature.Enabled())
		})
	}
}
