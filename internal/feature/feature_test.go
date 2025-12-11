package feature

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnabled(t *testing.T) {
	testCases := map[string]struct {
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

	for name, tc := range testCases {
		t.Run(name, func(tt *testing.T) {
			feature := Feature{
				EnvVariable:    "testFeatureFlag",
				defaultEnabled: tc.defaultEnabled,
			}
			tt.Setenv(feature.EnvVariable, tc.envVal)
			require.Equal(tt, tc.expected, feature.Enabled())
		})
	}
}

func TestIsKnownEnvVar(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  bool
	}{
		{
			name:  "known",
			input: testFeature.EnvVariable,
			want:  true,
		},
		{
			name:  "unknown",
			input: "REGISTRY_FF__TEST",
			want:  false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			require.Equal(tt, tc.want, KnownEnvVar(tc.input))
		})
	}
}
