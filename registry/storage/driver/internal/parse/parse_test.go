package parse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBool(t *testing.T) {
	tests := map[string]struct {
		param          any
		defaultt       bool
		expected       bool
		expectedErrMsg string
	}{
		"valid_boolean_string": {
			param:    "false",
			expected: false,
		},
		"valid_boolean_string_true": {
			param:    "true",
			expected: true,
		},
		"valid_boolean": {
			param:    false,
			expected: false,
		},
		"valid_boolean_true": {
			param:    true,
			expected: true,
		},
		"nil": {
			param:    nil,
			expected: false,
		},
		"nil_defaultt_true": {
			param:    nil,
			defaultt: true,
			expected: true,
		},
		"empty_string": {
			param:          "",
			expected:       false,
			expectedErrMsg: `cannot parse "param" string as bool: strconv.ParseBool: parsing "": invalid syntax`,
		},
		"empty_string_defaultt_true": {
			param:          "",
			defaultt:       true,
			expected:       true,
			expectedErrMsg: `cannot parse "param" string as bool: strconv.ParseBool: parsing "": invalid syntax`,
		},
		"invalid_string": {
			param:          "invalid",
			expected:       false,
			expectedErrMsg: `cannot parse "param" string as bool: strconv.ParseBool: parsing "invalid": invalid syntax`,
		},
		"invalid_string_defaultt_true": {
			param:          "invalid",
			defaultt:       true,
			expected:       true,
			expectedErrMsg: `cannot parse "param" string as bool: strconv.ParseBool: parsing "invalid": invalid syntax`,
		},
		"invalid_param": {
			param:          0,
			expected:       false,
			expectedErrMsg: `cannot parse "param" with type int as bool`,
		},
		"invalid_param_defaultt_true": {
			param:          0,
			defaultt:       true,
			expected:       true,
			expectedErrMsg: `cannot parse "param" with type int as bool`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := Bool(
				map[string]any{
					"param": test.param,
				},
				"param",
				test.defaultt,
			)

			if test.expectedErrMsg != "" {
				require.Error(t, err)
				require.EqualError(t, err, test.expectedErrMsg)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, test.expected, got)
		})
	}
}
