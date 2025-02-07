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

func TestInt32(t *testing.T) {
	tests := map[string]struct {
		param          any
		defaultt       int32
		expected       int32
		expectedErrMsg string
	}{
		"valid_int32_string": {
			param:    "42",
			expected: 42,
		},
		"valid_negative_int32_string": {
			param:    "-42",
			expected: -42,
		},
		"valid_int32": {
			param:    int32(42),
			expected: 42,
		},
		"valid_negative_int32": {
			param:    int32(-42),
			expected: -42,
		},
		"valid_int": {
			param:    42,
			expected: 42,
		},
		"valid_int64": {
			param:    int64(42),
			expected: 42,
		},
		"valid_float64": {
			param:    float64(42),
			expected: 42,
		},
		"nil": {
			param:    nil,
			expected: 0,
		},
		"nil_defaultt_99": {
			param:    nil,
			defaultt: 99,
			expected: 99,
		},
		"empty_string": {
			param:          "",
			expected:       0,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "": invalid syntax`,
		},
		"empty_string_defaultt_99": {
			param:          "",
			defaultt:       99,
			expected:       99,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "": invalid syntax`,
		},
		"invalid_string": {
			param:          "invalid",
			expected:       0,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "invalid": invalid syntax`,
		},
		"invalid_string_defaultt_99": {
			param:          "invalid",
			defaultt:       99,
			expected:       99,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "invalid": invalid syntax`,
		},
		"int_exceeds_max": {
			param:          2147483648,
			expected:       0,
			expectedErrMsg: `value 2147483648 for "param" exceeds int32 range`,
		},
		"int_exceeds_min": {
			param:          -2147483649,
			expected:       0,
			expectedErrMsg: `value -2147483649 for "param" exceeds int32 range`,
		},
		"int64_exceeds_max": {
			param:          int64(2147483648),
			expected:       0,
			expectedErrMsg: `value 2147483648 for "param" exceeds int32 range`,
		},
		"float64_exceeds_max": {
			param:          float64(2147483648),
			expected:       0,
			expectedErrMsg: `value 2147483648.000000 for "param" exceeds int32 range`,
		},
		"invalid_param": {
			param:          true,
			expected:       0,
			expectedErrMsg: `cannot parse "param" with type bool as int32`,
		},
		"invalid_param_defaultt_99": {
			param:          true,
			defaultt:       99,
			expected:       99,
			expectedErrMsg: `cannot parse "param" with type bool as int32`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := Int32(
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
