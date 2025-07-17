package parse

import (
	"math"
	"testing"
	"time"

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
		minimum        int32
		maximum        int32
		expected       int32
		expectedErrMsg string
	}{
		"valid_int32_string": {
			param:    "42",
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_negative_int32_string": {
			param:    "-42",
			minimum:  -100,
			maximum:  0,
			expected: -42,
		},
		"valid_int32": {
			param:    int32(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_negative_int32": {
			param:    int32(-42),
			minimum:  -100,
			maximum:  0,
			expected: -42,
		},
		"valid_int": {
			param:    42,
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_int64": {
			param:    int64(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_float64": {
			param:    float64(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"nil": {
			param:    nil,
			defaultt: 0,
			minimum:  0,
			maximum:  100,
			expected: 0,
		},
		"nil_defaultt_99": {
			param:    nil,
			defaultt: 99,
			minimum:  0,
			maximum:  100,
			expected: 99,
		},
		"empty_string": {
			param:          "",
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "": invalid syntax`,
		},
		"empty_string_defaultt_99": {
			param:          "",
			defaultt:       99,
			minimum:        0,
			maximum:        100,
			expected:       99,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "": invalid syntax`,
		},
		"invalid_string": {
			param:          "invalid",
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `cannot parse "param" string as int32: strconv.ParseInt: parsing "invalid": invalid syntax`,
		},
		"below_minimum": {
			param:          42,
			minimum:        50,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `the param 42 parameter should be a number between 50 and 100 (inclusive)`,
		},
		"above_maximum": {
			param:          142,
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `the param 142 parameter should be a number between 0 and 100 (inclusive)`,
		},
		"int_exceeds_max": {
			param:          int64(2147483648),
			minimum:        0,
			maximum:        math.MaxInt32,
			expected:       0,
			expectedErrMsg: `value 2147483648 for "param" exceeds int32 range`,
		},
		"int_exceeds_min": {
			param:          int64(-2147483649),
			minimum:        math.MinInt32,
			maximum:        0,
			expected:       0,
			expectedErrMsg: `value -2147483649 for "param" exceeds int32 range`,
		},
		"invalid_param": {
			param:          true,
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `cannot parse "param" with type bool as int32`,
		},
		"int64_exceeds_max": {
			param:          int64(2147483648),
			minimum:        0,
			maximum:        math.MaxInt32,
			expected:       0,
			expectedErrMsg: `value 2147483648 for "param" exceeds int32 range`,
		},
		"int64_exceeds_min": {
			param:          int64(-2147483649),
			minimum:        math.MinInt32,
			maximum:        0,
			expected:       0,
			expectedErrMsg: `value -2147483649 for "param" exceeds int32 range`,
		},
		"float64_exceeds_max": {
			param:          float64(2147483648),
			minimum:        0,
			maximum:        math.MaxInt32,
			expected:       0,
			expectedErrMsg: `value 2147483648.000000 for "param" exceeds int32 range`,
		},
		"float64_exceeds_min": {
			param:          float64(-2147483649),
			minimum:        math.MinInt32,
			maximum:        0,
			expected:       0,
			expectedErrMsg: `value -2147483649.000000 for "param" exceeds int32 range`,
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
				test.minimum,
				test.maximum,
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

func TestInt64(t *testing.T) {
	tests := map[string]struct {
		param          any
		defaultt       int64
		minimum        int64
		maximum        int64
		expected       int64
		expectedErrMsg string
	}{
		"valid_int64_string": {
			param:    "42",
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_negative_int64_string": {
			param:    "-42",
			minimum:  -100,
			maximum:  0,
			expected: -42,
		},
		"valid_int64": {
			param:    int64(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_negative_int64": {
			param:    int64(-42),
			minimum:  -100,
			maximum:  0,
			expected: -42,
		},
		"valid_int": {
			param:    42,
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_int32": {
			param:    int32(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_uint": {
			param:    uint(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_uint32": {
			param:    uint32(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"valid_uint64": {
			param:    uint64(42),
			minimum:  0,
			maximum:  100,
			expected: 42,
		},
		"nil": {
			param:    nil,
			defaultt: 0,
			minimum:  0,
			maximum:  100,
			expected: 0,
		},
		"nil_defaultt_99": {
			param:    nil,
			defaultt: 99,
			minimum:  0,
			maximum:  100,
			expected: 99,
		},
		"empty_string": {
			param:          "",
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `param parameter must be an integer,  invalid`,
		},
		"invalid_string": {
			param:          "invalid",
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `param parameter must be an integer, invalid invalid`,
		},
		"below_minimum": {
			param:          42,
			minimum:        50,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `the param 42 parameter should be a number between 50 and 100 (inclusive)`,
		},
		"above_maximum": {
			param:          142,
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `the param 142 parameter should be a number between 0 and 100 (inclusive)`,
		},
		"invalid_param": {
			param:          true,
			minimum:        0,
			maximum:        100,
			expected:       0,
			expectedErrMsg: `converting value for param: true`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := Int64(
				map[string]any{
					"param": test.param,
				},
				"param",
				test.defaultt,
				test.minimum,
				test.maximum,
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

func TestDuration(t *testing.T) {
	tests := map[string]struct {
		param          any
		defaultt       time.Duration
		expected       time.Duration
		expectedErrMsg string
	}{
		"valid_duration": {
			param:    42 * time.Second,
			expected: 42 * time.Second,
		},
		"valid_duration_string": {
			param:    "1h30m",
			expected: 90 * time.Minute,
		},
		"valid_duration_string_seconds": {
			param:    "30s",
			expected: 30 * time.Second,
		},
		"valid_duration_string_complex": {
			param:    "1h30m45s",
			expected: time.Hour + 30*time.Minute + 45*time.Second,
		},
		"valid_int": {
			param:    42,
			expected: 42 * time.Second,
		},
		"valid_int64": {
			param:    int64(120),
			expected: 120 * time.Second,
		},
		"valid_int32": {
			param:    int32(60),
			expected: 60 * time.Second,
		},
		"valid_uint32": {
			param:    uint32(45),
			expected: 45 * time.Second,
		},
		"valid_float64": {
			param:    float64(2.5),
			expected: 2500 * time.Millisecond,
		},
		"valid_float64_fractional": {
			param:    float64(1.23456),
			expected: time.Duration(1234560000), // 1.23456 seconds in nanoseconds
		},
		"zero_int": {
			param:    0,
			expected: 0,
		},
		"negative_int": {
			param:          -30,
			defaultt:       5 * time.Second,
			expected:       5 * time.Second,
			expectedErrMsg: `"param" must be non-negative, got -30`,
		},
		"nil": {
			param:    nil,
			defaultt: 0,
			expected: 0,
		},
		"nil_with_default": {
			param:    nil,
			defaultt: 5 * time.Minute,
			expected: 5 * time.Minute,
		},
		"empty_string": {
			param:          "",
			defaultt:       10 * time.Second,
			expected:       10 * time.Second,
			expectedErrMsg: `cannot parse "param" string as duration: time: invalid duration ""`,
		},
		"invalid_string": {
			param:          "invalid",
			defaultt:       15 * time.Second,
			expected:       15 * time.Second,
			expectedErrMsg: `cannot parse "param" string as duration: time: invalid duration "invalid"`,
		},
		"invalid_string_format": {
			param:          "1h30",
			defaultt:       20 * time.Second,
			expected:       20 * time.Second,
			expectedErrMsg: `cannot parse "param" string as duration: time: missing unit in duration "1h30"`,
		},
		"invalid_type_bool": {
			param:          true,
			defaultt:       25 * time.Second,
			expected:       25 * time.Second,
			expectedErrMsg: `cannot parse "param" with type bool as duration`,
		},
		"invalid_type_slice": {
			param:          []int{1, 2, 3},
			defaultt:       30 * time.Second,
			expected:       30 * time.Second,
			expectedErrMsg: `cannot parse "param" with type []int as duration`,
		},
		"invalid_type_map": {
			param:          map[string]int{"test": 1},
			defaultt:       35 * time.Second,
			expected:       35 * time.Second,
			expectedErrMsg: `cannot parse "param" with type map[string]int as duration`,
		},
		"large_int64": {
			param:    int64(86400), // 24 hours in seconds
			expected: 24 * time.Hour,
		},
		"microseconds_string": {
			param:    "100µs",
			expected: 100 * time.Microsecond,
		},
		"nanoseconds_string": {
			param:    "500ns",
			expected: 500 * time.Nanosecond,
		},
		"negative_duration_string": {
			param:          "-5m30s",
			defaultt:       10 * time.Second,
			expected:       10 * time.Second,
			expectedErrMsg: `"param" must be non-negative, got -5m30s`,
		},
		"negative_duration": {
			param:          -5 * time.Minute,
			defaultt:       10 * time.Second,
			expected:       10 * time.Second,
			expectedErrMsg: `"param" must be non-negative, got -5m0s`,
		},
		"negative_int64": {
			param:          int64(-120),
			defaultt:       15 * time.Second,
			expected:       15 * time.Second,
			expectedErrMsg: `"param" must be non-negative, got -120`,
		},
		"negative_int32": {
			param:          int32(-60),
			defaultt:       20 * time.Second,
			expected:       20 * time.Second,
			expectedErrMsg: `"param" must be non-negative, got -60`,
		},
		"negative_float64": {
			param:          float64(-2.5),
			defaultt:       25 * time.Second,
			expected:       25 * time.Second,
			expectedErrMsg: `"param" must be non-negative, got -2.500000`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := Duration(
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
