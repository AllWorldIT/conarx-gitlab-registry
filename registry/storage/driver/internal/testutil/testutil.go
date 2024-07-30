package testutil

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type Opts struct {
	Defaultt          interface{}
	Required          bool
	NilAllowed        bool
	EmptyAllowed      bool
	NonTypeAllowed    bool
	ParamName         string
	DriverParamName   string
	OriginalParams    map[string]interface{}
	ParseParametersFn func(map[string]interface{}) (interface{}, error)
}

func AssertByDefaultType(t *testing.T, opts Opts) {
	t.Helper()

	switch tt := opts.Defaultt.(type) {
	case bool:
		TestBoolValue(t, opts)
	case string:
		TestStringValue(t, opts)
	default:
		t.Fatalf("unknown type: %v", tt)
	}
}

func TestBoolValue(t *testing.T, opts Opts) {
	t.Helper()

	// Keep OriginalParams intact for idempotency.
	params := CopyMap(opts.OriginalParams)

	driverParams, err := opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "default value mismatch")

	params[opts.ParamName] = true
	driverParams, err = opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, true, "boolean true")

	params[opts.ParamName] = false
	driverParams, err = opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, false, "boolean false")

	params[opts.ParamName] = nil
	driverParams, err = opts.ParseParametersFn(params)
	require.NoError(t, err, "nil does not return: %v", err)

	AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "param is nil")

	params[opts.ParamName] = ""
	driverParams, err = opts.ParseParametersFn(params)
	require.Error(t, err, "empty string")

	params[opts.ParamName] = "invalid"
	driverParams, err = opts.ParseParametersFn(params)
	require.Error(t, err, "not boolean string")

	params[opts.ParamName] = 12
	driverParams, err = opts.ParseParametersFn(params)
	require.Error(t, err, "not boolean type")
}

func TestStringValue(t *testing.T, opts Opts) {
	t.Helper()

	// Keep OriginalParams intact for idempotency.
	params := CopyMap(opts.OriginalParams)

	value := "value"
	if opts.Required {
		value = params[opts.ParamName].(string)
	} else if opts.Defaultt != nil && opts.Defaultt.(string) != "" {
		value = opts.Defaultt.(string)
	}

	params[opts.ParamName] = value
	driverParams, err := opts.ParseParametersFn(params)
	require.NoError(t, err)

	AssertParam(t, driverParams, opts.DriverParamName, value, "string value")

	params[opts.ParamName] = nil
	driverParams, err = opts.ParseParametersFn(params)
	if opts.NilAllowed {
		require.NoError(t, err, "nil does not return: %v", err)
		AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "param is nil")
	} else {
		require.Error(t, err, "nil value should error")
	}

	params[opts.ParamName] = ""
	driverParams, err = opts.ParseParametersFn(params)
	if opts.EmptyAllowed {
		require.NoError(t, err, "empty does not return: %v", err)
		AssertParam(t, driverParams, opts.DriverParamName, opts.Defaultt, "param is empty")
	} else {
		require.Error(t, err, "empty string")
	}

	params[opts.ParamName] = 12
	driverParams, err = opts.ParseParametersFn(params)
	if opts.NonTypeAllowed {
		require.NoError(t, err, "not string type")
		AssertParam(t, driverParams, opts.DriverParamName, "12", "param is empty")
	} else {
		require.Error(t, err, "non string type")
	}
}

func AssertParam(t *testing.T, params interface{}, fieldName string, expected interface{}, msgs ...interface{}) {
	t.Helper()

	r := reflect.ValueOf(params)
	field := reflect.Indirect(r).FieldByName(fieldName)

	switch e := expected.(type) {
	case string:
		require.Equal(t, e, field.String(), msgs...)
	case bool:
		require.Equal(t, e, field.Bool(), msgs...)
	default:
		t.Fatalf("unhandled expected type: %T", e)
	}
}

func CopyMap(original map[string]interface{}) map[string]interface{} {
	newMap := make(map[string]interface{})
	for k, v := range original {
		newMap[k] = v
	}

	return newMap
}
