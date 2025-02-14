package parse

import (
	"fmt"
	"reflect"
	"strconv"
)

func Bool(parameters map[string]any, name string, defaultt bool) (bool, error) {
	switch value := parameters[name].(type) {
	case string:
		v, err := strconv.ParseBool(value)
		if err != nil {
			return defaultt, fmt.Errorf("cannot parse %q string as bool: %w", name, err)
		}

		return v, nil
	case bool:
		return value, nil
	case nil:
		return defaultt, nil
	default:
		return defaultt, fmt.Errorf("cannot parse %q with type %T as bool", name, value)
	}
}

func Int32(parameters map[string]any, name string, defaultt int32) (int32, error) {
	switch value := parameters[name].(type) {
	case string:
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return defaultt, fmt.Errorf("cannot parse %q string as int32: %w", name, err)
		}
		return int32(v), nil
	case int32:
		return value, nil
	case int:
		if value > 2147483647 || value < -2147483648 {
			return defaultt, fmt.Errorf("value %d for %q exceeds int32 range", value, name)
		}
		return int32(value), nil
	case int64:
		if value > 2147483647 || value < -2147483648 {
			return defaultt, fmt.Errorf("value %d for %q exceeds int32 range", value, name)
		}
		return int32(value), nil
	case float64:
		if value > 2147483647 || value < -2147483648 {
			return defaultt, fmt.Errorf("value %f for %q exceeds int32 range", value, name)
		}
		return int32(value), nil
	case nil:
		return defaultt, nil
	default:
		return defaultt, fmt.Errorf("cannot parse %q with type %T as int32", name, value)
	}
}

// Int64 converts parameters[name] to an int64 value (using
// default if nil), verifies it is no smaller than min, and returns it.
func Int64(parameters map[string]any, name string, defaultt, minimum, maximum int64) (int64, error) {
	rv := defaultt
	param := parameters[name]
	switch v := param.(type) {
	case string:
		vv, err := strconv.ParseInt(v, 0, 64)
		if err != nil {
			return 0, fmt.Errorf("%s parameter must be an integer, %v invalid", name, param)
		}
		rv = vv
	case int64:
		rv = v
	case int, uint, int32, uint32, uint64:
		rv = reflect.ValueOf(v).Convert(reflect.TypeOf(rv)).Int()
	case nil:
		// do nothing
	default:
		return 0, fmt.Errorf("converting value for %s: %#v", name, param)
	}

	if rv < minimum || rv > maximum {
		return 0, fmt.Errorf("the %s %#v parameter should be a number between %d and %d (inclusive)", name, rv, minimum, maximum)
	}

	return rv, nil
}
