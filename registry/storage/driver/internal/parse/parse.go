package parse

import (
	"fmt"
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
