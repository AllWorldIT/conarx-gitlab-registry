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
