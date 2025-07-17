package parse

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"time"
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

func Int32(parameters map[string]any, name string, defaultt, minimum, maximum int32) (int32, error) {
	var rv int32
	switch value := parameters[name].(type) {
	case string:
		v, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return defaultt, fmt.Errorf("cannot parse %q string as int32: %w", name, err)
		}
		rv = int32(v)
	case int32:
		rv = value
	case int:
		if value > math.MaxInt32 || value < math.MinInt32 {
			return defaultt, fmt.Errorf("value %d for %q exceeds int32 range", value, name)
		}
		rv = int32(value)
	case int64:
		if value > math.MaxInt32 || value < math.MinInt32 {
			return defaultt, fmt.Errorf("value %d for %q exceeds int32 range", value, name)
		}
		rv = int32(value)
	case float64:
		if value > math.MaxInt32 || value < math.MinInt32 {
			return defaultt, fmt.Errorf("value %f for %q exceeds int32 range", value, name)
		}
		rv = int32(value)
	case nil:
		return defaultt, nil
	default:
		return defaultt, fmt.Errorf("cannot parse %q with type %T as int32", name, value)
	}

	if rv < minimum || rv > maximum {
		return 0, fmt.Errorf("the %s %d parameter should be a number between %d and %d (inclusive)", name, rv, minimum, maximum)
	}

	return rv, nil
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

// Duration converts parameters[name] to a time.Duration value (using
// defaultt if nil). It only accepts non-negative durations.
// It accepts:
// - time.Duration values (must be non-negative)
// - strings in Go duration format (e.g., "1h30m", "30s")
// - int/int64/int32 values (interpreted as seconds, must be non-negative)
// - uint/uint32/uint64 values (interpreted as seconds)
// - float64 values (interpreted as seconds, must be non-negative)
func Duration(parameters map[string]any, name string, defaultt time.Duration) (time.Duration, error) {
	var dur time.Duration
	switch value := parameters[name].(type) {
	case time.Duration:
		dur = value
	case string:
		d, err := time.ParseDuration(value)
		if err != nil {
			return defaultt, fmt.Errorf("cannot parse %q string as duration: %w", name, err)
		}
		dur = d
	case int:
		if value < 0 {
			return defaultt, fmt.Errorf("%q must be non-negative, got %d", name, value)
		}
		dur = time.Duration(value) * time.Second
	case int64:
		if value < 0 {
			return defaultt, fmt.Errorf("%q must be non-negative, got %d", name, value)
		}
		dur = time.Duration(value) * time.Second
	case int32:
		if value < 0 {
			return defaultt, fmt.Errorf("%q must be non-negative, got %d", name, value)
		}
		dur = time.Duration(value) * time.Second
	case uint32:
		dur = time.Duration(value) * time.Second
	case float64:
		if value < 0 {
			return defaultt, fmt.Errorf("%q must be non-negative, got %f", name, value)
		}
		// Convert float seconds to duration
		// Note: this may lose precision for very large values
		dur = time.Duration(value * float64(time.Second))
	case nil:
		return defaultt, nil
	default:
		return defaultt, fmt.Errorf("cannot parse %q with type %T as duration", name, value)
	}

	// Check if the final duration is negative
	if dur < 0 {
		return defaultt, fmt.Errorf("%q must be non-negative, got %v", name, dur)
	}

	return dur, nil
}
