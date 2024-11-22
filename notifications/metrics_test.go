package notifications

import (
	"encoding/json"
	"expvar"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetricsExpvar(t *testing.T) {
	endpointsVar := expvar.Get("registry").(*expvar.Map).Get("notifications").(*expvar.Map).Get("endpoints")

	var v any
	err := json.Unmarshal([]byte(endpointsVar.String()), &v)
	require.NoError(t, err, "unexpected error unmarshaling endpoints")

	require.Nil(t, v, "expected nil")

	NewEndpoint("x", "y", EndpointConfig{})

	err = json.Unmarshal([]byte(endpointsVar.String()), &v)
	require.NoError(t, err, "unexpected error unmarshaling endpoints")

	if slice, ok := v.([]any); !ok || len(slice) != 1 {
		t.Logf("expected one-element []interface{}, got %#v", v)
	}
}
