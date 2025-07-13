package googlecdn

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/docker/distribution/registry/storage/driver/testdriver"
	"github.com/stretchr/testify/require"
)

func createTmpKeyFile(t *testing.T) *os.File {
	f, err := os.CreateTemp("", "")
	require.NoError(t, err)

	// nolint: revive // unhandled-error
	t.Cleanup(func() { os.Remove(f.Name()) })

	key := `c29tZS1zZWNyZXQ=`
	err = os.WriteFile(f.Name(), []byte(key), 0o600)
	require.NoError(t, err)

	return f
}

func TestNewGoogleCDNStorageMiddlewareOptions(t *testing.T) {
	keyFile := createTmpKeyFile(t)

	tt := []struct {
		name             string
		options          map[string]any
		expectedError    bool
		expectedErrorMsg string
	}{
		{
			name:             "no baseurl",
			options:          make(map[string]any, 0),
			expectedError:    true,
			expectedErrorMsg: "no baseurl provided",
		},
		{
			name: "baseurl not a string",
			options: map[string]any{
				"baseurl": 1,
			},
			expectedError:    true,
			expectedErrorMsg: "baseurl must be a string",
		},
		{
			name: "no privatekey",
			options: map[string]any{
				"baseurl": "https://my.google.cdn.com",
			},
			expectedError:    true,
			expectedErrorMsg: "no privatekey provided",
		},
		{
			name: "privatekey not a string",
			options: map[string]any{
				"baseurl":    "https://my.google.cdn.com",
				"privatekey": 1,
			},
			expectedError:    true,
			expectedErrorMsg: "privatekey must be a string",
		},
		{
			name: "privatekey not a file",
			options: map[string]any{
				"baseurl":    "https://my.google.cdn.com",
				"privatekey": "/path/to/key",
			},
			expectedError:    true,
			expectedErrorMsg: "failed to read privatekey file: failed to read key file: open /path/to/key: no such file or directory",
		},
		{
			name: "no keyname",
			options: map[string]any{
				"baseurl":    "https://my.google.cdn.com",
				"privatekey": keyFile.Name(),
			},
			expectedError:    true,
			expectedErrorMsg: "no keyname provided",
		},
		{
			name: "keyname not a string",
			options: map[string]any{
				"baseurl":    "https://my.google.cdn.com",
				"privatekey": keyFile.Name(),
				"keyname":    1,
			},
			expectedError:    true,
			expectedErrorMsg: "keyname must be a string",
		},
		{
			name: "duration not a duration string",
			options: map[string]any{
				"baseurl":    "https://my.google.cdn.com",
				"privatekey": keyFile.Name(),
				"keyname":    "my-key",
				"duration":   "foo",
			},
			expectedError:    true,
			expectedErrorMsg: `invalid duration: time: invalid duration "foo"`,
		},
		{
			name: "duration is a duration",
			options: map[string]any{
				"baseurl":    "https://my.google.cdn.com",
				"privatekey": keyFile.Name(),
				"keyname":    "my-key",
				"duration":   5 * time.Minute,
			},
			expectedError: false,
		},
		{
			name: "updatefrequency not a duration string",
			options: map[string]any{
				"baseurl":         "https://my.google.cdn.com",
				"privatekey":      keyFile.Name(),
				"keyname":         "my-key",
				"updatefrequency": "bar",
			},
			expectedError:    true,
			expectedErrorMsg: `invalid updatefrequency: time: invalid duration "bar"`,
		},
		{
			name: "updatefrequency is a duration",
			options: map[string]any{
				"baseurl":         "https://my.google.cdn.com",
				"privatekey":      keyFile.Name(),
				"keyname":         "my-key",
				"updatefrequency": 5 * time.Minute,
			},
			expectedError: false,
		},
		{
			name: "iprangesurl not a string",
			options: map[string]any{
				"baseurl":     "https://my.google.cdn.com",
				"privatekey":  keyFile.Name(),
				"keyname":     "my-key",
				"iprangesurl": []string{"foo"},
			},
			expectedError:    true,
			expectedErrorMsg: "iprangesurl must be a string",
		},
		{
			name: "ipfilteredby not a string",
			options: map[string]any{
				"baseurl":      "https://my.google.cdn.com",
				"privatekey":   keyFile.Name(),
				"keyname":      "my-key",
				"ipfilteredby": 1,
			},
			expectedError:    true,
			expectedErrorMsg: "ipfilteredby must be a string",
		},
		{
			name: "ipfilteredby invalid value",
			options: map[string]any{
				"baseurl":      "https://my.google.cdn.com",
				"privatekey":   keyFile.Name(),
				"keyname":      "my-key",
				"ipfilteredby": "foo",
			},
			expectedError:    true,
			expectedErrorMsg: "ipfilteredby must be one of the following values: none|gcp",
		},
		{
			name: "ipfilteredby set to none",
			options: map[string]any{
				"baseurl":      "https://my.google.cdn.com",
				"privatekey":   keyFile.Name(),
				"keyname":      "my-key",
				"ipfilteredby": "none",
			},
			expectedError: false,
		},
		{
			name: "ipfilteredby set to gcp",
			options: map[string]any{
				"baseurl":      "https://my.google.cdn.com",
				"privatekey":   keyFile.Name(),
				"keyname":      "my-key",
				"ipfilteredby": "gcp",
			},
			expectedError: false,
		},
		{
			name: "full valid options",
			options: map[string]any{
				"baseurl":         "https://my.google.cdn.com",
				"privatekey":      keyFile.Name(),
				"keyname":         "my-key",
				"duration":        "10ms",
				"updatefrequency": "5m",
				"iprangesurl":     "https://www.gstatic.com/ipranges/goog.json",
				"ipfilteredby":    "gcp",
			},
			expectedError: false,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			d, _, err := newGoogleCDNStorageMiddleware(nil, test.options)
			if test.expectedError {
				require.Nil(t, d)
				require.EqualError(t, err, test.expectedErrorMsg)
			} else {
				require.NotNil(t, d)
				require.NoError(t, err)
			}
		})
	}
}

func TestNewGoogleCDNStorageMiddlewareWrapsInnerDriver(t *testing.T) {
	inner := testdriver.New()
	outer, _, err := newGoogleCDNStorageMiddleware(inner, map[string]any{
		"baseurl":    "https://my.google.cdn.com",
		"privatekey": createTmpKeyFile(t).Name(),
		"keyname":    "my-key",
	})

	require.NoError(t, err)
	require.NotEqual(t, inner, outer)
	require.IsType(t, &testdriver.TestDriver{}, inner)
	require.IsType(t, &googleCDNStorageMiddleware{}, outer)
}

func TestURLForBypassIfNotGCSDriver(t *testing.T) {
	inMemDriver := testdriver.New()
	d, _, err := newGoogleCDNStorageMiddleware(inMemDriver, map[string]any{
		"baseurl":    "https://my.google.cdn.com",
		"privatekey": createTmpKeyFile(t).Name(),
		"keyname":    "my-key",
	})
	require.NoError(t, err)

	u, err := d.URLFor(context.Background(), "/foo/bar", nil)

	// the inmemory driver does not support URLFor, so we should see this error in case of a bypass
	require.EqualError(t, err, "inmemory: unsupported method")
	require.Empty(t, u)
}
