// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package googlecdn

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadKeyFile(t *testing.T) {
	f, err := os.CreateTemp("", "cdnkey")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	err = f.Close()
	require.NoError(t, err)

	key := `nZtRohdNF9m3cKM24IcK4w==`
	expected := []byte{
		0x9d, 0x9b, 0x51, 0xa2, 0x17, 0x4d, 0x17, 0xd9,
		0xb7, 0x70, 0xa3, 0x36, 0xe0, 0x87, 0x0a, 0xe3,
	}
	require.NoError(t, os.WriteFile(f.Name(), []byte(key), 0o600))
	b, err := readKeyFile(f.Name())
	require.NoError(t, err)
	require.Equal(t, expected, b, "the signed cookie value did not match")
}

func TestSignURLWithPrefix(t *testing.T) {
	testKey := []byte{
		0x9d, 0x9b, 0x51, 0xa2, 0x17, 0x4d, 0x17, 0xd9,
		0xb7, 0x70, 0xa3, 0x36, 0xe0, 0x87, 0x0a, 0xe3,
	} // base64url: nZtRohdNF9m3cKM24IcK4w==

	tests := []struct {
		name       string
		urlPrefix  string
		keyName    string
		expiration time.Time
		expected   string
		err        error
	}{
		{
			name:       "Domain and Simple Prefix",
			urlPrefix:  "https://media.example.com/segments/",
			keyName:    "my-key",
			expiration: time.Unix(1558131350, 0),
			expected:   "https://media.example.com/segments/?URLPrefix=aHR0cHM6Ly9tZWRpYS5leGFtcGxlLmNvbS9zZWdtZW50cy8=&Expires=1558131350&KeyName=my-key&Signature=HWE5tBTZgnYVoZzVLG7BtRnOsgk=",
		},
		{
			name:       "Domain Only",
			urlPrefix:  "https://www.google.com/",
			keyName:    "my-key",
			expiration: time.Unix(1549751401, 0),
			expected:   "https://www.google.com/?URLPrefix=aHR0cHM6Ly93d3cuZ29vZ2xlLmNvbS8=&Expires=1549751401&KeyName=my-key&Signature=o0zZ77jb7BgtGRPQEaXmX3cCLh8=",
		},
		{
			name:       "Includes query param",
			urlPrefix:  "https://www.google.com/?foo=bar",
			keyName:    "my-key",
			expiration: time.Unix(1549751401, 0),
			expected:   "",
			err:        errors.New("url must not include query params: https://www.google.com/?foo=bar"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			signedValue, err := signURLWithPrefix(test.urlPrefix, test.keyName, testKey, test.expiration)
			require.Equal(t, test.err, err)
			require.Equal(t, test.expected, signedValue)
		})
	}
}
