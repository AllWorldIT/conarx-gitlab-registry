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
	"crypto/hmac"
	//nolint: gosec
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"
)

// signURLWithPrefix creates a signed URL with a URL prefix for an endpoint on Cloud CDN.
// Prefixes allow access to any URL with the same prefix, and can be useful for
// granting access broader content without signing multiple URLs.
//
// - url must start with "https://" and should not include query parameters.
// - key should be in raw form (not base64url-encoded) which is 16-bytes long.
// - keyName must match a key added to the backend service or bucket.
func signURLWithPrefix(url, keyName string, key []byte, expiration time.Time) (string, error) {
	if strings.Contains(url, "?") {
		return "", fmt.Errorf("url must not include query params: %s", url)
	}

	encodedURLPrefix := base64.URLEncoding.EncodeToString([]byte(url))
	input := fmt.Sprintf("URLPrefix=%s&Expires=%d&KeyName=%s",
		encodedURLPrefix, expiration.Unix(), keyName)

	mac := hmac.New(sha1.New, key)
	mac.Write([]byte(input))
	sig := base64.URLEncoding.EncodeToString(mac.Sum(nil))

	signedValue := fmt.Sprintf("%s&Signature=%s", input, sig)

	return url + "?" + signedValue, nil
}

// readKeyFile reads the base64url-encoded key file and decodes it.
func readKeyFile(path string) ([]byte, error) {
	// nolint: gosec
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read key file: %+v", err)
	}
	d := make([]byte, base64.URLEncoding.DecodedLen(len(b)))
	n, err := base64.URLEncoding.Decode(d, b)
	if err != nil {
		return nil, fmt.Errorf("failed to base64url decode: %+v", err)
	}
	return d[:n], nil
}

type urlSigner struct {
	keyName string
	key     []byte
}

func newURLSigner(keyName string, key []byte) *urlSigner {
	return &urlSigner{keyName: keyName, key: key}
}

func (s *urlSigner) Sign(url string, expires time.Time) (string, error) {
	return signURLWithPrefix(url, s.keyName, s.key, expires)
}
