package datastore

import (
	"errors"
	"fmt"

	"github.com/opencontainers/go-digest"
)

// Digest is the database representation of a digest, stored in the format `<algorithm prefix><hex>`.
type Digest string

const (
	// Algorithm prefixes are sequences of two digits. These should never change, only additions are allowed.
	sha256DigestAlgorithmPrefix = "01"
	sha512DigestAlgorithmPrefix = "02"
)

// String implements the Stringer interface.
func (d Digest) String() string {
	return string(d)
}

// NewDigest builds a Digest based on a digest.Digest.
func NewDigest(d digest.Digest) (Digest, error) {
	if err := d.Validate(); err != nil {
		return "", err
	}

	var algPrefix string
	switch d.Algorithm() {
	case digest.SHA256:
		algPrefix = sha256DigestAlgorithmPrefix
	case digest.SHA512:
		algPrefix = sha512DigestAlgorithmPrefix
	default:
		return "", fmt.Errorf("unknown algorithm %q", d.Algorithm())
	}

	return Digest(fmt.Sprintf("%s%s", algPrefix, d.Hex())), nil
}

// Parse maps a Digest to a digest.Digest.
func (d Digest) Parse() (digest.Digest, error) {
	str := d.String()
	if len(str) == 0 {
		return "", errors.New("empty digest")
	}
	if len(str) < 2 {
		return "", errors.New("invalid digest length")
	}
	algPrefix := str[:2]
	if len(str) == 2 {
		return "", errors.New("no checksum")
	}

	var alg digest.Algorithm
	switch algPrefix {
	case sha256DigestAlgorithmPrefix:
		alg = digest.SHA256
	case sha512DigestAlgorithmPrefix:
		alg = digest.SHA512
	default:
		return "", fmt.Errorf("unknown algorithm prefix %q", algPrefix)
	}

	dgst := digest.NewDigestFromHex(alg.String(), str[2:])
	if err := dgst.Validate(); err != nil {
		return "", err
	}

	return dgst, nil
}

// HexDecode decodes binary data from a textual representation.
// The output is equivalent to the PostgreSQL binary `decode` function with the hex textual format. See
// https://www.postgresql.org/docs/14/functions-binarystring.html.
func (d Digest) HexDecode() string {
	return fmt.Sprintf("\\x%s", d.String())
}
