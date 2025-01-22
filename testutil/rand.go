package testutil

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

type SeedT [32]byte

func (s SeedT) String() string {
	var res string

	for i, byte := range s {
		if i < len(s)-1 {
			res += fmt.Sprintf("%02x:", byte)
		} else {
			res += fmt.Sprintf("%02x\n", byte) // no colon after last byte
		}
	}

	return res
}

func ChaChaSeed() (SeedT, error) {
	var seed SeedT
	// Number of bytes is always equal to the slice size if the error did not
	// occur.
	if _, err := rand.Read(seed[:]); err != nil {
		return SeedT{}, fmt.Errorf("generating random bytes: %w", err)
	}

	return seed, nil
}

func MustChaChaSeed(tb testing.TB) SeedT {
	tb.Helper()

	seed, err := ChaChaSeed()
	require.NoError(tb, err)
	// The text will be printed only if the test fails or the -test.v flag is
	// set. Meant to enable reproducibility in our tests.
	tb.Logf("using rng seed %v", seed)
	return seed
}

// SeedFromUnixNano generates a 32-byte array from a 64-bit seed (e.g., UnixNano()).
// It extracts the bytes from the seed, then fills a 32-byte buffer by repeating the seed's byte representation in 8-byte chunks.
func SeedFromUnixNano(seed int64) [32]byte {
	word := [8]uint8{}
	for i := 0; i < 8; i++ {
		word[i] = uint8(seed) //nolint
		seed >>= 8
	}
	buf := [32]byte{}
	for i := 0; i < 32; i += 8 {
		copy(buf[i:], word[:])
	}
	return buf
}
