package uuid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const iterations = 1000

func TestUUID4Generation(t *testing.T) {
	for i := 0; i < iterations; i++ {
		u := Generate()

		require.Equal(t, byte(0x40), u[6]&0xf0, "version byte not correctly set: %v, %08b %08b", u, u[6], u[6]&0xf0)
		require.Equal(t, byte(0x80), u[8]&0xc0, "top order 8th byte not correctly set: %v, %b", u, u[8])
	}
}

func TestParseAndEquality(t *testing.T) {
	for i := 0; i < iterations; i++ {
		u := Generate()

		parsed, err := Parse(u.String())
		require.NoError(t, err, "error parsing uuid %v", u)
		require.Equal(t, u, parsed, "parsing round trip failed")
	}

	for _, c := range []string{
		"bad",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",    // correct length, incorrect format
		"  20cc7775-2671-43c7-8742-51d1cfa23258",  // leading space
		"20cc7775-2671-43c7-8742-51d1cfa23258   ", // trailing space
		"00000000-0000-0000-0000-x00000000000",    // out of range character
	} {
		_, err := Parse(c)
		require.Error(t, err, "parsing %q should have failed", c)
	}
}
