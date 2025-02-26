package handlers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var blobUploadStates = []blobUploadState{
	{
		Name:   "hello",
		UUID:   "abcd-1234-qwer-0987",
		Offset: 0,
	},
	{
		Name:   "hello-world",
		UUID:   "abcd-1234-qwer-0987",
		Offset: 0,
	},
	{
		Name:   "h3ll0_w0rld",
		UUID:   "abcd-1234-qwer-0987",
		Offset: 1337,
	},
	{
		Name:   "ABCDEFG",
		UUID:   "ABCD-1234-QWER-0987",
		Offset: 1234567890,
	},
	{
		Name:   "this-is-A-sort-of-Long-name-for-Testing",
		UUID:   "dead-1234-beef-0987",
		Offset: 8675309,
	},
}

var secrets = []string{
	"supersecret",
	"12345",
	"a",
	"SuperSecret",
	"Sup3r... S3cr3t!",
	"This is a reasonably long secret key that is used for the purpose of testing.",
	"\u2603+\u2744", // snowman+snowflake
}

// TestLayerUploadTokens constructs stateTokens from LayerUploadStates and
// validates that the tokens can be used to reconstruct the proper upload state.
func TestLayerUploadTokens(t *testing.T) {
	secret := hmacKey("supersecret")

	for _, testcase := range blobUploadStates {
		token, err := secret.packUploadState(testcase)
		require.NoError(t, err)

		lus, err := secret.unpackUploadState(token)
		require.NoError(t, err)

		assertBlobUploadStateEquals(t, testcase, lus)
	}
}

// TestHMACValidate ensures that any HMAC token providers are compatible if and
// only if they share the same secret.
func TestHMACValidation(t *testing.T) {
	for _, secret := range secrets {
		secret1 := hmacKey(secret)
		secret2 := hmacKey(secret)
		badSecret := hmacKey("DifferentSecret")

		for _, testcase := range blobUploadStates {
			token, err := secret1.packUploadState(testcase)
			require.NoError(t, err)

			lus, err := secret2.unpackUploadState(token)
			require.NoError(t, err)

			assertBlobUploadStateEquals(t, testcase, lus)

			_, err = badSecret.unpackUploadState(token)
			require.Error(t, err, "expected token provider to fail at retrieving state from token: %s", token)

			badToken, err := badSecret.packUploadState(lus)
			require.NoError(t, err)

			_, err = secret1.unpackUploadState(badToken)
			require.Error(t, err, "expected token provider to fail at retrieving state from token: %s", badToken)

			_, err = secret2.unpackUploadState(badToken)
			require.Error(t, err, "expected token provider to fail at retrieving state from token: %s", badToken)
		}
	}
}

func assertBlobUploadStateEquals(t *testing.T, expected, received blobUploadState) {
	assert.Equal(t, expected.Name, received.Name)
	assert.Equal(t, expected.UUID, received.UUID)
	assert.Equal(t, expected.Offset, received.Offset)
}
