package schema1

import (
	"encoding/json"
	"testing"

	"github.com/docker/libtrust"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEnv struct {
	name, tag     string
	invalidSigned *SignedManifest
	signed        *SignedManifest
	pk            libtrust.PrivateKey
}

func TestManifestMarshaling(t *testing.T) {
	env := genEnv(t)

	// Check that the all field is the same as json.MarshalIndent with these
	// parameters.
	p, err := json.MarshalIndent(env.signed, "", "   ")
	require.NoError(t, err, "error marshaling manifest")

	require.Equal(t, p, env.signed.all)
}

func TestManifestUnmarshaling(t *testing.T) {
	env := genEnv(t)

	var signed SignedManifest
	require.NoError(t, json.Unmarshal(env.signed.all, &signed), "error unmarshaling signed manifest")

	require.Equal(t, signed, *env.signed, "manifests are different after unmarshaling")
}

func TestManifestVerification(t *testing.T) {
	env := genEnv(t)

	publicKeys, err := Verify(env.signed)
	require.NoError(t, err, "error verifying manifest")
	require.NotEmpty(t, publicKeys, "no public keys found in signature")

	var found bool
	publicKey := env.pk.PublicKey()
	// ensure that one of the extracted public keys matches the private key.
	for _, candidate := range publicKeys {
		if candidate.KeyID() == publicKey.KeyID() {
			found = true
			break
		}
	}

	assert.True(t, found, "expected public key, %v, not found in verified keys: %v", publicKey, publicKeys)

	// Check that an invalid manifest fails verification
	_, err = Verify(env.invalidSigned)
	assert.NoError(t, err, "Invalid manifest should not pass Verify()")
}

func genEnv(t *testing.T) *testEnv {
	pk, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "error generating test key")

	name, tag := "foo/bar", "test"

	invalid := Manifest{
		Versioned: SchemaVersion,
		Name:      name,
		Tag:       tag,
		FSLayers: []FSLayer{
			{
				BlobSum: "asdf",
			},
			{
				BlobSum: "qwer",
			},
		},
	}

	valid := Manifest{
		Versioned: SchemaVersion,
		Name:      name,
		Tag:       tag,
		FSLayers: []FSLayer{
			{
				BlobSum: "asdf",
			},
		},
		History: []History{
			{
				V1Compatibility: "",
			},
		},
	}

	sm, err := Sign(&valid, pk)
	require.NoError(t, err, "error signing manifest")

	invalidSigned, err := Sign(&invalid, pk)
	require.NoError(t, err, "error signing manifest")

	return &testEnv{
		name:          name,
		tag:           tag,
		invalidSigned: invalidSigned,
		signed:        sm,
		pk:            pk,
	}
}
