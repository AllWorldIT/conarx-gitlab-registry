package token

import (
	"context"
	"crypto"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/testutil"
	"github.com/docker/libtrust"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeRootKeys(numKeys int) ([]libtrust.PrivateKey, error) {
	keys := make([]libtrust.PrivateKey, 0, numKeys)

	for i := 0; i < numKeys; i++ {
		key, err := libtrust.GenerateECP256PrivateKey()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, nil
}

func makeSigningKeyWithChain(rootKey libtrust.PrivateKey, depth int) (libtrust.PrivateKey, error) {
	if depth == 0 {
		// Don't need to build a chain.
		return rootKey, nil
	}

	var (
		x5c       = make([]string, depth)
		parentKey = rootKey
		key       libtrust.PrivateKey
		cert      *x509.Certificate
		err       error
	)

	for depth > 0 {
		if key, err = libtrust.GenerateECP256PrivateKey(); err != nil {
			return nil, err
		}

		if cert, err = libtrust.GenerateCACert(parentKey, key); err != nil {
			return nil, err
		}

		depth--
		x5c[depth] = base64.StdEncoding.EncodeToString(cert.Raw)
		parentKey = key
	}

	key.AddExtendedField("x5c", x5c)

	return key, nil
}

func makeRootCerts(rootKeys []libtrust.PrivateKey) ([]*x509.Certificate, error) {
	certs := make([]*x509.Certificate, 0, len(rootKeys))

	for _, key := range rootKeys {
		cert, err := libtrust.GenerateCACert(key, key)
		if err != nil {
			return nil, err
		}
		certs = append(certs, cert)
	}

	return certs, nil
}

func makeTrustedKeyMap(rootKeys []libtrust.PrivateKey) map[string]libtrust.PublicKey {
	trustedKeys := make(map[string]libtrust.PublicKey, len(rootKeys))

	for _, key := range rootKeys {
		trustedKeys[key.KeyID()] = key.PublicKey()
	}

	return trustedKeys
}

func makeTestToken(tb testing.TB, issuer, audience string, access []*ResourceActions, rootKey libtrust.PrivateKey, depth int, now, exp time.Time) (*Token, error) {
	tb.Helper()

	signingKey, err := makeSigningKeyWithChain(rootKey, depth)
	if err != nil {
		return nil, fmt.Errorf("unable to make signing key with chain: %s", err)
	}

	var rawJWK json.RawMessage
	rawJWK, err = signingKey.PublicKey().MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("unable to marshal signing key to JSON: %s", err)
	}

	joseHeader := &Header{
		Type:       "JWT",
		SigningAlg: "ES256",
		RawJWK:     &rawJWK,
	}

	randomBytes := testutil.RandomBlob(tb, 15)

	claimSet := &ClaimSet{
		Issuer:     issuer,
		Subject:    "foo",
		AuthType:   "bar",
		Audience:   audience,
		Expiration: exp.Unix(),
		NotBefore:  now.Unix(),
		IssuedAt:   now.Unix(),
		JWTID:      base64.URLEncoding.EncodeToString(randomBytes),
		Access:     access,
	}

	var joseHeaderBytes, claimSetBytes []byte

	if joseHeaderBytes, err = json.Marshal(joseHeader); err != nil {
		return nil, fmt.Errorf("unable to marshal jose header: %s", err)
	}
	if claimSetBytes, err = json.Marshal(claimSet); err != nil {
		return nil, fmt.Errorf("unable to marshal claim set: %s", err)
	}

	encodedJoseHeader := joseBase64UrlEncode(joseHeaderBytes)
	encodedClaimSet := joseBase64UrlEncode(claimSetBytes)
	encodingToSign := fmt.Sprintf("%s.%s", encodedJoseHeader, encodedClaimSet)

	var signatureBytes []byte
	if signatureBytes, _, err = signingKey.Sign(strings.NewReader(encodingToSign), crypto.SHA256); err != nil {
		return nil, fmt.Errorf("unable to sign jwt payload: %s", err)
	}

	signature := joseBase64UrlEncode(signatureBytes)
	tokenString := fmt.Sprintf("%s.%s", encodingToSign, signature)

	return NewToken(tokenString)
}

// This test makes 4 tokens with a varying number of intermediate
// certificates ranging from no intermediate chain to a length of 3
// intermediates.
func TestTokenVerify(t *testing.T) {
	var (
		numTokens = 4
		issuer    = "test-issuer"
		audience  = "test-audience"
		access    = []*ResourceActions{
			{
				Type:    "repository",
				Name:    "foo/bar",
				Actions: []string{"pull", "push"},
			},
		}
	)

	rootKeys, err := makeRootKeys(numTokens)
	require.NoError(t, err)

	rootCerts, err := makeRootCerts(rootKeys)
	require.NoError(t, err)

	rootPool := x509.NewCertPool()
	for _, rootCert := range rootCerts {
		rootPool.AddCert(rootCert)
	}

	trustedKeys := makeTrustedKeyMap(rootKeys)

	tokens := make([]*Token, 0, numTokens)

	for i := 0; i < numTokens; i++ {
		token, err := makeTestToken(t, issuer, audience, access, rootKeys[i], i, time.Now(), time.Now().Add(5*time.Minute))
		require.NoError(t, err)
		tokens = append(tokens, token)
	}

	verifyOps := VerifyOptions{
		TrustedIssuers:    []string{issuer},
		AcceptedAudiences: []string{audience},
		Roots:             rootPool,
		TrustedKeys:       trustedKeys,
	}

	for _, token := range tokens {
		assert.NoError(t, token.Verify(verifyOps))
	}
}

// This tests that we don't fail tokens with nbf within
// the defined leeway in seconds
func TestLeeway(t *testing.T) {
	var (
		issuer   = "test-issuer"
		audience = "test-audience"
		access   = []*ResourceActions{
			{
				Type:    "repository",
				Name:    "foo/bar",
				Actions: []string{"pull", "push"},
			},
		}
	)

	rootKeys, err := makeRootKeys(1)
	require.NoError(t, err)

	trustedKeys := makeTrustedKeyMap(rootKeys)

	verifyOps := VerifyOptions{
		TrustedIssuers:    []string{issuer},
		AcceptedAudiences: []string{audience},
		Roots:             nil,
		TrustedKeys:       trustedKeys,
	}

	// nbf verification should pass within leeway
	futureNow := time.Now().Add(time.Duration(5) * time.Second)
	token, err := makeTestToken(t, issuer, audience, access, rootKeys[0], 0, futureNow, futureNow.Add(5*time.Minute))
	require.NoError(t, err)

	// nolint: testifylint // require-error
	assert.NoError(t, token.Verify(verifyOps))

	// nbf verification should fail with a skew larger than leeway
	futureNow = time.Now().Add(time.Duration(61) * time.Second)
	token, err = makeTestToken(t, issuer, audience, access, rootKeys[0], 0, futureNow, futureNow.Add(5*time.Minute))
	require.NoError(t, err)

	// nolint: testifylint // require-error
	assert.Error(t, token.Verify(verifyOps), "verification should fail for token with nbf in the future outside leeway")

	// exp verification should pass within leeway
	token, err = makeTestToken(t, issuer, audience, access, rootKeys[0], 0, time.Now(), time.Now().Add(-59*time.Second))
	require.NoError(t, err)

	// nolint: testifylint // require-error
	assert.NoError(t, token.Verify(verifyOps))

	// exp verification should fail with a skew larger than leeway
	token, err = makeTestToken(t, issuer, audience, access, rootKeys[0], 0, time.Now(), time.Now().Add(-60*time.Second))
	require.NoError(t, err)

	// nolint: testifylint // require-error
	assert.Error(t, token.Verify(verifyOps), "verification should fail for token with exp in the future outside leeway")
}

func writeTempRootCerts(rootKeys []libtrust.PrivateKey) (filename string, err error) {
	rootCerts, err := makeRootCerts(rootKeys)
	if err != nil {
		return "", err
	}

	tempFile, err := os.CreateTemp("", "rootCertBundle")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	for _, cert := range rootCerts {
		if err = pem.Encode(tempFile, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		}); err != nil {
			_ = os.Remove(tempFile.Name())
			return "", err
		}
	}

	return tempFile.Name(), nil
}

// TestAccessController tests complete integration of the token auth package.
// It starts by mocking the options for a token auth accessController which
// it creates. It then tries a few mock requests:
//   - don't supply a token; should error with challenge
//   - supply an invalid token; should error with challenge
//   - supply a token with insufficient access; should error with challenge
//   - supply a valid token; should not error
func TestAccessController(t *testing.T) {
	// Make 2 keys; only the first is to be a trusted root key.
	rootKeys, err := makeRootKeys(2)
	require.NoError(t, err)

	rootCertBundleFilename, err := writeTempRootCerts(rootKeys[:1])
	require.NoError(t, err)
	defer os.Remove(rootCertBundleFilename)

	realm := "https://auth.example.com/token/"
	issuer := "test-issuer.example.com"
	service := "test-service.example.com"

	options := map[string]any{
		"realm":          realm,
		"issuer":         issuer,
		"service":        service,
		"rootcertbundle": rootCertBundleFilename,
		"autoredirect":   false,
	}

	accessController, err := newAccessController(options)
	require.NoError(t, err)

	// 1. Make a mock http.Request with no token.
	req, err := http.NewRequest(http.MethodGet, "http://example.com/foo", nil)
	require.NoError(t, err)

	testAccess := auth.Access{
		Resource: auth.Resource{
			Type: "foo",
			Name: "bar",
		},
		Action: "baz",
	}

	ctx := dcontext.WithRequest(context.Background(), req)
	authCtx, err := accessController.Authorized(ctx, testAccess)
	challenge, ok := err.(auth.Challenge)
	assert.True(t, ok, "accessController did not return a challenge")

	assert.Equal(t, ErrTokenRequired.Error(), challenge.Error())

	assert.Nil(t, authCtx, "expected nil auth context")

	// 2. Supply an invalid token.
	token, err := makeTestToken(t,
		issuer, service,
		[]*ResourceActions{{
			Type:    testAccess.Type,
			Name:    testAccess.Name,
			Actions: []string{testAccess.Action},
		}},
		rootKeys[1], 1, time.Now(), time.Now().Add(5*time.Minute), // Everything is valid except the key which signed it.
	)
	require.NoError(t, err)

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err = accessController.Authorized(ctx, testAccess)
	challenge, ok = err.(auth.Challenge)
	assert.True(t, ok, "accessController did not return a challenge")

	assert.Equal(t, ErrInvalidToken.Error(), challenge.Error())

	assert.Nil(t, authCtx, "expected nil auth context")

	// 3. Supply a token with insufficient access.
	token, err = makeTestToken(t,
		issuer, service,
		make([]*ResourceActions, 0), // No access specified.
		rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	require.NoError(t, err)

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err = accessController.Authorized(ctx, testAccess)
	challenge, ok = err.(auth.Challenge)
	assert.True(t, ok, "accessController did not return a challenge")

	assert.Equal(t, ErrInsufficientScope.Error(), challenge.Error())

	assert.Nil(t, authCtx, "expected nil auth context")

	// 4. Supply the token we need, or deserve, or whatever.
	token, err = makeTestToken(t,
		issuer, service,
		[]*ResourceActions{{
			Type:    testAccess.Type,
			Name:    testAccess.Name,
			Actions: []string{testAccess.Action},
		}},
		rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	require.NoError(t, err)

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err = accessController.Authorized(ctx, testAccess)
	require.NoError(t, err)

	userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
	assert.True(t, ok, "token accessController did not set auth.user context")

	assert.Equal(t, "foo", userInfo.Name)

	assert.Equal(t, "bar", userInfo.Type)

	// 5. Supply a token with full admin rights, which is represented as "*".
	token, err = makeTestToken(t,
		issuer, service,
		[]*ResourceActions{{
			Type:    testAccess.Type,
			Name:    testAccess.Name,
			Actions: []string{"*"},
		}},
		rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	require.NoError(t, err)

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	_, err = accessController.Authorized(ctx, testAccess)
	require.NoError(t, err, "accessController returned unexpected error")
}

// This tests that newAccessController can handle PEM blocks in the certificate
// file other than certificates, for example a private key.
func TestNewAccessControllerPemBlock(t *testing.T) {
	rootKeys, err := makeRootKeys(2)
	require.NoError(t, err)

	rootCertBundleFilename, err := writeTempRootCerts(rootKeys)
	require.NoError(t, err)
	defer os.Remove(rootCertBundleFilename)

	// Add something other than a certificate to the rootcertbundle
	file, err := os.OpenFile(rootCertBundleFilename, os.O_WRONLY|os.O_APPEND, 0o666)
	require.NoError(t, err)
	keyBlock, err := rootKeys[0].PEMBlock()
	require.NoError(t, err)
	err = pem.Encode(file, keyBlock)
	require.NoError(t, err)
	err = file.Close()
	require.NoError(t, err)

	realm := "https://auth.example.com/token/"
	issuer := "test-issuer.example.com"
	service := "test-service.example.com"

	options := map[string]any{
		"realm":          realm,
		"issuer":         issuer,
		"service":        service,
		"rootcertbundle": rootCertBundleFilename,
		"autoredirect":   false,
	}

	ac, err := newAccessController(options)
	require.NoError(t, err)

	// nolint: staticcheck // needs more thorought investigation and fix
	require.Len(t, ac.(*accessController).rootCerts.Subjects(), 2, "accessController has the wrong number of certificates")
}

// TestAccessController_Meta tests that the meta data is correctly unmarshalled
// from the JWT into the context (if it exist) and that the meta data is retreivable from the context.
func TestAccessController_Meta(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "https://registry.gitlab.com/v2/myrepo/", nil)
	require.NoError(t, err)
	ctx := dcontext.WithRequest(dcontext.Background(), req)

	access := auth.Access{
		Resource: auth.Resource{
			Type: "repository",
			Name: "myrepo",
		},
		Action: "pull",
	}

	tests := []struct {
		name                 string
		meta                 []*Meta
		expectedProjectPaths []string
	}{
		{"no meta object", []*Meta{nil}, nil},
		{"one meta object with project", []*Meta{{ProjectPath: "foo/bar", ProjectID: int64(123), NamespaceID: int64(456)}}, []string{"foo/bar"}},
		{"multiple meta objects with projects", []*Meta{{ProjectPath: "foo/bar", ProjectID: int64(123), NamespaceID: int64(456)}, {ProjectPath: "bar/foo", ProjectID: int64(321), NamespaceID: int64(654)}}, []string{"foo/bar", "bar/foo"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var actions []*ResourceActions
			for _, meta := range test.meta {
				actions = append(actions, &ResourceActions{
					Type:    access.Type,
					Name:    access.Name,
					Meta:    meta,
					Actions: []string{access.Action},
				})
			}

			authCtx := newTestAuthContext(t, ctx, req, actions, access)

			// verify the meta value is correct
			actualProjectPaths, _ := authCtx.Value(auth.ResourceProjectPathsKey).([]string)
			require.ElementsMatch(t, test.expectedProjectPaths, actualProjectPaths)
		})
	}
}

// newTestAuthContext creates a valid JWT token with the requested access controls and passes it through the accesscontoller's `Authorized`
// in order to : assert the JWT is still valid (with a meta field embedded in it) AND to return a context with a possibly embedded meta object.
func newTestAuthContext(t *testing.T, ctx context.Context, req *http.Request, actions []*ResourceActions, access ...auth.Access) context.Context {
	rootKeys, err := makeRootKeys(1)
	require.NoError(t, err)

	rootCertBundleFilename, err := writeTempRootCerts(rootKeys)
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.Remove(rootCertBundleFilename) })

	testRealm := "https://gitlab.com/jwt/auth"
	testIssuer := "omnibus-gitlab-issuer"
	testService := "container_registry"

	options := map[string]any{
		"realm":          testRealm,
		"issuer":         testIssuer,
		"service":        testService,
		"rootcertbundle": rootCertBundleFilename,
		"autoredirect":   false,
	}

	accessController, err := newAccessController(options)
	require.NoError(t, err)

	token, err := makeTestToken(t,
		testIssuer, testService, actions, rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	require.NoError(t, err)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err := accessController.Authorized(ctx, access...)
	require.NoError(t, err)

	return authCtx
}
