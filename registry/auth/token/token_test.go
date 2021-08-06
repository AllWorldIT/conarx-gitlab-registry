package token

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/docker/libtrust"
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

func makeTestToken(issuer, audience string, access []*ResourceActions, rootKey libtrust.PrivateKey, depth int, now time.Time, exp time.Time) (*Token, error) {
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

	randomBytes := make([]byte, 15)
	if _, err = rand.Read(randomBytes); err != nil {
		return nil, fmt.Errorf("unable to read random bytes for jwt id: %s", err)
	}

	claimSet := &ClaimSet{
		Issuer:     issuer,
		Subject:    "foo",
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
	if err != nil {
		t.Fatal(err)
	}

	rootCerts, err := makeRootCerts(rootKeys)
	if err != nil {
		t.Fatal(err)
	}

	rootPool := x509.NewCertPool()
	for _, rootCert := range rootCerts {
		rootPool.AddCert(rootCert)
	}

	trustedKeys := makeTrustedKeyMap(rootKeys)

	tokens := make([]*Token, 0, numTokens)

	for i := 0; i < numTokens; i++ {
		token, err := makeTestToken(issuer, audience, access, rootKeys[i], i, time.Now(), time.Now().Add(5*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}

	verifyOps := VerifyOptions{
		TrustedIssuers:    []string{issuer},
		AcceptedAudiences: []string{audience},
		Roots:             rootPool,
		TrustedKeys:       trustedKeys,
	}

	for _, token := range tokens {
		if err := token.Verify(verifyOps); err != nil {
			t.Fatal(err)
		}
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
	if err != nil {
		t.Fatal(err)
	}

	trustedKeys := makeTrustedKeyMap(rootKeys)

	verifyOps := VerifyOptions{
		TrustedIssuers:    []string{issuer},
		AcceptedAudiences: []string{audience},
		Roots:             nil,
		TrustedKeys:       trustedKeys,
	}

	// nbf verification should pass within leeway
	futureNow := time.Now().Add(time.Duration(5) * time.Second)
	token, err := makeTestToken(issuer, audience, access, rootKeys[0], 0, futureNow, futureNow.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if err := token.Verify(verifyOps); err != nil {
		t.Fatal(err)
	}

	// nbf verification should fail with a skew larger than leeway
	futureNow = time.Now().Add(time.Duration(61) * time.Second)
	token, err = makeTestToken(issuer, audience, access, rootKeys[0], 0, futureNow, futureNow.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	if err = token.Verify(verifyOps); err == nil {
		t.Fatal("Verification should fail for token with nbf in the future outside leeway")
	}

	// exp verification should pass within leeway
	token, err = makeTestToken(issuer, audience, access, rootKeys[0], 0, time.Now(), time.Now().Add(-59*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if err = token.Verify(verifyOps); err != nil {
		t.Fatal(err)
	}

	// exp verification should fail with a skew larger than leeway
	token, err = makeTestToken(issuer, audience, access, rootKeys[0], 0, time.Now(), time.Now().Add(-60*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	if err = token.Verify(verifyOps); err == nil {
		t.Fatal("Verification should fail for token with exp in the future outside leeway")
	}
}

func writeTempRootCerts(rootKeys []libtrust.PrivateKey) (filename string, err error) {
	rootCerts, err := makeRootCerts(rootKeys)
	if err != nil {
		return "", err
	}

	tempFile, err := ioutil.TempFile("", "rootCertBundle")
	if err != nil {
		return "", err
	}
	defer tempFile.Close()

	for _, cert := range rootCerts {
		if err = pem.Encode(tempFile, &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert.Raw,
		}); err != nil {
			os.Remove(tempFile.Name())
			return "", err
		}
	}

	return tempFile.Name(), nil
}

// TestAccessController tests complete integration of the token auth package.
// It starts by mocking the options for a token auth accessController which
// it creates. It then tries a few mock requests:
// 		- don't supply a token; should error with challenge
//		- supply an invalid token; should error with challenge
// 		- supply a token with insufficient access; should error with challenge
//		- supply a valid token; should not error
func TestAccessController(t *testing.T) {
	// Make 2 keys; only the first is to be a trusted root key.
	rootKeys, err := makeRootKeys(2)
	if err != nil {
		t.Fatal(err)
	}

	rootCertBundleFilename, err := writeTempRootCerts(rootKeys[:1])
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(rootCertBundleFilename)

	realm := "https://auth.example.com/token/"
	issuer := "test-issuer.example.com"
	service := "test-service.example.com"

	options := map[string]interface{}{
		"realm":          realm,
		"issuer":         issuer,
		"service":        service,
		"rootcertbundle": rootCertBundleFilename,
		"autoredirect":   false,
	}

	accessController, err := newAccessController(options)
	if err != nil {
		t.Fatal(err)
	}

	// 1. Make a mock http.Request with no token.
	req, err := http.NewRequest("GET", "http://example.com/foo", nil)
	if err != nil {
		t.Fatal(err)
	}

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
	if !ok {
		t.Fatal("accessController did not return a challenge")
	}

	if challenge.Error() != ErrTokenRequired.Error() {
		t.Fatalf("accessControler did not get expected error - got %s - expected %s", challenge, ErrTokenRequired)
	}

	if authCtx != nil {
		t.Fatalf("expected nil auth context but got %s", authCtx)
	}

	// 2. Supply an invalid token.
	token, err := makeTestToken(
		issuer, service,
		[]*ResourceActions{{
			Type:    testAccess.Type,
			Name:    testAccess.Name,
			Actions: []string{testAccess.Action},
		}},
		rootKeys[1], 1, time.Now(), time.Now().Add(5*time.Minute), // Everything is valid except the key which signed it.
	)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err = accessController.Authorized(ctx, testAccess)
	challenge, ok = err.(auth.Challenge)
	if !ok {
		t.Fatal("accessController did not return a challenge")
	}

	if challenge.Error() != ErrInvalidToken.Error() {
		t.Fatalf("accessControler did not get expected error - got %s - expected %s", challenge, ErrTokenRequired)
	}

	if authCtx != nil {
		t.Fatalf("expected nil auth context but got %s", authCtx)
	}

	// 3. Supply a token with insufficient access.
	token, err = makeTestToken(
		issuer, service,
		[]*ResourceActions{}, // No access specified.
		rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err = accessController.Authorized(ctx, testAccess)
	challenge, ok = err.(auth.Challenge)
	if !ok {
		t.Fatal("accessController did not return a challenge")
	}

	if challenge.Error() != ErrInsufficientScope.Error() {
		t.Fatalf("accessControler did not get expected error - got %s - expected %s", challenge, ErrInsufficientScope)
	}

	if authCtx != nil {
		t.Fatalf("expected nil auth context but got %s", authCtx)
	}

	// 4. Supply the token we need, or deserve, or whatever.
	token, err = makeTestToken(
		issuer, service,
		[]*ResourceActions{{
			Type:    testAccess.Type,
			Name:    testAccess.Name,
			Actions: []string{testAccess.Action},
		}},
		rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err = accessController.Authorized(ctx, testAccess)
	if err != nil {
		t.Fatalf("accessController returned unexpected error: %s", err)
	}

	userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
	if !ok {
		t.Fatal("token accessController did not set auth.user context")
	}

	if userInfo.Name != "foo" {
		t.Fatalf("expected user name %q, got %q", "foo", userInfo.Name)
	}

	// 5. Supply a token with full admin rights, which is represented as "*".
	token, err = makeTestToken(
		issuer, service,
		[]*ResourceActions{{
			Type:    testAccess.Type,
			Name:    testAccess.Name,
			Actions: []string{"*"},
		}},
		rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	_, err = accessController.Authorized(ctx, testAccess)
	if err != nil {
		t.Fatalf("accessController returned unexpected error: %s", err)
	}
}

type migrationTest struct {
	name string
	flag *bool
}

func newTestMigrationAuthContext(t *testing.T, ctx context.Context, req *http.Request, actions []*ResourceActions, access ...auth.Access) context.Context {
	t.Helper()

	rootKeys, err := makeRootKeys(1)
	require.NoError(t, err)

	rootCertBundleFilename, err := writeTempRootCerts(rootKeys)
	require.NoError(t, err)
	t.Cleanup(func() { os.Remove(rootCertBundleFilename) })

	testRealm := "https://gitlab.com/jwt/auth"
	testIssuer := "omnibus-gitlab-issuer"
	testService := "container_registry"

	options := map[string]interface{}{
		"realm":          testRealm,
		"issuer":         testIssuer,
		"service":        testService,
		"rootcertbundle": rootCertBundleFilename,
		"autoredirect":   false,
	}

	accessController, err := newAccessController(options)
	require.NoError(t, err)

	token, err := makeTestToken(
		testIssuer, testService, actions, rootKeys[0], 1, time.Now(), time.Now().Add(5*time.Minute),
	)
	require.NoError(t, err)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.compactRaw()))

	authCtx, err := accessController.Authorized(ctx, access...)
	require.NoError(t, err)

	return authCtx
}

func TestAccessController_Migration_ReadRequest(t *testing.T) {
	req, err := http.NewRequest("GET", "https://registry.gitlab.com/v2/myrepo/tags/list", nil)
	require.NoError(t, err)
	ctx := dcontext.WithRequest(dcontext.Background(), req)

	access := auth.Access{
		Resource: auth.Resource{
			Type: "repository",
			Name: "myrepo",
		},
		Action: "pull",
	}

	trueVal := true
	falseVal := false
	tt := []migrationTest{
		{name: "no flag", flag: nil},
		{name: "false flag", flag: &falseVal},
		{name: "true flag", flag: &trueVal},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			actions := []*ResourceActions{{
				Type:              access.Type,
				Name:              access.Name,
				Actions:           []string{access.Action},
				MigrationEligible: test.flag,
			}}
			authCtx := newTestMigrationAuthContext(t, ctx, req, actions, access)

			if test.flag != nil {
				require.True(t, migration.HasEligibilityFlag(authCtx))
				require.Equal(t, *test.flag, migration.IsEligible(authCtx))
			} else {
				require.False(t, migration.HasEligibilityFlag(authCtx))
			}

			// ensure user info was not affected and is still accessible
			userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
			require.True(t, ok)
			require.Equal(t, "foo", userInfo.Name)
		})
	}
}

func TestAccessController_Migration_WriteRequest(t *testing.T) {
	req, err := http.NewRequest("POST", "https://registry.gitlab.com/v2/myrepo/blobs/uploads/", nil)
	require.NoError(t, err)
	ctx := dcontext.WithRequest(dcontext.Background(), req)

	access := auth.Access{
		Resource: auth.Resource{
			Type: "repository",
			Name: "myrepo",
		},
		Action: "push",
	}

	trueVal := true
	falseVal := false
	tt := []migrationTest{
		{name: "no flag", flag: nil},
		{name: "false flag", flag: &falseVal},
		{name: "true flag", flag: &trueVal},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			actions := []*ResourceActions{{
				Type:              access.Type,
				Name:              access.Name,
				Actions:           []string{access.Action},
				MigrationEligible: test.flag,
			}}

			authCtx := newTestMigrationAuthContext(t, ctx, req, actions, access)

			if test.flag != nil {
				require.True(t, migration.HasEligibilityFlag(authCtx))
				require.Equal(t, *test.flag, migration.IsEligible(authCtx))
			} else {
				require.False(t, migration.HasEligibilityFlag(authCtx))
			}

			// ensure user info was not affected and is still accessible
			userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
			require.True(t, ok)
			require.Equal(t, "foo", userInfo.Name)
		})
	}
}

func TestAccessController_Migration_DeleteRequest(t *testing.T) {
	req, err := http.NewRequest("DELETE", "https://registry.gitlab.com/v2/myrepo/tags/reference/latest", nil)
	require.NoError(t, err)
	ctx := dcontext.WithRequest(dcontext.Background(), req)

	access := auth.Access{
		Resource: auth.Resource{
			Type: "repository",
			Name: "myrepo",
		},
		Action: "delete",
	}

	trueVal := true
	falseVal := false
	tt := []migrationTest{
		{name: "no flag", flag: nil},
		{name: "false flag", flag: &falseVal},
		{name: "true flag", flag: &trueVal},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			actions := []*ResourceActions{{
				Type:              access.Type,
				Name:              access.Name,
				Actions:           []string{access.Action},
				MigrationEligible: test.flag,
			}}

			authCtx := newTestMigrationAuthContext(t, ctx, req, actions, access)

			if test.flag != nil {
				require.True(t, migration.HasEligibilityFlag(authCtx))
				require.Equal(t, *test.flag, migration.IsEligible(authCtx))
			} else {
				require.False(t, migration.HasEligibilityFlag(authCtx))
			}

			// ensure user info was not affected and is still accessible
			userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
			require.True(t, ok)
			require.Equal(t, "foo", userInfo.Name)
		})
	}
}

func TestAccessController_Migration_WriteRequest_MultipleAccess(t *testing.T) {
	url := "https://registry.gitlab.com/v2/myrepo-b/blobs/uploads/?mount=sha256:f72c9c3fbe478eac9b924b6bec463b6b1e0a5ebcd902085662a29f8a20df327d&from=myrepo-a"
	req, err := http.NewRequest("POST", url, nil)
	require.NoError(t, err)
	ctx := dcontext.WithRequest(dcontext.Background(), req)

	destPullAccess := auth.Access{
		Resource: auth.Resource{
			Type: "repository",
			Name: "myrepo-b",
		},
		Action: "pull",
	}
	destPushAccess := destPullAccess
	destPushAccess.Action = "push"

	srcPullAccess := auth.Access{
		Resource: auth.Resource{
			Type: "repository",
			Name: "myrepo-a",
		},
		Action: "pull",
	}

	trueVal := true
	falseVal := false
	tt := []migrationTest{
		{name: "no flag", flag: nil},
		{name: "false flag", flag: &falseVal},
		{name: "true flag", flag: &trueVal},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			actions := []*ResourceActions{
				{
					Type:    srcPullAccess.Type,
					Name:    srcPullAccess.Name,
					Actions: []string{srcPullAccess.Action},
				},
				{
					Type:              destPushAccess.Type,
					Name:              destPushAccess.Name,
					Actions:           []string{destPullAccess.Action, destPushAccess.Action},
					MigrationEligible: test.flag,
				},
			}

			authCtx := newTestMigrationAuthContext(t, ctx, req, actions, srcPullAccess, destPullAccess, destPushAccess)

			if test.flag != nil {
				require.True(t, migration.HasEligibilityFlag(authCtx))
				require.Equal(t, *test.flag, migration.IsEligible(authCtx))
			} else {
				require.False(t, migration.HasEligibilityFlag(authCtx))
			}

			// ensure user info was not affected and is still accessible
			userInfo, ok := authCtx.Value(auth.UserKey).(auth.UserInfo)
			require.True(t, ok)
			require.Equal(t, "foo", userInfo.Name)
		})
	}
}

// This tests that newAccessController can handle PEM blocks in the certificate
// file other than certificates, for example a private key.
func TestNewAccessControllerPemBlock(t *testing.T) {
	rootKeys, err := makeRootKeys(2)
	if err != nil {
		t.Fatal(err)
	}

	rootCertBundleFilename, err := writeTempRootCerts(rootKeys)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(rootCertBundleFilename)

	// Add something other than a certificate to the rootcertbundle
	file, err := os.OpenFile(rootCertBundleFilename, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		t.Fatal(err)
	}
	keyBlock, err := rootKeys[0].PEMBlock()
	if err != nil {
		t.Fatal(err)
	}
	err = pem.Encode(file, keyBlock)
	if err != nil {
		t.Fatal(err)
	}
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	realm := "https://auth.example.com/token/"
	issuer := "test-issuer.example.com"
	service := "test-service.example.com"

	options := map[string]interface{}{
		"realm":          realm,
		"issuer":         issuer,
		"service":        service,
		"rootcertbundle": rootCertBundleFilename,
		"autoredirect":   false,
	}

	ac, err := newAccessController(options)
	if err != nil {
		t.Fatal(err)
	}

	if len(ac.(*accessController).rootCerts.Subjects()) != 2 {
		t.Fatal("accessController has the wrong number of certificates")
	}
}
