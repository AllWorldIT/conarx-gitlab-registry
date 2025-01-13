package token

import (
	"crypto"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/docker/libtrust"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/auth"
)

const (
	// TokenSeparator is the value which separates the header, claims, and
	// signature in the compact serialization of a JSON Web Token.
	TokenSeparator = "."
	// Leeway is the Duration that will be added to NBF and EXP claim
	// checks to account for clock skew as per https://tools.ietf.org/html/rfc7519#section-4.1.5
	Leeway = 60 * time.Second
)

// Errors used by token parsing and verification.
var (
	ErrMalformedToken = errors.New("malformed token")
	ErrInvalidToken   = errors.New("invalid token")
)

// ResourceActions stores allowed actions on a named and typed resource.
type ResourceActions struct {
	Type    string   `json:"type"`
	Class   string   `json:"class,omitempty"`
	Name    string   `json:"name"`
	Actions []string `json:"actions"`
	Meta    *Meta    `json:"meta"`
}

// ClaimSet describes the main section of a JSON Web Token.
type ClaimSet struct {
	// Public claims
	Issuer     string `json:"iss"`
	Subject    string `json:"sub"`
	Audience   string `json:"aud"`
	Expiration int64  `json:"exp"`
	NotBefore  int64  `json:"nbf"`
	IssuedAt   int64  `json:"iat"`
	JWTID      string `json:"jti"`

	// Private claims
	Access   []*ResourceActions `json:"access"`
	AuthType string             `json:"auth_type"`
	// User is an encoded JWT that is used for tracking purposes
	// See https://gitlab.com/gitlab-org/container-registry/-/issues/1097
	User string `json:"user,omitempty"`
}

// Meta stores extra metadata available in the JSON Web Token passed by rails.
type Meta struct {
	// ProjectPath contains the full path of the GitLab project of a repository that a token was issued for.
	ProjectPath string `json:"project_path"`
	// ProjectID contains the GitLab project ID of a repository that a token was issued for.
	ProjectID int64 `json:"project_id"`
	// NamespaceID contains the GitLab root namespace ID of a repository that a token was issued for.
	NamespaceID int64 `json:"root_namespace_id"`
	// TagDenyAccessPatterns contains the patterns used to deny access to specific tags.
	TagDenyAccessPatterns *TagDenyAccessPatterns `json:"tag_deny_access_patterns"`
}

// TagDenyAccessPatterns stores the patterns used to deny access to specific tags for push and/or delete actions.
type TagDenyAccessPatterns struct {
	Push   []string `json:"push,omitempty"`
	Delete []string `json:"delete,omitempty"`
}

// Header describes the header section of a JSON Web Token.
type Header struct {
	Type       string           `json:"typ"`
	SigningAlg string           `json:"alg"`
	KeyID      string           `json:"kid,omitempty"`
	X5c        []string         `json:"x5c,omitempty"`
	RawJWK     *json.RawMessage `json:"jwk,omitempty"`
}

// Token describes a JSON Web Token.
type Token struct {
	Raw       string
	Header    *Header
	Claims    *ClaimSet
	Signature []byte
}

// VerifyOptions is used to specify
// options when verifying a JSON Web Token.
type VerifyOptions struct {
	TrustedIssuers    []string
	AcceptedAudiences []string
	Roots             *x509.CertPool
	TrustedKeys       map[string]libtrust.PublicKey
}

// NewToken parses the given raw token string
// and constructs an unverified JSON Web Token.
func NewToken(rawToken string) (*Token, error) {
	parts := strings.Split(rawToken, TokenSeparator)
	if len(parts) != 3 {
		return nil, ErrMalformedToken
	}

	var (
		rawHeader, rawClaims   = parts[0], parts[1]
		headerJSON, claimsJSON []byte
		err                    error
	)

	logger := log.GetLogger()

	defer func() {
		if err != nil {
			logger.WithError(err).Error("error while unmarshalling raw token")
		}
	}()

	if headerJSON, err = joseBase64UrlDecode(rawHeader); err != nil {
		logger.WithError(err).Error("unable to decode token header")
		return nil, ErrMalformedToken
	}

	if claimsJSON, err = joseBase64UrlDecode(rawClaims); err != nil {
		logger.WithError(err).Error("unable to decode token claims")
		return nil, ErrMalformedToken
	}

	token := new(Token)
	token.Header = new(Header)
	token.Claims = new(ClaimSet)

	token.Raw = strings.Join(parts[:2], TokenSeparator)
	if token.Signature, err = joseBase64UrlDecode(parts[2]); err != nil {
		logger.WithError(err).Error("unable to decode token signature")
		return nil, ErrMalformedToken
	}

	if err = json.Unmarshal(headerJSON, token.Header); err != nil {
		return nil, ErrMalformedToken
	}

	if err = json.Unmarshal(claimsJSON, token.Claims); err != nil {
		return nil, ErrMalformedToken
	}

	return token, nil
}

// Verify attempts to verify this token using the given options.
// Returns a nil error if the token is valid.
func (t *Token) Verify(verifyOpts VerifyOptions) error {
	logger := log.GetLogger()

	// Verify that the Issuer claim is a trusted authority.
	if !slices.Contains(verifyOpts.TrustedIssuers, t.Claims.Issuer) {
		logger.WithFields(log.Fields{"issuer": t.Claims.Issuer, "trusted_issuers": verifyOpts.TrustedIssuers}).
			Error("token from untrusted issuer")
		return ErrInvalidToken
	}

	// Verify that the Audience claim is allowed.
	if !slices.Contains(verifyOpts.AcceptedAudiences, t.Claims.Audience) {
		logger.WithFields(log.Fields{"audience": t.Claims.Audience, "accepted_audiences": verifyOpts.AcceptedAudiences}).
			Error("token intended for another audience")
		return ErrInvalidToken
	}

	// Verify that the token is currently usable and not expired.
	currentTime := time.Now()

	expWithLeeway := time.Unix(t.Claims.Expiration, 0).Add(Leeway)
	if currentTime.After(expWithLeeway) {
		logger.WithFields(log.Fields{"valid_until": expWithLeeway}).Warn("token has expired")
		return ErrInvalidToken
	}

	notBeforeWithLeeway := time.Unix(t.Claims.NotBefore, 0).Add(-Leeway)
	if currentTime.Before(notBeforeWithLeeway) {
		logger.WithFields(log.Fields{"valid_after": notBeforeWithLeeway}).Warn("token used before time")
		return ErrInvalidToken
	}

	// Verify the token signature.
	if len(t.Signature) == 0 {
		logger.Error("token has no signature")
		return ErrInvalidToken
	}

	// Verify that the signing key is trusted.
	signingKey, err := t.VerifySigningKey(verifyOpts)
	if err != nil {
		logger.WithError(err).Error("error verifying that signing key is trusted")
		return ErrInvalidToken
	}

	// Finally, verify the signature of the token using the key which signed it.
	if err := signingKey.Verify(strings.NewReader(t.Raw), t.Header.SigningAlg, t.Signature); err != nil {
		logger.WithError(err).Error("unable to verify token signature")
		return ErrInvalidToken
	}

	return nil
}

// VerifySigningKey attempts to get the key which was used to sign this token.
// The token header should contain either of these 3 fields:
//
//	`x5c` - The x509 certificate chain for the signing key. Needs to be
//	        verified.
//	`jwk` - The JSON Web Key representation of the signing key.
//	        May contain its own `x5c` field which needs to be verified.
//	`kid` - The unique identifier for the key. This library interprets it
//	        as a libtrust fingerprint. The key itself can be looked up in
//	        the trustedKeys field of the given verify options.
//
// Each of these methods are tried in that order of preference until the
// signing key is found or an error is returned.
func (t *Token) VerifySigningKey(verifyOpts VerifyOptions) (libtrust.PublicKey, error) {
	var signingKey libtrust.PublicKey
	var err error

	// First attempt to get an x509 certificate chain from the header.
	var (
		x5c    = t.Header.X5c
		rawJWK = t.Header.RawJWK
		keyID  = t.Header.KeyID
	)

	switch {
	case len(x5c) > 0:
		signingKey, err = parseAndVerifyCertChain(x5c, verifyOpts.Roots)
	case rawJWK != nil:
		signingKey, err = parseAndVerifyRawJWK(rawJWK, verifyOpts)
	case len(keyID) > 0:
		signingKey = verifyOpts.TrustedKeys[keyID]
		if signingKey == nil {
			err = fmt.Errorf("token signed by untrusted key with ID: %q", keyID)
		}
	default:
		err = errors.New("unable to get token signing key")
	}

	return signingKey, err
}

func parseAndVerifyCertChain(x5c []string, roots *x509.CertPool) (libtrust.PublicKey, error) {
	if len(x5c) == 0 {
		return nil, errors.New("empty x509 certificate chain")
	}

	// Ensure the first element is encoded correctly.
	leafCertDer, err := base64.StdEncoding.DecodeString(x5c[0])
	if err != nil {
		return nil, fmt.Errorf("unable to decode leaf certificate: %s", err)
	}

	// And that it is a valid x509 certificate.
	leafCert, err := x509.ParseCertificate(leafCertDer)
	if err != nil {
		return nil, fmt.Errorf("unable to parse leaf certificate: %s", err)
	}

	// The rest of the certificate chain are intermediate certificates.
	intermediates := x509.NewCertPool()
	for i := 1; i < len(x5c); i++ {
		intermediateCertDer, err := base64.StdEncoding.DecodeString(x5c[i])
		if err != nil {
			return nil, fmt.Errorf("unable to decode intermediate certificate: %s", err)
		}

		intermediateCert, err := x509.ParseCertificate(intermediateCertDer)
		if err != nil {
			return nil, fmt.Errorf("unable to parse intermediate certificate: %s", err)
		}

		intermediates.AddCert(intermediateCert)
	}

	verifyOpts := x509.VerifyOptions{
		Intermediates: intermediates,
		Roots:         roots,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	// TODO: this call returns certificate chains which we ignore for now, but
	// we should check them for revocations if we have the ability later.
	if _, err = leafCert.Verify(verifyOpts); err != nil {
		return nil, fmt.Errorf("unable to verify certificate chain: %s", err)
	}

	// Get the public key from the leaf certificate.
	leafCryptoKey, ok := leafCert.PublicKey.(crypto.PublicKey)
	if !ok {
		return nil, errors.New("unable to get leaf cert public key value")
	}

	leafKey, err := libtrust.FromCryptoPublicKey(leafCryptoKey)
	if err != nil {
		return nil, fmt.Errorf("unable to make libtrust public key from leaf certificate: %s", err)
	}
	return leafKey, nil
}

func parseAndVerifyRawJWK(rawJWK *json.RawMessage, verifyOpts VerifyOptions) (libtrust.PublicKey, error) {
	pubKey, err := libtrust.UnmarshalPublicKeyJWK([]byte(*rawJWK))
	if err != nil {
		return nil, fmt.Errorf("unable to decode raw JWK value: %s", err)
	}

	// Check to see if the key includes a certificate chain.
	x5cVal, ok := pubKey.GetExtendedField("x5c").([]any)
	if !ok {
		// The JWK should be one of the trusted root keys.
		if _, trusted := verifyOpts.TrustedKeys[pubKey.KeyID()]; !trusted {
			return nil, errors.New("untrusted JWK with no certificate chain")
		}

		// The JWK is one of the trusted keys.
		return pubKey, nil
	}

	// Ensure each item in the chain is of the correct type.
	x5c := make([]string, len(x5cVal))
	for i, val := range x5cVal {
		certString, ok := val.(string)
		if !ok || len(certString) == 0 {
			return nil, errors.New("malformed certificate chain")
		}
		x5c[i] = certString
	}

	// Ensure that the x509 certificate chain can
	// be verified up to one of our trusted roots.
	leafKey, err := parseAndVerifyCertChain(x5c, verifyOpts.Roots)
	if err != nil {
		return nil, fmt.Errorf("could not verify JWK certificate chain: %s", err)
	}

	// Verify that the public key in the leaf cert *is* the signing key.
	if pubKey.KeyID() != leafKey.KeyID() {
		return nil, errors.New("leaf certificate public key ID does not match JWK key ID")
	}

	return pubKey, nil
}

// accessSet returns a set of actions available for the resource
// actions listed in the `access` section of this token.
func (t *Token) accessSet() accessSet {
	if t.Claims == nil {
		return nil
	}

	accessSet := make(accessSet, len(t.Claims.Access))

	for _, resourceActions := range t.Claims.Access {
		resource := auth.Resource{
			Type: resourceActions.Type,
			Name: resourceActions.Name,
		}

		set, exists := accessSet[resource]
		if !exists {
			set = newActionSet()
			accessSet[resource] = set
		}

		for _, action := range resourceActions.Actions {
			set.add(action)
		}
	}

	return accessSet
}

func (t *Token) resources() []auth.Resource {
	if t.Claims == nil {
		return nil
	}

	resourceSet := make(map[auth.Resource]struct{}, 0)
	for _, resourceActions := range t.Claims.Access {
		resource := auth.Resource{
			Type:  resourceActions.Type,
			Class: resourceActions.Class,
			Name:  resourceActions.Name,
		}
		if resourceActions.Meta != nil {
			resource.ProjectPath = resourceActions.Meta.ProjectPath
		}
		resourceSet[resource] = struct{}{}
	}

	resources := make([]auth.Resource, 0, len(resourceSet))
	for resource := range resourceSet {
		resources = append(resources, resource)
	}

	return resources
}

func (t *Token) compactRaw() string {
	return fmt.Sprintf("%s.%s", t.Raw, joseBase64UrlEncode(t.Signature))
}
