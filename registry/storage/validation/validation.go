package validation

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
)

var (
	errUnexpectedURL = errors.New("unexpected URL on layer")
	errMissingURL    = errors.New("missing URL on layer")
	errInvalidURL    = errors.New("invalid URL on layer")
)

type baseValidator struct {
	manifestExister ManifestExister
	blobStatter     distribution.BlobStatter
	refLimit        int
	payloadLimit    int
}

func (v *baseValidator) exceedsRefLimit(mnfst distribution.Manifest) error {
	if v.refLimit <= 0 {
		return nil
	}

	refLen := len(mnfst.References())

	if refLen > v.refLimit {
		return distribution.ErrManifestReferencesExceedLimit{References: refLen, Limit: v.refLimit}
	}

	return nil
}

func (v *baseValidator) exceedsPayloadSizeLimit(mnfst distribution.Manifest) error {
	if v.payloadLimit <= 0 {
		return nil
	}
	_, b, err := mnfst.Payload()
	if err != nil {
		return fmt.Errorf("cannot get manifest payload: %w", err)
	}

	payloadLen := len(b)

	if payloadLen > v.payloadLimit {
		return distribution.ErrManifestPayloadSizeExceedsLimit{PayloadSize: payloadLen, Limit: v.payloadLimit}
	}

	return nil
}

// ManifestExister checks for the existance of a manifest.
type ManifestExister interface {
	// Exists returns true if the manifest exists.
	Exists(ctx context.Context, dgst digest.Digest) (bool, error)
}

// ManifestURLs holds regular expressions for controlling manifest URL allowlisting
type ManifestURLs struct {
	Allow *regexp.Regexp
	Deny  *regexp.Regexp
}

func validURL(s string, u ManifestURLs) bool {
	pu, err := url.Parse(s)
	if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Fragment != "" || (u.Allow != nil && !u.Allow.MatchString(s)) || (u.Deny != nil && u.Deny.MatchString(s)) {
		return false
	}

	return true
}
