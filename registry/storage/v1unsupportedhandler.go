package storage

import (
	"context"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
)

// signedManifestHandler is a ManifestHandler that unmarshals v1 manifests but
// refuses to Put v1 manifests
type v1UnsupportedHandler struct {
	pullsDisabled bool
	innerHandler  ManifestHandler
}

var _ ManifestHandler = &v1UnsupportedHandler{}

func (v *v1UnsupportedHandler) Unmarshal(ctx context.Context, dgst digest.Digest, content []byte) (distribution.Manifest, error) {
	if v.pullsDisabled {
		return nil, distribution.ErrSchemaV1Unsupported
	}

	return v.innerHandler.Unmarshal(ctx, dgst, content)
}

func (*v1UnsupportedHandler) Put(_ context.Context, _ distribution.Manifest) (digest.Digest, error) {
	return digest.Digest(""), distribution.ErrSchemaV1Unsupported
}
