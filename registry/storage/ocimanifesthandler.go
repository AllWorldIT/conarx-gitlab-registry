package storage

import (
	"context"
	"fmt"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/opencontainers/go-digest"
)

// ocischemaManifestHandler is a ManifestHandler that covers ocischema manifests.
type ocischemaManifestHandler struct {
	repository               distribution.Repository
	blobStore                distribution.BlobStore
	ctx                      context.Context
	manifestURLs             validation.ManifestURLs
	manifestRefLimit         int
	manifestPayloadSizeLimit int
}

var _ ManifestHandler = &ocischemaManifestHandler{}

func (ms *ocischemaManifestHandler) Unmarshal(ctx context.Context, dgst digest.Digest, content []byte) (distribution.Manifest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*ocischemaManifestHandler).Unmarshal")

	m := &ocischema.DeserializedManifest{}
	if err := m.UnmarshalJSON(content); err != nil {
		return nil, err
	}

	return m, nil
}

func (ms *ocischemaManifestHandler) Put(ctx context.Context, manifest distribution.Manifest) (digest.Digest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*ocischemaManifestHandler).Put")

	m, ok := manifest.(*ocischema.DeserializedManifest)
	if !ok {
		return "", fmt.Errorf("non-ocischema manifest put to ocischemaManifestHandler: %T", manifest)
	}

	if err := ms.verifyManifest(ms.ctx, m); err != nil {
		return "", err
	}

	mt, payload, err := m.Payload()
	if err != nil {
		return "", err
	}

	revision, err := ms.blobStore.Put(ctx, mt, payload)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error putting payload into blobstore: %v", err)
		return "", err
	}

	return revision.Digest, nil
}

// verifyManifest ensures that the manifest content is valid from the
// perspective of the registry. As a policy, the registry only tries to store
// valid content, leaving trust policies of that content up to consumers.
func (ms *ocischemaManifestHandler) verifyManifest(ctx context.Context, mnfst *ocischema.DeserializedManifest) error {
	manifestService, err := ms.repository.Manifests(ctx)
	if err != nil {
		return err
	}

	v := validation.NewOCIValidator(manifestService, ms.repository.Blobs(ctx), ms.manifestRefLimit, ms.manifestPayloadSizeLimit, ms.manifestURLs)

	return v.Validate(ctx, mnfst)
}
