package storage

import (
	"context"
	"fmt"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/opencontainers/go-digest"
)

// schema2ManifestHandler is a ManifestHandler that covers schema2 manifests.
type schema2ManifestHandler struct {
	repository               distribution.Repository
	blobStore                distribution.BlobStore
	ctx                      context.Context
	manifestURLs             validation.ManifestURLs
	manifestRefLimit         int
	manifestPayloadSizeLimit int
}

var _ ManifestHandler = &schema2ManifestHandler{}

func (ms *schema2ManifestHandler) Unmarshal(_ context.Context, _ digest.Digest, content []byte) (distribution.Manifest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*schema2ManifestHandler).Unmarshal")

	m := &schema2.DeserializedManifest{}
	if err := m.UnmarshalJSON(content); err != nil {
		return nil, err
	}

	return m, nil
}

func (ms *schema2ManifestHandler) Put(ctx context.Context, manifest distribution.Manifest) (digest.Digest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*schema2ManifestHandler).Put")

	m, ok := manifest.(*schema2.DeserializedManifest)
	if !ok {
		return "", fmt.Errorf("non-schema2 manifest put to schema2ManifestHandler: %T", manifest)
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
func (ms *schema2ManifestHandler) verifyManifest(ctx context.Context, mnfst *schema2.DeserializedManifest) error {
	manifestService, err := ms.repository.Manifests(ctx)
	if err != nil {
		return err
	}

	v := validation.NewSchema2Validator(manifestService, ms.repository.Blobs(ctx), ms.manifestRefLimit, ms.manifestPayloadSizeLimit, ms.manifestURLs)

	return v.Validate(ctx, mnfst)
}
