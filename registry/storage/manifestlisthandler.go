package storage

import (
	"context"
	"fmt"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/opencontainers/go-digest"
)

// manifestListHandler is a ManifestHandler that covers schema2 manifest lists.
type manifestListHandler struct {
	repository               distribution.Repository
	blobStore                distribution.BlobStore
	ctx                      context.Context
	manifestRefLimit         int
	manifestPayloadSizeLimit int
}

var _ ManifestHandler = &manifestListHandler{}

func (ms *manifestListHandler) Unmarshal(_ context.Context, _ digest.Digest, content []byte) (distribution.Manifest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestListHandler).Unmarshal")

	m := &manifestlist.DeserializedManifestList{}
	if err := m.UnmarshalJSON(content); err != nil {
		return nil, err
	}

	return m, nil
}

func (ms *manifestListHandler) Put(ctx context.Context, manifestList distribution.Manifest) (digest.Digest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestListHandler).Put")

	m, ok := manifestList.(*manifestlist.DeserializedManifestList)
	if !ok {
		return "", fmt.Errorf("wrong type put to manifestListHandler: %T", manifestList)
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
// perspective of the registry. As a policy, the registry only tries to
// store valid content, leaving trust policies of that content up to
// consumers.
func (ms *manifestListHandler) verifyManifest(ctx context.Context, mnfst *manifestlist.DeserializedManifestList) error {
	manifestService, err := ms.repository.Manifests(ctx)
	if err != nil {
		return err
	}

	blobService := ms.repository.Blobs(ctx)

	v := validation.NewManifestListValidator(manifestService, blobService, ms.manifestRefLimit, ms.manifestPayloadSizeLimit)

	return v.Validate(ctx, mnfst)
}
