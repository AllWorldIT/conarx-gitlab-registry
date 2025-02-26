package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// A ManifestHandler gets and puts manifests of a particular type.
type ManifestHandler interface {
	// Unmarshal unmarshals the manifest from a byte slice.
	Unmarshal(context.Context, digest.Digest, []byte) (distribution.Manifest, error)

	// Put creates or updates the given manifest returning the manifest digest.
	Put(context.Context, distribution.Manifest) (digest.Digest, error)
}

type manifestStore struct {
	repository *repository
	blobStore  *linkedBlobStore
	ctx        context.Context

	schema1Handler      ManifestHandler
	schema2Handler      ManifestHandler
	ocischemaHandler    ManifestHandler
	manifestListHandler ManifestHandler
}

var _ distribution.ManifestService = &manifestStore{}

func (ms *manifestStore) Exists(_ context.Context, dgst digest.Digest) (bool, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestStore).Exists")

	_, err := ms.blobStore.Stat(ms.ctx, dgst)
	if err != nil {
		if err == distribution.ErrBlobUnknown {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (ms *manifestStore) Get(ctx context.Context, dgst digest.Digest, _ ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	log := dcontext.GetLogger(ms.ctx)
	log.Debug("(*manifestStore).Get")

	content, err := ms.blobStore.Get(ctx, dgst)
	if err != nil {
		if err == distribution.ErrBlobUnknown {
			return nil, distribution.ErrManifestUnknownRevision{
				Name:     ms.repository.Named().Name(),
				Revision: dgst,
			}
		}

		return nil, err
	}

	// Guard against retrieving empty content from the storage backend.
	if len(content) == 0 {
		return nil, distribution.ErrManifestEmpty{
			Name:   ms.repository.Named().Name(),
			Digest: dgst,
		}
	}

	var versioned manifest.Versioned
	if err = json.Unmarshal(content, &versioned); err != nil {
		return nil, fmt.Errorf("failed to unmarshal manifest payload: %w", err)
	}

	switch versioned.SchemaVersion {
	case 1:
		return ms.schema1Handler.Unmarshal(ctx, dgst, content)
	case 2:
		// This can be an image manifest or a manifest list
		switch versioned.MediaType {
		case schema2.MediaTypeManifest:
			return ms.schema2Handler.Unmarshal(ctx, dgst, content)
		case v1.MediaTypeImageManifest:
			return ms.ocischemaHandler.Unmarshal(ctx, dgst, content)
		case manifestlist.MediaTypeManifestList, v1.MediaTypeImageIndex:
			return ms.manifestListHandler.Unmarshal(ctx, dgst, content)
		case "":
			// OCI image or image index - no media type in the content

			// First see if it looks like an image index
			res, err := ms.manifestListHandler.Unmarshal(ctx, dgst, content)
			// nolint: revive // unchecked-type-assertion
			resIndex := res.(*manifestlist.DeserializedManifestList)
			if err == nil && resIndex.Manifests != nil {
				return resIndex, nil
			}

			// Otherwise, assume it must be an image manifest
			return ms.ocischemaHandler.Unmarshal(ctx, dgst, content)
		default:
			return nil, distribution.ErrManifestVerification{fmt.Errorf("unrecognized manifest content type %s", versioned.MediaType)}
		}
	default:
		// This is not a valid manifest, and most likely an example of
		// https://gitlab.com/gitlab-org/container-registry/-/issues/411. We can't block uploads of manifest
		// lists/indexes with invalid references until we reach a conclusion on
		// https://gitlab.com/gitlab-org/container-registry/-/issues/409. Meanwhile, we should not raise a 500 error if
		// a user attempts to read a config/layer as a manifest. We should act as if it does not exist and return a
		// 404 Not Found.
		log.WithField("schema_version", versioned.SchemaVersion).Warn("unrecognized manifest schema version, ignoring manifest")
		return nil, distribution.ErrManifestUnknownRevision{
			Name:     ms.repository.Named().Name(),
			Revision: dgst,
		}
	}
}

func (ms *manifestStore) Put(ctx context.Context, m distribution.Manifest, _ ...distribution.ManifestServiceOption) (digest.Digest, error) {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestStore).Put")

	switch m.(type) {
	case *schema1.SignedManifest:
		return ms.schema1Handler.Put(ctx, m)
	case *schema2.DeserializedManifest:
		return ms.schema2Handler.Put(ctx, m)
	case *ocischema.DeserializedManifest:
		return ms.ocischemaHandler.Put(ctx, m)
	case *manifestlist.DeserializedManifestList:
		return ms.manifestListHandler.Put(ctx, m)
	}

	return "", fmt.Errorf("unrecognized manifest type %T", m)
}

// Delete removes the revision of the specified manifest.
func (ms *manifestStore) Delete(ctx context.Context, dgst digest.Digest) error {
	dcontext.GetLogger(ms.ctx).Debug("(*manifestStore).Delete")
	return ms.blobStore.Delete(ctx, dgst)
}

func (ms *manifestStore) Enumerate(ctx context.Context, ingester func(digest.Digest) error) error {
	err := ms.blobStore.Enumerate(ctx, func(desc distribution.Descriptor) error {
		err := ingester(desc.Digest)
		if err != nil {
			return err
		}
		return nil
	})
	return err
}
