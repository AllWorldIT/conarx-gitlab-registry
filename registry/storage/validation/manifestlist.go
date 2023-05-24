package validation

import (
	"context"
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
)

// ManifestListValidator ensures that a manifestlist is valid and optionally
// verifies all manifest references.
type ManifestListValidator struct {
	baseValidator
}

// NewManifestListValidator returns a new ManifestListValidator.
func NewManifestListValidator(exister ManifestExister, bs distribution.BlobStatter, refLimit, payloadLimit int) *ManifestListValidator {
	return &ManifestListValidator{
		baseValidator: baseValidator{
			manifestExister: exister,
			blobStatter:     bs,
			refLimit:        refLimit,
			payloadLimit:    payloadLimit,
		},
	}
}

// Validate ensures that the manifest content is valid from the
// perspective of the registry. As a policy, the registry only tries to store
// valid content, leaving trust policies of that content up to consumers.
func (v *ManifestListValidator) Validate(ctx context.Context, mnfst *manifestlist.DeserializedManifestList) error {
	var errs distribution.ErrManifestVerification

	if mnfst.SchemaVersion != 2 {
		return fmt.Errorf("unrecognized manifest list schema version %d", mnfst.SchemaVersion)
	}

	if err := v.exceedsPayloadSizeLimit(mnfst); err != nil {
		errs = append(errs, err)
		return errs
	}

	if err := v.exceedsRefLimit(mnfst); err != nil {
		errs = append(errs, err)
		return errs
	}

	// Docker buildkit uses OCI Image Indexes to store lists of layer blobs.
	// Ideally, we would not permit this behavior, but due to
	// https://gitlab.com/gitlab-org/container-registry/-/commit/06a098c632aee74619a06f88c23a06140f442a6f,
	// not being strictly backwards looking, historically it was possible to
	// retrieve a blob digest using manifest services during the validation step of
	// manifest puts, preventing the validation logic from rejecting these
	// manifests. Since buildkit is a fairly popular official docker tool, we
	// should allow only these manifest lists to contain layer blobs,
	// and reject all others.
	//
	// https://github.com/distribution/distribution/pull/864
	if mlcompat.LikelyBuildxCache(mnfst) {
		for _, blobDescriptor := range mnfst.References() {
			_, err := v.blobStatter.Stat(ctx, blobDescriptor.Digest)
			if err != nil {
				if err != distribution.ErrBlobUnknown {
					errs = append(errs, err)
				}

				// On error here, we always append unknown blob errors.
				errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: blobDescriptor.Digest})
			}
		}
	} else {
		for _, manifestDescriptor := range mnfst.References() {
			exists, err := v.manifestExister.Exists(ctx, manifestDescriptor.Digest)
			if err != nil && err != distribution.ErrBlobUnknown {
				errs = append(errs, err)
			}
			if err != nil || !exists {
				// On error here, we always append unknown blob errors.
				errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: manifestDescriptor.Digest})
			}
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
