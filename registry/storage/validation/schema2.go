package validation

import (
	"context"
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
)

// Schema2Validator ensures that a schema2 manifest is valid and optionally
// verifies all manifest references.
type Schema2Validator struct {
	baseValidator
	manifestURLs ManifestURLs
}

// NewSchema2Validator returns a new Schema2Validator.
func NewSchema2Validator(exister ManifestExister, statter distribution.BlobStatter, refLimit, payloadLimit int, manifestURLs ManifestURLs) *Schema2Validator {
	return &Schema2Validator{
		baseValidator: baseValidator{
			manifestExister: exister,
			blobStatter:     statter,
			refLimit:        refLimit,
			payloadLimit:    payloadLimit,
		},
		manifestURLs: manifestURLs,
	}
}

// Validate ensures that the manifest content is valid from the
// perspective of the registry. As a policy, the registry only tries to store
// valid content, leaving trust policies of that content up to consumers.
func (v *Schema2Validator) Validate(ctx context.Context, mnfst *schema2.DeserializedManifest) error {
	var errs distribution.ErrManifestVerification

	if mnfst.Manifest.SchemaVersion != 2 {
		return fmt.Errorf("unrecognized manifest schema version %d", mnfst.Manifest.SchemaVersion)
	}

	if err := v.exceedsPayloadSizeLimit(mnfst); err != nil {
		errs = append(errs, err)
		return errs
	}

	if err := v.exceedsRefLimit(mnfst); err != nil {
		errs = append(errs, err)
		return errs
	}

	for _, descriptor := range mnfst.References() {
		var err error

		switch descriptor.MediaType {
		case schema2.MediaTypeForeignLayer:
			// Clients download this layer from an external URL, so do not check for
			// its presence.
			if len(descriptor.URLs) == 0 {
				err = errMissingURL
			}

			for _, u := range descriptor.URLs {
				if !validURL(u, v.manifestURLs) {
					err = errInvalidURL
					break
				}
			}
		case schema2.MediaTypeManifest, schema1.MediaTypeManifest:
			// TODO (suleimi): This validation may be unnecessary. The mnfst.References() call above
			// only returns config and layer descriptors, not the subject field (which was added in
			// OCI Image Specification v1.1). Since config/layer descriptors cannot have Docker
			// manifest media types (application/vnd.docker.distribution.manifest.v2+json or
			// application/vnd.docker.distribution.manifest.v1+json), this check is possibly redundant.
			var exists bool
			exists, err = v.manifestExister.Exists(ctx, descriptor.Digest)
			if err != nil || !exists {
				err = distribution.ErrBlobUnknown // just coerce to unknown.
			}

			fallthrough // double check the blob store.
		default:
			// forward all else to blob storage
			if len(descriptor.URLs) == 0 {
				_, err = v.blobStatter.Stat(ctx, descriptor.Digest)
			}
		}

		if err != nil {
			if err != distribution.ErrBlobUnknown {
				errs = append(errs, err)
			}

			// On error here, we always append unknown blob errors.
			errs = append(errs, distribution.ErrManifestBlobUnknown{Digest: descriptor.Digest})
		}
	}

	if len(errs) != 0 {
		return errs
	}

	return nil
}
