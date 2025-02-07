package v2

import (
	"net/http"

	"github.com/docker/distribution/registry/api/errcode"
)

const errGroup = "registry.api.v2"

var (
	// ErrorCodeDigestInvalid is returned when uploading a blob if the
	// provided digest does not match the blob contents.
	ErrorCodeDigestInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "DIGEST_INVALID",
		Message: "provided digest did not match uploaded content",
		Description: `When a blob is uploaded, the registry will check that
		the content matches the digest provided by the client. The error may
		include a detail structure with the key "digest", including the
		invalid digest string. This error may also be returned when a manifest
		includes an invalid layer digest.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeSizeInvalid is returned when uploading a blob if the provided
	ErrorCodeSizeInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "SIZE_INVALID",
		Message: "provided length did not match content length",
		Description: `When a layer is uploaded, the provided size will be
		checked against the uploaded content. If they do not match, this error
		will be returned.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeNameInvalid is returned when the name in the manifest does not
	// match the provided name.
	ErrorCodeNameInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "NAME_INVALID",
		Message: "invalid repository name",
		Description: `Invalid repository name encountered either during
		manifest validation or any API operation.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeTagInvalid is returned when the tag in the manifest does not
	// match the provided tag.
	ErrorCodeTagInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "TAG_INVALID",
		Message: "manifest tag did not match URI",
		Description: `During a manifest upload, if the tag in the manifest
		does not match the uri tag, this error will be returned.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeNameUnknown when the repository name is not known.
	ErrorCodeNameUnknown = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "NAME_UNKNOWN",
		Message: "repository name not known to registry",
		Description: `This is returned if the name used during an operation is
		unknown to the registry.`,
		HTTPStatusCode: http.StatusNotFound,
	})

	// ErrorTagNameUnknown when the tag name for a given repository is not known.
	ErrorTagNameUnknown = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "TAG_UNKNOWN",
		Message: "tag name for repository does not exist",
		Description: `This is returend if the tagName used during an operation
		is unknown to the registry.`,
		HTTPStatusCode: http.StatusNotFound,
	})

	// ErrorCodeManifestUnknown returned when image manifest is unknown.
	ErrorCodeManifestUnknown = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_UNKNOWN",
		Message: "manifest unknown",
		Description: `This error is returned when the manifest, identified by
		name and tag is unknown to the repository.`,
		HTTPStatusCode: http.StatusNotFound,
	})

	// ErrorCodeManifestInvalid returned when an image manifest is invalid,
	// typically during a PUT operation. This error encompasses all errors
	// encountered during manifest validation that aren't signature errors.
	ErrorCodeManifestInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_INVALID",
		Message: "manifest invalid",
		Description: `During upload, manifests undergo several checks ensuring
		validity. If those checks fail, this error may be returned, unless a
		more specific error is included. The detail will contain information
		the failed validation.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeManifestUnverified is returned when the manifest fails
	// signature verification.
	ErrorCodeManifestUnverified = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_UNVERIFIED",
		Message: "manifest failed signature verification",
		Description: `During manifest upload, if the manifest fails signature
		verification, this error will be returned.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeManifestBlobUnknown is returned when a manifest blob is
	// unknown to the registry.
	ErrorCodeManifestBlobUnknown = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_BLOB_UNKNOWN",
		Message: "blob unknown to registry",
		Description: `This error may be returned when a manifest blob is
		unknown to the registry.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeManifestReferenceLimit is returned when a manifest has more
	// references than the configured limit.
	ErrorCodeManifestReferenceLimit = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_REFERENCE_LIMIT",
		Message: "too many manifest references",
		Description: `This error may be returned when a manifest references more than
		the configured limit allows.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeManifestPayloadSizeLimit is returned when a manifest payload is
	// bigger than the configured limit.
	ErrorCodeManifestPayloadSizeLimit = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_SIZE_LIMIT",
		Message: "payload size limit exceeded",
		Description: `This error may be returned when a manifest payload size is bigger than
		the configured limit allows.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeBlobUnknown is returned when a blob is unknown to the
	// registry. This can happen when the manifest references a nonexistent
	// layer or the result is not found by a blob fetch.
	ErrorCodeBlobUnknown = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "BLOB_UNKNOWN",
		Message: "blob unknown to registry",
		Description: `This error may be returned when a blob is unknown to the
		registry in a specified repository. This can be returned with a
		standard get or if a manifest references an unknown layer during
		upload.`,
		HTTPStatusCode: http.StatusNotFound,
	})

	// ErrorCodeBlobUploadUnknown is returned when an upload is unknown.
	ErrorCodeBlobUploadUnknown = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "BLOB_UPLOAD_UNKNOWN",
		Message: "blob upload unknown to registry",
		Description: `If a blob upload has been canceled or was never
		started, this error code may be returned.`,
		HTTPStatusCode: http.StatusNotFound,
	})

	// ErrorCodeBlobUploadInvalid is returned when an upload is invalid.
	ErrorCodeBlobUploadInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "BLOB_UPLOAD_INVALID",
		Message: "blob upload invalid",
		Description: `The blob upload encountered an error and can no
		longer proceed.`,
		HTTPStatusCode: http.StatusNotFound,
	})

	// ErrorCodeManifestReferencedInList is returned when attempting to delete a manifest that is still referenced by at
	// least one manifest list.
	ErrorCodeManifestReferencedInList = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "MANIFEST_REFERENCED",
		Message: "manifest referenced by a manifest list",
		Description: `The manifest is still referenced by at least one manifest list and therefore the delete cannot
		proceed.`,
		HTTPStatusCode: http.StatusConflict,
	})

	// ErrorCodeRenameInProgress when a repository base path is undergoing a rename.
	ErrorCodeRenameInProgress = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "RENAME_IN_PROGRESS",
		Message:        "the base repository path is undergoing a rename",
		Description:    "The GitLab project associated with the repository is undergoing a rename",
		HTTPStatusCode: http.StatusConflict,
	})

	// ErrorCodeInvalidContentRange is returned when uploading a blob if the provided
	// content range is invalid.
	ErrorCodeInvalidContentRange = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "CONTENT_RANGE_INVALID",
		Message:        "invalid content range",
		Description:    "If a layer chunk is uploaded with the range out of order, this error will be returned",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
	})

	// ErrorCodeInvalidLimitParam is returned when the value of the limit query parameter is invalid.
	ErrorCodeInvalidLimitParam = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "INVALID_LIMIT_PARAMETER_VALUE",
		Message:        "invalid limit query parameter value",
		Description:    "The value of a limit query parameter is invalid",
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodePaginationNumberInvalid is returned when the `n` parameter is not valid.
	ErrorCodePaginationNumberInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "PAGINATION_NUMBER_INVALID",
		Message: "invalid number of results requested",
		Description: `Returned when the "n" parameter (number of results
		to return) is not an integer, "n" is negative or "n" is bigger than the maximum allowed.`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeResumableBlobUploadInvalid is returned when a resumable upload is invalid.
	ErrorCodeResumableBlobUploadInvalid = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:   "RESUMABLE_BLOB_UPLOAD_INVALID",
		Message: "resumable blob upload invalid",
		Description: `The blob upload encountered an error and can no
		longer proceed.`,
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
	})

	// ErrorCodeInvalidTagProtectionPattern is returned when a provided tag protection pattern is invalid.
	ErrorCodeInvalidTagProtectionPattern = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "TAG_PROTECTION_PATTERN_INVALID",
		Message:        "invalid tag protection pattern",
		Description:    `The provided tag protection pattern is invalid`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeTagPatternCount is returned when the number of tag protection and immutability patterns exceeds the limit.
	ErrorCodeTagPatternCount = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "TAG_PATTERN_COUNT_LIMIT_EXCEEDED",
		Message:        "tag protection and immutability patterns count limit exceeded",
		Description:    `The number of tag protection and immutability patterns exceed the configured limit`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeProtectedTag is returned when attempting to push or delete a protected tag.
	ErrorCodeProtectedTag = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "PROTECTED_TAG",
		Message:        "insufficient permissions to push or delete protected tag",
		Description:    `Permission denied. Unable to push or delete tag due to configured protection policies.`,
		HTTPStatusCode: http.StatusUnauthorized,
	})

	// ErrorCodeProtectedManifest is returned when attempting to push or delete a protected or immutable tag.
	ErrorCodeProtectedManifest = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "PROTECTED_MANIFEST",
		Message:        "insufficient permissions to delete manifest due to tag protection or immutability policies",
		Description:    `Permission denied. Unable to delete manifest due to configured tag protection or immutability policies.`,
		HTTPStatusCode: http.StatusUnauthorized,
	})

	// ErrorCodeInvalidTagImmutabilityPattern is returned when a provided tag immutability pattern is invalid.
	ErrorCodeInvalidTagImmutabilityPattern = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "TAG_IMMUTABILITY_PATTERN_INVALID",
		Message:        "invalid tag immutability pattern",
		Description:    `The provided tag immutability pattern is invalid`,
		HTTPStatusCode: http.StatusBadRequest,
	})

	// ErrorCodeImmutableTag is returned when attempting to push or delete an immutable tag.
	ErrorCodeImmutableTag = errcode.Register(errGroup, errcode.ErrorDescriptor{
		Value:          "IMMUTABLE_TAG",
		Message:        "tag is immutable",
		Description:    `Permission denied. Unable to push or delete tag due to configured immutability policies.`,
		HTTPStatusCode: http.StatusUnauthorized,
	})
)
