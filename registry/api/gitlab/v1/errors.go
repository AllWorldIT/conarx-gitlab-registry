package v1

import (
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"

	"github.com/docker/distribution"
	"github.com/docker/distribution/registry/api/errcode"
)

const errGroup = "gitlab.api.v1"

// ErrorCodeInvalidQueryParamValue is returned when the value of a query parameter is invalid.
var ErrorCodeInvalidQueryParamValue = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "INVALID_QUERY_PARAMETER_VALUE",
	Message:        "invalid query parameter value",
	Description:    "The value of a request query parameter is invalid",
	HTTPStatusCode: http.StatusBadRequest,
})

func InvalidQueryParamValueErrorDetail(key string, validValues []string) string {
	return fmt.Sprintf("the '%s' query parameter must be set to one of: %s", key, strings.Join(validValues, ", "))
}

func InvalidQueryParamValueRangeErrorDetail(key string, min, max int) string {
	return fmt.Sprintf("the '%s' query parameter value must be between %d and %d", key, min, max)
}

func InvalidQueryParamValuePatternErrorDetail(key string, pattern *regexp.Regexp) string {
	return fmt.Sprintf("the '%s' query parameter value must match the pattern '%s'", key, pattern)
}

// ErrorCodeInvalidQueryParamType is returned when the value of a query parameter is of an invalid type.
var ErrorCodeInvalidQueryParamType = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "INVALID_QUERY_PARAMETER_TYPE",
	Message:        "invalid query parameter value type",
	Description:    "The value of a request query parameter is of an invalid type",
	HTTPStatusCode: http.StatusBadRequest,
})

// ErrorCodeInvalidBodyParamType is returned when the value of a body parameter is of an invalid type.
var ErrorCodeInvalidBodyParamType = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "INVALID_BODY_PARAMETER_TYPE",
	Message:        "invalid body parameter value type",
	Description:    "The value of a request body parameter is of an invalid type",
	HTTPStatusCode: http.StatusBadRequest,
})

// ErrorCodeInvalidJSONBody is returned when the body of a request is an invalid json.
var ErrorCodeInvalidJSONBody = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "INVALID_JSON_BODY",
	Message:        "invalid json body",
	Description:    "The body of the request is an invalid json",
	HTTPStatusCode: http.StatusBadRequest,
})

// ErrorCodeNotImplemented is returned when the requested operation has no corresponding implementation in the registry.
var ErrorCodeNotImplemented = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "NOT_IMPLEMENTED",
	Message:        "operation not available",
	Description:    "The requested operation is not available",
	HTTPStatusCode: http.StatusNotFound,
})

func InvalidQueryParamTypeErrorDetail(key string, validTypes []reflect.Kind) string {
	strTypes := make([]string, 0, len(validTypes))
	for _, t := range validTypes {
		strTypes = append(strTypes, t.String())
	}
	return fmt.Sprintf("the '%s' query parameter value type must be one of: %s", key, strings.Join(strTypes, ", "))
}

func InvalidPatchBodyTypeErrorDetail(key string, pattern *regexp.Regexp) string {
	return fmt.Sprintf("the '%s' body parameter value must match the pattern '%s'", key, pattern)
}

func MissingServerDependencyTypeErrorDetail(key string) string {
	return fmt.Sprintf("the server is missing the dependency '%s'", key)
}

// ErrorCodePreImportInProgress is returned when a repository is already pre importing.
var ErrorCodePreImportInProgress = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "PRE_IMPORT_IN_PROGRESS",
	Message:        "request cannot happen concurrently with pre import",
	Description:    "The repository is being pre imported",
	HTTPStatusCode: http.StatusTooEarly,
})

func ErrorCodePreImportInProgressErrorDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodeImportInProgress is returned when a repository is already importing.
var ErrorCodeImportInProgress = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "IMPORT_IN_PROGRESS",
	Message:        "request cannot happen concurrently with import",
	Description:    "The repository is being imported",
	HTTPStatusCode: http.StatusConflict,
})

func ErrorCodeImportInProgressErrorDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodePreImportFailed is returned when a repository failed a previous pre import and
// is attempting to run a final import.
var ErrorCodePreImportFailed = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "PRE_IMPORT_FAILED",
	Message:        "a previous pre import failed",
	Description:    "The repository failed to pre import",
	HTTPStatusCode: http.StatusFailedDependency,
})

func ErrorCodePreImportFailedErrorDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodePreImportCanceled is returned when a repository canceled a previous pre import and
// is attempting to run a final import.
var ErrorCodePreImportCanceled = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "PRE_IMPORT_CANCELED",
	Message:        "a previous pre import was canceled",
	Description:    "The repository pre import was canceled",
	HTTPStatusCode: http.StatusFailedDependency,
})

func ErrorCodePreImportCanceledErrorDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodeImportRateLimited is returned when a repository failed to begin a (pre)import due to maxconcurrentimports
var ErrorCodeImportRateLimited = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "IMPORT_RATE_LIMIT",
	Message:        "failed to begin (pre)import",
	Description:    "This instance of the container registry has reached its limit for (pre)import operations",
	HTTPStatusCode: http.StatusTooManyRequests,
})

func ErrorCodeImportRateLimitedDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodeImportRepositoryNotReady is returned when a repository has recently
// been updated and may not be in a consistent state yet.
var ErrorCodeImportRepositoryNotReady = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "IMPORT_REPOSITORY_NOT_READY",
	Message:        "failed to begin (pre)import",
	Description:    "The repository has recently been updated and may not be in a consistent state, try again later.",
	HTTPStatusCode: http.StatusTooManyRequests,
})

func ErrorCodeImportRepositoryNotReadyDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodePreImportRequired is returned when attempting to perform a final import for a repository that has not
// been pre imported successfully yet.
var ErrorCodePreImportRequired = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "PRE_IMPORT_REQUIRED",
	Message:        "a previous successful pre import is required",
	Description:    "The repository must be pre imported before the final import",
	HTTPStatusCode: http.StatusFailedDependency,
})

func ErrorCodePreImportRequiredDetail(repo distribution.Repository) string {
	return fmt.Sprintf("repository path %s", repo.Named().Name())
}

// ErrorCodeImportCannotBeCanceled is returned when a repository failed to cancel a (pre)import when the repository
// is already on the database as native or (pre)import has completed
var ErrorCodeImportCannotBeCanceled = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:          "IMPORT_CANNOT_BE_CANCELED",
	Message:        "failed to cancel (pre)import",
	Description:    "The repository (pre)import cannot be canceled after it has been completed or if it is native",
	HTTPStatusCode: http.StatusBadRequest,
})

// ErrorCodeRenameConflict when a repository name is already taken.
var ErrorCodeRenameConflict = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:   "RENAME_CONFLICT",
	Message: "repository name is already taken",
	Description: `This is returned if the name used during a rename operation is
			already in use in the registry.`,
	HTTPStatusCode: http.StatusConflict,
})

// ErrorCodeExceedsLimit when the repository has too many sub-repositories to perform an operation.
var ErrorCodeExceedsLimit = errcode.Register(errGroup, errcode.ErrorDescriptor{
	Value:   "EXCEEDS_LIMITS",
	Message: "repository contains too many sub-repositories for the operation",
	Description: `This is returned if the repository used during an operation 
		contains too many sub-repositories.`,
	HTTPStatusCode: http.StatusUnprocessableEntity,
})

func ExceedsRenameLimitErrorDetail(validValue int) string {
	return fmt.Sprintf("the repository must have less than %d sub-repositories:", validValue)
}

func ConflictWithOngoingRename(path string) string {
	return fmt.Sprintf("an ongoing rename operation in the registry is using the path: %s", path)
}

func ConflictWithExistingRepository(path string) string {
	return fmt.Sprintf("a repository already exists in the registry with the path: %s", path)
}
