package bbm

import "github.com/docker/distribution/registry/datastore/models"

// migrationFailureError represents errors that cause migration failures
type migrationFailureError struct {
	Err       error
	ErrorCode models.BBMErrorCode
}

func (e *migrationFailureError) Error() string {
	return e.Err.Error()
}

func (e *migrationFailureError) Unwrap() error {
	return e.Err
}

func newInvalidColumnError(err error) *migrationFailureError {
	return &migrationFailureError{
		Err:       err,
		ErrorCode: models.InvalidColumnBBMErrCode,
	}
}

func newInvalidTableError(err error) *migrationFailureError {
	return &migrationFailureError{
		Err:       err,
		ErrorCode: models.InvalidTableBBMErrCode,
	}
}

func newInvalidJobSignatureError(err error) *migrationFailureError {
	return &migrationFailureError{
		Err:       err,
		ErrorCode: models.InvalidJobSignatureBBMErrCode,
	}
}
