//go:generate mockgen -package mocks -destination mocks/storagedriver.go . StorageDeleter

package driver

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Version is a string representing the storage driver version, of the form
// Major.Minor.
// The registry must accept storage drivers with equal major version and greater
// minor version, but may not be compatible with older storage driver versions.
type Version string

// Major returns the major (primary) component of a version.
func (version Version) Major() uint {
	majorPart := strings.Split(string(version), ".")[0]
	major, _ := strconv.ParseUint(majorPart, 10, 0)
	return uint(major)
}

// Minor returns the minor (secondary) component of a version.
func (version Version) Minor() uint {
	minorPart := strings.Split(string(version), ".")[1]
	minor, _ := strconv.ParseUint(minorPart, 10, 0)
	return uint(minor)
}

// CurrentVersion is the current storage driver Version.
const CurrentVersion Version = "0.1"

// StorageDriver defines methods that a Storage Driver must implement for a
// filesystem-like key/value object storage. Storage Drivers are automatically
// registered via an internal registration mechanism, and generally created
// via the StorageDriverFactory interface (https://godoc.org/github.com/docker/distribution/registry/storage/driver/factory).
// Please see the aforementioned factory package for example code showing how to get an instance
// of a StorageDriver
type StorageDriver interface {
	StorageDeleter
	StorageLister
	StorageEnumerator

	// Name returns the human-readable "name" of the driver, useful in error
	// messages and logging. By convention, this will just be the registration
	// name, but drivers may provide other information here.
	Name() string

	// GetContent retrieves the content stored at "path" as a []byte.
	// This should primarily be used for small objects.
	GetContent(ctx context.Context, path string) ([]byte, error)

	// PutContent stores the []byte content at a location designated by "path".
	// This should primarily be used for small objects.
	PutContent(ctx context.Context, path string, content []byte) error

	// Reader retrieves an io.ReadCloser for the content stored at "path"
	// with a given byte offset.
	// May be used to resume reading a stream by providing a nonzero offset.
	Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error)

	// Writer returns a FileWriter which will store the content written to it
	// at the location designated by "path" after the call to Commit.
	Writer(context.Context, string, bool) (FileWriter, error)

	// Stat retrieves the FileInfo for the given path, including the current
	// size in bytes and the creation time.
	Stat(ctx context.Context, path string) (FileInfo, error)

	// Move moves an object stored at sourcePath to destPath, removing the
	// original object.
	// Note: This may be no more efficient than a copy followed by a delete for
	// many implementations.
	Move(ctx context.Context, sourcePath, destPath string) error

	// URLFor returns a URL which may be used to retrieve the content stored at
	// the given path, possibly using the given options.
	// May return an ErrUnsupportedMethod in certain StorageDriver
	// implementations.
	URLFor(ctx context.Context, path string, options map[string]any) (string, error)
}

// StorageDeleter defines methods that a Storage Driver must implement to delete objects.
// This allows using a narrower interface than StorageDriver when we only need the delete functionality, such as when
// mocking a storage driver for testing online garbage collection.
type StorageDeleter interface {
	// Delete recursively deletes all objects stored at "path" and its subpaths.
	Delete(ctx context.Context, path string) error

	// DeleteFiles deletes a set of files in bulk when supported by the
	// underlying storage system. If not supported, DeleteFiles iterates over
	// the path list and calls Delete for each one. The intention is to provide
	// a more efficient method to remove files directly by their full path and
	// in batches whenever possible. This should only be used for regular files
	// and not for directories. Returns the number of successfully deleted
	// files and any errors. This method is idempotent, no error is returned if
	// a file does not exist.
	DeleteFiles(ctx context.Context, paths []string) (int, error)
}

// StorageLister defines methods that a storage driver must implement to list
// objects under a path.
type StorageLister interface {
	// List returns a list of the objects that are direct descendants of the
	// given path.
	List(ctx context.Context, path string) ([]string, error)
}

// StorageEnumerator defines methods that a storage driver must implement to
// traverse filesystem paths.
type StorageEnumerator interface {
	// Walk traverses a filesystem defined within driver, starting
	// from the given path, calling f on each file.
	// If the returned error from the WalkFn is ErrSkipDir and fileInfo refers
	// to a directory, the directory will not be entered and Walk
	// will continue the traversal.  If fileInfo refers to a normal file, processing stops
	// Files are processed in a stable, lexicographically sorted order.
	Walk(ctx context.Context, path string, f WalkFn) error

	// WalkParallel is similar to walk, but the filesystem will be traversed in
	// parallel. This enables improved performance, but the order which files are
	// processed is not stable and WalkFn must be thread-safe.
	WalkParallel(ctx context.Context, path string, f WalkFn) error
}

// FileWriter provides an abstraction for an opened writable file-like object in
// the storage backend. The FileWriter must flush all content written to it on
// the call to Close, but is only required to make its content readable on a
// call to Commit.
type FileWriter interface {
	io.WriteCloser

	// Size returns the number of bytes written to this FileWriter.
	Size() int64

	// Cancel removes any written content from this FileWriter.
	Cancel() error

	// Commit flushes all content written to this FileWriter and makes it
	// available for future calls to StorageDriver.GetContent and
	// StorageDriver.Reader.
	Commit() error
}

// PathRegexp is the regular expression which each file path must match. A
// file path is absolute, beginning with a slash and containing a positive
// number of path components separated by slashes, where each component is
// restricted to alphanumeric characters or a period, underscore, or
// hyphen.
var PathRegexp = regexp.MustCompile(`^(/[A-Za-z0-9._-]+)+$`)

// ErrUnsupportedMethod may be returned in the case where a StorageDriver implementation does not support an optional method.
type ErrUnsupportedMethod struct {
	DriverName string
}

func (err ErrUnsupportedMethod) Error() string {
	return fmt.Sprintf("%s: unsupported method", err.DriverName)
}

// PathNotFoundError is returned when operating on a nonexistent path.
type PathNotFoundError struct {
	Path       string
	DriverName string
}

func (err PathNotFoundError) Error() string {
	return fmt.Sprintf("%s: Path not found: %s", err.DriverName, err.Path)
}

// TooManyRequestsError is returned when the user has sent too
// many requests in a given amount of time.
type TooManyRequestsError struct {
	Cause error
}

func (err TooManyRequestsError) Unwrap() error {
	return err.Cause
}

func (err TooManyRequestsError) Error() string {
	return fmt.Sprintf("too many requests: %s", err.Cause)
}

// InvalidPathError is returned when the provided path is malformed.
type InvalidPathError struct {
	Path       string
	DriverName string
}

func (err InvalidPathError) Error() string {
	return fmt.Sprintf("%s: invalid path: %s", err.DriverName, err.Path)
}

// InvalidOffsetError is returned when attempting to read or write from an
// invalid offset.
type InvalidOffsetError struct {
	Path       string
	Offset     int64
	DriverName string
}

func (err InvalidOffsetError) Error() string {
	return fmt.Sprintf("%s: invalid offset: %d for path: %s", err.DriverName, err.Offset, err.Path)
}

// Error is a catch-all error type which captures an error string and
// the driver type on which it occurred.
type Error struct {
	DriverName string
	Enclosed   error
}

func (err Error) Error() string {
	return fmt.Sprintf("%s: %s", err.DriverName, err.Enclosed)
}

func (err Error) Unwrap() error {
	return err.Enclosed
}

var (
	ErrAlreadyClosed   = fmt.Errorf("already closed")
	ErrAlreadyCommited = fmt.Errorf("already committed")
	ErrAlreadyCanceled = fmt.Errorf("already canceled")
)

const ParamLogger = "logger"
