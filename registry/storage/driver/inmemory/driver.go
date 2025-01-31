package inmemory

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const (
	driverName         = "inmemory"
	maxWalkConcurrency = 25
)

func init() {
	factory.Register(driverName, &inMemoryDriverFactory{})
}

// inMemoryDriverFacotry implements the factory.StorageDriverFactory interface.
type inMemoryDriverFactory struct{}

func (*inMemoryDriverFactory) Create(_ map[string]any) (storagedriver.StorageDriver, error) {
	return New(), nil
}

type driver struct {
	root  *dir
	mutex sync.RWMutex
}

// baseEmbed allows us to hide the Base embed.
type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local map.
// Intended solely for example and testing purposes.
type Driver struct {
	baseEmbed // embedded, hidden base driver.
}

var _ storagedriver.StorageDriver = &Driver{}

// New constructs a new Driver.
func New() *Driver {
	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: &driver{
					root: &dir{
						common: common{
							p:   "/",
							mod: time.Now(),
						},
					},
				},
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface.

func (*driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	rc, err := d.readerImpl(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	return io.ReadAll(rc)
}

// PutContent stores the []byte content at a location designated by "targetPath".
func (d *driver) PutContent(_ context.Context, targetPath string, contents []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	normalized := normalize(targetPath)

	f, err := d.root.mkfile(normalized)
	if err != nil {
		return fmt.Errorf("failed to create or retrieve file: %w", err)
	}

	f.truncate()
	_, err = f.WriteAt(contents, 0)
	if err != nil {
		return fmt.Errorf("writing contents: %w", err)
	}

	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.readerImpl(ctx, path, offset)
}

func (d *driver) readerImpl(_ context.Context, path string, offset int64) (io.ReadCloser, error) {
	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: driverName}
	}

	normalized := normalize(path)
	found := d.root.find(normalized)

	if found.path() != normalized {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}

	if found.isdir() {
		return nil, fmt.Errorf("%q is a directory", path)
	}

	return io.NopCloser(found.(*file).sectionReader(offset)), nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(_ context.Context, path string, doAppend bool) (storagedriver.FileWriter, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	normalized := normalize(path)

	if doAppend {
		found := d.root.find(normalized)

		if found.path() != normalized {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}
	}

	f, err := d.root.mkfile(normalized)
	if err != nil {
		return nil, fmt.Errorf("not a file")
	}

	if !doAppend {
		f.truncate()
	}

	return d.newWriter(f), nil
}

// Stat returns info about the provided path.
func (d *driver) Stat(_ context.Context, path string) (storagedriver.FileInfo, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	normalized := normalize(path)
	found := d.root.find(normalized)

	if found.path() != normalized {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}

	fi := storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   found.isdir(),
		ModTime: found.modtime(),
	}

	if !fi.IsDir {
		fi.Size = int64(len(found.(*file).data))
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(_ context.Context, path string) ([]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	normalized := normalize(path)

	found := d.root.find(normalized)

	if !found.isdir() {
		return nil, fmt.Errorf("not a directory")
	}

	entries, err := found.(*dir).list(normalized)
	if err != nil {
		switch err {
		case errNotExists:
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		case errIsNotDir:
			return nil, fmt.Errorf("not a directory")
		default:
			return nil, err
		}
	}

	return entries, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(_ context.Context, sourcePath, destPath string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	normalizedSrc, normalizedDst := normalize(sourcePath), normalize(destPath)

	return d.root.move(normalizedSrc, normalizedDst)
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(_ context.Context, path string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	normalized := normalize(path)

	return d.root.delete(normalized)
}

// DeleteFiles deletes a set of files by iterating over their full path list and invoking Delete for each. Returns the
// number of successfully deleted files and any errors. This method is idempotent, no error is returned if a file does
// not exist.
func (d *driver) DeleteFiles(ctx context.Context, paths []string) (int, error) {
	count := 0
	for _, path := range paths {
		if err := d.Delete(ctx, path); err != nil {
			if !errors.As(err, new(storagedriver.PathNotFoundError)) {
				return count, err
			}
		}
		count++
	}
	return count, nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (*driver) URLFor(_ context.Context, _ string, _ map[string]any) (string, error) {
	return "", storagedriver.ErrUnsupportedMethod{DriverName: driverName}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// WalkParallel traverses a filesystem defined within driver in parallel, starting
// from the given path, calling f on each file.
func (d *driver) WalkParallel(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallbackParallel(ctx, d, maxWalkConcurrency, path, f)
}

type writer struct {
	d         *driver
	f         *file
	closed    bool
	committed bool
	canceled  bool
}

func (d *driver) newWriter(f *file) storagedriver.FileWriter {
	return &writer{
		d: d,
		f: f,
	}
}

func (w *writer) Write(p []byte) (int, error) {
	switch {
	case w.closed:
		return 0, storagedriver.ErrAlreadyClosed
	case w.committed:
		return 0, storagedriver.ErrAlreadyCommited
	case w.canceled:
		return 0, storagedriver.ErrAlreadyCanceled
	}

	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()

	w.f.Append(p)
	return len(p), nil
}

func (w *writer) Size() int64 {
	w.d.mutex.RLock()
	defer w.d.mutex.RUnlock()

	return int64(len(w.f.data))
}

func (w *writer) Close() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	}
	w.closed = true

	return nil
}

func (w *writer) Cancel() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	} else if w.committed {
		return storagedriver.ErrAlreadyCommited
	}
	w.canceled = true

	w.d.mutex.Lock()
	defer w.d.mutex.Unlock()
	err := w.d.root.delete(w.f.path())
	if err != nil {
		if errors.As(err, new(storagedriver.PathNotFoundError)) {
			return nil
		}
		return fmt.Errorf("removing canceled blob: %w", err)
	}
	return nil
}

func (w *writer) Commit() error {
	switch {
	case w.closed:
		return storagedriver.ErrAlreadyClosed
	case w.committed:
		return storagedriver.ErrAlreadyCommited
	case w.canceled:
		return storagedriver.ErrAlreadyCanceled
	}
	w.committed = true
	return nil
}
