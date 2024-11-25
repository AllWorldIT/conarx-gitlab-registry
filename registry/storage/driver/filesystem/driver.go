package filesystem

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
)

const (
	driverName           = "filesystem"
	defaultRootDirectory = "/var/lib/registry"
	defaultMaxThreads    = uint64(100)

	// minThreads is the minimum value for the maxthreads configuration
	// parameter. If the driver's parameters are less than this we set
	// the parameters to minThreads
	minThreads = uint64(25)
)

// DriverParameters represents all configuration options available for the
// filesystem driver
type DriverParameters struct {
	RootDirectory string
	MaxThreads    uint64
}

func init() {
	factory.Register(driverName, &filesystemDriverFactory{})
}

// filesystemDriverFactory implements the factory.StorageDriverFactory interface
type filesystemDriverFactory struct{}

func (*filesystemDriverFactory) Create(parameters map[string]any) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	rootDirectory string
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by a local
// filesystem. All provided paths will be subpaths of the RootDirectory.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Optional Parameters:
// - rootdirectory
// - maxthreads
func FromParameters(parameters map[string]any) (*Driver, error) {
	params, err := fromParametersImpl(parameters)
	if err != nil || params == nil {
		return nil, err
	}
	return New(*params), nil
}

func fromParametersImpl(parameters map[string]any) (*DriverParameters, error) {
	var (
		err           error
		maxThreads    = defaultMaxThreads
		rootDirectory = defaultRootDirectory
	)

	if parameters != nil {
		if rootDir, ok := parameters["rootdirectory"]; ok {
			rootDirectory = fmt.Sprint(rootDir)
		}

		maxThreads, err = base.GetLimitFromParameter(parameters["maxthreads"], minThreads, defaultMaxThreads)
		if err != nil {
			return nil, fmt.Errorf("maxthreads config error: %s", err.Error())
		}
	}

	params := &DriverParameters{
		RootDirectory: rootDirectory,
		MaxThreads:    maxThreads,
	}
	return params, nil
}

// New constructs a new Driver with a given rootDirectory
func New(params DriverParameters) *Driver {
	fsDriver := &driver{rootDirectory: params.RootDirectory}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(fsDriver, params.MaxThreads),
			},
		},
	}
}

// Implement the storagedriver.StorageDriver interface

func (*driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, p string) ([]byte, error) {
	rc, err := d.Reader(ctx, p, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, subPath string, contents []byte) error {
	writer, err := d.Writer(ctx, subPath, false)
	if err != nil {
		return err
	}
	defer writer.Close()
	_, err = io.Copy(writer, bytes.NewReader(contents))
	if err != nil {
		_ = writer.Cancel()
		return err
	}
	return writer.Commit()
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(_ context.Context, p string, offset int64) (io.ReadCloser, error) {
	// NOTE(prozlach): mode does not matter as we do not pass O_CREATE flag anyway
	file, err := os.OpenFile(d.fullPath(p), os.O_RDONLY, 0o600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: p, DriverName: driverName}
		}

		return nil, err
	}

	seekPos, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		_ = file.Close()
		return nil, err
	} else if seekPos < offset {
		_ = file.Close()
		return nil, storagedriver.InvalidOffsetError{Path: p, Offset: offset, DriverName: driverName}
	}

	return file, nil
}

func (d *driver) Writer(_ context.Context, subPath string, doAppend bool) (storagedriver.FileWriter, error) {
	fullPath := d.fullPath(subPath)
	parentDir := path.Dir(fullPath)
	//nolint:gosec // needs some more research so that we do not break anything
	if err := os.MkdirAll(parentDir, 0o777); err != nil {
		return nil, err
	}

	//nolint: gosec
	fp, err := os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return nil, err
	}

	var offset int64

	if !doAppend {
		err := fp.Truncate(0)
		if err != nil {
			_ = fp.Close()
			return nil, err
		}
	} else {
		n, err := fp.Seek(0, io.SeekEnd)
		if err != nil {
			_ = fp.Close()
			return nil, err
		}
		offset = n
	}

	return newFileWriter(fp, offset), nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(_ context.Context, subPath string) (storagedriver.FileInfo, error) {
	fullPath := d.fullPath(subPath)

	fi, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath, DriverName: driverName}
		}

		return nil, err
	}

	return fileInfo{
		path:     subPath,
		FileInfo: fi,
	}, nil
}

// List returns a list of the objects that are direct descendants of the given
// path.
func (d *driver) List(_ context.Context, subPath string) ([]string, error) {
	fullPath := d.fullPath(subPath)

	//nolint: gosec
	dir, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storagedriver.PathNotFoundError{Path: subPath, DriverName: driverName}
		}
		return nil, err
	}

	defer dir.Close()

	fileNames, err := dir.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(fileNames))
	for _, fileName := range fileNames {
		keys = append(keys, path.Join(subPath, fileName))
	}

	// Ensure consistent sorting across platforms.
	sort.Strings(keys)

	return keys, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(_ context.Context, sourcePath, destPath string) error {
	source := d.fullPath(sourcePath)
	dest := d.fullPath(destPath)

	if _, err := os.Stat(source); os.IsNotExist(err) {
		return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: driverName}
	}

	//nolint:gosec // needs some more research so that we do not break anything
	if err := os.MkdirAll(path.Dir(dest), 0o755); err != nil {
		return err
	}

	err := os.Rename(source, dest)
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(_ context.Context, subPath string) error {
	fullPath := d.fullPath(subPath)

	_, err := os.Stat(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	} else if err != nil {
		return storagedriver.PathNotFoundError{Path: subPath, DriverName: driverName}
	}

	err = os.RemoveAll(fullPath)
	return err
}

// DeleteFiles deletes a set of files by iterating over their full path list and invoking Delete for each. The parent
// directory of each file is automatically removed if empty. Returns the number of successfully deleted files (parent
// directories not included) and any errors. This method is idempotent, no error is returned if a file does not exist.
func (d *driver) DeleteFiles(ctx context.Context, paths []string) (int, error) {
	count := 0
	for _, p := range paths {
		if err := d.Delete(ctx, p); err != nil {
			if !errors.As(err, new(storagedriver.PathNotFoundError)) {
				return count, err
			}
		}
		count++

		// delete parent directory as well if empty
		p := d.fullPath(filepath.Dir(p))
		// nolint:gosec
		f, err := os.Open(p)
		if err != nil {
			// ignore if not found
			if _, ok := err.(*os.PathError); !ok {
				return count, err
			}
			continue
		}

		// Attempt to read info about 1 file within this directory,
		// if err is of type io.EOF than the directory is empty.
		if _, err = f.Readdir(1); err == io.EOF {
			if err := os.Remove(p); err != nil {
				_ = f.Close()
				return count, err
			}
		}
		_ = f.Close()
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
func (d *driver) Walk(ctx context.Context, p string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, p, f)
}

// WalkParallel traverses a filesystem defined within driver in parallel, starting
// from the given path, calling f on each file.
func (d *driver) WalkParallel(ctx context.Context, p string, f storagedriver.WalkFn) error {
	// TODO: Verify that this driver can reliably handle parallel workloads before
	// using storagedriver.WalkFallbackParallel
	return d.Walk(ctx, p, f)
}

// fullPath returns the absolute path of a key within the Driver's storage.
func (d *driver) fullPath(subPath string) string {
	return path.Join(d.rootDirectory, subPath)
}

type fileInfo struct {
	os.FileInfo
	path string
}

var _ storagedriver.FileInfo = fileInfo{}

// Path provides the full path of the target of this file info.
func (fi fileInfo) Path() string {
	return fi.path
}

// Size returns current length in bytes of the file. The return value can
// be used to write to the end of the file at path. The value is
// meaningless if IsDir returns true.
func (fi fileInfo) Size() int64 {
	if fi.IsDir() {
		return 0
	}

	return fi.FileInfo.Size()
}

// ModTime returns the modification time for the file. For backends that
// don't have a modification time, the creation time should be returned.
func (fi fileInfo) ModTime() time.Time {
	return fi.FileInfo.ModTime()
}

// IsDir returns true if the path is a directory.
func (fi fileInfo) IsDir() bool {
	return fi.FileInfo.IsDir()
}

type fileWriter struct {
	file      *os.File
	size      int64
	bw        *bufio.Writer
	closed    bool
	committed bool
	canceled  bool
}

func newFileWriter(file *os.File, size int64) *fileWriter {
	return &fileWriter{
		file: file,
		size: size,
		bw:   bufio.NewWriter(file),
	}
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	switch {
	case fw.closed:
		return 0, fmt.Errorf("already closed")
	case fw.committed:
		return 0, fmt.Errorf("already committed")
	case fw.canceled:
		return 0, fmt.Errorf("already canceled")
	}
	n, err := fw.bw.Write(p)
	fw.size += int64(n)
	return n, err
}

func (fw *fileWriter) Size() int64 {
	return fw.size
}

func (fw *fileWriter) Close() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	if err := fw.file.Close(); err != nil {
		return err
	}
	fw.closed = true
	return nil
}

func (fw *fileWriter) Cancel() error {
	if fw.closed {
		return fmt.Errorf("already closed")
	}

	fw.canceled = true
	_ = fw.file.Close()
	return os.Remove(fw.file.Name())
}

func (fw *fileWriter) Commit() error {
	switch {
	case fw.closed:
		return fmt.Errorf("already closed")
	case fw.committed:
		return fmt.Errorf("already committed")
	case fw.canceled:
		return fmt.Errorf("already canceled")
	}

	if err := fw.bw.Flush(); err != nil {
		return err
	}

	if err := fw.file.Sync(); err != nil {
		return err
	}

	fw.committed = true
	return nil
}
