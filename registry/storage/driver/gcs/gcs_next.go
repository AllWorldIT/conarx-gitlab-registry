// Package gcs provides a storagedriver.StorageDriver implementation to
// store blobs in Google cloud storage.
//
// This package leverages the google.golang.org/cloud/storage client library
// for interfacing with gcs.
//
// Because gcs is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// Note that the contents of incomplete uploads are not accessible even though
// Stat returns their length

package gcs

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"

	// nolint: revive,gosec // imports-blocklist
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/docker/distribution/log"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// NewNext constructs a new driver
func NewNext(params *driverParameters) (storagedriver.StorageDriver, error) {
	rootDirectory := strings.Trim(params.rootDirectory, "/")
	if rootDirectory != "" {
		rootDirectory += "/"
	}
	if params.chunkSize <= 0 || params.chunkSize%minChunkSize != 0 {
		return nil, fmt.Errorf("invalid chunksize: %d is not a positive multiple of %d", params.chunkSize, minChunkSize)
	}
	d := &driverNext{
		bucket:        params.storageClient.Bucket(params.bucket).Retryer(storage.WithErrorFunc(ShouldRetry)),
		rootDirectory: rootDirectory,
		email:         params.email,
		privateKey:    params.privateKey,
		client:        params.client,
		chunkSize:     params.chunkSize,
		parallelWalk:  params.parallelWalk,
	}

	return &Wrapper{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: base.NewRegulator(d, params.maxConcurrency),
			},
		},
	}, nil
}

// driverNext is a storagedriver.StorageDriver implementation backed by GCS
// Objects are stored at absolute keys in the provided bucket.
type driverNext struct {
	client        *http.Client
	bucket        *storage.BucketHandle
	email         string
	privateKey    []byte
	rootDirectory string
	chunkSize     int64
	parallelWalk  bool
}

func (*driverNext) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driverNext) GetContent(ctx context.Context, path string) ([]byte, error) {
	name := d.pathToKey(path)
	// NOTE(prozlach): NewReader is considered idempotent by GCS, hence it is
	// retried without a limit until context is canceled. The retries
	// themselves are enabled by default.
	// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
	rc, err := d.bucket.Object(name).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	if err != nil {
		return nil, fmt.Errorf("creating new reader: %w", err)
	}
	defer rc.Close()

	p, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("reading data: %w", err)
	}
	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driverNext) PutContent(ctx context.Context, path string, contents []byte) error {
	// nolint: gosec
	h := md5.New()
	_, err := h.Write(contents)
	if err != nil {
		return fmt.Errorf("calculating hash: %w", err)
	}
	md5sum := h.Sum(nil)

	if len(contents) == 0 {
		d.logger(ctx).WithFields(logrus.Fields{
			"path":   path,
			"length": len(contents),
			"stack":  string(debug.Stack()),
		}).Info("PutContent called with 0 bytes")
	}

	// NOTE(prozlach): Custom retry mechanism has a point here, because we want
	// to override the whole object if it exist and the upload may happen in a
	// chunked form, so simply `always retring` a single chunk could create
	// duplicates. By always starting "from begning" we workaround this issue
	// in a very simple manner at the expense of re-sending whole data.
	return retry(func() error {
		wc := d.bucket.Object(d.pathToKey(path)).NewWriter(ctx)
		wc.ContentType = "application/octet-stream"
		wc.MD5 = md5sum

		return putContentsCloseNext(wc, contents)
	})
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset. May be used to resume reading a stream by providing a
// nonzero offset.
func (d *driverNext) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	obj := d.bucket.Object(d.pathToKey(path))
	// NOTE(milosgajdos/prozlach): If length is negative, the object is read
	// until the end. The request is retried until the context is canceled as
	// the operation is idempotent. See links below for more info:
	// https://pkg.go.dev/cloud.google.com/go/storage#ObjectHandle.NewRangeReader
	// https://cloud.google.com/storage/docs/retry-strategy
	r, err := obj.NewRangeReader(ctx, offset, -1)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}

		var status *googleapi.Error
		if errors.As(err, &status) {
			// nolint: revive // max-control-nesting
			switch status.Code {
			case http.StatusNotFound:
				return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
			case http.StatusRequestedRangeNotSatisfiable:
				attrs, err := obj.Attrs(ctx)
				if err != nil {
					return nil, fmt.Errorf("fetching object attributes: %w", err)
				}
				if offset == attrs.Size {
					return io.NopCloser(bytes.NewReader(make([]byte, 0))), nil
				}
				return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: driverName}
			default:
				return nil, fmt.Errorf("unexpected Google API error: %w", status)
			}
		}
		return nil, err
	}
	if r.Attrs.ContentType == uploadSessionContentType {
		_ = r.Close()
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	if offset == r.Attrs.Size {
		d.logger(ctx).WithFields(logrus.Fields{
			"path":            path,
			"fileSize":        r.Attrs.Size,
			"requestedOffset": offset,
		}).Info("Range request at EOF, returning empty reader")
	}
	return r, nil
}

// logger returns a log.Logger decorated with a key/value pair that uniquely
// identifies all entries as being emitted by this storage driver  component.
// Instead of relying on a fixed log.Logger instance, this method allows
// retrieving and extending a base logger embedded in the input context (if
// any) to preserve relevant key/value pairs introduced upstream (such as a
// correlation ID, present when calling from the API handlers).
func (*driverNext) logger(ctx context.Context) log.Logger {
	return log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"component": "registry.storage.gcs_next.internal",
	})
}

func putContentsCloseNext(wc *storage.Writer, contents []byte) error {
	size := len(contents)
	var nn int
	var err error
	for nn < size {
		n, err := wc.Write(contents[nn:size])
		nn += n
		if err != nil {
			break
		}
	}
	if err != nil {
		// nolint: staticcheck,gosec // this needs some refactoring and a deeper research
		wc.CloseWithError(err)
		return err
	}
	return wc.Close()
}

// logger returns a log.Logger decorated with a key/value pair that uniquely
// identifies all entries as being emitted by this storage driver  component.
// Instead of relying on a fixed log.Logger instance, this method allows
// retrieving and extending a base logger embedded in the input context (if
// any) to preserve relevant key/value pairs introduced upstream (such as a
// correlation ID, present when calling from the API handlers).
func (w *writerNext) logger() log.Logger {
	return log.GetLogger(log.WithContext(w.ctx)).WithFields(log.Fields{
		"component": "registry.storage.gcs_next.writer",
	})
}

// putChunkNext either completes upload or submits another chunk.
func (w *writerNext) putChunkNext(chunk []byte, totalSize int64) (int64, error) {
	w.logger().WithFields(logrus.Fields{
		"chunkStart": w.offset,
		"chunkEnd":   w.offset + int64(len(chunk)) - 1,
		"chunkSize":  len(chunk),
		"totalSize":  totalSize,
	}).Debug("Uploading chunk")

	// Documentation: https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
	bytesPut := int64(0)
	err := retry(func() error {
		req, err := http.NewRequestWithContext(w.ctx, http.MethodPut, w.sessionURI, bytes.NewReader(chunk))
		if err != nil {
			return fmt.Errorf("creating new http request: %w", err)
		}
		length := int64(len(chunk))
		to := w.offset + length - 1
		req.Header.Set("Content-Type", "application/octet-stream")
		// NOTE(prozlach) fake-gcs-server documents this behavior well:
		// https://github.com/fsouza/fake-gcs-server/blob/1e954726309326217b4c533bb9646e30f683fa66/fakestorage/upload.go#L467-L501
		//
		// A resumable upload is sent in one or more chunks. The request's
		// "Content-Range" header is used to determine if more data is expected.
		//
		// When sending streaming content, the total size is unknown until the stream
		// is exhausted. The Go client always sends streaming content. The sequence of
		// "Content-Range" headers for 2600-byte content sent in 1000-byte chunks are:
		//
		//	Content-Range: bytes 0-999/*
		//	Content-Range: bytes 1000-1999/*
		//	Content-Range: bytes 2000-2599/*
		//	Content-Range: bytes */2600
		//
		// When sending chunked content of a known size, the total size is sent as
		// well. The Python client uses this method to upload files and in-memory
		// content. The sequence of "Content-Range" headers for the 2600-byte content
		// sent in 1000-byte chunks are:
		//
		//	Content-Range: bytes 0-999/2600
		//	Content-Range: bytes 1000-1999/2600
		//	Content-Range: bytes 2000-2599/2600
		//
		// The server collects the content, analyzes the "Content-Range", and returns a
		// "308 Permanent Redirect" response if more chunks are expected, and a
		// "200 OK" response if the upload is complete (the Go client also accepts a
		// "201 Created" response). The "Range" header in the response should be set to
		// the size of the content received so far, such as:
		//
		//	Range: bytes 0-2000
		//
		// The client (such as the Go client) can send a header "X-Guploader-No-308" if
		// it can't process a native "308 Permanent Redirect". The in-process response
		// then has a status of "200 OK", with a header "X-Http-Status-Code-Override"
		// set to "308".
		size := "*"
		if totalSize >= 0 {
			size = strconv.FormatInt(totalSize, 10)
		}
		if w.offset == to+1 {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes */%v", size))
		} else {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes %v-%v/%v", w.offset, to, size))
		}
		req.Header.Set("Content-Length", strconv.FormatInt(length, 10))

		resp, err := w.client.Do(req)
		if err != nil {
			return fmt.Errorf("executing http request: %w", err)
		}
		defer resp.Body.Close()
		if totalSize < 0 && resp.StatusCode == 308 {
			// NOTE(prozlach): Quoting google docs:
			// https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
			//
			// A 200 OK or 201 Created response indicates that the upload was
			// completed, and no further action is necessary
			//
			// A 308 Resume Incomplete response indicates resumable upload is
			// in progress. If Cloud Storage has not yet persisted any bytes,
			// the 308 response does not have a Range header. In this case, we
			// should start upload from the beginning. Otherwise, the 308
			// response has a Range header, which specifies which bytes Cloud
			// Storage has persisted so far.
			groups := rangeHeader.FindStringSubmatch(resp.Header.Get("Range"))
			end, err := strconv.ParseInt(groups[2], 10, 64)
			if err != nil {
				return fmt.Errorf("parsing Range header: %w", err)
			}
			bytesPut = end - w.offset + 1

			w.logger().WithFields(logrus.Fields{
				"bytesUploaded": bytesPut,
				"totalExpected": len(chunk),
				"rangeHeader":   resp.Header.Get("Range"),
			}).Debug("Partial chunk upload, continuing")
			return nil
		}
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return fmt.Errorf("committing resumable upload: %w", err)
		}
		bytesPut = to - w.offset + 1
		return nil
	})
	return bytesPut, err
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driverNext) Writer(ctx context.Context, path string, doAppend bool) (storagedriver.FileWriter, error) {
	w := &writerNext{
		ctx:    ctx,
		client: d.client,
		object: d.bucket.Object(d.pathToKey(path)),
		buffer: make([]byte, d.chunkSize),
	}

	// NOTE(prozlach): Dear future maintainer of this code, the concurency
	// model that GCS has has some severe limitations.
	//
	// In case when we create a new Writer, a new session URL is obtained,
	// which in turn creates new resumable upload. GCS has last-write-wins
	// semantic, so in case when multiple Writers are targeting the same path,
	// the last write commit silently wins. Not ideal, but at least there will
	// not be data corruption.
	// The problem starts when there is more than one writer resuming the same
	// upload - they will read back the same session URL from the temporary
	// object, and hence try to append to the same in-progress file upload. GCS
	// ignores data for the offsets that it already committed, so the outcomes
	// aren't predicable and there is going to be data corruption.
	// Fortunately there is no need to fix this ATM, as the way that
	// container-registry creates paths makes collisions impossible(?),
	// and the filesystem driver is not that consistent too. Only Azure v2
	// and S3 v2 are ATM.
	// Fixing it would require some non-trivial refactoring, let's wait until
	// we have a use-case which would justify it.

	if doAppend {
		err := w.init()
		if err != nil {
			return nil, err
		}
	}
	return w, nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driverNext) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	var fi storagedriver.FileInfoFields

	// NOTE(prozlach): Attrs is considered idempotent by GCS, hence it is
	// retried without a limit until context is canceled. The retries
	// themselves are enabled by default.
	// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
	// try to get as file
	obj, err := d.bucket.Object(d.pathToKey(path)).Attrs(ctx)
	if err == nil {
		if obj.ContentType == uploadSessionContentType {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}
		fi = storagedriver.FileInfoFields{
			Path:    path,
			Size:    obj.Size,
			ModTime: obj.Updated,
			IsDir:   false,
		}
		return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
	}
	// try to get as folder
	dirpath := d.pathToDirKey(path)

	query := &storage.Query{}
	query.Prefix = dirpath

	it := d.bucket.Objects(ctx, query)
	// NOTE(prozlach): Listing objects is considered idempotent by GCS, hence
	// it is retried without a limit until context is canceled. The retries
	// themselves are enabled by default.
	// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
	attrs, err := it.Next()
	if errors.Is(err, iterator.Done) {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	if err != nil {
		return nil, fmt.Errorf("fetching next page from iterator: %w", err)
	}

	fi = storagedriver.FileInfoFields{
		Path:  path,
		IsDir: true,
	}
	if attrs.Name == dirpath {
		fi.Size = attrs.Size
		fi.ModTime = attrs.Updated
	}
	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the
// given path.
func (d *driverNext) List(ctx context.Context, path string) ([]string, error) {
	query := &storage.Query{}
	query.Delimiter = "/"
	query.Prefix = d.pathToDirKey(path)
	if query.Prefix == "/" {
		query.Prefix = ""
	}
	list := make([]string, 0, 64)

	it := d.bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		// NOTE(prozlach): Listing objects is considered idempotent by GCS,
		// hence it is retried without a limit until context is canceled. The
		// retries themselves are enabled by default.
		// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
		if err != nil {
			return nil, fmt.Errorf("fetching next page from iterator: %w", err)
		}

		if !attrs.Deleted.IsZero() {
			d.logger(ctx).WithFields(logrus.Fields{
				"path":        d.keyToPath(attrs.Name),
				"deletedTime": attrs.Deleted,
			}).Debug("Filtered out deleted object from listing due to eventual consistency")
		}

		if attrs.ContentType == uploadSessionContentType {
			d.logger(ctx).WithFields(logrus.Fields{
				"path": d.keyToPath(attrs.Name),
			}).Debug("Filtered out temporary upload session object from listing")
		}

		// GCS does not guarantee strong consistency between
		// DELETE and LIST operations. Check that the object is not deleted,
		// and filter out any objects with a non-zero time-deleted
		if attrs.Deleted.IsZero() && attrs.ContentType != uploadSessionContentType {
			var name string

			if len(attrs.Prefix) > 0 {
				name = attrs.Prefix
			} else {
				name = attrs.Name
			}

			list = append(list, d.keyToPath(name))
		}
	}
	if path != "/" && len(list) == 0 {
		// Treat empty response as missing directory, since we don't actually
		// have directories in Google Cloud Storage.
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return list, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
func (d *driverNext) Move(ctx context.Context, sourcePath, destPath string) error {
	// NOTE(prozlach): Both Delete and Copy operations are conditionally
	// idempotent, but in our case we do not care - we just want to copy the
	// original object and then simply remove the old one. Hence we override the
	// idempotent policy to always retry until the context is canceled. The
	// retries themselves are enabled by default.
	// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
	alwaysRetryBucket := d.bucket.Retryer(storage.WithPolicy(storage.RetryAlways))

	src := alwaysRetryBucket.Object(d.pathToKey(sourcePath))
	dst := alwaysRetryBucket.Object(d.pathToKey(destPath))
	_, err := dst.CopierFrom(src).Run(ctx)
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: driverName}
			}
		}
		return err
	}
	err = alwaysRetryBucket.Object(d.pathToKey(sourcePath)).Delete(ctx)
	// if deleting the file fails, log the error, but do not fail; the file was successfully copied,
	// and the original should eventually be cleaned when purging the uploads folder.
	if err != nil {
		d.logger(ctx).WithFields(logrus.Fields{
			"sourcePath":    sourcePath,
			"destPath":      destPath,
			"copySucceeded": true,
			"deleteError":   err.Error(),
		}).Info("Move operation: copy succeeded but delete failed, file will need cleanup")
	}
	return nil
}

// listAll recursively lists all names of objects stored at "prefix" and its subpaths.
func (d *driverNext) listAll(ctx context.Context, prefix string) ([]string, error) {
	list := make([]string, 0, 64)
	query := &storage.Query{}
	query.Prefix = prefix
	query.Versions = false

	it := d.bucket.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		// NOTE(prozlach): Listing objects is considered idempotent by GCS,
		// hence it is retried without a limit until context is canceled. The
		// retries themselves are enabled by default. Docs:
		// https://cloud.google.com/storage/docs/retry-strategy#go
		if err != nil {
			return nil, fmt.Errorf("fetching next page from iterator: %w", err)
		}
		// GCS does not guarantee strong consistency between
		// DELETE and LIST operations. Check that the object is not deleted,
		// and filter out any objects with a non-zero time-deleted
		if attrs.Deleted.IsZero() {
			list = append(list, attrs.Name)
		}
	}
	return list, nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driverNext) Delete(ctx context.Context, path string) error {
	// NOTE(prozlach): Delete operation is conditionally idempotent, but in our
	// case we do not care - we just want to delete the object. Hence we override
	// the idempotent policy to always retry until the context is canceled.
	// The retries themselves are enabled by default.
	// by default.
	// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
	alwaysRetryBucket := d.bucket.Retryer(storage.WithPolicy(storage.RetryAlways))

	prefix := d.pathToDirKey(path)
	keys, err := d.listAll(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		// NOTE(milosgajdos/prozlach): d.listAll calls (BucketHandle).Objects
		// Objects will be iterated over lexicographically by name. This means
		// we don't have to reverse order the slice; we can range over the keys
		// slice in reverse order
		// docs:
		// https://pkg.go.dev/cloud.google.com/go/storage#BucketHandle.Objects
		for i := len(keys) - 1; i >= 0; i-- {
			err := alwaysRetryBucket.Object(keys[i]).Delete(ctx)
			// GCS only guarantees eventual consistency, so listAll might return
			// paths that no longer exist. If this happens, just ignore any not
			// found error
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return fmt.Errorf("deleting object: %w", err)
			}
		}
		return nil
	}
	err = alwaysRetryBucket.Object(d.pathToKey(path)).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return err
}

// DeleteFiles deletes a set of files concurrently, using a separate goroutine
// for each, up to a maximum of maxDeleteConcurrency. Returns the number of
// successfully deleted files and any errors. This method is idempotent, no
// error is returned if a file does not exist.
func (d *driverNext) DeleteFiles(ctx context.Context, paths []string) (int, error) {
	errs := new(multierror.Error)
	errsMutex := new(sync.Mutex)

	// Count the number of successfully deleted files across concurrent requests
	// NOTE(prozlach): Using int32, this gives us up to 2 147 483 648 files
	// that can be deleted, so it should suffice. By using int64 we would get
	// linter warnings, esp. on 32bit platforms.
	count := new(atomic.Int32)

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxDeleteConcurrency)

	d.logger(ctx).WithFields(logrus.Fields{
		"totalFiles":     len(paths),
		"maxConcurrency": maxDeleteConcurrency,
	}).Debug("Starting concurrent file deletion")

	for _, path := range paths {
		g.Go(func() error {
			// Check if any context was canceled, if so - skip calling Delete
			// as it will fail anyway.
			select {
			case <-gctx.Done():
				errsMutex.Lock()
				defer errsMutex.Unlock()

				errs = multierror.Append(errs, gctx.Err())
				return nil
			default:
			}

			// NOTE(prozlach): Delete operation is conditionally idempotent,
			// but in our case we do not care - we just want the object to go
			// away, so we override the idempotent policy to always retry
			// until the context is canceled. The retries themselves are
			// enabled by default. Docs:
			// https://cloud.google.com/storage/docs/retry-strategy#go
			err := d.bucket.Retryer(storage.WithPolicy(storage.RetryAlways)).Object(d.pathToKey(path)).Delete(gctx)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				errsMutex.Lock()
				defer errsMutex.Unlock()

				errs = multierror.Append(errs, err)
			} else {
				// count successfully deleted files
				count.Add(1)
			}

			return nil
		})
	}

	// Wait for all goroutines. g.Wait() will return the first error returned
	// by any g.Go func, or nil if all returned nil. Since we always return nil
	// from g.Go in this example, we rely solely on the errors collected
	// multierror var
	_ = g.Wait()

	d.logger(ctx).WithFields(logrus.Fields{
		"totalFiles":        len(paths),
		"successfulDeletes": int(count.Load()),
		"errors":            errs.Len(),
	}).Debug("Completed concurrent file deletion")

	return int(count.Load()), errs.ErrorOrNil()
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// Returns ErrUnsupportedMethod if this driver has no privateKey
func (d *driverNext) URLFor(ctx context.Context, path string, options map[string]any) (string, error) {
	name := d.pathToKey(path)
	methodString := http.MethodGet
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != http.MethodGet && methodString != http.MethodHead) {
			return "", storagedriver.ErrUnsupportedMethod{DriverName: driverName}
		}
	}

	expiresTime := systemClock.Now().Add(20 * time.Minute)
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}

	opts := &storage.SignedURLOptions{
		Method:          methodString,
		Expires:         expiresTime,
		QueryParameters: storagedriver.CustomParams(options, customParamKeys),
		Scheme:          storage.SigningSchemeV4,
	}

	if d.privateKey != nil && d.email != "" {
		// If we have a private key and email from service account JSON, use them directly
		opts.GoogleAccessID = d.email
		opts.PrivateKey = d.privateKey

		d.logger(ctx).WithFields(logrus.Fields{
			"path":          path,
			"method":        methodString,
			"signingMethod": "service_account_key",
			"expires":       expiresTime,
		}).Debug("Generating signed URL with service account credentials")
	} else {
		d.logger(ctx).WithFields(logrus.Fields{
			"path":          path,
			"method":        methodString,
			"signingMethod": "instance_profile",
			"expires":       expiresTime,
		}).Debug("Generating signed URL with instance profile credentials")
	}

	// NOTE(prozlach): Signing a URL requires credentials authorized to sign a
	// URL. They can be passed through SignedURLOptions with one of the
	// following options:
	//    a. a Google service account private key, obtainable from the Google Developers Console
	//    b. a Google Access ID with iam.serviceAccounts.signBlob permissions
	//    c. a SignBytes function implementing custom signing.
	// In this case none of these options are used, which means the SignedURL
	// function attempts to use the same authentication that was used to
	// instantiate the Storage client and in our case this is instance profile
	// credentials.
	// Doc: https://cloud.google.com/storage/docs/access-control/signing-urls-with-helpers#download-object
	return d.bucket.SignedURL(name, opts)
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driverNext) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// WalkParallel traverses a filesystem defined within driver in parallel, starting
// from the given path, calling f on each file.
func (d *driverNext) WalkParallel(ctx context.Context, path string, f storagedriver.WalkFn) error {
	// If the ParallelWalk feature flag is not set, fall back to standard sequential walk.
	if !d.parallelWalk {
		return d.Walk(ctx, path, f)
	}

	return storagedriver.WalkFallbackParallel(ctx, d, maxWalkConcurrency, path, f)
}

// startSessionNext starts a new resumable upload session. Documentation can be
// found here: https://cloud.google.com/storage/docs/performing-resumable-uploads#initiate-session
func (w *writerNext) startSessionNext() (uri string, err error) {
	u := &url.URL{
		Scheme: "https",
		// https://cloud.google.com/storage/docs/request-endpoints#typical
		Host:     "storage.googleapis.com",
		Path:     fmt.Sprintf("/upload/storage/v1/b/%v/o", w.object.BucketName()),
		RawQuery: fmt.Sprintf("uploadType=resumable&name=%v", w.object.ObjectName()),
	}
	err = retry(func() error {
		req, err := http.NewRequestWithContext(w.ctx, http.MethodPost, u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Upload-Content-Type", "application/octet-stream")
		req.Header.Set("Content-Length", "0")
		resp, err := w.client.Do(req)
		if err != nil {
			return fmt.Errorf("executing http request: %w", err)
		}
		defer resp.Body.Close()
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return fmt.Errorf("starting new resumable upload session: %w", err)
		}
		uri = resp.Header.Get("Location")
		return nil
	})
	w.logger().WithFields(logrus.Fields{
		"bucket": w.object.BucketName(),
		"object": w.object.ObjectName(),
	}).Debug("Created new resumable upload session")
	return uri, err
}

func (d *driverNext) pathToKey(path string) string {
	return strings.TrimSpace(strings.TrimRight(d.rootDirectory+strings.TrimLeft(path, "/"), "/"))
}

func (d *driverNext) pathToDirKey(path string) string {
	return d.pathToKey(path) + "/"
}

func (d *driverNext) keyToPath(key string) string {
	return "/" + strings.Trim(strings.TrimPrefix(key, d.rootDirectory), "/")
}

type writerNext struct {
	ctx    context.Context
	client *http.Client
	object *storage.ObjectHandle
	// size is the amount of data written to this writter using Write(), but
	// not necessarily committed yet (i.e. offset + buffSize). If Writer was
	// resumed, then it includes the offset of the data already persisted in
	// the previous session.
	size int64
	// offset is the amount of data persisted in the backend, be it as
	// resumable upload yet to be committed or data written in
	// putContentsCloseNext. It does not mean that this data is visible in the
	// bucket, as resumable uploads require finall call to Commit() to finish
	// the upload and make the data accessible.
	offset     int64
	closed     bool
	committed  bool
	canceled   bool
	sessionURI string
	buffer     []byte
	buffSize   int64
}

// Cancel removes any written content from this FileWriter.
func (w *writerNext) Cancel() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	} else if w.committed {
		return storagedriver.ErrAlreadyCommited
	}
	w.canceled = true

	// NOTE(prozlach): Delete operation is conditionally idempotent, but in our
	// case we do not care - we just want to delete the object. Hence we
	// override the idempotent policy to always retry until the context is
	// canceled. The retries themselves are enabled by default.
	// Docs: https://cloud.google.com/storage/docs/retry-strategy#go
	err := w.object.Retryer(storage.WithPolicy(storage.RetryAlways)).Delete(w.ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil
		}

		var gerr *googleapi.Error
		if errors.As(err, &gerr) && gerr.Code == http.StatusNotFound {
			return nil
		}

		return fmt.Errorf("deleting object while canceling writer: %w", err)
	}
	return nil
}

// Close stores the current resumable upload session in a temporary object
// provided that the session has not been committed nor canceled yet.
func (w *writerNext) Close() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	}
	w.closed = true

	if w.canceled {
		// NOTE(prozlach): If the writer has already been canceled, then there
		// is nothing to flush to the backend as the target file has already
		// been deleted.
		return nil
	}
	if w.committed {
		// we are done already, return early
		return nil
	}

	err := w.writeChunk()
	if err != nil {
		return fmt.Errorf("writing chunk: %w", err)
	}

	// Copy the remaining bytes from the buffer to the upload session Normally
	// buffSize will be smaller than minChunkSize. However, in the unlikely
	// event that the upload session failed to start, this number could be
	// higher. In this case we can safely clip the remaining bytes to the
	// minChunkSize
	if w.buffSize > minChunkSize {
		w.buffSize = minChunkSize
	}

	// commit the writes by updating the upload session
	err = retry(func() error {
		wc := w.object.NewWriter(w.ctx)
		wc.ContentType = uploadSessionContentType
		wc.Metadata = map[string]string{
			"Session-URI": w.sessionURI,
			"Offset":      strconv.FormatInt(w.offset, 10),
		}
		return putContentsCloseNext(wc, w.buffer[0:w.buffSize])
	})
	if err != nil {
		return fmt.Errorf("writing contents while closing writer: %w", err)
	}
	w.buffSize = 0
	return nil
}

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (w *writerNext) Commit() error {
	switch {
	case w.closed:
		return storagedriver.ErrAlreadyClosed
	case w.committed:
		return storagedriver.ErrAlreadyCommited
	case w.canceled:
		return storagedriver.ErrAlreadyCanceled
	}
	w.committed = true

	// no session started yet just perform a simple upload
	if w.sessionURI == "" {
		err := retry(func() error {
			wc := w.object.NewWriter(w.ctx)
			wc.ContentType = "application/octet-stream"
			return putContentsCloseNext(wc, w.buffer[0:w.buffSize])
		})
		if err != nil {
			return err
		}
		// putContentsCloseNext overwrites the object if it was already
		// present, so offset is going to be equal to w.size.
		w.offset = w.size
		w.buffSize = 0
		return nil
	}
	var nn int64
	// NOTE(prozlach): loop must be performed at least once to ensure the file
	// is committed even when the buffer is empty. The empty/first loop with
	// zero-length will actually commit the file.
	for {
		n, err := w.putChunkNext(w.buffer[nn:w.buffSize], w.size)
		nn += n
		w.offset += n
		if err != nil {
			w.buffSize = int64(copy(w.buffer, w.buffer[nn:w.buffSize])) // nolint: gosec // copy() is always going to be non-negative
			return err
		}
		if nn == w.buffSize {
			break
		}
	}
	w.buffSize = 0
	return nil
}

func (w *writerNext) writeChunk() error {
	var err error
	// chunks can be uploaded only in multiples of minChunkSize
	// chunkSize is a multiple of minChunkSize less than or equal to buffSize
	chunkSize := w.buffSize - (w.buffSize % minChunkSize)
	if chunkSize == 0 {
		return nil
	}
	// if their is no sessionURI yet, obtain one by starting the session
	if w.sessionURI == "" {
		w.sessionURI, err = w.startSessionNext()
	}
	if err != nil {
		return err
	}
	nn, err := w.putChunkNext(w.buffer[0:chunkSize], -1)
	w.offset += nn
	// shift the remaining bytes to the start of the buffer
	w.buffSize = int64(copy(w.buffer, w.buffer[nn:w.buffSize])) // nolint: gosec // copy() is always going to be non-negative

	return err
}

func (w *writerNext) Write(p []byte) (int, error) {
	switch {
	case w.closed:
		return 0, storagedriver.ErrAlreadyClosed
	case w.committed:
		return 0, storagedriver.ErrAlreadyCommited
	case w.canceled:
		return 0, storagedriver.ErrAlreadyCanceled
	}

	var err error
	var nn int

	for nn < len(p) {
		n := copy(w.buffer[w.buffSize:], p[nn:])
		w.buffSize += int64(n) // nolint: gosec // copy() is always going to be non-negative
		// NOTE(prozlach): It should be safe to bump the size of the writer
		// before the data is actually committed to the backend for two reasons:
		// * data is loaded into buffer, even if this attempt to upload data
		// fails, the caller may retry and the data may get uploaded.
		// * the writeChunk->putChunkNext methods prune the buffer only when
		// the data was successfully uploaded.
		// * this follows the semantics of Azure and S3 drivers
		w.size += int64(n)
		if w.buffSize == int64(cap(w.buffer)) {
			w.logger().WithFields(logrus.Fields{
				"path":         w.object.ObjectName(),
				"bufferSize":   w.buffSize,
				"totalWritten": w.size,
				"chunkNumber":  w.offset/int64(cap(w.buffer)) + 1,
			}).Debug("Buffer full, initiating chunk upload")

			err = w.writeChunk()
			if err != nil {
				break
			}
		}
		nn += n
	}
	return nn, err
}

// Size returns the number of bytes written to this FileWriter.
func (w *writerNext) Size() int64 {
	return w.size
}

// init restores resumable upload basing on the Offset and Session-URL metadata
// and the `tail` of the data that are stored in the temporary object.
// Due to miminum chunk size limitation for GCS, we can't upload all the bytes
// remaining in the buffer if there are fewer than minChunkSize, so they are
// stored in the temporary object created during Close() call, and then read
// back into the buffer here. The temporary object and the resumable upload coexist
// until the resumable upload is committed. The Commit operation overrides the
// temporary object.
// The buffer size is never smaller than minChunkSize and the amount if tail
// data is never higher than minChunkSize, hence there is never going to be an
// overflow/there will always be enough place in the buffer to read back the
// temporary data in this call.
func (w *writerNext) init() error {
	attrs, err := w.object.Attrs(w.ctx)
	if err != nil {
		var gcsErr *googleapi.Error
		if errors.As(err, &gcsErr) && gcsErr.Code == http.StatusNotFound {
			return storagedriver.PathNotFoundError{Path: w.object.ObjectName(), DriverName: driverName}
		}

		return fmt.Errorf("fetching object: %w", err)
	}
	if attrs.ContentType != uploadSessionContentType {
		return storagedriver.PathNotFoundError{Path: w.object.ObjectName(), DriverName: driverName}
	}

	if attrs.Metadata["Offset"] == "" {
		return fmt.Errorf("`X-Goog-Meta-Offset` HTTP header has not been set for path %s", w.object.ObjectName())
	}
	offset, err := strconv.ParseInt(attrs.Metadata["Offset"], 10, 64)
	if err != nil {
		return fmt.Errorf("parsing `X-Goog-Meta-Offset` HTTP header: %w", err)
	}

	r, err := w.object.NewReader(w.ctx)
	if err != nil {
		return fmt.Errorf("parsing `X-Goog-Meta-Offset` HTTP header: %w", err)
	}
	defer r.Close()

	for err == nil && w.buffSize < int64(len(w.buffer)) {
		var n int
		n, err = r.Read(w.buffer[w.buffSize:])
		w.buffSize += int64(n)
	}
	if err != nil && err != io.EOF {
		return fmt.Errorf("reading `tail` data during writer resume: %w", err)
	}

	w.sessionURI = attrs.Metadata["Session-URI"]
	w.offset = offset
	w.size = offset + w.buffSize

	w.logger().WithFields(logrus.Fields{
		"path":          w.object.ObjectName(),
		"resumedOffset": offset,
		"bufferedBytes": w.buffSize,
	}).Debug("Resumed upload session")

	return nil
}
