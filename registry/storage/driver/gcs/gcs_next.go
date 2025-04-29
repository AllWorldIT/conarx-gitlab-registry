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

//go:build include_gcs

package gcs

import (
	"bytes"
	"context"

	// nolint: revive,gosec // imports-blocklist
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
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
		bucket:        params.bucket,
		rootDirectory: rootDirectory,
		email:         params.email,
		privateKey:    params.privateKey,
		client:        params.client,
		storageClient: params.storageClient,
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
	storageClient *storage.Client
	bucket        string
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
	var rc io.ReadCloser
	err := retry(func() error {
		var err error
		rc, err = d.storageClient.Bucket(d.bucket).Object(name).NewReader(ctx)
		return err
	})
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
	return retry(func() error {
		wc := d.storageClient.Bucket(d.bucket).Object(d.pathToKey(path)).NewWriter(ctx)
		wc.ContentType = "application/octet-stream"
		// nolint: gosec
		h := md5.New()
		_, err := h.Write(contents)
		if err != nil {
			return fmt.Errorf("calculating hash: %w", err)
		}
		wc.MD5 = h.Sum(nil)

		if len(contents) == 0 {
			logrus.WithFields(logrus.Fields{
				"path":   path,
				"length": len(contents),
				"stack":  string(debug.Stack()),
			}).Info("PutContent called with 0 bytes")
		}

		return putContentsCloseNext(wc, contents)
	})
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driverNext) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	res, err := getObjectNext(d.client, d.bucket, d.pathToKey(path), offset)
	if err != nil {
		var gcsErr *googleapi.Error
		if !errors.As(err, &gcsErr) {
			return nil, fmt.Errorf("fetching object: %w", err)
		}

		if gcsErr.Code == http.StatusNotFound {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}

		if gcsErr.Code == http.StatusRequestedRangeNotSatisfiable {
			obj, err := storageStatObjectNext(ctx, d.storageClient, d.bucket, d.pathToKey(path))
			if err != nil {
				return nil, fmt.Errorf("fetching meta information about the object: %w", err)
			}
			if offset == obj.Size {
				return io.NopCloser(bytes.NewReader(make([]byte, 0))), nil
			}
			return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: driverName}
		}
	}
	if res.Header.Get("Content-Type") == uploadSessionContentType {
		defer res.Body.Close()
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return res.Body, nil
}

func storageDeleteObjectNext(ctx context.Context, client *storage.Client, bucket, name string) error {
	return retry(func() error {
		return client.Bucket(bucket).Object(name).Delete(ctx)
	})
}

func storageStatObjectNext(ctx context.Context, client *storage.Client, bucket, name string) (*storage.ObjectAttrs, error) {
	var obj *storage.ObjectAttrs
	err := retry(func() error {
		var err error
		obj, err = client.Bucket(bucket).Object(name).Attrs(ctx)
		return err
	})
	return obj, err
}

func storageListObjectsNext(ctx context.Context, client *storage.Client, bucket string, q *storage.Query) (*storage.ObjectIterator, error) {
	var objs *storage.ObjectIterator
	err := retry(func() error {
		var err error
		objs = client.Bucket(bucket).Objects(ctx, q)
		return err
	})
	return objs, err
}

func storageCopyObjectNext(ctx context.Context, client *storage.Client, srcBucket, srcName, destBucket, destName string) (*storage.ObjectAttrs, error) {
	var obj *storage.ObjectAttrs
	err := retry(func() error {
		var err error
		src := client.Bucket(srcBucket).Object(srcName)
		dst := client.Bucket(destBucket).Object(destName)
		obj, err = dst.CopierFrom(src).Run(ctx)
		return err
	})
	return obj, err
}

func getObjectNext(client *http.Client, bucket, name string, offset int64) (*http.Response, error) {
	// copied from google.golang.org/cloud/storage#NewReader :
	// to set the additional "Range" header
	u := &url.URL{
		Scheme: "https",
		// https://cloud.google.com/storage/docs/request-endpoints#typical
		Host: "storage.googleapis.com",
		Path: fmt.Sprintf("/%s/%s", bucket, name),
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("creating new http request: %w", err)
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
	}
	var res *http.Response
	err = retry(func() error {
		var err error
		// nolint: bodyclose // body is closed bit later in the code
		res, err = client.Do(req)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("executing http request: %w", err)
	}
	if err := googleapi.CheckMediaResponse(res); err != nil {
		_ = res.Body.Close()
		return nil, fmt.Errorf("checking media response: %w", err)
	}
	return res, nil
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

// putChunkNext either completes upload or submits another chunk.
func putChunkNext(client *http.Client, sessionURI string, chunk []byte, from, totalSize int64) (int64, error) {
	// Documentation: https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
	bytesPut := int64(0)
	err := retry(func() error {
		req, err := http.NewRequest(http.MethodPut, sessionURI, bytes.NewReader(chunk))
		if err != nil {
			return fmt.Errorf("creating new http request: %w", err)
		}
		length := int64(len(chunk))
		to := from + length - 1
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
		if from == to+1 {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes */%v", size))
		} else {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes %v-%v/%v", from, to, size))
		}
		req.Header.Set("Content-Length", strconv.FormatInt(length, 10))

		resp, err := client.Do(req)
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
			bytesPut = end - from + 1
			return nil
		}
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return fmt.Errorf("committing resumable upload: %w", err)
		}
		bytesPut = to - from + 1
		return nil
	})
	return bytesPut, err
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driverNext) Writer(_ context.Context, path string, doAppend bool) (storagedriver.FileWriter, error) {
	writer := &writerNext{
		client:        d.client,
		storageClient: d.storageClient,
		bucket:        d.bucket,
		name:          d.pathToKey(path),
		buffer:        make([]byte, d.chunkSize),
	}

	if doAppend {
		err := writer.init(path)
		if err != nil {
			return nil, err
		}
	}
	return writer, nil
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driverNext) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	var fi storagedriver.FileInfoFields
	// try to get as file
	obj, err := storageStatObjectNext(ctx, d.storageClient, d.bucket, d.pathToKey(path))
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

	it, err := storageListObjectsNext(ctx, d.storageClient, d.bucket, query)
	if err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}

	attrs, err := it.Next()
	if err == iterator.Done {
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

	it, err := storageListObjectsNext(ctx, d.storageClient, d.bucket, query)
	if err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("fetching next page from iterator: %w", err)
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
	_, err := storageCopyObjectNext(ctx, d.storageClient, d.bucket, d.pathToKey(sourcePath), d.bucket, d.pathToKey(destPath))
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: driverName}
			}
		}
		return err
	}
	err = storageDeleteObjectNext(ctx, d.storageClient, d.bucket, d.pathToKey(sourcePath))
	// if deleting the file fails, log the error, but do not fail; the file was successfully copied,
	// and the original should eventually be cleaned when purging the uploads folder.
	if err != nil {
		logrus.Infof("error deleting file: %v due to %v", sourcePath, err)
	}
	return nil
}

// listAll recursively lists all names of objects stored at "prefix" and its subpaths.
func (d *driverNext) listAll(ctx context.Context, prefix string) ([]string, error) {
	list := make([]string, 0, 64)
	query := &storage.Query{}
	query.Prefix = prefix
	query.Versions = false
	it, err := storageListObjectsNext(ctx, d.storageClient, d.bucket, query)
	if err != nil {
		return nil, fmt.Errorf("listing objects: %w", err)
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
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
	prefix := d.pathToDirKey(path)
	keys, err := d.listAll(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
		for _, key := range keys {
			err := storageDeleteObjectNext(ctx, d.storageClient, d.bucket, key)
			// GCS only guarantees eventual consistency, so listAll might return
			// paths that no longer exist. If this happens, just ignore any not
			// found error
			if errors.Is(err, storage.ErrObjectNotExist) {
				err = nil
			}
			if err != nil {
				return fmt.Errorf("deleting object: %w", err)
			}
		}
		return nil
	}
	err = storageDeleteObjectNext(ctx, d.storageClient, d.bucket, d.pathToKey(path))
	if errors.Is(err, storage.ErrObjectNotExist) {
		return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return err
}

// DeleteFiles deletes a set of files concurrently, using a separate goroutine for each, up to a maximum of
// maxDeleteConcurrency. Returns the number of successfully deleted files and any errors. This method is idempotent, no
// error is returned if a file does not exist.
func (d *driverNext) DeleteFiles(ctx context.Context, paths []string) (int, error) {
	// collect errors from concurrent requests
	var errs error
	errCh := make(chan error)
	errDone := make(chan struct{})
	go func() {
		for err := range errCh {
			if err != nil {
				errs = multierror.Append(errs, err)
			}
		}
		errDone <- struct{}{}
	}()

	// count the number of successfully deleted files across concurrent requests
	count := 0
	countCh := make(chan struct{})
	countDone := make(chan struct{})
	go func() {
		for range countCh {
			count++
		}
		countDone <- struct{}{}
	}()

	var wg sync.WaitGroup
	// limit the number of active goroutines to maxDeleteConcurrency
	semaphore := make(chan struct{}, maxDeleteConcurrency)

	for _, path := range paths {
		// block if there are maxDeleteConcurrency goroutines
		semaphore <- struct{}{}
		wg.Add(1)

		go func(p string) {
			defer func() {
				wg.Done()
				// signal free spot for another goroutine
				<-semaphore
			}()

			if err := storageDeleteObjectNext(ctx, d.storageClient, d.bucket, d.pathToKey(p)); err != nil {
				if !errors.Is(err, storage.ErrObjectNotExist) {
					errCh <- err
					return
				}
			}
			// count successfully deleted files
			countCh <- struct{}{}
		}(path)
	}

	wg.Wait()
	close(semaphore)
	close(errCh)
	<-errDone
	close(countCh)
	<-countDone

	return count, errs
}

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// Returns ErrUnsupportedMethod if this driver has no privateKey
func (d *driverNext) URLFor(_ context.Context, path string, options map[string]any) (string, error) {
	if d.privateKey == nil {
		return "", storagedriver.ErrUnsupportedMethod{DriverName: driverName}
	}

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
		GoogleAccessID:  d.email,
		PrivateKey:      d.privateKey,
		Method:          methodString,
		Expires:         expiresTime,
		QueryParameters: storagedriver.CustomParams(options, customParamKeys),
		Scheme:          storage.SigningSchemeV4,
	}
	return storage.SignedURL(d.bucket, name, opts)
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
func startSessionNext(client *http.Client, bucket, name string) (uri string, err error) {
	u := &url.URL{
		Scheme: "https",
		// https://cloud.google.com/storage/docs/request-endpoints#typical
		Host:     "storage.googleapis.com",
		Path:     fmt.Sprintf("/upload/storage/v1/b/%v/o", bucket),
		RawQuery: fmt.Sprintf("uploadType=resumable&name=%v", name),
	}
	err = retry(func() error {
		req, err := http.NewRequest(http.MethodPost, u.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-Upload-Content-Type", "application/octet-stream")
		req.Header.Set("Content-Length", "0")
		resp, err := client.Do(req)
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
	client        *http.Client
	storageClient *storage.Client
	bucket        string
	name          string
	size          int64
	offset        int64
	closed        bool
	committed     bool
	canceled      bool
	sessionURI    string
	buffer        []byte
	buffSize      int64
}

// Cancel removes any written content from this FileWriter.
func (w *writerNext) Cancel() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	} else if w.committed {
		return storagedriver.ErrAlreadyCommited
	}
	w.canceled = true

	err := storageDeleteObjectNext(context.Background(), w.storageClient, w.bucket, w.name)
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
		context := context.Background()
		wc := w.storageClient.Bucket(w.bucket).Object(w.name).NewWriter(context)
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
	w.size = w.offset + w.buffSize
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
			context := context.Background()
			wc := w.storageClient.Bucket(w.bucket).Object(w.name).NewWriter(context)
			wc.ContentType = "application/octet-stream"
			return putContentsCloseNext(wc, w.buffer[0:w.buffSize])
		})
		if err != nil {
			return err
		}
		w.size = w.offset + w.buffSize
		w.buffSize = 0
		return nil
	}
	size := w.offset + w.buffSize
	var nn int64
	// NOTE(prozlach): loop must be performed at least once to ensure the file
	// is committed even when the buffer is empty. The empty/first loop with
	// zero-length will actually commit the file.
	for {
		n, err := putChunkNext(w.client, w.sessionURI, w.buffer[nn:w.buffSize], w.offset, size)
		nn += n
		w.offset += n
		w.size = w.offset
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
		w.sessionURI, err = startSessionNext(w.client, w.bucket, w.name)
	}
	if err != nil {
		return err
	}
	nn, err := putChunkNext(w.client, w.sessionURI, w.buffer[0:chunkSize], w.offset, -1)
	w.offset += nn
	if w.offset > w.size {
		w.size = w.offset
	}
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
		if w.buffSize == int64(cap(w.buffer)) {
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
func (w *writerNext) init(path string) error {
	res, err := getObjectNext(w.client, w.bucket, w.name, 0)
	if err != nil {
		var gcsErr *googleapi.Error
		if errors.As(err, &gcsErr) && gcsErr.Code == http.StatusNotFound {
			return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
		}

		return fmt.Errorf("fetching object: %w", err)
	}
	defer res.Body.Close()
	if res.Header.Get("Content-Type") != uploadSessionContentType {
		return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	offset, err := strconv.ParseInt(res.Header.Get("X-Goog-Meta-Offset"), 10, 64)
	if err != nil {
		return fmt.Errorf("parsing `X-Goog-Meta-Offset` HTTP header: %w", err)
	}
	buffer, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading `tail` data during writer resume: %w", err)
	}
	w.sessionURI = res.Header.Get("X-Goog-Meta-Session-URI")
	w.buffSize = int64(copy(w.buffer, buffer)) // nolint: gosec // copy() is always going to be non-negative
	w.offset = offset
	w.size = offset + w.buffSize
	return nil
}
