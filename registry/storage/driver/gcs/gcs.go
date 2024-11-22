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
	"crypto/md5" //nolint: gosec
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/registry/internal"
	dstorage "github.com/docker/distribution/registry/storage"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
	"github.com/docker/distribution/registry/storage/internal/metrics"
	"github.com/hashicorp/go-multierror"
	"github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	driverName = "gcs"

	uploadSessionContentType = "application/x-docker-upload-session"
	minChunkSize             = 256 * 1024
	defaultChunkSize         = 20 * minChunkSize
	defaultMaxConcurrency    = 50
	minConcurrency           = 25
	maxDeleteConcurrency     = 150
	maxWalkConcurrency       = 100
	maxTries                 = 5
)

var rangeHeader = regexp.MustCompile(`^bytes=([0-9])+-([0-9]+)$`)

// customGitlabGoogle... are the query params appended to gcs signed redirect url
const (
	customGitlabGoogleNamespaceIdParam = "x-goog-custom-audit-gitlab-namespace-id"
	customGitlabGoogleProjectIdParam   = "x-goog-custom-audit-gitlab-project-id"
	customGitlabGoogleAuthTypeParam    = "x-goog-custom-audit-gitlab-auth-type"
	customGitlabGoogleObjectSizeParam  = "x-goog-custom-audit-gitlab-size-bytes"
)

// customParamKeys is the mapping between gitlab keys to gcs signed-redirect-url query parameter keys
var customParamKeys = map[string]string{
	dstorage.NamespaceIdKey: customGitlabGoogleNamespaceIdParam,
	dstorage.ProjectIdKey:   customGitlabGoogleProjectIdParam,
	dstorage.AuthTypeKey:    customGitlabGoogleAuthTypeParam,
	dstorage.SizeBytesKey:   customGitlabGoogleObjectSizeParam,
}

// driverParameters is a struct that encapsulates all the driver parameters after all values have been set
type driverParameters struct {
	bucket        string
	email         string
	privateKey    []byte
	client        *http.Client
	storageClient *storage.Client
	rootDirectory string
	chunkSize     int

	// maxConcurrency limits the number of concurrent driver operations
	// to GCS, which ultimately increases reliability of many simultaneous
	// pushes by ensuring we aren't DoSing our own server with many
	// connections.
	maxConcurrency uint64

	// parallelWalk enables or disables concurrently walking the filesystem.
	parallelWalk bool
}

func init() {
	factory.Register(driverName, &gcsDriverFactory{})
}

// gcsDriverFactory implements the factory.StorageDriverFactory interface
type gcsDriverFactory struct{}

// Create StorageDriver from parameters
func (factory *gcsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// driver is a storagedriver.StorageDriver implementation backed by GCS
// Objects are stored at absolute keys in the provided bucket.
type driver struct {
	client        *http.Client
	storageClient *storage.Client
	bucket        string
	email         string
	privateKey    []byte
	rootDirectory string
	chunkSize     int
	parallelWalk  bool
}

// Wrapper wraps `driver` with a throttler, ensuring that no more than N
// GCS actions can occur concurrently. The default limit is 75.
type Wrapper struct {
	baseEmbed
}

type baseEmbed struct {
	base.Base
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - bucket
func FromParameters(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	params, err := parseParameters(parameters)
	if err != nil {
		return nil, err
	}

	return New(params)
}

func parseParameters(parameters map[string]interface{}) (*driverParameters, error) {
	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("no bucket parameter provided")
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	chunkSize := defaultChunkSize
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int, uint, int32, uint32, uint64, int64:
			chunkSize = int(reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int())
		default:
			return nil, fmt.Errorf("invalid valud for chunksize: %#v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("the chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}

		if chunkSize%minChunkSize != 0 {
			return nil, fmt.Errorf("chunksize should be a multiple of %d", minChunkSize)
		}
	}

	var ts oauth2.TokenSource
	jwtConf := new(jwt.Config)
	if keyfile, ok := parameters["keyfile"]; ok {
		jsonKey, err := os.ReadFile(fmt.Sprint(keyfile))
		if err != nil {
			return nil, err
		}
		jwtConf, err = google.JWTConfigFromJSON(jsonKey, storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
		ts = jwtConf.TokenSource(context.Background())
	} else if credentials, ok := parameters["credentials"]; ok {
		credentialMap, ok := credentials.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("the credentials were not specified in the correct format")
		}

		stringMap := map[string]interface{}{}
		for k, v := range credentialMap {
			key, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("one of the credential keys was not a string: %s", fmt.Sprint(k))
			}
			stringMap[key] = v
		}

		data, err := json.Marshal(stringMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal gcs credentials to json")
		}

		jwtConf, err = google.JWTConfigFromJSON(data, storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
		ts = jwtConf.TokenSource(context.Background())
	} else {
		var err error
		ts, err = google.DefaultTokenSource(context.Background(), storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
	}

	maxConcurrency, err := base.GetLimitFromParameter(parameters["maxconcurrency"], minConcurrency, defaultMaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("maxconcurrency config error: %s", err)
	}

	storageClient, err := storage.NewClient(context.Background(), option.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("storage client error: %s", err)
	}

	parallelWalkBool, err := parse.Bool(parameters, "parallelwalk", false)
	if err != nil {
		return nil, err
	}

	return &driverParameters{
		bucket:         fmt.Sprint(bucket),
		rootDirectory:  fmt.Sprint(rootDirectory),
		email:          jwtConf.Email,
		privateKey:     jwtConf.PrivateKey,
		client:         oauth2.NewClient(context.Background(), ts),
		storageClient:  storageClient,
		chunkSize:      chunkSize,
		maxConcurrency: maxConcurrency,
		parallelWalk:   parallelWalkBool,
	}, nil
}

// New constructs a new driver
func New(params *driverParameters) (storagedriver.StorageDriver, error) {
	rootDirectory := strings.Trim(params.rootDirectory, "/")
	if rootDirectory != "" {
		rootDirectory += "/"
	}
	if params.chunkSize <= 0 || params.chunkSize%minChunkSize != 0 {
		return nil, fmt.Errorf("invalid chunksize: %d is not a positive multiple of %d", params.chunkSize, minChunkSize)
	}
	d := &driver{
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

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
// This should primarily be used for small objects.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	name := d.pathToKey(path)
	var rc io.ReadCloser
	err := retry(func() error {
		var err error
		rc, err = d.storageClient.Bucket(d.bucket).Object(name).NewReader(ctx)
		return err
	})
	if err == storage.ErrObjectNotExist {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// PutContent stores the []byte content at a location designated by "path".
// This should primarily be used for small objects.
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	return retry(func() error {
		wc := d.storageClient.Bucket(d.bucket).Object(d.pathToKey(path)).NewWriter(ctx)
		wc.ContentType = "application/octet-stream"
		// nolint: gosec
		h := md5.New()
		h.Write(contents)
		wc.MD5 = h.Sum(nil)

		if len(contents) == 0 {
			logrus.WithFields(logrus.Fields{
				"path":   path,
				"length": len(contents),
				"stack":  string(debug.Stack()),
			}).Info("PutContent called with 0 bytes")
		}

		return putContentsClose(wc, contents)
	})
}

// Reader retrieves an io.ReadCloser for the content stored at "path"
// with a given byte offset.
// May be used to resume reading a stream by providing a nonzero offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	res, err := getObject(d.client, d.bucket, d.pathToKey(path), offset)
	if err != nil {
		var gcsErr *googleapi.Error
		if errors.As(err, &gcsErr) {
			if gcsErr.Code == http.StatusNotFound {
				return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
			}

			if gcsErr.Code == http.StatusRequestedRangeNotSatisfiable {
				obj, err := storageStatObject(ctx, d.storageClient, d.bucket, d.pathToKey(path))
				if err != nil {
					return nil, err
				}
				if offset == obj.Size {
					return io.NopCloser(bytes.NewReader([]byte{})), nil
				}
				return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: driverName}
			}
		}
		return nil, err
	}
	if res.Header.Get("Content-Type") == uploadSessionContentType {
		defer res.Body.Close()
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return res.Body, nil
}

func getObject(client *http.Client, bucket, name string, offset int64) (*http.Response, error) {
	// copied from google.golang.org/cloud/storage#NewReader :
	// to set the additional "Range" header
	u := &url.URL{
		Scheme: "https",
		Host:   "storage.googleapis.com",
		Path:   fmt.Sprintf("/%s/%s", bucket, name),
	}
	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%v-", offset))
	}
	var res *http.Response
	err = retry(func() error {
		var err error
		//nolint: bodyclose // body is closed bit later in the code
		res, err = client.Do(req)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := googleapi.CheckMediaResponse(res); err != nil {
		_ = res.Body.Close()
		return nil, err
	}
	return res, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	writer := &writer{
		client:        d.client,
		storageClient: d.storageClient,
		bucket:        d.bucket,
		name:          d.pathToKey(path),
		buffer:        make([]byte, d.chunkSize),
	}

	if append {
		err := writer.init(path)
		if err != nil {
			return nil, err
		}
	}
	return writer, nil
}

type writer struct {
	client        *http.Client
	storageClient *storage.Client
	bucket        string
	name          string
	size          int64
	offset        int64
	closed        bool
	sessionURI    string
	buffer        []byte
	buffSize      int
}

// Cancel removes any written content from this FileWriter.
func (w *writer) Cancel() error {
	w.closed = true
	err := storageDeleteObject(context.Background(), w.storageClient, w.bucket, w.name)
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				err = nil
			}
		}
	}
	return err
}

func (w *writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	err := w.writeChunk()
	if err != nil {
		return err
	}

	// Copy the remaining bytes from the buffer to the upload session
	// Normally buffSize will be smaller than minChunkSize. However, in the
	// unlikely event that the upload session failed to start, this number could be higher.
	// In this case we can safely clip the remaining bytes to the minChunkSize
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
		return putContentsClose(wc, w.buffer[0:w.buffSize])
	})
	if err != nil {
		return err
	}
	w.size = w.offset + int64(w.buffSize)
	w.buffSize = 0
	return nil
}

func putContentsClose(wc *storage.Writer, contents []byte) error {
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

// Commit flushes all content written to this FileWriter and makes it
// available for future calls to StorageDriver.GetContent and
// StorageDriver.Reader.
func (w *writer) Commit() error {
	if err := w.checkClosed(); err != nil {
		return err
	}
	w.closed = true

	// no session started yet just perform a simple upload
	if w.sessionURI == "" {
		err := retry(func() error {
			context := context.Background()
			wc := w.storageClient.Bucket(w.bucket).Object(w.name).NewWriter(context)
			wc.ContentType = "application/octet-stream"
			return putContentsClose(wc, w.buffer[0:w.buffSize])
		})
		if err != nil {
			return err
		}
		w.size = w.offset + int64(w.buffSize)
		w.buffSize = 0
		return nil
	}
	size := w.offset + int64(w.buffSize)
	var nn int
	// loop must be performed at least once to ensure the file is committed even when
	// the buffer is empty
	for {
		n, err := putChunk(w.client, w.sessionURI, w.buffer[nn:w.buffSize], w.offset, size)
		nn += int(n)
		w.offset += n
		w.size = w.offset
		if err != nil {
			w.buffSize = copy(w.buffer, w.buffer[nn:w.buffSize])
			return err
		}
		if nn == w.buffSize {
			break
		}
	}
	w.buffSize = 0
	return nil
}

func (w *writer) checkClosed() error {
	if w.closed {
		return fmt.Errorf("Writer already closed")
	}
	return nil
}

func (w *writer) writeChunk() error {
	var err error
	// chunks can be uploaded only in multiples of minChunkSize
	// chunkSize is a multiple of minChunkSize less than or equal to buffSize
	chunkSize := w.buffSize - (w.buffSize % minChunkSize)
	if chunkSize == 0 {
		return nil
	}
	// if their is no sessionURI yet, obtain one by starting the session
	if w.sessionURI == "" {
		w.sessionURI, err = startSession(w.client, w.bucket, w.name)
	}
	if err != nil {
		return err
	}
	nn, err := putChunk(w.client, w.sessionURI, w.buffer[0:chunkSize], w.offset, -1)
	w.offset += nn
	if w.offset > w.size {
		w.size = w.offset
	}
	// shift the remaining bytes to the start of the buffer
	w.buffSize = copy(w.buffer, w.buffer[int(nn):w.buffSize])

	return err
}

func (w *writer) Write(p []byte) (int, error) {
	err := w.checkClosed()
	if err != nil {
		return 0, err
	}

	var nn int
	for nn < len(p) {
		n := copy(w.buffer[w.buffSize:], p[nn:])
		w.buffSize += n
		if w.buffSize == cap(w.buffer) {
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
func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) init(path string) error {
	res, err := getObject(w.client, w.bucket, w.name, 0)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.Header.Get("Content-Type") != uploadSessionContentType {
		return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	offset, err := strconv.ParseInt(res.Header.Get("X-Goog-Meta-Offset"), 10, 64)
	if err != nil {
		return err
	}
	buffer, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	w.sessionURI = res.Header.Get("X-Goog-Meta-Session-URI")
	w.buffSize = copy(w.buffer, buffer)
	w.offset = offset
	w.size = offset + int64(w.buffSize)
	return nil
}

type request func() error

func retry(req request) error {
	backoff := time.Second
	var err error
	for i := 0; i < maxTries; i++ {
		err = req()
		if err == nil {
			return nil
		}

		status, ok := err.(*googleapi.Error)
		if !ok || (status.Code != 429 && status.Code < http.StatusInternalServerError) {
			return err
		}

		if status.Code == http.StatusTooManyRequests {
			metrics.StorageRatelimit()
		}

		//nolint:gosec // this is just a random number for rety backoff
		time.Sleep(backoff - time.Second + (time.Duration(rand.Int31n(1000)) * time.Millisecond))
		if i <= 4 {
			backoff *= 2
		}
	}

	var gerr *googleapi.Error
	if ok := errors.As(err, &gerr); ok && gerr.Code == http.StatusTooManyRequests {
		return storagedriver.TooManyRequestsError{Cause: err}
	}

	return err
}

// Stat retrieves the FileInfo for the given path, including the current
// size in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	var fi storagedriver.FileInfoFields
	// try to get as file
	obj, err := storageStatObject(ctx, d.storageClient, d.bucket, d.pathToKey(path))
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

	it, err := storageListObjects(ctx, d.storageClient, d.bucket, query)
	if err != nil {
		return nil, err
	}

	attrs, err := it.Next()
	if err == iterator.Done {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	if err != nil {
		return nil, err
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
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	query := &storage.Query{}
	query.Delimiter = "/"
	query.Prefix = d.pathToDirKey(path)
	list := make([]string, 0, 64)

	it, err := storageListObjects(ctx, d.storageClient, d.bucket, query)
	if err != nil {
		return nil, err
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
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
func (d *driver) Move(ctx context.Context, sourcePath, destPath string) error {
	_, err := storageCopyObject(ctx, d.storageClient, d.bucket, d.pathToKey(sourcePath), d.bucket, d.pathToKey(destPath))
	if err != nil {
		var gerr *googleapi.Error
		if errors.As(err, &gerr) {
			if gerr.Code == http.StatusNotFound {
				return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: driverName}
			}
		}
		return err
	}
	err = storageDeleteObject(ctx, d.storageClient, d.bucket, d.pathToKey(sourcePath))
	// if deleting the file fails, log the error, but do not fail; the file was successfully copied,
	// and the original should eventually be cleaned when purging the uploads folder.
	if err != nil {
		logrus.Infof("error deleting file: %v due to %v", sourcePath, err)
	}
	return nil
}

// listAll recursively lists all names of objects stored at "prefix" and its subpaths.
func (d *driver) listAll(ctx context.Context, prefix string) ([]string, error) {
	list := make([]string, 0, 64)
	query := &storage.Query{}
	query.Prefix = prefix
	query.Versions = false
	it, err := storageListObjects(ctx, d.storageClient, d.bucket, query)
	if err != nil {
		return nil, err
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
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
func (d *driver) Delete(ctx context.Context, path string) error {
	prefix := d.pathToDirKey(path)
	keys, err := d.listAll(ctx, prefix)
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
		for _, key := range keys {
			err := storageDeleteObject(ctx, d.storageClient, d.bucket, key)
			// GCS only guarantees eventual consistency, so listAll might return
			// paths that no longer exist. If this happens, just ignore any not
			// found error
			if err == storage.ErrObjectNotExist {
				err = nil
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	err = storageDeleteObject(ctx, d.storageClient, d.bucket, d.pathToKey(path))
	if err == storage.ErrObjectNotExist {
		return storagedriver.PathNotFoundError{Path: path, DriverName: driverName}
	}
	return err
}

// DeleteFiles deletes a set of files concurrently, using a separate goroutine for each, up to a maximum of
// maxDeleteConcurrency. Returns the number of successfully deleted files and any errors. This method is idempotent, no
// error is returned if a file does not exist.
func (d *driver) DeleteFiles(ctx context.Context, paths []string) (int, error) {
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

			if err := storageDeleteObject(ctx, d.storageClient, d.bucket, d.pathToKey(p)); err != nil {
				if err != storage.ErrObjectNotExist {
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

func storageDeleteObject(ctx context.Context, client *storage.Client, bucket, name string) error {
	return retry(func() error {
		return client.Bucket(bucket).Object(name).Delete(ctx)
	})
}

func storageStatObject(ctx context.Context, client *storage.Client, bucket, name string) (*storage.ObjectAttrs, error) {
	var obj *storage.ObjectAttrs
	err := retry(func() error {
		var err error
		obj, err = client.Bucket(bucket).Object(name).Attrs(ctx)
		return err
	})
	return obj, err
}

func storageListObjects(ctx context.Context, client *storage.Client, bucket string, q *storage.Query) (*storage.ObjectIterator, error) {
	var objs *storage.ObjectIterator
	err := retry(func() error {
		var err error
		objs = client.Bucket(bucket).Objects(ctx, q)
		return err
	})
	return objs, err
}

func storageCopyObject(ctx context.Context, client *storage.Client, srcBucket, srcName, destBucket, destName string) (*storage.ObjectAttrs, error) {
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

// for testing purposes
var systemClock internal.Clock = clock.New()

// URLFor returns a URL which may be used to retrieve the content stored at
// the given path, possibly using the given options.
// Returns ErrUnsupportedMethod if this driver has no privateKey
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
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
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// WalkParallel traverses a filesystem defined within driver in parallel, starting
// from the given path, calling f on each file.
func (d *driver) WalkParallel(ctx context.Context, path string, f storagedriver.WalkFn) error {
	// If the ParallelWalk feature flag is not set, fall back to standard sequential walk.
	if !d.parallelWalk {
		return d.Walk(ctx, path, f)
	}

	return storagedriver.WalkFallbackParallel(ctx, d, maxWalkConcurrency, path, f)
}

// ExistsPath is a performance optimized version of Stat to be used specifically for checking if a given path (not
// object) exists. For GCS this means doing a single list request with no recursion and minimum attribute retrieval.
func (d *driver) ExistsPath(ctx context.Context, path string) (bool, error) {
	prefix := d.pathToDirKey(path)

	query := &storage.Query{
		Prefix:     prefix,
		Delimiter:  "/",                     // do not walk into child prefixes (if any)
		Projection: storage.ProjectionNoACL, // no need for Owner and ACL data (faster)
	}

	// We don't care about attributes, we only care if the prefix exists, but we have to explicitly request one
	// attribute as the default is all and that comes with a performance penalty.
	if err := query.SetAttrSelection([]string{"Name"}); err != nil {
		return false, err
	}

	it := d.storageClient.Bucket(d.bucket).Objects(ctx, query)
	it.PageInfo().MaxSize = 1

	// we only fetch one page, we don't care if there are others
	if _, err := it.Next(); err != nil {
		if errors.Is(err, iterator.Done) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func startSession(client *http.Client, bucket, name string) (uri string, err error) {
	u := &url.URL{
		Scheme:   "https",
		Host:     "www.googleapis.com",
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
			return err
		}
		defer resp.Body.Close()
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return err
		}
		uri = resp.Header.Get("Location")
		return nil
	})
	return uri, err
}

func putChunk(client *http.Client, sessionURI string, chunk []byte, from, totalSize int64) (int64, error) {
	bytesPut := int64(0)
	err := retry(func() error {
		req, err := http.NewRequest(http.MethodPut, sessionURI, bytes.NewReader(chunk))
		if err != nil {
			return err
		}
		length := int64(len(chunk))
		to := from + length - 1
		size := "*"
		if totalSize >= 0 {
			size = strconv.FormatInt(totalSize, 10)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		if from == to+1 {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes */%v", size))
		} else {
			req.Header.Set("Content-Range", fmt.Sprintf("bytes %v-%v/%v", from, to, size))
		}
		req.Header.Set("Content-Length", strconv.FormatInt(length, 10))

		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if totalSize < 0 && resp.StatusCode == 308 {
			groups := rangeHeader.FindStringSubmatch(resp.Header.Get("Range"))
			end, err := strconv.ParseInt(groups[2], 10, 64)
			if err != nil {
				return err
			}
			bytesPut = end - from + 1
			return nil
		}
		err = googleapi.CheckMediaResponse(resp)
		if err != nil {
			return err
		}
		bytesPut = to - from + 1
		return nil
	})
	return bytesPut, err
}

func (d *driver) pathToKey(path string) string {
	return strings.TrimSpace(strings.TrimRight(d.rootDirectory+strings.TrimLeft(path, "/"), "/"))
}

func (d *driver) pathToDirKey(path string) string {
	return d.pathToKey(path) + "/"
}

func (d *driver) keyToPath(key string) string {
	return "/" + strings.Trim(strings.TrimPrefix(key, d.rootDirectory), "/")
}

// GCSBucketKey returns the GCS bucket key for the given storage driver path.
func (d *Wrapper) GCSBucketKey(path string) string {
	// This is currently used exclusively by the Google Cloud CDN middleware. During an online migration we have to
	// maintain two separate storage drivers, each with a different root directory. Because of that we have no other
	// option than hand over the object full path construction to the underlying GCS driver, instead of manually
	// concatenating the CDN endpoint with the object path.
	return d.StorageDriver.(*base.Regulator).StorageDriver.(*driver).pathToKey(path)
}
