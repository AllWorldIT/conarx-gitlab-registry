// Package azure provides a storagedriver.StorageDriver implementation to
// store blobs in Microsoft Azure Blob Storage Service.
package v2

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/cenkalti/backoff/v4"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/docker/distribution/registry/storage/driver/base"
)

const DriverName = "azure_v2"

const (
	maxChunkSize = 4 * 1024 * 1024

	// NOTE(prozlach): values chosen arbitrarily
	DefaultPoolInitialInterval = 100 * time.Millisecond
	DefaultPoolMaxInterval     = 1 * time.Second
	DefaultPoolMaxElapsedTime  = 5 * time.Second

	DefaultSignedURLExpiry = 20 * time.Minute
)

var ErrCopyStatusPending = errors.New("copy still pending")

type driver struct {
	common.Pather

	client *container.Client
	signer urlSigner

	poolInitialInterval time.Duration
	poolMaxInterval     time.Duration
	poolMaxElapsedTime  time.Duration
}

type baseEmbed struct{ base.Base }

// Driver is a storagedriver.StorageDriver implementation backed by
// Microsoft Azure Blob Storage Service.
type Driver struct{ baseEmbed }

type AzureDriverFactory struct{}

func (*AzureDriverFactory) Create(parameters map[string]any) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// FromParameters constructs a new Driver with a given parameters map.
func FromParameters(parameters map[string]any) (storagedriver.StorageDriver, error) {
	params, err := ParseParameters(parameters)
	if err != nil {
		return nil, err
	}

	return New(params)
}

// New constructs a new Driver with the given Azure Storage Account credentials
func New(in any) (storagedriver.StorageDriver, error) {
	params := in.(*driverParameters)
	switch params.credentialsType {
	case common.CredentialsTypeSharedKey:
		return newSharedKeyCredentialsClient(params)
	case common.CredentialsTypeClientSecret, common.CredentialsTypeDefaultCredentials:
		return newTokenClient(params)
	default:
		return nil, fmt.Errorf("invalid credentials type: %q", params.credentialsType)
	}
}

// Implement the storagedriver.StorageDriver interface.
func (*driver) Name() string {
	return DriverName
}

// GetContent retrieves the content stored at "targetPath" as a []byte.
func (d *driver) GetContent(ctx context.Context, targetPath string) ([]byte, error) {
	resp, err := d.client.NewBlobClient(d.PathToKey(targetPath)).DownloadStream(ctx, nil)
	if err != nil {
		if is404(err) {
			return nil, storagedriver.PathNotFoundError{Path: targetPath, DriverName: DriverName}
		}
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	// max size for block blobs uploaded via single "Put Blob" for version after "2016-05-31"
	// https://docs.microsoft.com/en-us/rest/api/storageservices/put-blob#remarks
	if len(contents) > blockblob.MaxUploadBlobBytes {
		return fmt.Errorf(
			"uploading %d bytes with PutContent is not supported; limit: %d bytes",
			len(contents),
			blockblob.MaxUploadBlobBytes,
		)
	}

	// Historically, blobs uploaded via PutContent used to be of type AppendBlob
	// (https://github.com/distribution/distribution/pull/1438). We can't replace
	// these blobs atomically via a single "Put Blob" operation without
	// deleting them first. Once we detect they are BlockBlob type, we can
	// overwrite them with an atomically "Put Blob" operation.
	//
	// While we delete the blob and create a new one, there will be a small
	// window of inconsistency and if the Put Blob fails, we may end up with
	// losing the existing data while migrating it to BlockBlob type. However,
	// expectation isthe clients pushing will be retrying when they get an error
	// response.
	blobName := d.PathToKey(path)
	blobRef := d.client.NewBlobClient(blobName)
	props, err := blobRef.GetProperties(ctx, nil)
	if err != nil && !is404(err) {
		return fmt.Errorf("failed to get blob properties: %w", err)
	}
	if err == nil && props.BlobType != nil && *props.BlobType != blob.BlobTypeBlockBlob {
		if _, err := blobRef.Delete(ctx, nil); err != nil {
			return fmt.Errorf("failed to delete legacy blob (%s): %w", *props.BlobType, err)
		}
	}

	_, err = d.client.NewBlockBlobClient(blobName).UploadBuffer(ctx, contents, nil)
	if err != nil {
		return fmt.Errorf("creating new block blob client: %w", err)
	}
	return nil
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	blobRef := d.client.NewBlobClient(d.PathToKey(path))
	options := blob.DownloadStreamOptions{
		Range: blob.HTTPRange{
			Offset: offset,
		},
	}
	props, err := blobRef.GetProperties(ctx, nil)
	if err != nil {
		if is404(err) {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
		}
		return nil, fmt.Errorf("failed to get blob properties: %v", err)
	}
	if props.ContentLength == nil {
		return nil, fmt.Errorf("missing ContentLength: %s", path)
	}
	size := *props.ContentLength
	if offset >= size {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	resp, err := blobRef.DownloadStream(ctx, &options)
	if err != nil {
		if is404(err) {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
		}
		return nil, err
	}
	return resp.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, doAppend bool) (storagedriver.FileWriter, error) {
	blobName := d.PathToKey(path)
	blobRef := d.client.NewBlobClient(blobName)

	props, err := blobRef.GetProperties(ctx, nil)
	blobExists := true
	if err != nil {
		if !is404(err) {
			return nil, fmt.Errorf("getting blob properties: %w", err)
		}
		blobExists = false
	}

	var size int64
	if blobExists {
		if doAppend {
			if props.ContentLength == nil {
				return nil, fmt.Errorf("missing ContentLength: %s", blobName)
			}
			size = *props.ContentLength
		} else {
			_, err = blobRef.Delete(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("deleting existing blob before write: %w", err)
			}
			_, err = d.client.NewAppendBlobClient(blobName).Create(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("creating new append blob: %w", err)
			}
		}
	} else {
		if doAppend {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
		}
		_, err = d.client.NewAppendBlobClient(blobName).Create(ctx, nil)
		if err != nil {
			return nil, fmt.Errorf("creating new append blob: %w", err)
		}
	}

	return d.newWriter(ctx, d.PathToKey(path), size), nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	// If we try to get "/" as a blob, pathToKey will return "" when no root
	// directory is specified and we are not in legacy path mode, which causes
	// Azure to return a **400** when that object doesn't exist. So we need to
	// skip to trying to list the blobs under "/", which should result in zero
	// blobs, so we can return the expected 404 if we don't find any.
	if path != "/" {
		blobName := d.PathToKey(path)
		blobRef := d.client.NewBlobClient(blobName)
		// Check if the path is a blob
		props, err := blobRef.GetProperties(ctx, nil)
		if err == nil {
			var missing []string
			if props.ContentLength == nil {
				missing = append(missing, "ContentLength")
			}
			if props.LastModified == nil {
				missing = append(missing, "LastModified")
			}

			if len(missing) > 0 {
				return nil, fmt.Errorf("missing required prroperties (%s) for blob %q", strings.Join(missing, ","), blobName)
			}

			return storagedriver.FileInfoInternal{
				FileInfoFields: storagedriver.FileInfoFields{
					Path:    path,
					Size:    *props.ContentLength,
					ModTime: *props.LastModified,
					IsDir:   false,
				},
			}, nil
		}

		if !is404(err) {
			return nil, fmt.Errorf("fetching blob %q properties: %w", blobName, err)
		}

		// There is no such blob, let's see if this is a virtual-container.
	}

	pager := d.client.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:     to.Ptr(d.PathToDirKey(path)),
		MaxResults: to.Ptr((int32)(1)),
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("next page when listing blobs: %w", err)
		}
		if len(resp.Segment.BlobItems) > 0 {
			// path is a virtual container
			return storagedriver.FileInfoInternal{
				FileInfoFields: storagedriver.FileInfoFields{
					Path:  path,
					IsDir: true,
				},
			}, nil
		}
	}

	// path is not a blob or virtual container
	return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
}

// List returns a list of objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {
	prefix := d.PathToDirKey(path)

	// If we aren't using a particular root directory, we should not add the extra
	// ending slash that pathToDirKey adds.
	if !d.HasRootDirectory() && path == "/" {
		prefix = d.PathToKey(path)
	}

	list, err := d.listImpl(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if path != "/" && len(list) == 0 {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
	}

	return list, nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
// NOTE(prozlach): Azure SDK allows for synchronous copy of files up to 256MiB
// in size, bigger blobs need to be copied asynchronously. In order to keep
// things (esp. testing) simple, we use asynchronous copying for all blob
// sizes.
func (d *driver) Move(ctx context.Context, sourcePath, destPath string) error {
	srcBlobRef := d.client.NewBlobClient(d.PathToKey(sourcePath))
	// NOTE(prozlach): No need to sign the src URL, as the credentials for the
	// dst blob will be used for accessing src blob when calling StartCopyFromURL()
	srcBlobURL := srcBlobRef.URL()

	dstBlobRef := d.client.NewBlobClient(d.PathToKey(destPath))
	resp, err := dstBlobRef.StartCopyFromURL(ctx, srcBlobURL, nil)
	if err != nil {
		if is404(err) {
			return storagedriver.PathNotFoundError{Path: sourcePath, DriverName: DriverName}
		}
		return err
	}

	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(d.poolInitialInterval),
		backoff.WithMaxInterval(d.poolMaxInterval),
		backoff.WithMaxElapsedTime(d.poolMaxElapsedTime),
	)
	ctxB := backoff.WithContext(b, ctx)

	// Operation to check copy status
	operation := func() error {
		props, err := dstBlobRef.GetProperties(ctx, nil)
		if err != nil {
			// NOTE(prozlach): We do not treat this as a permament error and
			// retry instead as this may be a transient error and the copy
			// operation may still be progressing on the Azure side.
			// In the worst case we will abort the whole copy operation after
			// `d.poolMaxElapsedTime` has been reached in case when this error
			// was a permament one after all due to e.g. network connectivity
			// issues, but this seems like a lesser evil than letting the copy
			// operation finish in the background and having both src file and
			// dst file in the backend.
			return fmt.Errorf("getting blob properties: %w", err)
		}

		if props.CopyStatus == nil {
			return errors.New("copy status has not been set")
		}

		switch *props.CopyStatus {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypePending:
			return ErrCopyStatusPending
		case blob.CopyStatusTypeAborted:
			if props.CopyStatusDescription != nil {
				err = fmt.Errorf("move blob with copy id %s has been aborted: %s", *props.CopyID, *props.CopyStatusDescription)
			} else {
				err = fmt.Errorf("move blob with copy id %s has been aborted", *props.CopyID)
			}
			return backoff.Permanent(err)
		case blob.CopyStatusTypeFailed:
			if props.CopyStatusDescription != nil {
				err = fmt.Errorf("move blob with copy id %s has failed on the Azure backend: %s", *props.CopyID, *props.CopyStatusDescription)
			} else {
				err = fmt.Errorf("move blob with copy id %s has failed on the Azure backend", *props.CopyID)
			}
			return backoff.Permanent(err)
		default:
			// NOTE(prozlach): this may be a transient error, give it a benefit
			// of the doubt and retry until we have a solid signal to abort
			return fmt.Errorf("unknown copy status: %s", *props.CopyStatus)
		}
	}

	// Use backoff retry for polling
	err = backoff.Retry(operation, ctxB)
	if err != nil {
		if errors.Is(err, ErrCopyStatusPending) {
			// Blob copy has not finished yet and we can't wait any longer.
			// Abort the operation and return the error.
			if _, errAbort := dstBlobRef.AbortCopyFromURL(ctx, *resp.CopyID, nil); errAbort != nil {
				return fmt.Errorf("aborting copy operation: %w, while handling move operation timeout", errAbort)
			}
			return fmt.Errorf("move blob did not finish after %s", b.GetElapsedTime())
		}
		return fmt.Errorf("move blob: %w", err)
	}

	_, err = srcBlobRef.Delete(ctx, nil)
	if err != nil {
		return fmt.Errorf("deleting source blob: %w", err)
	}
	return nil
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {
	blobRef := d.client.NewBlobClient(d.PathToKey(path))
	_, err := blobRef.Delete(ctx, nil)
	if err == nil {
		// was a blob and deleted, return
		return nil
	}
	if !is404(err) {
		return fmt.Errorf("deleting blob %s: %w", path, err)
	}

	// Not a blob, see if path is a virtual container with blobs
	blobs, err := d.listBlobs(ctx, d.PathToDirKey(path))
	if err != nil {
		return fmt.Errorf("listing blobs in virtual container before deletion: %w", err)
	}

	if len(blobs) == 0 {
		return storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
	}

	for _, b := range blobs {
		blobRef = d.client.NewBlobClient(d.PathToKey(b))
		if _, err = blobRef.Delete(ctx, nil); err != nil {
			return fmt.Errorf("deleting blob %s: %w", b, err)
		}
	}

	return nil
}

// DeleteFiles deletes a set of files by iterating over their full path list
// and invoking Delete for each. Returns the number of successfully deleted
// files and any errors. This method is idempotent, no error is returned if a
// file does not exist.
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

// URLFor returns a publicly accessible URL for the blob stored at given path
// for specified duration by making use of Azure Storage Shared Access Signatures (SAS).
// See https://msdn.microsoft.com/en-us/library/azure/ee395415.aspx for more info.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]any) (string, error) {
	expiresTime := common.SystemClock.Now().UTC().Add(DefaultSignedURLExpiry)
	expires, ok := options["expiry"]
	if ok {
		t, ok := expires.(time.Time)
		if ok {
			expiresTime = t.UTC()
		}
	}
	blobRef := d.client.NewBlobClient(d.PathToKey(path))
	return d.signer.SignBlobURL(ctx, blobRef.URL(), expiresTime)
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

// WalkParallel traverses a filesystem defined within driver in parallel, starting
// from the given path, calling f on each file.
func (d *driver) WalkParallel(ctx context.Context, path string, f storagedriver.WalkFn) error {
	// NOTE(prozlach): WalkParallel will go away at some point, see
	// https://gitlab.com/gitlab-org/container-registry/-/issues/1182#note_2258251909
	// for more context.
	return d.Walk(ctx, path, f)
}

// listImpl simulates a filesystem style listImpl in which both files (blobs) and
// directories (virtual containers) are returned for a given prefix.
func (d *driver) listImpl(ctx context.Context, prefix string) ([]string, error) {
	return d.listWithDelimiter(ctx, prefix, "/")
}

// listBlobs lists all blobs whose names begin with the specified prefix.
func (d *driver) listBlobs(ctx context.Context, prefix string) ([]string, error) {
	return d.listWithDelimiter(ctx, prefix, "")
}

func (d *driver) listWithDelimiter(ctx context.Context, prefix, delimiter string) ([]string, error) {
	out := make([]string, 0)
	pager := d.client.NewListBlobsHierarchyPager(
		delimiter, &container.ListBlobsHierarchyOptions{
			Prefix:     &prefix,
			MaxResults: to.Ptr((int32)(common.ListMax)),
		})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("next page when listing blobs: %w", err)
		}

		for _, blobItem := range page.Segment.BlobItems {
			if blobItem.Name == nil {
				return nil, fmt.Errorf("missing blob Name when listing prefix: %s", prefix)
			}
			out = append(out, d.KeyToPath(*blobItem.Name))
		}

		for _, blobPrefix := range page.Segment.BlobPrefixes {
			if blobPrefix.Name == nil {
				return nil, fmt.Errorf("missing blob prefix Name when listing prefix: %s", prefix)
			}
			out = append(out, d.KeyToPath(*blobPrefix.Name))
		}
	}
	return out, nil
}

func is404(err error) bool {
	return bloberror.HasCode(
		err,
		bloberror.BlobNotFound,
		bloberror.ContainerNotFound,
		bloberror.ResourceNotFound,
		// Azure will return CannotVerifyCopySource with a 404 status code from
		// a call to Move when the source blob does not exist. Details:
		//
		// https://learn.microsoft.com/en-us/rest/api/storageservices/blob-service-error-codes.
		bloberror.CannotVerifyCopySource,
	)
}

type writer struct {
	driver    *driver
	path      string
	size      int64
	ctx       context.Context
	bw        *bufio.Writer
	closed    bool
	committed bool
	canceled  bool
}

func (d *driver) newWriter(ctx context.Context, path string, size int64) storagedriver.FileWriter {
	return &writer{
		driver: d,
		path:   path,
		size:   size,
		ctx:    ctx,
		bw: bufio.NewWriterSize(&blockWriter{
			client: d.client,
			ctx:    ctx,
			path:   path,
		}, maxChunkSize),
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

	n, err := w.bw.Write(p)
	w.size += int64(n)
	return n, err
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	}
	w.closed = true
	err := w.bw.Flush()
	if err != nil {
		return fmt.Errorf("flushing while closing writer: %w", err)
	}
	return nil
}

func (w *writer) Cancel() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	} else if w.committed {
		return storagedriver.ErrAlreadyCommited
	}
	w.canceled = true
	blobRef := w.driver.client.NewBlobClient(w.path)
	_, err := blobRef.Delete(w.ctx, nil)
	if err != nil {
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
	err := w.bw.Flush()
	if err != nil {
		return fmt.Errorf("flushing while committing writer: %w", err)
	}
	return nil
}

type blockWriter struct {
	client *container.Client
	path   string
	ctx    context.Context
}

func (bw *blockWriter) Write(p []byte) (int, error) {
	blobRef := bw.client.NewAppendBlobClient(bw.path)
	// NOTE(prozlach): wrapping `blockWriter` writter into the
	// `bufio.NewWriterSize` already guarantees that we will write in
	// `maxChunkSize` blocks, no need to chunk it here as well.
	_, err := blobRef.AppendBlock(bw.ctx, streaming.NopCloser(bytes.NewReader(p)), nil)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
