// Package s3 provides a storagedriver.StorageDriver implementation to
// store blobs in Amazon S3 cloud storage.
//
// This package leverages the official aws client library for interfacing with
// S3.
//
// Because S3 is a key, value store the Stat call does not support last modification
// time for directories (directories are an abstraction for key, value stores)
//
// Keep in mind that S3 guarantees only read-after-write consistency for new
// objects, but no read-after-update or list-after-write consistency.
package v2

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/middleware"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
	"github.com/aws/smithy-go/ptr"
	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	"github.com/docker/distribution/version"
	"github.com/hashicorp/go-multierror"
	"gitlab.com/gitlab-org/labkit/fips"
	"golang.org/x/sync/errgroup"
)

// maxListRespLoop is the max number of traversed loops/parts-pages allowed to be pushed.
// It is set to 10000 part pages, which signifies 10000 * 10485760 bytes (i.e. 100GB) of data.
// This is defined in order to prevent infinite loops (i.e. an unbounded amount of parts-pages).
const maxListRespLoop = 10000

// listMax is the largest amount of objects you can request from S3 in a list call
const listMax int32 = 1000

// ErrMaxListRespExceeded signifies a multi part layer upload has exceeded the allowable maximum size
var ErrMaxListRespExceeded = fmt.Errorf("layer parts pages exceeds the maximum of %d allowed", maxListRespLoop)

// S3DriverFactory implements the factory.StorageDriverFactory interface
type S3DriverFactory struct{}

func (*S3DriverFactory) Create(parameters map[string]any) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	S3                          *s3.Client
	Bucket                      string
	ChunkSize                   int64
	Encrypt                     bool
	KeyID                       string
	MultipartCopyChunkSize      int64
	MultipartCopyMaxConcurrency int
	MultipartCopyThresholdSize  int64
	RootDirectory               string
	StorageClass                string
	ObjectACL                   string
	ObjectOwnership             bool
	ParallelWalk                bool
	ChecksumDisabled            bool
	ChecksumAlgorithm           types.ChecksumAlgorithm
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Amazon S3
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// NewAWSLoggerWrapper returns an aws.Logger which will write log messages to
// given logger. It is meant as a thin wrapper.
func NewAWSLoggerWrapper(logger dcontext.Logger) logging.Logger {
	return &awsLoggerWrapper{
		logger: logger,
	}
}

// A defaultLogger provides a minimalistic logger satisfying the aws.Logger
// interface.
type awsLoggerWrapper struct {
	logger dcontext.Logger
}

// Log logs the parameters to the configured logger.
func (l awsLoggerWrapper) Logf(classification logging.Classification, format string, v ...any) {
	if len(classification) > 0 {
		format = "[" + string(classification) + "] " + format
	}

	l.logger.Debugf(format, v...)
}

// FromParameters constructs a new Driver with a given parameters map
func FromParameters(parameters map[string]any) (storagedriver.StorageDriver, error) {
	params, err := common.ParseParameters(common.V2DriverName, parameters)
	if err != nil {
		return nil, err
	}

	return New(params)
}

// NewS3API constructs a new native s3 client with the given AWS credentials,
// region, encryption flag, and bucketName
func NewS3API(params *common.DriverParameters) (*s3.Client, error) {
	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(params.Region),
		config.WithRetryer(NewCustomRetryer(params.MaxRetries, params.MaxRequestsPerSecond, common.DefaultBurst)),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to load aws SDK default config, %w", err)
	}

	if params.LogLevel > 0 {
		cfg.Logger = NewAWSLoggerWrapper(params.Logger)
		cfg.ClientLogMode = aws.ClientLogMode(params.LogLevel)
	}

	// Add static credentials if provided, use default ones otherwise:
	if params.AccessKey != "" && params.SecretKey != "" {
		cfg.Credentials = aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				params.AccessKey,
				params.SecretKey,
				params.SessionToken,
			),
		)
	}

	if params.ChecksumDisabled {
		cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
		cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	} else {
		cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenSupported
		cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenSupported
	}

	// configure http client
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.MaxIdleConnsPerHost = 10
	// nolint: gosec
	httpTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: params.SkipVerify,
	}
	cfg.HTTPClient = &http.Client{
		Transport: httpTransport,
	}

	// Add user agent
	cfg.APIOptions = append(
		cfg.APIOptions,
		middleware.AddUserAgentKey("docker-distribution/"+version.Version+"/"+runtime.Version()),
	)

	if params.RegionEndpoint != "" {
		cfg.BaseEndpoint = ptr.String(params.RegionEndpoint)
	}

	client := s3.NewFromConfig(
		cfg,
		func(o *s3.Options) {
			o.UsePathStyle = params.PathStyle
			o.EndpointOptions.DisableHTTPS = !params.Secure
			if fips.Enabled() {
				o.EndpointOptions.UseFIPSEndpoint = aws.FIPSEndpointStateEnabled
			}
		},
	)

	return client, nil
}

func New(params *common.DriverParameters) (storagedriver.StorageDriver, error) {
	// TODO Currently multipart uploads have no timestamps, so this would be unwise
	// if you initiated a new s3driver while another one is running on the same bucket.
	// multis, _, err := bucket.ListMulti("", "")
	// if err != nil {
	// 	return nil, err
	// }

	// for _, multi := range multis {
	// 	err := multi.Abort()
	// 	//TODO appropriate to do this error checking?
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	d := &driver{
		Bucket:                      params.Bucket,
		ChunkSize:                   params.ChunkSize,
		Encrypt:                     params.Encrypt,
		KeyID:                       params.KeyID,
		MultipartCopyChunkSize:      params.MultipartCopyChunkSize,
		MultipartCopyMaxConcurrency: params.MultipartCopyMaxConcurrency,
		MultipartCopyThresholdSize:  params.MultipartCopyThresholdSize,
		RootDirectory:               params.RootDirectory,
		StorageClass:                params.StorageClass,
		ObjectACL:                   params.ObjectACL,
		ParallelWalk:                params.ParallelWalk,
		ObjectOwnership:             params.ObjectOwnership,
	}

	d.ChecksumDisabled = params.ChecksumDisabled
	if !params.ChecksumDisabled {
		d.ChecksumAlgorithm = params.ChecksumAlgorithm
	}

	s3API, err := NewS3API(params)
	if err != nil {
		return nil, fmt.Errorf("creating new s3 driver implementation: %w", err)
	}
	d.S3 = s3API

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (*driver) Name() string {
	return common.V2DriverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := d.Reader(ctx, path, 0)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	inputArgs := &s3.PutObjectInput{
		Bucket:               ptr.String(d.Bucket),
		Key:                  ptr.String(d.s3Path(path)),
		ContentType:          d.getContentType(),
		ACL:                  d.getACL(),
		ServerSideEncryption: d.getEncryptionMode(),
		SSEKMSKeyId:          d.getSSEKMSKeyID(),
		StorageClass:         d.getStorageClass(),
		Body:                 bytes.NewReader(contents),
	}
	if !d.ChecksumDisabled {
		inputArgs.ChecksumAlgorithm = d.ChecksumAlgorithm
	}
	_, err := d.S3.PutObject(ctx, inputArgs)
	return parseError(path, err)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	resp, err := d.S3.GetObject(
		ctx,
		&s3.GetObjectInput{
			Bucket: ptr.String(d.Bucket),
			Key:    ptr.String(d.s3Path(path)),
			Range:  ptr.String("bytes=" + strconv.FormatInt(offset, 10) + "-"),
		})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "InvalidRange" {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}

		return nil, parseError(path, err)
	}
	return resp.Body, nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, appendParam bool) (storagedriver.FileWriter, error) {
	key := d.s3Path(path)

	// NOTE(prozlach): The S3 driver uses multipart uploads to implement
	// Container Registry's blob upload protocol (start -> chunks ->
	// commit/cancel). Unlike Azure and Filesystem drivers that allow direct
	// appending to blobs, S3 objects are immutable and this behavior is
	// worked-around using s3' native multipart upload API.
	//
	// Container Registry's abstraction layer permits that data isn't visible
	// until the final Commit() call. This matches S3's behavior where uploads
	// aren't accessible as regular objects until the multipart upload is
	// completed.
	//
	// When appendParam is false, we create a new multipart upload after
	// canceling any existing uploads for this path. This cancellation is
	// necessary because CR assumes it always appends to the same file path,
	// and S3 can accumulate multiple incomplete multipart uploads for the same
	// key. While CR currently uses content-addressable paths (SHAsum) or
	// random UIDs which mitigates collision risks, explicitly cleaning up
	// ensures consistent behavior regardless of path construction patterns.
	//
	// When appendParam is true, we find and resume the existing upload by
	// listing parts that have already been uploaded, allowing the writer to
	// continue where it left off.

	resp, err := d.S3.ListMultipartUploads(
		ctx,
		&s3.ListMultipartUploadsInput{
			Bucket: ptr.String(d.Bucket),
			Prefix: ptr.String(key),
		})
	if err != nil {
		return nil, parseError(path, err)
	}

	idx := slices.IndexFunc(resp.Uploads, func(v types.MultipartUpload) bool { return *v.Key == key })

	if !appendParam {
		if idx >= 0 {
			// Cancel any in-progress uploads for the same path
			for _, upload := range resp.Uploads[idx:] {
				if *upload.Key != key {
					break
				}
				_, err := d.S3.AbortMultipartUpload(
					context.Background(),
					&s3.AbortMultipartUploadInput{
						Bucket:   ptr.String(d.Bucket),
						Key:      ptr.String(key),
						UploadId: ptr.String(*upload.UploadId),
					})
				if err != nil {
					return nil, fmt.Errorf("aborting s3 multipart upload %s: %w", *upload.UploadId, err)
				}
			}
		}

		inputArgs := &s3.CreateMultipartUploadInput{
			Bucket:               ptr.String(d.Bucket),
			Key:                  ptr.String(key),
			ContentType:          d.getContentType(),
			ACL:                  d.getACL(),
			ServerSideEncryption: d.getEncryptionMode(),
			SSEKMSKeyId:          d.getSSEKMSKeyID(),
			StorageClass:         d.getStorageClass(),
		}
		if !d.ChecksumDisabled {
			inputArgs.ChecksumAlgorithm = d.ChecksumAlgorithm
		}
		mpUpload, err := d.S3.CreateMultipartUpload(ctx, inputArgs)
		if err != nil {
			return nil, fmt.Errorf("creating new multipart upload: %w", err)
		}

		return d.newWriter(key, *mpUpload.UploadId, nil), nil
	}

	if idx == -1 {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: common.V2DriverName}
	}
	mpUpload := resp.Uploads[idx]

	// respLoopCount is the number of response pages traversed. Each increment
	// of respLoopCount signifies that (at most) 1 full page of parts was
	// pushed, where one full page of parts is equivalent to 1,000 uploaded
	// parts which in turn is equivalent to 10485760 bytes of data.
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
	respLoopCount := 0

	allParts := make([]types.Part, 0)
	listResp := &s3.ListPartsOutput{
		IsTruncated: ptr.Bool(true),
	}
	for resp.IsTruncated != nil && *listResp.IsTruncated {
		// error out if we have pushed more than 100GB of parts
		if respLoopCount > maxListRespLoop {
			return nil, ErrMaxListRespExceeded
		}
		listResp, err = d.S3.ListParts(
			ctx,
			&s3.ListPartsInput{
				Bucket:           ptr.String(d.Bucket),
				Key:              ptr.String(key),
				UploadId:         mpUpload.UploadId,
				PartNumberMarker: listResp.NextPartNumberMarker,
			})
		if err != nil {
			return nil, parseError(path, err)
		}
		allParts = append(allParts, listResp.Parts...)
		respLoopCount++
	}
	return d.newWriter(key, *mpUpload.UploadId, allParts), nil
}

func (d *driver) statHead(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	resp, err := d.S3.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptr.String(d.Bucket),
			Key:    ptr.String(d.s3Path(path)),
		},
	)
	if err != nil {
		return nil, err
	}
	return &storagedriver.FileInfoFields{
		Path:    path,
		IsDir:   false,
		Size:    *resp.ContentLength,
		ModTime: *resp.LastModified,
	}, nil
}

func (d *driver) statList(ctx context.Context, path string) (*storagedriver.FileInfoFields, error) {
	s3Path := d.s3Path(path)
	resp, err := d.S3.ListObjectsV2(
		ctx,
		&s3.ListObjectsV2Input{
			Bucket: ptr.String(d.Bucket),
			Prefix: ptr.String(s3Path),
			// NOTE(prozlach): Yes, AWS returns objects in lexicographical
			// order based on their key names for general purpose buckets.
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
			MaxKeys: ptr.Int32(1),
		})
	if err != nil {
		return nil, err
	}

	if len(resp.Contents) != 1 {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: common.V2DriverName}
	}

	entry := resp.Contents[0]
	fi := &storagedriver.FileInfoFields{
		Path: path,
	}

	if *entry.Key != s3Path {
		if len(*entry.Key) > len(s3Path) && (*entry.Key)[len(s3Path)] != '/' {
			return nil, storagedriver.PathNotFoundError{Path: path, DriverName: common.V2DriverName}
		}
		fi.IsDir = true
	} else {
		fi.IsDir = false
		fi.Size = *entry.Size
		fi.ModTime = *entry.LastModified
	}

	return fi, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fi, err := d.statHead(ctx, path)
	if err != nil {
		// For AWS errors, we fail over to ListObjects: Though the official
		// docs
		//
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_Errors
		//
		// are slightly outdated, the HeadObject actually returns NotFound
		// error if querying a key which doesn't exist or a key which has
		// nested keys and Forbidden if IAM/ACL permissions do not allow Head
		// but allow List.
		if errors.As(err, new(smithy.APIError)) {
			fi, err := d.statList(ctx, path)
			if err != nil {
				return nil, parseError(path, err)
			}
			return storagedriver.FileInfoInternal{FileInfoFields: *fi}, nil
		}
		// For non-AWS errors, return the error directly
		return nil, err
	}
	return storagedriver.FileInfoInternal{FileInfoFields: *fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
	// NOTE(prozlach): This prevents issues with partial matching.
	if path != "/" && path[len(path)-1] != '/' {
		path += "/"
	}

	// This is to cover for the cases when the rootDirectory of the driver is
	// either "" or "/". In those cases, there is no root prefix to replace and
	// we must actually add a "/" to all results in order to keep them as valid
	// paths as recognized by storagedriver.PathRegexp
	prefix := ""
	if d.s3Path("") == "" {
		prefix = "/"
	}

	resp, err := d.S3.ListObjectsV2(
		ctx,
		&s3.ListObjectsV2Input{
			Bucket:    ptr.String(d.Bucket),
			Prefix:    ptr.String(d.s3Path(path)),
			Delimiter: ptr.String("/"),
			MaxKeys:   ptr.Int32(listMax),
		})
	if err != nil {
		return nil, parseError(opath, err)
	}

	files := make([]string, 0)
	directories := make([]string, 0)

	for {
		for _, key := range resp.Contents {
			files = append(files, strings.Replace(*key.Key, d.s3Path(""), prefix, 1))
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			commonPrefix := *commonPrefix.Prefix
			directories = append(directories, strings.Replace(commonPrefix[0:len(commonPrefix)-1], d.s3Path(""), prefix, 1))
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}

		resp, err = d.S3.ListObjectsV2(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:            ptr.String(d.Bucket),
				Prefix:            ptr.String(d.s3Path(path)),
				Delimiter:         ptr.String("/"),
				MaxKeys:           ptr.Int32(listMax),
				ContinuationToken: resp.NextContinuationToken,
			})
		if err != nil {
			return nil, err
		}
	}

	if opath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: opath, DriverName: common.V2DriverName}
		}
	}

	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath, destPath string) error {
	/* This is terrible, but aws doesn't have an actual move. */
	if err := d.copy(ctx, sourcePath, destPath); err != nil {
		return err
	}
	return d.Delete(ctx, sourcePath)
}

// copy copies an object stored at sourcePath to destPath.
func (d *driver) copy(ctx context.Context, sourcePath, destPath string) error {
	// S3 can copy objects up to 5 GB in size with a single PUT Object - Copy
	// operation. For larger objects, the multipart upload API must be used.
	//
	// Empirically, multipart copy is fastest with 32 MB parts and is faster
	// than PUT Object - Copy for objects larger than 32 MB.

	fileInfo, err := d.Stat(ctx, sourcePath)
	if err != nil {
		return parseError(sourcePath, err)
	}

	if fileInfo.Size() <= d.MultipartCopyThresholdSize {
		inputArgs := &s3.CopyObjectInput{
			Bucket:               ptr.String(d.Bucket),
			Key:                  ptr.String(d.s3Path(destPath)),
			ContentType:          d.getContentType(),
			ACL:                  d.getACL(),
			ServerSideEncryption: d.getEncryptionMode(),
			SSEKMSKeyId:          d.getSSEKMSKeyID(),
			StorageClass:         d.getStorageClass(),
			CopySource:           ptr.String(d.Bucket + "/" + d.s3Path(sourcePath)),
		}
		if !d.ChecksumDisabled {
			inputArgs.ChecksumAlgorithm = d.ChecksumAlgorithm
		}
		_, err = d.S3.CopyObject(ctx, inputArgs)
		if err != nil {
			return parseError(sourcePath, err)
		}
		return nil
	}

	inputArgs := &s3.CreateMultipartUploadInput{
		Bucket:               ptr.String(d.Bucket),
		Key:                  ptr.String(d.s3Path(destPath)),
		ContentType:          d.getContentType(),
		ACL:                  d.getACL(),
		SSEKMSKeyId:          d.getSSEKMSKeyID(),
		ServerSideEncryption: d.getEncryptionMode(),
		StorageClass:         d.getStorageClass(),
	}
	if !d.ChecksumDisabled {
		inputArgs.ChecksumAlgorithm = d.ChecksumAlgorithm
	}
	createResp, err := d.S3.CreateMultipartUpload(ctx, inputArgs)
	if err != nil {
		return err
	}

	numParts := (fileInfo.Size() + d.MultipartCopyChunkSize - 1) / d.MultipartCopyChunkSize
	completedParts := make([]types.CompletedPart, numParts)

	g, gctx := errgroup.WithContext(ctx)

	// Reduce the client/server exposure to long lived connections regardless of
	// how many requests per second are allowed.
	g.SetLimit(d.MultipartCopyMaxConcurrency)

	for i := range completedParts {
		g.Go(func() error {
			// Check if any other goroutine has failed
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
				firstByte := int64(i) * d.MultipartCopyChunkSize
				lastByte := firstByte + d.MultipartCopyChunkSize - 1
				if lastByte >= fileInfo.Size() {
					lastByte = fileInfo.Size() - 1
				}

				uploadResp, err := d.S3.UploadPartCopy(
					gctx,
					&s3.UploadPartCopyInput{
						Bucket:          ptr.String(d.Bucket),
						CopySource:      ptr.String(d.Bucket + "/" + d.s3Path(sourcePath)),
						Key:             ptr.String(d.s3Path(destPath)),
						PartNumber:      ptr.Int32(int32(i + 1)), // nolint: gosec // index will always be a non-negative number
						UploadId:        createResp.UploadId,
						CopySourceRange: ptr.String(fmt.Sprintf("bytes=%d-%d", firstByte, lastByte)),
					})
				if err == nil {
					completedParts[i] = types.CompletedPart{
						ETag:              uploadResp.CopyPartResult.ETag,
						PartNumber:        ptr.Int32(int32(i + 1)), // nolint: gosec // index will always be a non-negative number
						ChecksumCRC32:     uploadResp.CopyPartResult.ChecksumCRC32,
						ChecksumCRC32C:    uploadResp.CopyPartResult.ChecksumCRC32C,
						ChecksumCRC64NVME: uploadResp.CopyPartResult.ChecksumCRC64NVME,
						ChecksumSHA1:      uploadResp.CopyPartResult.ChecksumSHA1,
						ChecksumSHA256:    uploadResp.CopyPartResult.ChecksumSHA256,
					}
					return nil
				}
				return fmt.Errorf("multipart upload %d failed: %w", i, err)
			}
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	_, err = d.S3.CompleteMultipartUpload(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:          ptr.String(d.Bucket),
			Key:             ptr.String(d.s3Path(destPath)),
			UploadId:        createResp.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
		})
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
// We must be careful since S3 does not guarantee read after delete consistency
func (d *driver) Delete(ctx context.Context, path string) error {
	s3Objects := make([]types.ObjectIdentifier, 0, int(listMax))
	s3Path := d.s3Path(path)
	listObjectsV2Input := &s3.ListObjectsV2Input{
		Bucket: ptr.String(d.Bucket),
		Prefix: ptr.String(s3Path),
	}
ListLoop:
	for {
		// list all the objects
		resp, err := d.S3.ListObjectsV2(ctx, listObjectsV2Input)
		// NOTE(prozlach): According to AWS S3 REST API specs, ListObjectsV2*
		// may return only a NoSuchBucket error, inexistant paths would be
		// handled by `len(resp.Contents) == 0` branch below.
		//
		// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html#API_ListObjectsV2_Errors
		if err != nil {
			return parseError(path, err)
		}

		// resp.Contents can only be empty on the first call if there were no
		// more results to return after the first call, resp.IsTruncated would
		// have been false and the loop would be exited without recalling
		// ListObjects
		if len(resp.Contents) == 0 {
			return storagedriver.PathNotFoundError{Path: path, DriverName: common.V2DriverName}
		}

		for _, key := range resp.Contents {
			// Stop if we encounter a key that is not a subpath (so that
			// deleting "/a" does not delete "/ab").
			// NOTE(prozlach): Yes, AWS returns objects in lexicographical
			// order based on their key names for general purpose buckets.
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
			if len(*key.Key) > len(s3Path) && (*key.Key)[len(s3Path)] != '/' {
				break ListLoop
			}
			s3Objects = append(s3Objects, types.ObjectIdentifier{
				Key: key.Key,
			})
		}

		// resp.Contents must have at least one element or we would have
		// returned not found
		listObjectsV2Input.StartAfter = resp.Contents[len(resp.Contents)-1].Key

		// from the s3 api docs, IsTruncated "specifies whether (true) or not
		// (false) all of the results were returned" if everything has been
		// returned, break
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
	}

	// need to chunk objects into groups of deleteMax per s3 restrictions
	total := len(s3Objects)
	for i := 0; i < total; i += common.DeleteMax {
		inputArgs := &s3.DeleteObjectsInput{
			Bucket: ptr.String(d.Bucket),
			Delete: &types.Delete{
				Objects: s3Objects[i:min(i+common.DeleteMax, total)],
				Quiet:   ptr.Bool(false),
			},
		}
		if !d.ChecksumDisabled {
			inputArgs.ChecksumAlgorithm = d.ChecksumAlgorithm
		}
		resp, err := d.S3.DeleteObjects(ctx, inputArgs)
		if err != nil {
			return err
		}

		// even if err is nil (200 OK response) it's not guaranteed that all
		// files have been successfully deleted, we need to check the
		// []*s3.Error slice within the S3 response and make sure it's empty
		if len(resp.Errors) > 0 {
			// parse s3.Error errors and return a single storagedriver.MultiError
			var errs error
			for _, s3e := range resp.Errors {
				err := fmt.Errorf("deleting file '%s': '%s'", *s3e.Key, *s3e.Message)
				errs = multierror.Append(errs, err)
			}
			return errs
		}
	}
	return nil
}

// DeleteFiles deletes a set of files using the S3 bulk delete feature, with up
// to deleteMax files per request. If deleting more than deleteMax files,
// DeleteFiles will split files in deleteMax requests automatically. Contrary
// to Delete, which is a generic method to delete any kind of object,
// DeleteFiles does not send a ListObjects request before DeleteObjects.
// Returns the number of successfully deleted files and any errors. This method
// is idempotent, no error is returned if a file does not exist.
func (d *driver) DeleteFiles(ctx context.Context, paths []string) (int, error) {
	s3Objects := make([]types.ObjectIdentifier, 0, len(paths))
	for i := range paths {
		p := d.s3Path(paths[i])
		s3Objects = append(s3Objects, types.ObjectIdentifier{Key: &p})
	}

	var (
		result       *multierror.Error
		deletedCount int
	)

	// chunk files into batches of deleteMax (as per S3 restrictions).
	total := len(s3Objects)
	for i := 0; i < total; i += common.DeleteMax {
		inputArgs := &s3.DeleteObjectsInput{
			Bucket: ptr.String(d.Bucket),
			Delete: &types.Delete{
				Objects: s3Objects[i:min(i+common.DeleteMax, total)],
				Quiet:   ptr.Bool(false),
			},
		}
		if !d.ChecksumDisabled {
			inputArgs.ChecksumAlgorithm = d.ChecksumAlgorithm
		}
		resp, err := d.S3.DeleteObjects(ctx, inputArgs)
		// If one batch fails, append the error and continue to give a best-effort
		// attempt at deleting the most files possible.
		if err != nil {
			result = multierror.Append(result, err)
		}
		// Guard against nil response values, which can occur during testing and
		// S3 has proven to be unpredictable in practice as well.
		if resp == nil {
			continue
		}

		deletedCount += len(resp.Deleted)

		// even if err is nil (200 OK response) it's not guaranteed that all files have been successfully deleted,
		// we need to check the []*s3.Error slice within the S3 response and make sure it's empty
		if len(resp.Errors) > 0 {
			// parse s3.Error errors and append them to the multierror.
			for _, s3e := range resp.Errors {
				err := fmt.Errorf("deleting file '%s': '%s'", *s3e.Key, *s3e.Message)
				result = multierror.Append(result, err)
			}
		}
	}

	return deletedCount, result.ErrorOrNil()
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]any) (string, error) {
	methodString := http.MethodGet
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != http.MethodGet && methodString != http.MethodHead) {
			return "", storagedriver.ErrUnsupportedMethod{DriverName: common.V2DriverName}
		}
	}

	expiresIn := 20 * time.Minute
	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresIn = et.Sub(common.SystemClock.Now())
		}
	}

	var req *v4.PresignedHTTPRequest
	var err error
	svc := s3.NewPresignClient(d.S3)
	switch methodString {
	case http.MethodGet:
		req, err = svc.PresignGetObject(
			ctx,
			&s3.GetObjectInput{
				Bucket: ptr.String(d.Bucket),
				Key:    ptr.String(d.s3Path(path)),
			},
			func(o *s3.PresignOptions) {
				o.Expires = expiresIn
			},
		)
	case http.MethodHead:
		req, err = svc.PresignHeadObject(
			ctx,
			&s3.HeadObjectInput{
				Bucket: ptr.String(d.Bucket),
				Key:    ptr.String(d.s3Path(path)),
			},
			func(o *s3.PresignOptions) {
				o.Expires = expiresIn
			},
		)
	default:
		panic("unreachable")
	}
	if err != nil {
		return "", fmt.Errorf("presigning path %q failed: %w", path, err)
	}

	return req.URL, nil
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (d *driver) Walk(ctx context.Context, from string, f storagedriver.WalkFn) error {
	path := from
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	prefix := ""
	if d.s3Path("") == "" {
		prefix = "/"
	}

	var objectCount int64
	if err := d.doWalk(ctx, &objectCount, d.s3Path(path), prefix, f); err != nil {
		return err
	}

	// S3 doesn't have the concept of empty directories, so it'll return path not found if there are no objects
	if objectCount == 0 {
		return storagedriver.PathNotFoundError{Path: from, DriverName: common.V2DriverName}
	}

	return nil
}

// WalkParallel traverses a filesystem defined within driver, starting from the
// given path, calling f on each file.
func (d *driver) WalkParallel(ctx context.Context, from string, f storagedriver.WalkFn) error {
	// If the ParallelWalk feature flag is not set, fall back to standard sequential walk.
	if !d.ParallelWalk {
		return d.Walk(ctx, from, f)
	}

	path := from
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	prefix := ""
	if d.s3Path("") == "" {
		prefix = "/"
	}

	var objectCount int64
	var retError error
	countChan := make(chan int64)
	countDone := make(chan struct{})
	errCh := make(chan error)
	errDone := make(chan struct{})
	quit := make(chan struct{})

	// Consume object counts from each doWalkParallel call asynchronusly to avoid blocking.
	go func() {
		for i := range countChan {
			objectCount += i
		}
		countDone <- struct{}{}
	}()

	// If we encounter an error from any goroutine called from within doWalkParallel,
	// return early from any new goroutines and return that error.
	go func() {
		var closed bool
		// Consume all errors to prevent goroutines from blocking and to
		// report errors from goroutines that were already in progress.
		for err := range errCh {
			// Signal goroutines to quit only once on the first error.
			if !closed {
				close(quit)
				closed = true
			}

			if err != nil {
				retError = multierror.Append(retError, err)
			}
		}
		errDone <- struct{}{}
	}()

	// doWalkParallel spawns and manages it's own goroutines, but it also calls
	// itself recursively. Passing in a WaitGroup allows us to wait for the
	// entire walk to complete without blocking on each doWalkParallel call.
	var wg sync.WaitGroup

	d.doWalkParallel(ctx, &wg, countChan, quit, errCh, d.s3Path(path), prefix, f)

	wg.Wait()

	// Ensure that all object counts have been totaled before continuing.
	close(countChan)
	close(errCh)
	<-countDone
	<-errDone

	// S3 doesn't have the concept of empty directories, so it'll return path not found if there are no objects
	if objectCount == 0 {
		return storagedriver.PathNotFoundError{Path: from, DriverName: common.V2DriverName}
	}

	return retError
}

type walkInfoContainer struct {
	storagedriver.FileInfoFields
	prefix *string
}

// Path provides the full path of the target of this file info.
func (wi walkInfoContainer) Path() string {
	return wi.FileInfoFields.Path
}

// Size returns current length in bytes of the file. The return value can
// be used to write to the end of the file at path. The value is
// meaningless if IsDir returns true.
func (wi walkInfoContainer) Size() int64 {
	return wi.FileInfoFields.Size
}

// ModTime returns the modification time for the file. For backends that
// don't have a modification time, the creation time should be returned.
func (wi walkInfoContainer) ModTime() time.Time {
	return wi.FileInfoFields.ModTime
}

// IsDir returns true if the path is a directory.
func (wi walkInfoContainer) IsDir() bool {
	return wi.FileInfoFields.IsDir
}

func (d *driver) doWalk(parentCtx context.Context, objectCount *int64, path, prefix string, f storagedriver.WalkFn) error {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket:    ptr.String(d.Bucket),
		Prefix:    ptr.String(path),
		Delimiter: ptr.String("/"),
		MaxKeys:   ptr.Int32(listMax),
	}

	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("s3aws.ListObjectsV2Pages(%s)", path)

	paginator := s3.NewListObjectsV2Paginator(d.S3, listObjectsInput)
	for paginator.HasMorePages() {
		objects, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("fetching new page with objects: %w", err)
		}

		var count int64
		// KeyCount was introduced with version 2 of the GET Bucket operation in S3.
		// Some S3 implementations don't support V2 now, so we fall back to manual
		// calculation of the key count if required
		if objects.KeyCount != nil {
			count = int64(*objects.KeyCount)
		} else {
			count = int64(len(objects.Contents)) + int64(len(objects.CommonPrefixes)) // nolint: gosec // len() is always going to be non-negative
		}

		*objectCount += count

		walkInfos := make([]walkInfoContainer, 0, count)

		for _, dir := range objects.CommonPrefixes {
			commonPrefix := *dir.Prefix
			walkInfos = append(walkInfos, walkInfoContainer{
				prefix: dir.Prefix,
				FileInfoFields: storagedriver.FileInfoFields{
					IsDir: true,
					Path:  strings.Replace(commonPrefix[:len(commonPrefix)-1], d.s3Path(""), prefix, 1),
				},
			})
		}

		for _, file := range objects.Contents {
			// In some cases the _uploads dir might be empty. When this
			// happens, it would be appended twice to the walkInfos slice, once
			// as [...]/_uploads and once more erroneously as [...]/_uploads/.
			// the easiest way to avoid this is to skip appending filePath to
			// walkInfos if it ends in "/". the loop through dirs will already
			// have handled it in that case, so it's safe to continue this
			// loop.
			if strings.HasSuffix(*file.Key, "/") {
				continue
			}
			walkInfos = append(walkInfos, walkInfoContainer{
				FileInfoFields: storagedriver.FileInfoFields{
					IsDir:   false,
					Size:    *file.Size,
					ModTime: *file.LastModified,
					Path:    strings.Replace(*file.Key, d.s3Path(""), prefix, 1),
				},
			})
		}

		sort.SliceStable(walkInfos, func(i, j int) bool { return walkInfos[i].FileInfoFields.Path < walkInfos[j].FileInfoFields.Path })

		for _, walkInfo := range walkInfos {
			err := f(walkInfo)

			if errors.Is(err, storagedriver.ErrSkipDir) {
				if walkInfo.IsDir() {
					continue
				}
				break
			} else if err != nil {
				return fmt.Errorf("passed walk-func failed: %w", err)
			}

			if walkInfo.IsDir() {
				if err := d.doWalk(ctx, objectCount, *walkInfo.prefix, prefix, f); err != nil {
					return fmt.Errorf("recursive call to doWalk(): %w", err)
				}
			}
		}
	}

	return nil
}

func (d *driver) doWalkParallel(parentCtx context.Context, wg *sync.WaitGroup, countChan chan<- int64, quit <-chan struct{}, errCh chan<- error, path, prefix string, f storagedriver.WalkFn) {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket:    ptr.String(d.Bucket),
		Prefix:    ptr.String(path),
		Delimiter: ptr.String("/"),
		MaxKeys:   ptr.Int32(listMax),
	}

	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("s3aws.ListObjectsV2Pages(%s)", path)

	paginator := s3.NewListObjectsV2Paginator(d.S3, listObjectsInput)
	for paginator.HasMorePages() {
		objects, err := paginator.NextPage(ctx)
		if err != nil {
			errCh <- fmt.Errorf("fetching new page with objects: %w", err)
			return
		}

		select {
		// The walk was canceled, return to stop requests for pages and prevent
		// gorountines from leaking.
		case <-quit:
			return
		default:
			var count int64
			// KeyCount was introduced with version 2 of the GET Bucket operation in S3.
			// Some S3 implementations don't support V2 now, so we fall back to manual
			// calculation of the key count if required
			if objects.KeyCount != nil {
				count = int64(*objects.KeyCount)
			} else {
				count = int64(len(objects.Contents)) + int64(len(objects.CommonPrefixes)) // nolint: gosec // len() is always going to be non-negative
			}
			countChan <- count

			walkInfos := make([]walkInfoContainer, 0, count)

			for _, dir := range objects.CommonPrefixes {
				commonPrefix := *dir.Prefix
				walkInfos = append(walkInfos, walkInfoContainer{
					prefix: dir.Prefix,
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir: true,
						Path:  strings.Replace(commonPrefix[:len(commonPrefix)-1], d.s3Path(""), prefix, 1),
					},
				})
			}

			for _, file := range objects.Contents {
				// In some cases the _uploads dir might be empty. When this
				// happens, it would be appended twice to the walkInfos slice,
				// once as [...]/_uploads and once more erroneously as
				// [...]/_uploads/. the easiest way to avoid this is to skip
				// appending filePath to walkInfos if it ends in "/". the loop
				// through dirs will already have handled it in that case, so
				// it's safe to continue this loop.
				if strings.HasSuffix(*file.Key, "/") {
					continue
				}

				walkInfos = append(walkInfos, walkInfoContainer{
					FileInfoFields: storagedriver.FileInfoFields{
						IsDir:   false,
						Size:    *file.Size,
						ModTime: *file.LastModified,
						Path:    strings.Replace(*file.Key, d.s3Path(""), prefix, 1),
					},
				})
			}

			for _, walkInfo := range walkInfos {
				wg.Add(1)
				wInfo := walkInfo
				go func() {
					defer wg.Done()

					err := f(wInfo)

					if errors.Is(err, storagedriver.ErrSkipDir) && wInfo.IsDir() {
						return
					}

					if err != nil {
						errCh <- fmt.Errorf("passed walk-func failed: %w", err)
					}

					if wInfo.IsDir() {
						d.doWalkParallel(ctx, wg, countChan, quit, errCh, *wInfo.prefix, prefix, f)
					}
				}()
			}
		}
	}
}

func (d *driver) s3Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.RootDirectory, "/")+path, "/")
}

// S3BucketKey returns the s3 bucket key for the given storage driver path.
func (d *Driver) S3BucketKey(path string) string {
	return d.StorageDriver.(*driver).s3Path(path)
}

func parseError(path string, err error) error {
	errAs := new(types.NoSuchKey)
	if errors.As(err, &errAs) {
		return storagedriver.PathNotFoundError{Path: path, DriverName: common.V2DriverName}
	}

	return err
}

func (d *driver) getEncryptionMode() types.ServerSideEncryption {
	if !d.Encrypt {
		return ""
	}
	if d.KeyID == "" {
		return types.ServerSideEncryptionAes256
	}
	return types.ServerSideEncryptionAwsKms
}

func (d *driver) getSSEKMSKeyID() *string {
	if d.KeyID != "" {
		return ptr.String(d.KeyID)
	}
	return nil
}

func (*driver) getContentType() *string {
	return ptr.String("application/octet-stream")
}

func (d *driver) getACL() types.ObjectCannedACL {
	if d.ObjectOwnership {
		return ""
	}
	return types.ObjectCannedACL(d.ObjectACL)
}

func (d *driver) getStorageClass() types.StorageClass {
	if d.StorageClass == common.StorageClassNone {
		return ""
	}
	return types.StorageClass(d.StorageClass)
}

// writer attempts to upload parts to S3 in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver                      *driver
	key                         string
	uploadID                    string
	parts                       []types.Part
	size                        int64
	buffer                      *bytes.Buffer
	closed                      bool
	committed                   bool
	canceled                    bool
	chunkSize                   int64
	multipartCopyMaxConcurrency int
	checksumDisabled            bool
	checksumAlgorithm           types.ChecksumAlgorithm
}

func (d *driver) newWriter(key, uploadID string, parts []types.Part) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += *part.Size
	}
	return &writer{
		driver:                      d,
		key:                         key,
		uploadID:                    uploadID,
		parts:                       parts,
		size:                        size,
		buffer:                      new(bytes.Buffer),
		chunkSize:                   d.ChunkSize,
		multipartCopyMaxConcurrency: d.MultipartCopyMaxConcurrency,
		checksumDisabled:            d.ChecksumDisabled,
		checksumAlgorithm:           d.ChecksumAlgorithm,
	}
}

type completedParts []types.CompletedPart

func (a completedParts) Len() int           { return len(a) }
func (a completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

func (w *writer) Write(p []byte) (int, error) {
	ctx := context.Background()

	switch {
	case w.closed:
		return 0, storagedriver.ErrAlreadyClosed
	case w.committed:
		return 0, storagedriver.ErrAlreadyCommited
	case w.canceled:
		return 0, storagedriver.ErrAlreadyCanceled
	}

	// If the length of the last written part is different than chunkSize,
	// we need to make a new multipart upload to even things out.
	if len(w.parts) > 0 && *w.parts[len(w.parts)-1].Size != w.chunkSize {
		var completedUploadedParts completedParts
		for _, part := range w.parts {
			completedUploadedParts = append(completedUploadedParts, types.CompletedPart{
				ETag:              part.ETag,
				PartNumber:        part.PartNumber,
				ChecksumCRC32:     part.ChecksumCRC32,
				ChecksumCRC32C:    part.ChecksumCRC32C,
				ChecksumCRC64NVME: part.ChecksumCRC64NVME,
				ChecksumSHA1:      part.ChecksumSHA1,
				ChecksumSHA256:    part.ChecksumSHA256,
			})
		}

		sort.Sort(completedUploadedParts)

		_, err := w.driver.S3.CompleteMultipartUpload(
			ctx,
			&s3.CompleteMultipartUploadInput{
				Bucket:   ptr.String(w.driver.Bucket),
				Key:      ptr.String(w.key),
				UploadId: ptr.String(w.uploadID),
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: completedUploadedParts,
				},
			})
		if err != nil {
			_, errIn := w.driver.S3.AbortMultipartUpload(
				ctx,
				&s3.AbortMultipartUploadInput{
					Bucket:   ptr.String(w.driver.Bucket),
					Key:      ptr.String(w.key),
					UploadId: ptr.String(w.uploadID),
				})
			if errIn != nil {
				return 0, fmt.Errorf("aborting upload failed while handling error %w: %w", err, errIn)
			}
			return 0, err
		}

		inputArgs := &s3.CreateMultipartUploadInput{
			Bucket:               ptr.String(w.driver.Bucket),
			Key:                  ptr.String(w.key),
			ContentType:          w.driver.getContentType(),
			ACL:                  w.driver.getACL(),
			ServerSideEncryption: w.driver.getEncryptionMode(),
			StorageClass:         w.driver.getStorageClass(),
		}
		if !w.checksumDisabled {
			inputArgs.ChecksumAlgorithm = w.checksumAlgorithm
		}
		resp, err := w.driver.S3.CreateMultipartUpload(ctx, inputArgs)
		if err != nil {
			return 0, err
		}
		w.uploadID = *resp.UploadId

		partCount := (w.size + w.chunkSize - 1) / w.chunkSize
		w.parts = make([]types.Part, 0, partCount)
		partsMutex := new(sync.Mutex)

		g, gctx := errgroup.WithContext(ctx)

		// Reduce the client/server exposure to long lived connections regardless of
		// how many requests per second are allowed.
		g.SetLimit(w.multipartCopyMaxConcurrency)

		for i := int64(0); i < partCount; i++ {
			g.Go(func() error {
				// Check if any other goroutine has failed
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				startByte := w.chunkSize * i
				endByte := startByte + w.chunkSize - 1
				if endByte > w.size-1 {
					endByte = w.size - 1
					// NOTE(prozlach): Special case when there is simply not
					// enough data for a full chunk. It handles both cases when
					// there is only one chunk and multiple chunks plus partial
					// a one. We just slurp in what we have and carry on with
					// the data passed to the Write() call.
					byteRange := fmt.Sprintf("bytes=%d-%d", startByte, endByte)
					resp, err := w.driver.S3.GetObject(
						gctx,
						&s3.GetObjectInput{
							Bucket: ptr.String(w.driver.Bucket),
							Key:    ptr.String(w.key),
							Range:  ptr.String(byteRange),
						})
					if err != nil {
						return fmt.Errorf("fetching object from backend during re-upload: %w", err)
					}
					defer resp.Body.Close()

					_, err = w.buffer.ReadFrom(resp.Body)
					if err != nil {
						return fmt.Errorf("reading remaining bytes during data re-upload: %w", err)
					}
					return nil
				}

				// Part numbers are positive integers in the range 1 <= n <= 10000
				partNumber := i + 1

				// Specify the byte range to copy. `CopySourceRange` factors
				// in both starting and ending byte.
				byteRange := fmt.Sprintf("bytes=%d-%d", startByte, endByte)

				copyPartResp, err := w.driver.S3.UploadPartCopy(
					gctx,
					&s3.UploadPartCopyInput{
						Bucket:          ptr.String(w.driver.Bucket),
						CopySource:      ptr.String(w.driver.Bucket + "/" + w.key),
						CopySourceRange: ptr.String(byteRange),
						Key:             ptr.String(w.key),
						PartNumber:      ptr.Int32(int32(partNumber)), // nolint: gosec // partNumber will always be a non-negative number
						UploadId:        resp.UploadId,
					})
				if err != nil {
					return fmt.Errorf("re-uploading chunk as multipart upload part: %w", err)
				}

				// NOTE(prozlach): We can't pre-allocate parts slice and then
				// address it with `i` if we want to handle the case where
				// there is not enough data for a single chunk. At the expense
				// of this small extra loop and extra mutex we get the same
				// path for both cases.
				partsMutex.Lock()
				for int64(len(w.parts))-1 < i {
					w.parts = append(w.parts, types.Part{})
				}

				// Add this part to our list
				w.parts[i] = types.Part{
					ETag:       copyPartResp.CopyPartResult.ETag,
					PartNumber: ptr.Int32(int32(partNumber)), // nolint: gosec // partNumber will always be a non-negative number
					// `CopySourceRange` factors in both starting and
					// ending byte, hence `+ 1`.
					Size: ptr.Int64(endByte - startByte + 1),
				}
				partsMutex.Unlock()

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return 0, err
		}
	}

	var n int64

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := w.driver.ChunkSize - int64(w.buffer.Len()); neededBytes > 0 {
			if int64(len(p)) >= neededBytes {
				_, _ = w.buffer.Write(p[:neededBytes]) // err is always nil
				n += neededBytes
				p = p[neededBytes:]

				err := w.flush()
				// nolint: revive // max-control-nesting
				if err != nil {
					w.size += n
					return int(n), err // nolint: gosec // n is never going to be negative
				}
			} else {
				_, _ = w.buffer.Write(p)
				n += int64(len(p))
				p = nil
			}
		}
	}

	w.size += n
	return int(n), nil // nolint: gosec // n is never going to be negative
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return storagedriver.ErrAlreadyClosed
	}
	w.closed = true

	if w.canceled {
		// NOTE(prozlach): If the writer has been already canceled, then there
		// is nothing to flush to the backend as the multipart upload has
		// already been deleted.
		return nil
	}

	err := w.flush()
	if err != nil {
		return fmt.Errorf("flushing buffers while closing writer: %w", err)
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

	_, err := w.driver.S3.AbortMultipartUpload(
		context.Background(),
		&s3.AbortMultipartUploadInput{
			Bucket:   ptr.String(w.driver.Bucket),
			Key:      ptr.String(w.key),
			UploadId: ptr.String(w.uploadID),
		})
	if err != nil {
		return fmt.Errorf("aborting s3 multipart upload: %w", err)
	}
	return nil
}

func (w *writer) Commit() error {
	ctx := context.Background()

	switch {
	case w.closed:
		return storagedriver.ErrAlreadyClosed
	case w.committed:
		return storagedriver.ErrAlreadyCommited
	case w.canceled:
		return storagedriver.ErrAlreadyCanceled
	}
	err := w.flush()
	if err != nil {
		return err
	}
	w.committed = true

	var completedUploadedParts completedParts
	for _, part := range w.parts {
		completedUploadedParts = append(completedUploadedParts, types.CompletedPart{
			ETag:              part.ETag,
			PartNumber:        part.PartNumber,
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
		})
	}

	sort.Sort(completedUploadedParts)

	_, err = w.driver.S3.CompleteMultipartUpload(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:   ptr.String(w.driver.Bucket),
			Key:      ptr.String(w.key),
			UploadId: ptr.String(w.uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completedUploadedParts,
			},
		})
	if err != nil {
		_, errIn := w.driver.S3.AbortMultipartUpload(
			ctx,
			&s3.AbortMultipartUploadInput{
				Bucket:   ptr.String(w.driver.Bucket),
				Key:      ptr.String(w.key),
				UploadId: ptr.String(w.uploadID),
			})
		if errIn != nil {
			return fmt.Errorf("aborting upload failed while handling error %w: %w", err, errIn)
		}
		return err
	}
	return nil
}

// flush flushes the buffer to S3.
func (w *writer) flush() error {
	if w.buffer.Len() == 0 {
		// nothing to write
		return nil
	}

	partNumber := ptr.Int32(int32(len(w.parts)) + 1) // nolint: gosec // len() is always going to be non-negative
	part := types.Part{
		PartNumber: partNumber,
		Size:       ptr.Int64(int64(w.buffer.Len())),
	}
	r := bytes.NewReader(w.buffer.Bytes())

	inputArgs := &s3.UploadPartInput{
		Bucket:     ptr.String(w.driver.Bucket),
		Key:        ptr.String(w.key),
		PartNumber: partNumber,
		UploadId:   ptr.String(w.uploadID),
		Body:       r,
	}
	if !w.checksumDisabled {
		inputArgs.ChecksumAlgorithm = w.checksumAlgorithm
	}
	resp, err := w.driver.S3.UploadPart(context.Background(), inputArgs)
	if err != nil {
		return err
	}

	part.ETag = resp.ETag
	part.ChecksumCRC32 = resp.ChecksumCRC32
	part.ChecksumCRC32C = resp.ChecksumCRC32C
	part.ChecksumCRC64NVME = resp.ChecksumCRC64NVME
	part.ChecksumSHA1 = resp.ChecksumSHA1
	part.ChecksumSHA256 = resp.ChecksumSHA256
	w.parts = append(w.parts, part)
	w.buffer.Reset()
	return nil
}
