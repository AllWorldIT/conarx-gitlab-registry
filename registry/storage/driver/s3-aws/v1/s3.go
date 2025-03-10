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
package v1

import (
	"bytes"
	"context"
	"crypto/tls"
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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	dcontext "github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/s3-aws/common"
	"github.com/docker/distribution/version"
	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/fips"
)

const (
	DriverName    = "s3aws"
	DriverNameAlt = "s3"
)

// maxListRespLoop is the max number of traversed loops/parts-pages allowed to be pushed.
// It is set to 10000 part pages, which signifies 10000 * 10485760 bytes (i.e. 100GB) of data.
// This is defined in order to prevent infinite loops (i.e. an unbounded amount of parts-pages).
const maxListRespLoop = 10000

// listMax is the largest amount of objects you can request from S3 in a list call
const listMax = 1000

// defaultBurst is how many limiter tokens may be reserved at once. Currently,
// we only reserve one at a time via Limiter.Wait()
const defaultBurst = 1

// ErrMaxListRespExceeded signifies a multi part layer upload has exceeded the allowable maximum size
var ErrMaxListRespExceeded = fmt.Errorf("layer parts pages exceeds the maximum of %d allowed", maxListRespLoop)

// S3DriverFactory implements the factory.StorageDriverFactory interface
type S3DriverFactory struct{}

func (*S3DriverFactory) Create(parameters map[string]any) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	S3                          common.S3WrapperIf
	Bucket                      string
	ChunkSize                   int64
	Encrypt                     bool
	KeyID                       string
	MultipartCopyChunkSize      int64
	MultipartCopyMaxConcurrency int64
	MultipartCopyThresholdSize  int64
	RootDirectory               string
	StorageClass                string
	ObjectACL                   string
	ObjectOwnership             bool
	ParallelWalk                bool
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Amazon S3
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - region
// - bucket
// - encrypt
func FromParameters(parameters map[string]any) (storagedriver.StorageDriver, error) {
	params, err := common.ParseParameters(parameters)
	if err != nil {
		return nil, err
	}

	return New(params)
}

// NewS3API constructs a new native s3 client with the given AWS credentials,
// region, encryption flag, and bucketName
func NewS3API(params *common.DriverParameters) (s3iface.S3API, error) {
	if !params.V4Auth &&
		(params.RegionEndpoint == "" ||
			strings.Contains(params.RegionEndpoint, "s3.amazonaws.com")) {
		return nil, fmt.Errorf("on Amazon S3 this storage driver can only be used with v4 authentication")
	}

	awsConfig := aws.NewConfig().WithLogLevel(params.LogLevel)
	if params.AccessKey != "" && params.SecretKey != "" {
		creds := credentials.NewStaticCredentials(
			params.AccessKey, params.SecretKey, params.SessionToken,
		)
		awsConfig.WithCredentials(creds)
	}

	if params.RegionEndpoint != "" {
		awsConfig.WithEndpoint(params.RegionEndpoint)
	}

	awsConfig.WithS3ForcePathStyle(params.PathStyle)
	awsConfig.WithRegion(params.Region)
	awsConfig.WithDisableSSL(!params.Secure)

	// configure http client
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.MaxIdleConnsPerHost = 10
	// nolint: gosec
	httpTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: params.SkipVerify}
	awsConfig.WithHTTPClient(&http.Client{
		Transport: httpTransport,
	})

	// disable MD5 header when fips is enabled
	awsConfig.WithS3DisableContentMD5Validation(fips.Enabled())

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("creating a new session with aws config: %w", err)
	}

	userAgentHandler := request.NamedHandler{
		Name: "user-agent",
		Fn:   request.MakeAddToUserAgentHandler("docker-distribution", version.Version, runtime.Version()),
	}
	sess.Handlers.Build.PushFrontNamed(userAgentHandler)

	s3obj := s3.New(sess)

	// enable S3 compatible signature v2 signing instead
	if !params.V4Auth {
		setv2Handlers(s3obj)
	}

	return s3obj, nil
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

	if params.S3APIImpl != nil {
		d.S3 = params.S3APIImpl
	} else {
		s3obj, err := NewS3API(params)
		if err != nil {
			return nil, fmt.Errorf("creating new s3 driver implementation: %w", err)
		}
		d.S3 = common.NewS3Wrapper(
			s3obj,
			common.WithRateLimit(params.MaxRequestsPerSecond, defaultBurst),
			common.WithExponentialBackoff(params.MaxRetries),
			common.WithBackoffNotify(func(err error, t time.Duration) {
				log.WithFields(log.Fields{"error": err, "delay_s": t.Seconds()}).Info("S3: retrying after error")
			}),
		)
	}

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
	return DriverName
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
	_, err := d.S3.PutObjectWithContext(
		ctx,
		&s3.PutObjectInput{
			Bucket:               aws.String(d.Bucket),
			Key:                  aws.String(d.s3Path(path)),
			ContentType:          d.getContentType(),
			ACL:                  d.getACL(),
			ServerSideEncryption: d.getEncryptionMode(),
			SSEKMSKeyId:          d.getSSEKMSKeyID(),
			StorageClass:         d.getStorageClass(),
			Body:                 bytes.NewReader(contents),
		})
	return parseError(path, err)
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	resp, err := d.S3.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(d.s3Path(path)),
			Range:  aws.String("bytes=" + strconv.FormatInt(offset, 10) + "-"),
		})
	if err != nil {
		if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "InvalidRange" {
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
	if !appendParam {
		// TODO (brianbland): cancel other uploads at this path

		resp, err := d.S3.CreateMultipartUploadWithContext(
			ctx,
			&s3.CreateMultipartUploadInput{
				Bucket:               aws.String(d.Bucket),
				Key:                  aws.String(key),
				ContentType:          d.getContentType(),
				ACL:                  d.getACL(),
				ServerSideEncryption: d.getEncryptionMode(),
				SSEKMSKeyId:          d.getSSEKMSKeyID(),
				StorageClass:         d.getStorageClass(),
			})
		if err != nil {
			return nil, err
		}
		return d.newWriter(key, *resp.UploadId, nil), nil
	}

	resp, err := d.S3.ListMultipartUploadsWithContext(
		ctx,
		&s3.ListMultipartUploadsInput{
			Bucket: aws.String(d.Bucket),
			Prefix: aws.String(key),
		})
	if err != nil {
		return nil, parseError(path, err)
	}

	idx := slices.IndexFunc(resp.Uploads, func(v *s3.MultipartUpload) bool { return *v.Key == key })
	if idx == -1 {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
	}
	mpUpload := resp.Uploads[idx]

	// respLoopCount is the number of response pages traversed. Each increment
	// of respLoopCount signifies that (at most) 1 full page of parts was
	// pushed, where one full page of parts is equivalent to 1,000 uploaded
	// parts which in turn is equivalent to 10485760 bytes of data.
	// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListParts.html
	respLoopCount := 0

	allParts := make([]*s3.Part, 0)
	listResp := &s3.ListPartsOutput{
		IsTruncated: aws.Bool(true),
	}
	for *listResp.IsTruncated {
		// error out if we have pushed more than 100GB of parts
		if respLoopCount > maxListRespLoop {
			return nil, ErrMaxListRespExceeded
		}
		listResp, err = d.S3.ListPartsWithContext(
			ctx,
			&s3.ListPartsInput{
				Bucket:           aws.String(d.Bucket),
				Key:              aws.String(key),
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

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	s3Path := d.s3Path(path)
	resp, err := d.S3.ListObjectsV2WithContext(
		ctx,
		&s3.ListObjectsV2Input{
			Bucket: aws.String(d.Bucket),
			Prefix: aws.String(s3Path),
			// NOTE(prozlach): Yes, AWS returns objects in lexicographical
			// order based on their key names for general purpose buckets.
			// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
			MaxKeys: aws.Int64(1),
		})
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	if len(resp.Contents) == 1 {
		entry := resp.Contents[0]
		if *entry.Key != s3Path {
			if len(*entry.Key) > len(s3Path) && (*entry.Key)[len(s3Path)] != '/' {
				return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
			}
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = *entry.Size
			fi.ModTime = *entry.LastModified
		}
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, opath string) ([]string, error) {
	path := opath
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

	resp, err := d.S3.ListObjectsV2WithContext(
		ctx,
		&s3.ListObjectsV2Input{
			Bucket:    aws.String(d.Bucket),
			Prefix:    aws.String(d.s3Path(path)),
			Delimiter: aws.String("/"),
			MaxKeys:   aws.Int64(listMax),
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

		if !*resp.IsTruncated {
			break
		}

		resp, err = d.S3.ListObjectsV2WithContext(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:            aws.String(d.Bucket),
				Prefix:            aws.String(d.s3Path(path)),
				Delimiter:         aws.String("/"),
				MaxKeys:           aws.Int64(listMax),
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
			return nil, storagedriver.PathNotFoundError{Path: opath, DriverName: DriverName}
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
		_, err = d.S3.CopyObjectWithContext(
			ctx,
			&s3.CopyObjectInput{
				Bucket:               aws.String(d.Bucket),
				Key:                  aws.String(d.s3Path(destPath)),
				ContentType:          d.getContentType(),
				ACL:                  d.getACL(),
				ServerSideEncryption: d.getEncryptionMode(),
				SSEKMSKeyId:          d.getSSEKMSKeyID(),
				StorageClass:         d.getStorageClass(),
				CopySource:           aws.String(d.Bucket + "/" + d.s3Path(sourcePath)),
			})
		if err != nil {
			return parseError(sourcePath, err)
		}
		return nil
	}

	createResp, err := d.S3.CreateMultipartUploadWithContext(
		ctx,
		&s3.CreateMultipartUploadInput{
			Bucket:               aws.String(d.Bucket),
			Key:                  aws.String(d.s3Path(destPath)),
			ContentType:          d.getContentType(),
			ACL:                  d.getACL(),
			SSEKMSKeyId:          d.getSSEKMSKeyID(),
			ServerSideEncryption: d.getEncryptionMode(),
			StorageClass:         d.getStorageClass(),
		})
	if err != nil {
		return err
	}

	numParts := (fileInfo.Size() + d.MultipartCopyChunkSize - 1) / d.MultipartCopyChunkSize
	completedParts := make([]*s3.CompletedPart, numParts)
	errChan := make(chan error, numParts)

	// Reduce the client/server exposure to long lived connections regardless of
	// how many requests per second are allowed.
	limiter := make(chan struct{}, d.MultipartCopyMaxConcurrency)

	for i := range completedParts {
		i := int64(i) // nolint: gosec // index will always be a non-negative number
		go func() {
			limiter <- struct{}{}

			firstByte := i * d.MultipartCopyChunkSize
			lastByte := firstByte + d.MultipartCopyChunkSize - 1
			if lastByte >= fileInfo.Size() {
				lastByte = fileInfo.Size() - 1
			}

			uploadResp, err := d.S3.UploadPartCopyWithContext(
				ctx,
				&s3.UploadPartCopyInput{
					Bucket:          aws.String(d.Bucket),
					CopySource:      aws.String(d.Bucket + "/" + d.s3Path(sourcePath)),
					Key:             aws.String(d.s3Path(destPath)),
					PartNumber:      aws.Int64(i + 1),
					UploadId:        createResp.UploadId,
					CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", firstByte, lastByte)),
				})
			if err == nil {
				completedParts[i] = &s3.CompletedPart{
					ETag:       uploadResp.CopyPartResult.ETag,
					PartNumber: aws.Int64(i + 1),
				}
			}
			errChan <- err
			<-limiter
		}()
	}

	for range completedParts {
		err := <-errChan
		if err != nil {
			return err
		}
	}

	_, err = d.S3.CompleteMultipartUploadWithContext(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(d.Bucket),
			Key:             aws.String(d.s3Path(destPath)),
			UploadId:        createResp.UploadId,
			MultipartUpload: &s3.CompletedMultipartUpload{Parts: completedParts},
		})
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
// We must be careful since S3 does not guarantee read after delete consistency
func (d *driver) Delete(ctx context.Context, path string) error {
	s3Objects := make([]*s3.ObjectIdentifier, 0, listMax)
	s3Path := d.s3Path(path)
	listObjectsV2Input := &s3.ListObjectsV2Input{
		Bucket: aws.String(d.Bucket),
		Prefix: aws.String(s3Path),
	}
ListLoop:
	for {
		// list all the objects
		resp, err := d.S3.ListObjectsV2WithContext(ctx, listObjectsV2Input)

		// resp.Contents can only be empty on the first call if there were no
		// more results to return after the first call, resp.IsTruncated would
		// have been false and the loop would be exited without recalling
		// ListObjects
		if err != nil || len(resp.Contents) == 0 {
			return storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
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
			s3Objects = append(s3Objects, &s3.ObjectIdentifier{
				Key: key.Key,
			})
		}

		// resp.Contents must have at least one element or we would have
		// returned not found
		listObjectsV2Input.StartAfter = resp.Contents[len(resp.Contents)-1].Key

		// from the s3 api docs, IsTruncated "specifies whether (true) or not (false) all of the results were returned"
		// if everything has been returned, break
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
	}

	// need to chunk objects into groups of deleteMax per s3 restrictions
	total := len(s3Objects)
	for i := 0; i < total; i += common.DeleteMax {
		resp, err := d.S3.DeleteObjectsWithContext(
			ctx,
			&s3.DeleteObjectsInput{
				Bucket: aws.String(d.Bucket),
				Delete: &s3.Delete{
					Objects: s3Objects[i:min(i+common.DeleteMax, total)],
					Quiet:   aws.Bool(false),
				},
			})
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
	s3Objects := make([]*s3.ObjectIdentifier, 0, len(paths))
	for i := range paths {
		p := d.s3Path(paths[i])
		s3Objects = append(s3Objects, &s3.ObjectIdentifier{Key: &p})
	}

	var (
		result       *multierror.Error
		deletedCount int
	)

	// chunk files into batches of deleteMax (as per S3 restrictions).
	total := len(s3Objects)
	for i := 0; i < total; i += common.DeleteMax {
		resp, err := d.S3.DeleteObjectsWithContext(
			ctx,
			&s3.DeleteObjectsInput{
				Bucket: aws.String(d.Bucket),
				Delete: &s3.Delete{
					Objects: s3Objects[i:min(i+common.DeleteMax, total)],
					Quiet:   aws.Bool(false),
				},
			})
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
func (d *driver) URLFor(_ context.Context, path string, options map[string]any) (string, error) {
	methodString := http.MethodGet
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != http.MethodGet && methodString != http.MethodHead) {
			return "", storagedriver.ErrUnsupportedMethod{DriverName: DriverName}
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

	var req *request.Request

	switch methodString {
	case http.MethodGet:
		req, _ = d.S3.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(d.s3Path(path)),
		})
	case http.MethodHead:
		req, _ = d.S3.HeadObjectRequest(&s3.HeadObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(d.s3Path(path)),
		})
	default:
		panic("unreachable")
	}

	return req.Presign(expiresIn)
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
		return storagedriver.PathNotFoundError{Path: from, DriverName: DriverName}
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
		return storagedriver.PathNotFoundError{Path: from, DriverName: DriverName}
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
	var retError error

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket:    aws.String(d.Bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(listMax),
	}

	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("s3aws.ListObjectsV2Pages(%s)", path)

	listObjectErr := d.S3.ListObjectsV2PagesWithContext(ctx, listObjectsInput, func(objects *s3.ListObjectsV2Output, _ bool) bool {
		var count int64
		// KeyCount was introduced with version 2 of the GET Bucket operation in S3.
		// Some S3 implementations don't support V2 now, so we fall back to manual
		// calculation of the key count if required
		if objects.KeyCount != nil {
			count = *objects.KeyCount
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

			if err == storagedriver.ErrSkipDir {
				if walkInfo.IsDir() {
					continue
				}
				break
			} else if err != nil {
				retError = err
				return false
			}

			if walkInfo.IsDir() {
				if err := d.doWalk(ctx, objectCount, *walkInfo.prefix, prefix, f); err != nil {
					retError = err
					return false
				}
			}
		}
		return true
	})

	if retError != nil {
		return retError
	}

	if listObjectErr != nil {
		return listObjectErr
	}

	return nil
}

func (d *driver) doWalkParallel(parentCtx context.Context, wg *sync.WaitGroup, countChan chan<- int64, quit <-chan struct{}, errCh chan<- error, path, prefix string, f storagedriver.WalkFn) {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket:    aws.String(d.Bucket),
		Prefix:    aws.String(path),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(listMax),
	}

	ctx, done := dcontext.WithTrace(parentCtx)
	defer done("s3aws.ListObjectsV2Pages(%s)", path)

	listObjectErr := d.S3.ListObjectsV2PagesWithContext(ctx, listObjectsInput, func(objects *s3.ListObjectsV2Output, _ bool) bool {
		select {
		// The walk was canceled, return to stop requests for pages and prevent gorountines from leaking.
		case <-quit:
			return false
		default:

			var count int64
			// KeyCount was introduced with version 2 of the GET Bucket operation in S3.
			// Some S3 implementations don't support V2 now, so we fall back to manual
			// calculation of the key count if required
			if objects.KeyCount != nil {
				count = *objects.KeyCount
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

					if err == storagedriver.ErrSkipDir && wInfo.IsDir() {
						return
					}

					if err != nil {
						errCh <- err
					}

					if wInfo.IsDir() {
						d.doWalkParallel(ctx, wg, countChan, quit, errCh, *wInfo.prefix, prefix, f)
					}
				}()
			}
		}
		return true
	})

	if listObjectErr != nil {
		errCh <- listObjectErr
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
	if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "NoSuchKey" {
		return storagedriver.PathNotFoundError{Path: path, DriverName: DriverName}
	}

	return err
}

func (d *driver) getEncryptionMode() *string {
	if !d.Encrypt {
		return nil
	}
	if d.KeyID == "" {
		return aws.String("AES256")
	}
	return aws.String("aws:kms")
}

func (d *driver) getSSEKMSKeyID() *string {
	if d.KeyID != "" {
		return aws.String(d.KeyID)
	}
	return nil
}

func (*driver) getContentType() *string {
	return aws.String("application/octet-stream")
}

func (d *driver) getACL() *string {
	if d.ObjectOwnership {
		return nil
	}
	return aws.String(d.ObjectACL)
}

func (d *driver) getStorageClass() *string {
	if d.StorageClass == common.StorageClassNone {
		return nil
	}
	return aws.String(d.StorageClass)
}

// writer attempts to upload parts to S3 in a buffered fashion where the last
// part is at least as large as the chunksize, so the multipart upload could be
// cleanly resumed in the future. This is violated if Close is called after less
// than a full chunk is written.
type writer struct {
	driver      *driver
	key         string
	uploadID    string
	parts       []*s3.Part
	size        int64
	readyPart   []byte
	pendingPart []byte
	closed      bool
	committed   bool
	canceled    bool
}

func (d *driver) newWriter(key, uploadID string, parts []*s3.Part) storagedriver.FileWriter {
	var size int64
	for _, part := range parts {
		size += *part.Size
	}
	return &writer{
		driver:   d,
		key:      key,
		uploadID: uploadID,
		parts:    parts,
		size:     size,
	}
}

type completedParts []*s3.CompletedPart

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

	// If the last written part is smaller than minChunkSize, we need to make a
	// new multipart upload
	if len(w.parts) > 0 && *w.parts[len(w.parts)-1].Size < common.MinChunkSize {
		var completedUploadedParts completedParts
		for _, part := range w.parts {
			completedUploadedParts = append(completedUploadedParts, &s3.CompletedPart{
				ETag:       part.ETag,
				PartNumber: part.PartNumber,
			})
		}

		sort.Sort(completedUploadedParts)

		_, err := w.driver.S3.CompleteMultipartUploadWithContext(
			ctx,
			&s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(w.driver.Bucket),
				Key:      aws.String(w.key),
				UploadId: aws.String(w.uploadID),
				MultipartUpload: &s3.CompletedMultipartUpload{
					Parts: completedUploadedParts,
				},
			})
		if err != nil {
			_, errIn := w.driver.S3.AbortMultipartUploadWithContext(
				ctx,
				&s3.AbortMultipartUploadInput{
					Bucket:   aws.String(w.driver.Bucket),
					Key:      aws.String(w.key),
					UploadId: aws.String(w.uploadID),
				})
			if errIn != nil {
				return 0, fmt.Errorf("aborting upload failed while handling error %w: %w", err, errIn)
			}
			return 0, err
		}

		resp, err := w.driver.S3.CreateMultipartUploadWithContext(
			ctx,
			&s3.CreateMultipartUploadInput{
				Bucket:               aws.String(w.driver.Bucket),
				Key:                  aws.String(w.key),
				ContentType:          w.driver.getContentType(),
				ACL:                  w.driver.getACL(),
				ServerSideEncryption: w.driver.getEncryptionMode(),
				StorageClass:         w.driver.getStorageClass(),
			})
		if err != nil {
			return 0, err
		}
		w.uploadID = *resp.UploadId

		// If the entire written file is smaller than minChunkSize, we need to make
		// a new part from scratch
		if w.size < common.MinChunkSize {
			resp, err := w.driver.S3.GetObjectWithContext(
				ctx,
				&s3.GetObjectInput{
					Bucket: aws.String(w.driver.Bucket),
					Key:    aws.String(w.key),
				})
			if err != nil {
				return 0, err
			}
			defer resp.Body.Close()
			w.parts = nil
			w.readyPart, err = io.ReadAll(resp.Body)
			if err != nil {
				return 0, err
			}
		} else {
			// Otherwise we can use the old file as the new first part
			copyPartResp, err := w.driver.S3.UploadPartCopyWithContext(
				ctx,
				&s3.UploadPartCopyInput{
					Bucket:     aws.String(w.driver.Bucket),
					CopySource: aws.String(w.driver.Bucket + "/" + w.key),
					Key:        aws.String(w.key),
					PartNumber: aws.Int64(1),
					UploadId:   resp.UploadId,
				})
			if err != nil {
				return 0, err
			}
			w.parts = []*s3.Part{
				{
					ETag:       copyPartResp.CopyPartResult.ETag,
					PartNumber: aws.Int64(1),
					Size:       aws.Int64(w.size),
				},
			}
		}
	}

	var n int64

	for len(p) > 0 {
		// If no parts are ready to write, fill up the first part
		if neededBytes := w.driver.ChunkSize - int64(len(w.readyPart)); neededBytes > 0 {
			if int64(len(p)) >= neededBytes {
				w.readyPart = append(w.readyPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
			} else {
				w.readyPart = append(w.readyPart, p...)
				n += int64(len(p))
				p = nil
			}
		}

		if neededBytes := w.driver.ChunkSize - int64(len(w.pendingPart)); neededBytes > 0 {
			if int64(len(p)) >= neededBytes {
				w.pendingPart = append(w.pendingPart, p[:neededBytes]...)
				n += neededBytes
				p = p[neededBytes:]
				err := w.flushPart()
				// nolint: revive // max-control-nesting: control flow nesting exceeds 3
				if err != nil {
					w.size += n
					return int(n), err // nolint: gosec // n is never going to be negative
				}
			} else {
				w.pendingPart = append(w.pendingPart, p...)
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

	err := w.flushPart()
	if err != nil {
		return fmt.Errorf("fluxing buffers while closing writer: %w", err)
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

	_, err := w.driver.S3.AbortMultipartUploadWithContext(
		context.Background(),
		&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(w.driver.Bucket),
			Key:      aws.String(w.key),
			UploadId: aws.String(w.uploadID),
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
	err := w.flushPart()
	if err != nil {
		return err
	}
	w.committed = true

	var completedUploadedParts completedParts
	for _, part := range w.parts {
		completedUploadedParts = append(completedUploadedParts, &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}

	sort.Sort(completedUploadedParts)

	_, err = w.driver.S3.CompleteMultipartUploadWithContext(
		ctx,
		&s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(w.driver.Bucket),
			Key:      aws.String(w.key),
			UploadId: aws.String(w.uploadID),
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: completedUploadedParts,
			},
		})
	if err != nil {
		_, errIn := w.driver.S3.AbortMultipartUploadWithContext(
			ctx,
			&s3.AbortMultipartUploadInput{
				Bucket:   aws.String(w.driver.Bucket),
				Key:      aws.String(w.key),
				UploadId: aws.String(w.uploadID),
			})
		if errIn != nil {
			return fmt.Errorf("aborting upload failed while handling error %w: %w", err, errIn)
		}
		return err
	}
	return nil
}

// flushPart flushes buffers to write a part to S3.
// Only called by Write (with both buffers full) and Close/Commit (always)
func (w *writer) flushPart() error {
	if len(w.readyPart) == 0 && len(w.pendingPart) == 0 {
		// nothing to write
		return nil
	}
	if int64(len(w.pendingPart)) < w.driver.ChunkSize { // nolint: gosec // len() is always going to be non-negative
		// closing with a small pending part
		// combine ready and pending to avoid writing a small part
		w.readyPart = append(w.readyPart, w.pendingPart...)
		w.pendingPart = nil
	}

	partNumber := aws.Int64(int64(len(w.parts)) + 1) // nolint: gosec // len() is always going to be non-negative
	resp, err := w.driver.S3.UploadPartWithContext(
		context.Background(),
		&s3.UploadPartInput{
			Bucket:     aws.String(w.driver.Bucket),
			Key:        aws.String(w.key),
			PartNumber: partNumber,
			UploadId:   aws.String(w.uploadID),
			Body:       bytes.NewReader(w.readyPart),
		})
	if err != nil {
		return err
	}
	w.parts = append(w.parts, &s3.Part{
		ETag:       resp.ETag,
		PartNumber: partNumber,
		Size:       aws.Int64(int64(len(w.readyPart))),
	})
	w.readyPart = w.pendingPart
	w.pendingPart = nil
	return nil
}
