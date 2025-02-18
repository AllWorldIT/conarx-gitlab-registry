package common

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
)

const (
	// Authentication parameters
	ParamAccessKey    = "accesskey"
	ParamSecretKey    = "secretkey"
	ParamSessionToken = "sessiontoken"

	// Region configuration
	ParamRegion         = "region"
	ParamRegionEndpoint = "regionendpoint"

	// Bucket configuration
	ParamBucket          = "bucket"
	ParamRootDirectory   = "rootdirectory"
	ParamStorageClass    = "storageclass"
	ParamObjectACL       = "objectacl"
	ParamObjectOwnership = "objectownership"

	// Security and encryption
	ParamEncrypt    = "encrypt"
	ParamKeyID      = "keyid"
	ParamSecure     = "secure"
	ParamSkipVerify = "skipverify"
	ParamV4Auth     = "v4auth"

	// Chunk size configuration
	ParamChunkSize                   = "chunksize"
	ParamMultipartCopyChunkSize      = "multipartcopychunksize"
	ParamMultipartCopyMaxConcurrency = "multipartcopymaxconcurrency"
	ParamMultipartCopyThresholdSize  = "multipartcopythresholdsize"

	// Request configuration
	ParamMaxRequestsPerSecond = "maxrequestspersecond"
	ParamMaxRetries           = "maxretries"

	// Path and logging configuration
	ParamPathStyle    = "pathstyle"
	ParamParallelWalk = "parallelwalk"
	ParamLogLevel     = "loglevel"

	// Log level values
	LogLevelOff                      = "logoff"
	LogLevelDebug                    = "logdebug"
	LogLevelDebugWithSigning         = "logdebugwithsigning"
	LogLevelDebugWithHTTPBody        = "logdebugwithhttpbody"
	LogLevelDebugWithRequestRetries  = "logdebugwithrequestretries"
	LogLevelDebugWithRequestErrors   = "logdebugwithrequesterrors"
	LogLevelDebugWithEventStreamBody = "logdebugwitheventstreambody"

	// Storage class values
	StorageClassNone              = "NONE"
	StorageClassStandard          = s3.StorageClassStandard
	StorageClassReducedRedundancy = s3.StorageClassReducedRedundancy
)

const (
	// EnvDriverVersion defines the version of the S3 storage driver to use
	EnvDriverVersion = "S3_DRIVER_VERSION"

	// EnvAccessKey defines the AWS access key for S3 authentication
	EnvAccessKey = "AWS_ACCESS_KEY"

	// EnvSecretKey defines the AWS secret key for S3 authentication
	EnvSecretKey = "AWS_SECRET_KEY" // nolint: gosec // this is just and env var name

	// EnvBucket defines the target S3 bucket name
	EnvBucket = "S3_BUCKET"

	// EnvEncrypt enables server-side encryption for S3 objects
	EnvEncrypt = "S3_ENCRYPT"

	// EnvKeyID specifies the KMS key ID for server-side encryption
	EnvKeyID = "S3_KEY_ID"

	// EnvSecure enables HTTPS for S3 connections
	EnvSecure = "S3_SECURE"

	// EnvSkipVerify disables SSL certificate verification
	EnvSkipVerify = "S3_SKIP_VERIFY"

	// EnvV4Auth is used to disable AWS Signature Version 4 authentication
	EnvV4Auth = "S3_V4_AUTH"

	// EnvRegion specifies the AWS region for S3 operations
	EnvRegion = "AWS_REGION"

	// EnvObjectACL defines the access control list for S3 objects
	EnvObjectACL = "S3_OBJECT_ACL"

	// EnvRegionEndpoint specifies a custom S3 endpoint URL
	EnvRegionEndpoint = "REGION_ENDPOINT"

	// EnvSessionToken provides temporary AWS credentials
	EnvSessionToken = "AWS_SESSION_TOKEN" // nolint: gosec // this is just and env var name

	// EnvPathStyle enables path-style S3 URLs instead of virtual-hosted-style
	EnvPathStyle = "AWS_PATH_STYLE"

	// EnvMaxRequestsPerSecond limits the rate of S3 API requests
	EnvMaxRequestsPerSecond = "S3_MAX_REQUESTS_PER_SEC"

	// EnvMaxRetries specifies the maximum number of retry attempts for failed S3 operations
	EnvMaxRetries = "S3_MAX_RETRIES"

	// EnvLogLevel sets the logging verbosity for S3 operations
	EnvLogLevel = "S3_LOG_LEVEL"

	// EnvObjectOwnership configures the object ownership settings for the S3 bucket
	EnvObjectOwnership = "S3_OBJECT_OWNERSHIP"
)

const (
	// MinChunkSize defines the minimum multipart upload chunk size S3 API
	// requires multipart upload chunks to be at least 5MB
	MinChunkSize = 5 << 20

	// MaxChunkSize defines the maximum multipart upload chunk size allowed by
	// S3.
	MaxChunkSize = 5 << 30

	DefaultChunkSize = 2 * MinChunkSize

	// DefaultMultipartCopyChunkSize defines the default chunk size for all but
	// the last Upload Part - Copy operation of a multipart copy. Empirically,
	// 32 MB is optimal.
	DefaultMultipartCopyChunkSize = 32 << 20

	// DefaultMultipartCopyMaxConcurrency defines the default maximum number of
	// concurrent Upload Part - Copy operations for a multipart copy.
	DefaultMultipartCopyMaxConcurrency = 100

	// DefaultMultipartCopyThresholdSize defines the default object size above
	// which multipart copy will be used. (PUT Object - Copy is used for
	// objects at or below this size.)  Empirically, 32 MB is optimal.
	DefaultMultipartCopyThresholdSize = 32 << 20

	// DefaultMaxRequestsPerSecond defines the default maximum number of
	// requests per second that can be made to the S3 API per driver instance.
	// 350 is 10% of the requestsPerSecondUpperLimit based on the figures
	// listed in:
	// https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html
	DefaultMaxRequestsPerSecond = 350

	// DefaultMaxRetries is how many times the driver will retry failed
	// requests.
	DefaultMaxRetries = 5
)

// validRegions maps known s3 region identifiers to region descriptors
var validRegions = make(map[string]struct{})

// validObjectACLs contains known s3 object Acls
var validObjectACLs = make(map[string]struct{})

func init() {
	partitions := endpoints.DefaultPartitions()
	for _, p := range partitions {
		for region := range p.Regions() {
			validRegions[region] = struct{}{}
		}
	}

	for _, objectACL := range []string{
		s3.ObjectCannedACLPrivate,
		s3.ObjectCannedACLPublicRead,
		s3.ObjectCannedACLPublicReadWrite,
		s3.ObjectCannedACLAuthenticatedRead,
		s3.ObjectCannedACLAwsExecRead,
		s3.ObjectCannedACLBucketOwnerRead,
		s3.ObjectCannedACLBucketOwnerFullControl,
	} {
		validObjectACLs[objectACL] = struct{}{}
	}
}

// DriverParameters A struct that encapsulates all of the driver parameters
// after all values have been set
type DriverParameters struct {
	AccessKey                   string
	SecretKey                   string
	Bucket                      string
	Region                      string
	RegionEndpoint              string
	Encrypt                     bool
	KeyID                       string
	Secure                      bool
	SkipVerify                  bool
	V4Auth                      bool
	ChunkSize                   int64
	MultipartCopyChunkSize      int64
	MultipartCopyMaxConcurrency int64
	MultipartCopyThresholdSize  int64
	RootDirectory               string
	StorageClass                string
	ObjectACL                   string
	SessionToken                string
	PathStyle                   bool
	MaxRequestsPerSecond        int64
	MaxRetries                  int64
	ParallelWalk                bool
	LogLevel                    aws.LogLevelType
	ObjectOwnership             bool
}

func ParseLogLevelParam(param any) aws.LogLevelType {
	logLevel := aws.LogOff

	if param != nil {
		if ll, ok := param.(aws.LogLevelType); ok {
			return ll
		}

		switch strings.ToLower(param.(string)) {
		case LogLevelOff:
			log.Infof("S3 logging level set to %q", LogLevelOff)
			logLevel = aws.LogOff
		case LogLevelDebug:
			log.Infof("S3 logging level set to %q", LogLevelDebug)
			logLevel = aws.LogDebug
		case LogLevelDebugWithSigning:
			log.Infof("S3 logging level set to %q", LogLevelDebugWithSigning)
			logLevel = aws.LogDebugWithSigning
		case LogLevelDebugWithHTTPBody:
			log.Infof("S3 logging level set to %q", LogLevelDebugWithHTTPBody)
			logLevel = aws.LogDebugWithHTTPBody
		case LogLevelDebugWithRequestRetries:
			log.Infof("S3 logging level set to %q", LogLevelDebugWithRequestRetries)
			logLevel = aws.LogDebugWithRequestRetries
		case LogLevelDebugWithRequestErrors:
			log.Infof("S3 logging level set to %q", LogLevelDebugWithRequestErrors)
			logLevel = aws.LogDebugWithRequestErrors
		case LogLevelDebugWithEventStreamBody:
			log.Infof("S3 logging level set to %q", LogLevelDebugWithEventStreamBody)
			logLevel = aws.LogDebugWithEventStreamBody
		default:
			log.Infof("unknown loglevel %q, S3 logging level set to %q", param, LogLevelOff)
		}
	}

	return logLevel
}

func ParseParameters(parameters map[string]any) (*DriverParameters, error) {
	var mErr *multierror.Error
	res := new(DriverParameters)

	// Providing no values for these is valid in case the user is authenticating
	// with an IAM on an ec2 instance (in which case the instance credentials will
	// be summoned when GetAuth is called)
	accessKey := parameters[ParamAccessKey]
	if accessKey == nil {
		accessKey = ""
	}
	res.AccessKey = fmt.Sprint(accessKey)

	secretKey := parameters[ParamSecretKey]
	if secretKey == nil {
		// nolint: gosec // G101 -- This is a false positive
		secretKey = ""
	}
	res.SecretKey = fmt.Sprint(secretKey)

	regionEndpoint := parameters[ParamRegionEndpoint]
	if regionEndpoint == nil {
		regionEndpoint = ""
	}
	res.RegionEndpoint = fmt.Sprint(regionEndpoint)

	regionName := parameters[ParamRegion]
	if regionName == nil || fmt.Sprint(regionName) == "" {
		err := fmt.Errorf("no %q parameter provided", ParamRegion)
		mErr = multierror.Append(mErr, err)
	}
	region := fmt.Sprint(regionName)
	res.Region = region
	// Don't check the region value if a custom endpoint is provided.
	if regionEndpoint == "" {
		if _, ok := validRegions[region]; !ok {
			err := fmt.Errorf("validating region provided: %v", region)
			mErr = multierror.Append(mErr, err)
		}
	}
	res.RegionEndpoint = fmt.Sprint(regionEndpoint)

	bucket := parameters[ParamBucket]
	if bucket == nil || fmt.Sprint(bucket) == "" {
		err := errors.New("no bucket parameter provided")
		mErr = multierror.Append(mErr, err)
	}
	res.Bucket = fmt.Sprint(bucket)

	encryptEnable, err := parse.Bool(parameters, ParamEncrypt, false)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.Encrypt = encryptEnable

	secureEnable, err := parse.Bool(parameters, ParamSecure, true)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.Secure = secureEnable

	skipVerifyEnable, err := parse.Bool(parameters, ParamSkipVerify, false)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.SkipVerify = skipVerifyEnable

	v4Enable, err := parse.Bool(parameters, ParamV4Auth, true)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.V4Auth = v4Enable

	keyID := parameters[ParamKeyID]
	if keyID == nil {
		keyID = ""
	}
	res.KeyID = fmt.Sprint(keyID)

	chunkSize, err := parse.Int64(
		parameters,
		ParamChunkSize,
		DefaultChunkSize, MinChunkSize, MaxChunkSize,
	)
	if err != nil {
		err := fmt.Errorf("converting %q to int64: %w", ParamChunkSize, err)
		mErr = multierror.Append(mErr, err)
	}
	res.ChunkSize = chunkSize

	multipartCopyChunkSize, err := parse.Int64(
		parameters,
		ParamMultipartCopyChunkSize,
		DefaultMultipartCopyChunkSize, MinChunkSize, MaxChunkSize,
	)
	if err != nil {
		err := fmt.Errorf("converting %q to valid int64: %w", ParamMultipartCopyChunkSize, err)
		mErr = multierror.Append(mErr, err)
	}
	res.MultipartCopyChunkSize = multipartCopyChunkSize

	multipartCopyMaxConcurrency, err := parse.Int64(
		parameters,
		ParamMultipartCopyMaxConcurrency,
		DefaultMultipartCopyMaxConcurrency, 1, math.MaxInt64,
	)
	if err != nil {
		err := fmt.Errorf("converting %q to valid int64: %w", ParamMultipartCopyMaxConcurrency, err)
		mErr = multierror.Append(mErr, err)
	}
	res.MultipartCopyMaxConcurrency = multipartCopyMaxConcurrency

	multipartCopyThresholdSize, err := parse.Int64(
		parameters,
		ParamMultipartCopyThresholdSize,
		DefaultMultipartCopyThresholdSize, 0, MaxChunkSize,
	)
	if err != nil {
		err := fmt.Errorf("converting %q to valid int64: %w", ParamMultipartCopyThresholdSize, err)
		mErr = multierror.Append(mErr, err)
	}
	res.MultipartCopyThresholdSize = multipartCopyThresholdSize

	rootDirectory := parameters[ParamRootDirectory]
	if rootDirectory == nil {
		rootDirectory = ""
	}
	res.RootDirectory = fmt.Sprint(rootDirectory)

	storageClass := s3.StorageClassStandard
	storageClassParam := parameters[ParamStorageClass]
	if storageClassParam != nil {
		storageClassString, ok := storageClassParam.(string)
		if !ok {
			err := fmt.Errorf("the storageclass parameter must be one of %v, %v invalid",
				[]string{s3.StorageClassStandard, s3.StorageClassReducedRedundancy}, storageClassParam)
			mErr = multierror.Append(mErr, err)
		}
		// All valid storage class parameters are UPPERCASE, so be a bit more flexible here
		storageClassString = strings.ToUpper(storageClassString)
		if storageClassString != StorageClassNone &&
			storageClassString != s3.StorageClassStandard &&
			storageClassString != s3.StorageClassReducedRedundancy {
			err := fmt.Errorf("the storageclass parameter must be one of %v, %v invalid",
				[]string{StorageClassNone, s3.StorageClassStandard, s3.StorageClassReducedRedundancy}, storageClassParam)
			mErr = multierror.Append(mErr, err)
		}
		storageClass = storageClassString
	}
	res.StorageClass = storageClass

	objectOwnership, err := parse.Bool(parameters, ParamObjectOwnership, false)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.ObjectOwnership = objectOwnership

	objectACL := s3.ObjectCannedACLPrivate
	objectACLParam := parameters[ParamObjectACL]
	if objectACLParam != nil {
		if objectOwnership {
			err := fmt.Errorf("object ACL parameter should not be set when object ownership is enabled")
			mErr = multierror.Append(mErr, err)
		}

		objectACLString, ok := objectACLParam.(string)
		if !ok {
			err := fmt.Errorf("object ACL parameter should be a string: %v", objectACLParam)
			mErr = multierror.Append(mErr, err)
		}

		if _, ok = validObjectACLs[objectACLString]; !ok {
			var objectACLkeys []string
			for key := range validObjectACLs {
				objectACLkeys = append(objectACLkeys, key)
			}
			err := fmt.Errorf("object ACL parameter should be one of %v: %v", objectACLkeys, objectACLParam)
			mErr = multierror.Append(mErr, err)
		}
		objectACL = objectACLString
	}
	res.ObjectACL = objectACL

	// If regionEndpoint is set, default to forcing pathstyle to preserve legacy behavior.
	defaultPathStyle := regionEndpoint != ""
	pathStyleBool, err := parse.Bool(parameters, ParamPathStyle, defaultPathStyle)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.PathStyle = pathStyleBool

	parallelWalkBool, err := parse.Bool(parameters, ParamParallelWalk, false)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.ParallelWalk = parallelWalkBool

	maxRequestsPerSecond, err := parse.Int64(parameters, ParamMaxRequestsPerSecond, DefaultMaxRequestsPerSecond, 0, math.MaxInt64)
	if err != nil {
		err = fmt.Errorf("converting maxrequestspersecond to valid int64: %w", err)
		mErr = multierror.Append(mErr, err)
	}
	res.MaxRequestsPerSecond = maxRequestsPerSecond

	maxRetries, err := parse.Int64(
		parameters,
		ParamMaxRetries,
		DefaultMaxRetries, 0, math.MaxInt64,
	)
	if err != nil {
		err := fmt.Errorf("converting maxrequestspersecond to valid int64: %w", err)
		mErr = multierror.Append(mErr, err)
	}
	res.MaxRetries = maxRetries

	if err := mErr.ErrorOrNil(); err != nil {
		return nil, err
	}

	res.LogLevel = ParseLogLevelParam(parameters[ParamLogLevel])

	return res, nil
}
