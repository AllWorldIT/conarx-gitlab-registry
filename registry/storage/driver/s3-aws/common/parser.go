package common

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/service/s3"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
	"github.com/hashicorp/go-multierror"

	v2_aws "github.com/aws/aws-sdk-go-v2/aws"
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
	// * common for v1 and v2:
	LogLevelOff = "logoff"
	// * v1-specific
	LogLevelDebug                    = "logdebug"
	LogLevelDebugWithSigning         = "logdebugwithsigning"
	LogLevelDebugWithHTTPBody        = "logdebugwithhttpbody"
	LogLevelDebugWithRequestRetries  = "logdebugwithrequestretries"
	LogLevelDebugWithRequestErrors   = "logdebugwithrequesterrors"
	LogLevelDebugWithEventStreamBody = "logdebugwitheventstreambody"
	// * v2-specific
	LogSigning              = "logsigning"
	LogRetries              = "logretries"
	LogRequest              = "logrequest"
	LogRequestWithBody      = "logrequestwithbody"
	LogResponse             = "logresponse"
	LogResponseWithBody     = "logresponsewithbody"
	LogDeprecatedUsage      = "logdeprecatedusage"
	LogRequestEventMessage  = "logrequesteventmessage"
	LogResponseEventMessage = "logresponseeventmessage"

	// Storage class values
	StorageClassNone = "NONE"

	// Content verification
	ParamChecksumDisabled  = "checksum_disabled"
	ParamChecksumAlgorithm = "checksum_algorithm"
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

	// EnvChecksumAlgorithm specifies the algorithm to use for checksums
	EnvChecksumDisabled  = "S3_CHECKSUM_DISABLED"
	EnvChecksumAlgorithm = "S3_CHECKSUM_ALGORITHM"
)

// validRegions maps known s3 region identifiers to region descriptors
var validRegions = make(map[string]struct{})

var validStorageClassesV1 = []string{
	StorageClassNone,
	s3.StorageClassStandard,
	s3.StorageClassReducedRedundancy,
	s3.StorageClassStandardIa,
	s3.StorageClassOnezoneIa,
	s3.StorageClassIntelligentTiering,
	s3.StorageClassOutposts,
	s3.StorageClassGlacierIr,
	s3.StorageClassExpressOnezone,
}

var validStorageClassesV2 = []string{
	StorageClassNone,
	string(types.StorageClassStandard),
	string(types.StorageClassReducedRedundancy),
	string(types.StorageClassStandardIa),
	string(types.StorageClassOnezoneIa),
	string(types.StorageClassIntelligentTiering),
	string(types.StorageClassOutposts),
	string(types.StorageClassGlacierIr),
	string(types.StorageClassExpressOnezone),
}

func init() {
	partitions := endpoints.DefaultPartitions()
	for _, p := range partitions {
		for region := range p.Regions() {
			validRegions[region] = struct{}{}
		}
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
	ChunkSize                   int64
	MultipartCopyChunkSize      int64
	MultipartCopyMaxConcurrency int
	MultipartCopyThresholdSize  int64
	RootDirectory               string
	StorageClass                string
	ObjectACL                   string
	SessionToken                string
	PathStyle                   bool
	MaxRequestsPerSecond        int64
	MaxRetries                  int64
	ParallelWalk                bool
	Logger                      dcontext.Logger
	// In order to keep the code dry, we reuse the same struct field and store
	// the result in a wider (i.e. uint64 instead of uint) type to accommodate
	// both sdks.
	LogLevel        uint64
	ObjectOwnership bool

	// v1 specific:
	V4Auth    bool
	S3APIImpl S3WrapperIf

	// v2 specific:
	ChecksumDisabled  bool
	ChecksumAlgorithm types.ChecksumAlgorithm
	Transport         http.RoundTripper
}

// ParseLogLevelParamV1 parses given loglevel into a value that sdk v1 accepts.
// The parameter itself is a comma-separated list of flags/loglevels that user
// wants to enable.
func ParseLogLevelParamV1(logger dcontext.Logger, param any) aws.LogLevelType {
	if param == nil {
		logger.Debugf("S3 logging level is not set, defaulting to %q", LogLevelOff)
		return aws.LogOff
	}

	if ll, ok := param.(aws.LogLevelType); ok {
		return ll
	}

	var res aws.LogLevelType
	var logLevelsSet []string

	for _, v := range strings.Split(strings.ToLower(param.(string)), ",") {
		switch v {
		case LogLevelOff:
			// LogLevelOff in the list of loglevels overrides all other log
			// levels and disables logging
			logger.Debugf("S3 logging level set to %q", LogLevelOff)
			return aws.LogOff
		case LogLevelDebug:
			res |= aws.LogDebug
			logLevelsSet = append(logLevelsSet, LogLevelDebug)
		case LogLevelDebugWithSigning:
			res |= aws.LogDebugWithSigning
			logLevelsSet = append(logLevelsSet, LogLevelDebugWithSigning)
		case LogLevelDebugWithHTTPBody:
			res |= aws.LogDebugWithHTTPBody
			logLevelsSet = append(logLevelsSet, LogLevelDebugWithHTTPBody)
		case LogLevelDebugWithRequestRetries:
			res |= aws.LogDebugWithRequestRetries
			logLevelsSet = append(logLevelsSet, LogLevelDebugWithRequestRetries)
		case LogLevelDebugWithRequestErrors:
			res |= aws.LogDebugWithRequestErrors
			logLevelsSet = append(logLevelsSet, LogLevelDebugWithRequestErrors)
		case LogLevelDebugWithEventStreamBody:
			res |= aws.LogDebugWithEventStreamBody
			logLevelsSet = append(logLevelsSet, LogLevelDebugWithEventStreamBody)
		// Check for v2 log levels that shouldn't be used with v1
		case LogSigning, LogRetries, LogRequest, LogRequestWithBody,
			LogResponse, LogResponseWithBody, LogDeprecatedUsage,
			LogRequestEventMessage, LogResponseEventMessage:
			logger.Warnf("S3 driver v2 log level %q has been passed to S3 driver v1. Ignoring. Please adjust your configuration", v)
		default:
			logger.Warnf("unknown loglevel %q, S3 logging level set to %q", param, LogLevelOff)
			return aws.LogOff
		}
	}

	logger.Infof("S3 logging level set to %q", strings.Join(logLevelsSet, ","))

	return res
}

// ParseLogLevelParamV2 parses given loglevel into a value that sdk v2 accepts.
// The parameter itself is a comma-separated list of flags/loglevels that user
// wants to enable.
func ParseLogLevelParamV2(logger dcontext.Logger, param any) v2_aws.ClientLogMode {
	if param == nil {
		logger.Debugf("S3 logging level is not set, defaulting to %q", LogLevelOff)
		// aws sdk v2 does not have a constant for this:
		return v2_aws.ClientLogMode(0)
	}

	if ll, ok := param.(v2_aws.ClientLogMode); ok {
		return ll
	}

	var res v2_aws.ClientLogMode
	var logLevelsSet []string

	for _, v := range strings.Split(strings.ToLower(param.(string)), ",") {
		switch v {
		// LogLevelOff in the list of loglevels overrides all other log levels
		// and disables logging
		case LogLevelOff:
			logger.Debugf("S3 logging level set to %q", LogLevelOff)
			// aws sdk v2 does not have a constant for this:
			return v2_aws.ClientLogMode(0)
		case LogSigning:
			res |= v2_aws.LogSigning
			logLevelsSet = append(logLevelsSet, LogSigning)
		case LogRetries:
			res |= v2_aws.LogRetries
			logLevelsSet = append(logLevelsSet, LogRetries)
		case LogRequest:
			res |= v2_aws.LogRequest
			logLevelsSet = append(logLevelsSet, LogRequest)
		case LogRequestWithBody:
			res |= v2_aws.LogRequestWithBody
			logLevelsSet = append(logLevelsSet, LogRequestWithBody)
		case LogResponse:
			res |= v2_aws.LogResponse
			logLevelsSet = append(logLevelsSet, LogResponse)
		case LogResponseWithBody:
			res |= v2_aws.LogResponseWithBody
			logLevelsSet = append(logLevelsSet, LogResponseWithBody)
		case LogDeprecatedUsage:
			res |= v2_aws.LogDeprecatedUsage
			logLevelsSet = append(logLevelsSet, LogDeprecatedUsage)
		case LogRequestEventMessage:
			res |= v2_aws.LogRequestEventMessage
			logLevelsSet = append(logLevelsSet, LogRequestEventMessage)
		case LogResponseEventMessage:
			res |= v2_aws.LogResponseEventMessage
			logLevelsSet = append(logLevelsSet, LogResponseEventMessage)
		// Check for v1 log levels that shouldn't be used with v2
		case LogLevelDebug, LogLevelDebugWithSigning, LogLevelDebugWithHTTPBody,
			LogLevelDebugWithRequestRetries, LogLevelDebugWithRequestErrors,
			LogLevelDebugWithEventStreamBody:
			logger.Warnf("S3 driver v1 log level %q has been passed to S3 driver v2. Ignoring. Please adjust your configuration", v)
		default:
			logger.Warnf("unknown loglevel %q, S3 logging level set to %q", param, LogLevelOff)
			return v2_aws.ClientLogMode(0)
		}
	}

	logger.Infof("S3 logging level set to %q", strings.Join(logLevelsSet, ","))

	return res
}

func ParseParameters(driverVersion string, parameters map[string]any) (*DriverParameters, error) {
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
		// NOTE(prozlach): We are using two buffers, each one can hold up to
		// chunksize bytes, and in some circumstances their contents may be
		// concatenated and committed as a single part. For example:
		//
		// https://gitlab.com/gitlab-org/container-registry/-/blob/0604f5b44093b9647dcbf5f4f7a0d6ab824ff0a4/registry/storage/driver/s3-aws/v2/s3.go?page=2#L1447-L1450
		//
		// We need to halve the limit in order to not to exceed MaxChunkSize.
		DefaultChunkSize, MinChunkSize, MaxChunkSize/2,
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

	multipartCopyMaxConcurrency, err := parse.Int32(
		parameters,
		ParamMultipartCopyMaxConcurrency,
		DefaultMultipartCopyMaxConcurrency, 1, math.MaxInt32,
	)
	if err != nil {
		err := fmt.Errorf("converting %q to valid int64: %w", ParamMultipartCopyMaxConcurrency, err)
		mErr = multierror.Append(mErr, err)
	}
	res.MultipartCopyMaxConcurrency = int(multipartCopyMaxConcurrency)

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

	var storageClass string
	storageClassParam := parameters[ParamStorageClass]
	if storageClassParam != nil {
		storageClassString, ok := storageClassParam.(string)
		switch {
		case !ok:
			err := fmt.Errorf("the storageclass parameter must be a string: %v", storageClassParam)
			mErr = multierror.Append(mErr, err)
		case (driverVersion == V1DriverName || driverVersion == V1DriverNameAlt):
			// All valid storage class parameters are UPPERCASE, so be a bit more flexible here
			storageClassString = strings.ToUpper(storageClassString)
			if !slices.Contains(validStorageClassesV1, storageClassString) {
				err := fmt.Errorf(
					"the storageclass parameter must be one of %v, %v is invalid",
					strings.Join(validStorageClassesV1, ","), storageClassParam,
				)
				mErr = multierror.Append(mErr, err)
			} else {
				storageClass = storageClassString
			}
		case driverVersion == V2DriverName:
			storageClassString = strings.ToUpper(storageClassString)
			if !slices.Contains(validStorageClassesV2, storageClassString) {
				err := fmt.Errorf(
					"the storageclass parameter must be one of %v, %v is invalid",
					strings.Join(validStorageClassesV2, ","), storageClassParam,
				)
				mErr = multierror.Append(mErr, err)
			} else {
				storageClass = storageClassString
			}
		default:
			storageClass = storageClassString
		}
	} else {
		switch {
		case (driverVersion == V1DriverName || driverVersion == V1DriverNameAlt):
			storageClass = string(s3.StorageClassStandard)
		case driverVersion == V2DriverName:
			storageClass = string(types.StorageClassStandard)
		}
	}
	res.StorageClass = storageClass

	// Parse checksum_disabled parameter
	checksumDisabled, err := parse.Bool(parameters, ParamChecksumDisabled, false)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.ChecksumDisabled = checksumDisabled

	// Parse checksum algorithm
	defaultChecksumAlgorithm := types.ChecksumAlgorithmCrc64nvme
	checksumAlgorithmParam := parameters[ParamChecksumAlgorithm]
	if checksumDisabled {
		// If checksum_disabled is true, ignore checksum_algorithm
		if checksumAlgorithmParam != nil {
			logger := parameters[driver.ParamLogger].(dcontext.Logger)
			logger.Warnf("Both %s and %s parameters provided, %s takes precedence",
				ParamChecksumDisabled, ParamChecksumAlgorithm, ParamChecksumDisabled)
		}
		defaultChecksumAlgorithm = ""
	} else if checksumAlgorithmParam != nil {
		checksumAlgorithm, ok := checksumAlgorithmParam.(string)
		if !ok {
			err := fmt.Errorf("the checksum_algorithm parameter must be a string: %v", checksumAlgorithmParam)
			mErr = multierror.Append(mErr, err)
		} else {
			// Convert to uppercase for consistency and check if it's valid
			checksumAlgorithmTyped := (types.ChecksumAlgorithm)(strings.ToUpper(checksumAlgorithm))
			// nolint: revive // max-control-nesting
			if !slices.Contains(checksumAlgorithmTyped.Values(), checksumAlgorithmTyped) {
				err := fmt.Errorf("the checksum_algorithm parameter must be one of %v, %q is invalid", checksumAlgorithmTyped.Values(), checksumAlgorithmParam)
				mErr = multierror.Append(mErr, err)
			} else {
				defaultChecksumAlgorithm = checksumAlgorithmTyped
			}
		}
	}
	res.ChecksumAlgorithm = defaultChecksumAlgorithm

	objectOwnership, err := parse.Bool(parameters, ParamObjectOwnership, false)
	if err != nil {
		mErr = multierror.Append(mErr, err)
	}
	res.ObjectOwnership = objectOwnership

	var objectACL string
	objectACLParam := parameters[ParamObjectACL]
	if objectACLParam != nil {
		if objectOwnership {
			err := fmt.Errorf("object ACL parameter should not be set when object ownership is enabled")
			mErr = multierror.Append(mErr, err)
		} else {
			objectACLString, ok := objectACLParam.(string)
			switch {
			case !ok:
				err := fmt.Errorf("object ACL parameter should be a string: %v", objectACLParam)
				mErr = multierror.Append(mErr, err)
			case (driverVersion == V1DriverName || driverVersion == V1DriverNameAlt) && !slices.Contains(s3.ObjectCannedACL_Values(), objectACLString):
				err := fmt.Errorf("object ACL parameter should be one of %v: %v", strings.Join(s3.ObjectCannedACL_Values(), ","), objectACLParam)
				mErr = multierror.Append(mErr, err)
			case driverVersion == V2DriverName && !slices.Contains(types.ObjectCannedACLPrivate.Values(), (types.ObjectCannedACL)(objectACLString)):
				// typecast:
				strValues := make([]string, len(types.ObjectCannedACLPrivate.Values()))
				for i, v := range types.ObjectCannedACLPrivate.Values() {
					strValues[i] = string(v)
				}
				err := fmt.Errorf("object ACL parameter should be one of %v: %v", strings.Join(strValues, ","), objectACLParam)
				mErr = multierror.Append(mErr, err)
			default:
				objectACL = objectACLString
			}
		}
	} else {
		switch {
		case (driverVersion == V1DriverName || driverVersion == V1DriverNameAlt):
			objectACL = string(s3.ObjectCannedACLPrivate)
		case driverVersion == V2DriverName:
			objectACL = string(types.ObjectCannedACLPrivate)
		}
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

	logger := parameters[driver.ParamLogger].(dcontext.Logger)
	res.Logger = logger
	switch driverVersion {
	case V1DriverName, V1DriverNameAlt:
		res.LogLevel = uint64(ParseLogLevelParamV1(logger, parameters[ParamLogLevel]))
	case V2DriverName:
		res.LogLevel = uint64(ParseLogLevelParamV2(logger, parameters[ParamLogLevel]))
	}

	return res, nil
}
