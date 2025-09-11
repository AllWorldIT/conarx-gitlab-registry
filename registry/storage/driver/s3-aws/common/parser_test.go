package common

import (
	"bytes"
	"fmt"
	"testing"

	v2_aws "github.com/aws/aws-sdk-go-v2/aws"
	v2_types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/docker/distribution/log"
	dtestutil "github.com/docker/distribution/registry/storage/driver/internal/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseParameters_in_groups(t *testing.T) {
	tests := []struct {
		name                  string
		restrictDriverVersion string
		params                map[string]any
		expected              *DriverParameters
		expectedError         bool
		errorContains         string
	}{
		{
			name: "valid minimal configuration",
			params: map[string]any{
				ParamRegion: "us-west-2",
				ParamBucket: "test-bucket",
			},
			expected: &DriverParameters{
				AccessKey:                   "",
				SecretKey:                   "",
				Region:                      "us-west-2",
				RegionEndpoint:              "",
				Bucket:                      "test-bucket",
				Encrypt:                     false,
				Secure:                      true,
				V4Auth:                      true,
				ChunkSize:                   DefaultChunkSize,
				MultipartCopyChunkSize:      DefaultMultipartCopyChunkSize,
				MultipartCopyMaxConcurrency: DefaultMultipartCopyMaxConcurrency,
				MultipartCopyThresholdSize:  DefaultMultipartCopyThresholdSize,
				RootDirectory:               "",
				StorageClass:                s3.StorageClassStandard,
				ObjectACL:                   s3.ObjectCannedACLPrivate,
				PathStyle:                   false,
				MaxRequestsPerSecond:        DefaultMaxRequestsPerSecond,
				MaxRetries:                  DefaultMaxRetries,
				ParallelWalk:                false,
				LogLevel:                    uint64(aws.LogOff),
				ObjectOwnership:             false,
				KeyID:                       "",
				ChecksumDisabled:            false,
				ChecksumAlgorithm:           v2_types.ChecksumAlgorithmCrc64nvme,
			},
			expectedError: false,
		},
		{
			name:                  "full configuration with all parameters v1",
			restrictDriverVersion: V1DriverName,
			params: map[string]any{
				ParamRegion:                      "us-east-1",
				ParamBucket:                      "test-bucket-full",
				ParamAccessKey:                   "test-access-key",
				ParamSecretKey:                   "test-secret-key",
				ParamEncrypt:                     true,
				ParamKeyID:                       "test-key-id",
				ParamSecure:                      false,
				ParamSkipVerify:                  true,
				ParamV4Auth:                      false,
				ParamChunkSize:                   10 << 20, // 10MB
				ParamMultipartCopyChunkSize:      64 << 20, // 64MB
				ParamMultipartCopyMaxConcurrency: 50,
				ParamMultipartCopyThresholdSize:  128 << 20, // 128MB
				ParamRootDirectory:               "/test/root",
				ParamStorageClass:                s3.StorageClassReducedRedundancy,
				ParamObjectACL:                   s3.ObjectCannedACLPublicRead,
				ParamPathStyle:                   true,
				ParamMaxRequestsPerSecond:        500,
				ParamMaxRetries:                  10,
				ParamParallelWalk:                true,
				ParamLogLevel:                    LogLevelDebug,
			},
			expected: &DriverParameters{
				AccessKey:                   "test-access-key",
				SecretKey:                   "test-secret-key",
				Region:                      "us-east-1",
				RegionEndpoint:              "",
				Bucket:                      "test-bucket-full",
				Encrypt:                     true,
				KeyID:                       "test-key-id",
				Secure:                      false,
				SkipVerify:                  true,
				V4Auth:                      false,
				ChunkSize:                   10 << 20,
				MultipartCopyChunkSize:      64 << 20,
				MultipartCopyMaxConcurrency: 50,
				MultipartCopyThresholdSize:  128 << 20,
				RootDirectory:               "/test/root",
				StorageClass:                s3.StorageClassReducedRedundancy,
				ObjectACL:                   s3.ObjectCannedACLPublicRead,
				PathStyle:                   true,
				MaxRequestsPerSecond:        500,
				MaxRetries:                  10,
				ParallelWalk:                true,
				LogLevel:                    uint64(aws.LogDebug),
				ObjectOwnership:             false,
				ChecksumDisabled:            false,
				ChecksumAlgorithm:           v2_types.ChecksumAlgorithmCrc64nvme,
			},
			expectedError: false,
		},
		{
			name:                  "full configuration with all parameters v2",
			restrictDriverVersion: V2DriverName,
			params: map[string]any{
				ParamRegion:                      "us-east-1",
				ParamBucket:                      "test-bucket-full",
				ParamAccessKey:                   "test-access-key",
				ParamSecretKey:                   "test-secret-key",
				ParamEncrypt:                     true,
				ParamKeyID:                       "test-key-id",
				ParamSecure:                      false,
				ParamSkipVerify:                  true,
				ParamV4Auth:                      false,
				ParamChunkSize:                   10 << 20, // 10MB
				ParamMultipartCopyChunkSize:      64 << 20, // 64MB
				ParamMultipartCopyMaxConcurrency: 50,
				ParamMultipartCopyThresholdSize:  128 << 20, // 128MB
				ParamRootDirectory:               "/test/root",
				ParamStorageClass:                s3.StorageClassReducedRedundancy,
				ParamObjectACL:                   s3.ObjectCannedACLPublicRead,
				ParamPathStyle:                   true,
				ParamMaxRequestsPerSecond:        500,
				ParamMaxRetries:                  10,
				ParamParallelWalk:                true,
				ParamLogLevel:                    LogRequestEventMessage,
				ParamChecksumDisabled:            false,
				ParamChecksumAlgorithm:           string(v2_types.ChecksumAlgorithmCrc32),
			},
			expected: &DriverParameters{
				AccessKey:                   "test-access-key",
				SecretKey:                   "test-secret-key",
				Region:                      "us-east-1",
				RegionEndpoint:              "",
				Bucket:                      "test-bucket-full",
				Encrypt:                     true,
				KeyID:                       "test-key-id",
				Secure:                      false,
				SkipVerify:                  true,
				V4Auth:                      false,
				ChunkSize:                   10 << 20,
				MultipartCopyChunkSize:      64 << 20,
				MultipartCopyMaxConcurrency: 50,
				MultipartCopyThresholdSize:  128 << 20,
				RootDirectory:               "/test/root",
				StorageClass:                s3.StorageClassReducedRedundancy,
				ObjectACL:                   s3.ObjectCannedACLPublicRead,
				PathStyle:                   true,
				MaxRequestsPerSecond:        500,
				MaxRetries:                  10,
				ParallelWalk:                true,
				LogLevel:                    uint64(v2_aws.LogRequestEventMessage),
				ObjectOwnership:             false,
				ChecksumDisabled:            false,
				ChecksumAlgorithm:           v2_types.ChecksumAlgorithmCrc32,
			},
			expectedError: false,
		},
		{
			name: "configuration with object ownership enabled",
			params: map[string]any{
				ParamRegion:          "us-west-2",
				ParamBucket:          "test-bucket",
				ParamObjectOwnership: true,
				ParamStorageClass:    StorageClassNone,
				ParamRootDirectory:   "/custom/path",
				ParamSecure:          true,
				ParamV4Auth:          true,
			},
			expected: &DriverParameters{
				AccessKey:                   "",
				SecretKey:                   "",
				Region:                      "us-west-2",
				RegionEndpoint:              "",
				Bucket:                      "test-bucket",
				Encrypt:                     false,
				KeyID:                       "",
				Secure:                      true,
				SkipVerify:                  false,
				V4Auth:                      true,
				ChunkSize:                   DefaultChunkSize,
				MultipartCopyChunkSize:      DefaultMultipartCopyChunkSize,
				MultipartCopyMaxConcurrency: DefaultMultipartCopyMaxConcurrency,
				MultipartCopyThresholdSize:  DefaultMultipartCopyThresholdSize,
				RootDirectory:               "/custom/path",
				StorageClass:                StorageClassNone,
				ObjectACL:                   s3.ObjectCannedACLPrivate,
				PathStyle:                   false,
				MaxRequestsPerSecond:        DefaultMaxRequestsPerSecond,
				MaxRetries:                  DefaultMaxRetries,
				ParallelWalk:                false,
				LogLevel:                    uint64(aws.LogOff),
				ObjectOwnership:             true,
				ChecksumDisabled:            false,
				ChecksumAlgorithm:           v2_types.ChecksumAlgorithmCrc64nvme,
			},
			expectedError: false,
		},
		{
			name: "missing required parameters",
			params: map[string]any{
				ParamAccessKey: "testkey",
			},
			expectedError: true,
			errorContains: "no \"region\" parameter provided",
		},
		{
			name: "invalid region",
			params: map[string]any{
				ParamRegion: "invalid-region",
				ParamBucket: "test-bucket",
			},
			expectedError: true,
			errorContains: "validating region provided",
		},
		{
			name: "custom endpoint skips region validation",
			params: map[string]any{
				ParamRegion:         "custom-region",
				ParamRegionEndpoint: "https://custom.endpoint",
				ParamBucket:         "test-bucket",
			},
			expected: &DriverParameters{
				Region:                      "custom-region",
				RegionEndpoint:              "https://custom.endpoint",
				Bucket:                      "test-bucket",
				PathStyle:                   true, // Should default to true with custom endpoint
				MultipartCopyChunkSize:      DefaultMultipartCopyChunkSize,
				MultipartCopyMaxConcurrency: DefaultMultipartCopyMaxConcurrency,
				MultipartCopyThresholdSize:  DefaultMultipartCopyThresholdSize,
				MaxRequestsPerSecond:        DefaultMaxRequestsPerSecond,
				MaxRetries:                  DefaultMaxRetries,
				V4Auth:                      true,
				ChunkSize:                   DefaultChunkSize,
				Secure:                      true,
				StorageClass:                s3.StorageClassStandard,
				ObjectACL:                   s3.ObjectCannedACLPrivate,
				ChecksumDisabled:            false,
				ChecksumAlgorithm:           v2_types.ChecksumAlgorithmCrc64nvme,
			},
			expectedError: false,
		},
		{
			name: "invalid chunk size",
			params: map[string]any{
				ParamRegion:    "us-west-2",
				ParamBucket:    "test-bucket",
				ParamChunkSize: "invalid",
			},
			expectedError: true,
			errorContains: "converting \"chunksize\" to int64",
		},
		{
			name: "chunk size below minimum",
			params: map[string]any{
				ParamRegion:    "us-west-2",
				ParamBucket:    "test-bucket",
				ParamChunkSize: MinChunkSize - 1,
			},
			expectedError: true,
			errorContains: "chunksize",
		},
		{
			name: "invalid encrypt parameter",
			params: map[string]any{
				ParamRegion:  "us-west-2",
				ParamBucket:  "test-bucket",
				ParamEncrypt: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"encrypt\" string as bool",
		},
		{
			name: "invalid secure parameter",
			params: map[string]any{
				ParamRegion: "us-west-2",
				ParamBucket: "test-bucket",
				ParamSecure: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"secure\" string as bool",
		},
		{
			name: "invalid skip verify parameter",
			params: map[string]any{
				ParamRegion:     "us-west-2",
				ParamBucket:     "test-bucket",
				ParamSkipVerify: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"skipverify\" string as bool",
		},
		{
			name:                  "invalid v4auth parameter",
			restrictDriverVersion: V1DriverName,
			params: map[string]any{
				ParamRegion: "us-west-2",
				ParamBucket: "test-bucket",
				ParamV4Auth: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"v4auth\" string as bool",
		},
		{
			name:                  "invalid multipart copy chunk size",
			restrictDriverVersion: V1DriverName,
			params: map[string]any{
				ParamRegion:                 "us-west-2",
				ParamBucket:                 "test-bucket",
				ParamMultipartCopyChunkSize: "invalid",
			},
			expectedError: true,
			errorContains: "converting \"multipartcopychunksize\" to valid int64",
		},
		{
			name: "invalid multipart copy max concurrency",
			params: map[string]any{
				ParamRegion:                      "us-west-2",
				ParamBucket:                      "test-bucket",
				ParamMultipartCopyMaxConcurrency: "invalid",
			},
			expectedError: true,
			errorContains: "converting \"multipartcopymaxconcurrency\" to valid int64",
		},
		{
			name: "invalid multipart copy threshold size",
			params: map[string]any{
				ParamRegion:                     "us-west-2",
				ParamBucket:                     "test-bucket",
				ParamMultipartCopyThresholdSize: "invalid",
			},
			expectedError: true,
			errorContains: "converting \"multipartcopythresholdsize\" to valid int64",
		},
		{
			name: "storage class not a string",
			params: map[string]any{
				ParamRegion:       "us-west-2",
				ParamBucket:       "test-bucket",
				ParamStorageClass: 123,
			},
			expectedError: true,
			errorContains: "the storageclass parameter must be a string",
		},
		{
			name: "object ACL not a string",
			params: map[string]any{
				ParamRegion:    "us-west-2",
				ParamBucket:    "test-bucket",
				ParamObjectACL: 123,
			},
			expectedError: true,
			errorContains: "object ACL parameter should be a string",
		},
		{
			name: "invalid object ACL value",
			params: map[string]any{
				ParamRegion:    "us-west-2",
				ParamBucket:    "test-bucket",
				ParamObjectACL: "invalid-acl",
			},
			expectedError: true,
			errorContains: "object ACL parameter should be one of",
		},
		{
			name: "invalid path style parameter",
			params: map[string]any{
				ParamRegion:    "us-west-2",
				ParamBucket:    "test-bucket",
				ParamPathStyle: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"pathstyle\" string as bool",
		},
		{
			name: "invalid parallel walk parameter",
			params: map[string]any{
				ParamRegion:       "us-west-2",
				ParamBucket:       "test-bucket",
				ParamParallelWalk: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"parallelwalk\" string as bool",
		},
		{
			name: "invalid max requests per second",
			params: map[string]any{
				ParamRegion:               "us-west-2",
				ParamBucket:               "test-bucket",
				ParamMaxRequestsPerSecond: "invalid",
			},
			expectedError: true,
			errorContains: "converting maxrequestspersecond to valid int64",
		},
		{
			name: "invalid max retries",
			params: map[string]any{
				ParamRegion:     "us-west-2",
				ParamBucket:     "test-bucket",
				ParamMaxRetries: "invalid",
			},
			expectedError: true,
			errorContains: "converting maxrequestspersecond to valid int64",
		},
		{
			name: "invalid storage class",
			params: map[string]any{
				ParamRegion:       "us-west-2",
				ParamBucket:       "test-bucket",
				ParamStorageClass: "INVALID_CLASS",
			},
			expectedError: true,
			errorContains: "storageclass parameter must be one of",
		},
		{
			name: "object ACL with ownership enabled",
			params: map[string]any{
				ParamRegion:          "us-west-2",
				ParamBucket:          "test-bucket",
				ParamObjectOwnership: true,
				ParamObjectACL:       s3.ObjectCannedACLPublicRead,
			},
			expectedError: true,
			errorContains: "object ACL parameter should not be set when object ownership is enabled",
		},
		{
			name:                  "configuration with checksum_disabled",
			restrictDriverVersion: V2DriverName,
			params: map[string]any{
				ParamRegion:           "us-west-2",
				ParamBucket:           "test-bucket",
				ParamChecksumDisabled: true,
			},
			expected: &DriverParameters{
				Region:                      "us-west-2",
				Bucket:                      "test-bucket",
				PathStyle:                   false,
				MultipartCopyChunkSize:      DefaultMultipartCopyChunkSize,
				MultipartCopyMaxConcurrency: DefaultMultipartCopyMaxConcurrency,
				MultipartCopyThresholdSize:  DefaultMultipartCopyThresholdSize,
				MaxRequestsPerSecond:        DefaultMaxRequestsPerSecond,
				MaxRetries:                  DefaultMaxRetries,
				V4Auth:                      true,
				ChunkSize:                   DefaultChunkSize,
				Secure:                      true,
				StorageClass:                s3.StorageClassStandard,
				ObjectACL:                   s3.ObjectCannedACLPrivate,
				ChecksumDisabled:            true,
				ChecksumAlgorithm:           "",
			},
			expectedError: false,
		},
		{
			name:                  "configuration with checksum_disabled and checksum algo specified",
			restrictDriverVersion: V2DriverName,
			params: map[string]any{
				ParamRegion:            "us-west-2",
				ParamBucket:            "test-bucket",
				ParamChecksumDisabled:  true,
				ParamChecksumAlgorithm: string(v2_types.ChecksumAlgorithmCrc32),
			},
			expected: &DriverParameters{
				Region:                      "us-west-2",
				Bucket:                      "test-bucket",
				PathStyle:                   false,
				MultipartCopyChunkSize:      DefaultMultipartCopyChunkSize,
				MultipartCopyMaxConcurrency: DefaultMultipartCopyMaxConcurrency,
				MultipartCopyThresholdSize:  DefaultMultipartCopyThresholdSize,
				MaxRequestsPerSecond:        DefaultMaxRequestsPerSecond,
				MaxRetries:                  DefaultMaxRetries,
				V4Auth:                      true,
				ChunkSize:                   DefaultChunkSize,
				Secure:                      true,
				StorageClass:                s3.StorageClassStandard,
				ObjectACL:                   s3.ObjectCannedACLPrivate,
				ChecksumDisabled:            true,
				ChecksumAlgorithm:           "",
			},
			expectedError: false,
		},
		{
			name:                  "invalid checksum algorithm",
			restrictDriverVersion: V2DriverName,
			params: map[string]any{
				ParamRegion:            "us-west-2",
				ParamBucket:            "test-bucket",
				ParamChecksumAlgorithm: "INVALID_ALGORITHM",
			},
			expectedError: true,
			errorContains: "the checksum_algorithm parameter must be one of",
		},
		{
			name:                  "checksum algorithm not a string",
			restrictDriverVersion: V2DriverName,
			params: map[string]any{
				ParamRegion:            "us-west-2",
				ParamBucket:            "test-bucket",
				ParamChecksumAlgorithm: 123,
			},
			expectedError: true,
			errorContains: "the checksum_algorithm parameter must be a string",
		},
	}

	for _, tt := range tests {
		for _, dv := range []string{V1DriverName, V2DriverName} {
			t.Run(fmt.Sprintf("%s/%s", tt.name, dv), func(t *testing.T) {
				if tt.restrictDriverVersion != "" && tt.restrictDriverVersion != dv {
					t.Skip("skipping subtests as it does not apply for this driver")
				}

				result, err := ParseParameters(dv, tt.params)

				if tt.expectedError {
					assert.Error(t, err)
					if tt.errorContains != "" {
						assert.Contains(t, err.Error(), tt.errorContains)
					}
					return
				}

				require.NoError(t, err)
				require.NotNil(t, result)

				if tt.expected != nil {
					// Check all fields in DriverParameters
					assert.Equal(t, tt.expected.AccessKey, result.AccessKey, "AccessKey mismatch")
					assert.Equal(t, tt.expected.SecretKey, result.SecretKey, "SecretKey mismatch")
					assert.Equal(t, tt.expected.Region, result.Region, "Region mismatch")
					assert.Equal(t, tt.expected.RegionEndpoint, result.RegionEndpoint, "RegionEndpoint mismatch")
					assert.Equal(t, tt.expected.Bucket, result.Bucket, "Bucket mismatch")
					assert.Equal(t, tt.expected.RootDirectory, result.RootDirectory, "RootDirectory mismatch")
					assert.Equal(t, tt.expected.StorageClass, result.StorageClass, "StorageClass mismatch")
					assert.Equal(t, tt.expected.ObjectACL, result.ObjectACL, "ObjectACL mismatch")
					assert.Equal(t, tt.expected.ObjectOwnership, result.ObjectOwnership, "ObjectOwnership mismatch")

					// Boolean flags
					assert.Equal(t, tt.expected.Encrypt, result.Encrypt, "Encrypt mismatch")
					assert.Equal(t, tt.expected.Secure, result.Secure, "Secure mismatch")
					assert.Equal(t, tt.expected.SkipVerify, result.SkipVerify, "SkipVerify mismatch")
					assert.Equal(t, tt.expected.V4Auth, result.V4Auth, "V4Auth mismatch")
					assert.Equal(t, tt.expected.PathStyle, result.PathStyle, "PathStyle mismatch")
					assert.Equal(t, tt.expected.ParallelWalk, result.ParallelWalk, "ParallelWalk mismatch")

					// Numeric parameters
					assert.Equal(t, tt.expected.ChunkSize, result.ChunkSize, "ChunkSize mismatch")
					assert.Equal(t, tt.expected.MultipartCopyChunkSize, result.MultipartCopyChunkSize, "MultipartCopyChunkSize mismatch")
					assert.Equal(t, tt.expected.MultipartCopyMaxConcurrency, result.MultipartCopyMaxConcurrency, "MultipartCopyMaxConcurrency mismatch")
					assert.Equal(t, tt.expected.MultipartCopyThresholdSize, result.MultipartCopyThresholdSize, "MultipartCopyThresholdSize mismatch")
					assert.Equal(t, tt.expected.MaxRequestsPerSecond, result.MaxRequestsPerSecond, "MaxRequestsPerSecond mismatch")
					assert.Equal(t, tt.expected.MaxRetries, result.MaxRetries, "MaxRetries mismatch")

					// Other configurations
					assert.Equal(t, tt.expected.KeyID, result.KeyID, "KeyID mismatch")
					assert.Equal(t, tt.expected.LogLevel, result.LogLevel, "LogLevel mismatch")
					assert.Equal(t, tt.expected.ChecksumAlgorithm, result.ChecksumAlgorithm, "ChecksumAlgorithm mismatch")
				}
			})
		}
	}
}

func TestParseParameters_individually(t *testing.T) {
	p := map[string]any{
		ParamRegion: "us-west-2",
		ParamBucket: "test",
		ParamV4Auth: "true",
	}

	testFn := func(params map[string]any) (any, error) {
		// NOTE(prozlach): We are not testing here loglevel parsing, so it is
		// OK to hardcode driver version.
		return ParseParameters(V2DriverName, params)
	}

	tcs := map[string]struct {
		parameters      map[string]any
		paramName       string
		driverParamName string
		required        bool
		nilAllowed      bool
		emptyAllowed    bool
		nonTypeAllowed  bool
		defaultt        any
	}{
		"secure": {
			parameters:      p,
			paramName:       "secure",
			driverParamName: "Secure",
			defaultt:        true,
		},
		"encrypt": {
			parameters:      p,
			paramName:       "encrypt",
			driverParamName: "Encrypt",
			defaultt:        false,
		},
		"pathstyle_without_region_endpoint": {
			parameters:      p,
			paramName:       "pathstyle",
			driverParamName: "PathStyle",
			defaultt:        false,
		},
		"pathstyle_with_region_endpoint": {
			parameters: func() map[string]any {
				pp := dtestutil.CopyMap(p)
				pp["regionendpoint"] = "region/endpoint"

				return pp
			}(),
			paramName:       "pathstyle",
			driverParamName: "PathStyle",
			defaultt:        true,
		},
		"skipverify": {
			parameters:      p,
			paramName:       "skipverify",
			driverParamName: "SkipVerify",
			defaultt:        false,
		},
		"v4auth": {
			parameters:      p,
			paramName:       "v4auth",
			driverParamName: "V4Auth",
			required:        true,
			defaultt:        true,
		},
		"parallelwalk": {
			parameters:      p,
			paramName:       "parallelwalk",
			driverParamName: "ParallelWalk",
			defaultt:        false,
		},
		"accesskey": {
			parameters:      p,
			paramName:       "accesskey",
			driverParamName: "AccessKey",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"secretkey": {
			parameters:      p,
			paramName:       "secretkey",
			driverParamName: "SecretKey",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"regionendpoint": {
			parameters:      p,
			paramName:       "regionendpoint",
			driverParamName: "RegionEndpoint",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"region": {
			parameters:      p,
			paramName:       "region",
			driverParamName: "Region",
			nilAllowed:      false,
			emptyAllowed:    false,
			// not allowed because we check validRegions[region] when regionendpoint is empty
			nonTypeAllowed: false,
			required:       true,
			defaultt:       "",
		},
		"region_with_regionendpoint": {
			parameters: func() map[string]any {
				pp := dtestutil.CopyMap(p)
				pp["regionendpoint"] = "region/endpoint"

				return pp
			}(),
			paramName:       "region",
			driverParamName: "Region",
			nilAllowed:      false,
			emptyAllowed:    false,
			// allowed because we don't check validRegions[region] when regionendpoint is not empty
			nonTypeAllowed: true,
			required:       true,
			defaultt:       "",
		},
		"bucket": {
			parameters:      p,
			paramName:       "bucket",
			driverParamName: "Bucket",
			nilAllowed:      false,
			emptyAllowed:    false,
			nonTypeAllowed:  true,
			required:        true,
			defaultt:        "",
		},
		"keyid": {
			parameters:      p,
			paramName:       "keyid",
			driverParamName: "KeyID",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"rootdirectory": {
			parameters:      p,
			paramName:       "rootdirectory",
			driverParamName: "RootDirectory",
			nilAllowed:      true,
			emptyAllowed:    true,
			nonTypeAllowed:  true,
			defaultt:        "",
		},
		"storageclass": {
			parameters:      p,
			paramName:       "storageclass",
			driverParamName: "StorageClass",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        s3.StorageClassStandard,
		},
		"objectacl": {
			parameters:      p,
			paramName:       "objectacl",
			driverParamName: "ObjectACL",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        s3.ObjectCannedACLPrivate,
		},
		"objectownership": {
			parameters:      p,
			paramName:       "objectownership",
			driverParamName: "ObjectOwnership",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        false,
		},
		"checksumalgorithm": {
			parameters:      p,
			paramName:       "checksum_algorithm",
			driverParamName: "ChecksumAlgorithm",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        string(v2_types.ChecksumAlgorithmCrc64nvme),
		},
		"checksumdisabled": {
			parameters:      p,
			paramName:       "checksum_disabled",
			driverParamName: "ChecksumDisabled",
			nilAllowed:      true,
			emptyAllowed:    false,
			nonTypeAllowed:  false,
			defaultt:        false,
		},
	}

	for tn, tt := range tcs {
		t.Run(tn, func(t *testing.T) {
			opts := dtestutil.Opts{
				Defaultt:          tt.defaultt,
				ParamName:         tt.paramName,
				DriverParamName:   tt.driverParamName,
				OriginalParams:    tt.parameters,
				NilAllowed:        tt.nilAllowed,
				EmptyAllowed:      tt.emptyAllowed,
				NonTypeAllowed:    tt.nonTypeAllowed,
				Required:          tt.required,
				ParseParametersFn: testFn,
			}

			dtestutil.AssertByDefaultType(t, opts)
		})
	}
}

func TestParseLogLevelParam(t *testing.T) {
	tests := []struct {
		name            string
		param           any
		expectedV1      aws.LogLevelType
		expectedV2      v2_aws.ClientLogMode
		expectWarningV1 bool
		expectWarningV2 bool
	}{
		{
			name:            "nil parameter",
			param:           nil,
			expectedV1:      aws.LogOff,
			expectedV2:      v2_aws.ClientLogMode(0),
			expectWarningV1: false,
			expectWarningV2: false,
		},
		{
			name:            "log off",
			param:           LogLevelOff,
			expectedV1:      aws.LogOff,
			expectedV2:      v2_aws.ClientLogMode(0),
			expectWarningV1: false,
			expectWarningV2: false,
		},
		// V1 specific log levels
		{
			name:            "log debug",
			param:           LogLevelDebug,
			expectedV1:      aws.LogDebug,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		{
			name:            "log debug with signing",
			param:           LogLevelDebugWithSigning,
			expectedV1:      aws.LogDebugWithSigning,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		{
			name:            "log debug with http body",
			param:           LogLevelDebugWithHTTPBody,
			expectedV1:      aws.LogDebugWithHTTPBody,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		{
			name:            "log debug with request retries",
			param:           LogLevelDebugWithRequestRetries,
			expectedV1:      aws.LogDebugWithRequestRetries,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		{
			name:            "log debug with request errors",
			param:           LogLevelDebugWithRequestErrors,
			expectedV1:      aws.LogDebugWithRequestErrors,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		{
			name:            "log debug with event stream body",
			param:           LogLevelDebugWithEventStreamBody,
			expectedV1:      aws.LogDebugWithEventStreamBody,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		// V2 specific log levels
		{
			name:            "log signing",
			param:           LogSigning,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogSigning,
			expectWarningV1: false,
			expectWarningV2: false,
		},
		{
			name:            "log retries",
			param:           LogRetries,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogRetries,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log request",
			param:           LogRequest,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogRequest,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log request with body",
			param:           LogRequestWithBody,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogRequestWithBody,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log response",
			param:           LogResponse,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogResponse,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log response with body",
			param:           LogResponseWithBody,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogResponseWithBody,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log deprecated usage",
			param:           LogDeprecatedUsage,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogDeprecatedUsage,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log request event message",
			param:           LogRequestEventMessage,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogRequestEventMessage,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "log response event message",
			param:           LogResponseEventMessage,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogResponseEventMessage,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "invalid log level",
			param:           "invalid",
			expectedV1:      aws.LogOff,
			expectedV2:      v2_aws.ClientLogMode(0),
			expectWarningV1: false,
			expectWarningV2: false,
		},
		// Test combined log levels
		{
			name:            "combined v1 log levels",
			param:           LogLevelDebug + "," + LogLevelDebugWithSigning,
			expectedV1:      aws.LogDebug | aws.LogDebugWithSigning,
			expectedV2:      v2_aws.ClientLogMode(0), // Not applicable for V2
			expectWarningV1: false,
			expectWarningV2: true,
		},
		{
			name:            "combined v2 log levels",
			param:           LogSigning + "," + LogRetries,
			expectedV1:      aws.LogOff, // Not applicable for V1
			expectedV2:      v2_aws.LogSigning | v2_aws.LogRetries,
			expectWarningV1: true,
			expectWarningV2: false,
		},
		{
			name:            "combined v2 log levels with off",
			param:           LogSigning + "," + LogRetries + "," + LogLevelOff,
			expectedV1:      aws.LogOff,
			expectedV2:      v2_aws.ClientLogMode(0),
			expectWarningV1: true,
			expectWarningV2: false,
		},
	}

	// Create a test logger that we can check for warnings
	logBuffer := new(bytes.Buffer)
	logger := logrus.New()
	logger.SetOutput(logBuffer)
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetLevel(logrus.DebugLevel)

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			resultV1 := ParseLogLevelParamV1(log.FromLogrusLogger(logger), tc.param)
			assert.Equal(tt, tc.expectedV1, resultV1)

			if tc.expectWarningV1 {
				assert.Contains(tt, logBuffer.String(), "has been passed to S3 driver v1. Ignoring.")
			}
			logBuffer.Reset()

			resultV2 := ParseLogLevelParamV2(log.FromLogrusLogger(logger), tc.param)
			assert.Equal(tt, tc.expectedV2, resultV2)

			if tc.expectWarningV2 {
				assert.Contains(tt, logBuffer.String(), "has been passed to S3 driver v2. Ignoring.")
			}
		})
	}
}

func TestChecksumDisabledParameter(t *testing.T) {
	tests := []struct {
		name                 string
		checksumDisabled     any
		checksumAlgorithm    string
		expectedDisabled     bool
		expectedAlgorithm    v2_types.ChecksumAlgorithm
		expectWarningMessage bool
	}{
		{
			name:              "checksum_disabled true",
			checksumDisabled:  true,
			expectedDisabled:  true,
			expectedAlgorithm: "",
		},
		{
			name:                 "checksum_disabled true with algorithm",
			checksumDisabled:     true,
			checksumAlgorithm:    (string)(v2_types.ChecksumAlgorithmCrc32),
			expectedDisabled:     true,
			expectedAlgorithm:    "",
			expectWarningMessage: true,
		},
		{
			name:              "checksum_disabled false",
			checksumDisabled:  false,
			expectedDisabled:  false,
			expectedAlgorithm: v2_types.ChecksumAlgorithmCrc64nvme,
		},
		{
			name:              "checksum_disabled false with algorithm",
			checksumDisabled:  false,
			checksumAlgorithm: (string)(v2_types.ChecksumAlgorithmCrc32),
			expectedDisabled:  false,
			expectedAlgorithm: v2_types.ChecksumAlgorithmCrc32,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			params := map[string]any{
				ParamRegion:           "us-west-2",
				ParamBucket:           "test-bucket",
				ParamChecksumDisabled: tt.checksumDisabled,
			}

			if tt.checksumAlgorithm != "" {
				params[ParamChecksumAlgorithm] = tt.checksumAlgorithm
			}

			result, err := ParseParameters(V2DriverName, params)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedDisabled, result.ChecksumDisabled)
			assert.Equal(t, tt.expectedAlgorithm, result.ChecksumAlgorithm)
		})
	}
}
