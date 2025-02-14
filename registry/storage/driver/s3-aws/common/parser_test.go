package common

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseParameters(t *testing.T) {
	tests := []struct {
		name          string
		params        map[string]any
		expected      *DriverParameters
		expectedError bool
		errorContains string
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
				LogLevel:                    aws.LogOff,
				ObjectOwnership:             false,
				KeyID:                       "",
			},
			expectedError: false,
		},
		{
			name: "full configuration with all parameters",
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
				LogLevel:                    aws.LogDebug,
				ObjectOwnership:             false,
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
				LogLevel:                    aws.LogOff,
				ObjectOwnership:             true,
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
				StorageClass:                StorageClassStandard,
				ObjectACL:                   s3.ObjectCannedACLPrivate,
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
			name: "invalid v4auth parameter",
			params: map[string]any{
				ParamRegion: "us-west-2",
				ParamBucket: "test-bucket",
				ParamV4Auth: "invalid",
			},
			expectedError: true,
			errorContains: "cannot parse \"v4auth\" string as bool",
		},
		{
			name: "invalid multipart copy chunk size",
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
			errorContains: "the storageclass parameter must be one of",
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseParameters(tt.params)

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
			}
		})
	}
}

func TestParseLogLevelParam(t *testing.T) {
	tests := []struct {
		name     string
		param    any
		expected aws.LogLevelType
	}{
		{
			name:     "nil parameter",
			param:    nil,
			expected: aws.LogOff,
		},
		{
			name:     "log off",
			param:    LogLevelOff,
			expected: aws.LogOff,
		},
		{
			name:     "log debug",
			param:    LogLevelDebug,
			expected: aws.LogDebug,
		},
		{
			name:     "log debug with signing",
			param:    LogLevelDebugWithSigning,
			expected: aws.LogDebugWithSigning,
		},
		{
			name:     "log debug with http body",
			param:    LogLevelDebugWithHTTPBody,
			expected: aws.LogDebugWithHTTPBody,
		},
		{
			name:     "log debug with request retries",
			param:    LogLevelDebugWithRequestRetries,
			expected: aws.LogDebugWithRequestRetries,
		},
		{
			name:     "log debug with request errors",
			param:    LogLevelDebugWithRequestErrors,
			expected: aws.LogDebugWithRequestErrors,
		},
		{
			name:     "log debug with event stream body",
			param:    LogLevelDebugWithEventStreamBody,
			expected: aws.LogDebugWithEventStreamBody,
		},
		{
			name:     "invalid log level",
			param:    "invalid",
			expected: aws.LogOff,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseLogLevelParam(tt.param)
			assert.Equal(t, tt.expected, result)
		})
	}
}
