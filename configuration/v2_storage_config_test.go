package configuration

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParseWithAzureV2Storage validates that environment variables properly work with azure_v2 storage driver
func TestParseWithAzureV2Storage(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  azure_v2:
    accountname: myaccount
    accountkey: mykey
    container: mycontainer
`
	// Test 1: Parse without environment overrides
	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "azure_v2", config.Storage.Type())
	require.Equal(t, "myaccount", config.Storage.Parameters()["accountname"])
	require.Equal(t, "mykey", config.Storage.Parameters()["accountkey"])
	require.Equal(t, "mycontainer", config.Storage.Parameters()["container"])

	// Test 2: Override accountname via environment variable
	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME", "newaccount")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME")

	config, err = Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "azure_v2", config.Storage.Type())
	require.Equal(t, "newaccount", config.Storage.Parameters()["accountname"])
	require.Equal(t, "mykey", config.Storage.Parameters()["accountkey"])
	require.Equal(t, "mycontainer", config.Storage.Parameters()["container"])
}

func TestParseAzureV2ExtendedParameters(t *testing.T) {
	configYaml := `version: 0.1`

	envVars := map[string]string{
		"REGISTRY_STORAGE":                               "azure_v2",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME":          "myaccount",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY":           "mykey",
		"REGISTRY_STORAGE_AZURE_V2_CONTAINER":            "mycontainer",
		"REGISTRY_STORAGE_AZURE_V2_SERVICEURL":           "https://custom.blob.core.windows.net",
		"REGISTRY_STORAGE_AZURE_V2_ROOTDIRECTORY":        "/registry/",
		"REGISTRY_STORAGE_AZURE_V2_LEGACYROOTPREFIX":     "true",
		"REGISTRY_STORAGE_AZURE_V2_TRIMLEGACYROOTPREFIX": "false",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)

	params := config.Storage.Parameters()
	require.Equal(t, "https://custom.blob.core.windows.net", params["serviceurl"])
	require.Equal(t, "/registry/", params["rootdirectory"])
	require.Equal(t, true, params["legacyrootprefix"])
	require.Equal(t, false, params["trimlegacyrootprefix"])
}

// TestParseWithAzureV2CompleteEnv validates setting azure_v2 storage entirely via environment variables
func TestParseWithAzureV2CompleteEnv(t *testing.T) {
	configYaml := `version: 0.1`

	err := os.Setenv("REGISTRY_STORAGE", "azure_v2")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME", "envaccount")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY", "envkey")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_CONTAINER", "envcontainer")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_CONTAINER")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_REALM", "core.chinacloudapi.cn")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_REALM")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "azure_v2", config.Storage.Type())
	require.Equal(t, "envaccount", config.Storage.Parameters()["accountname"])
	require.Equal(t, "envkey", config.Storage.Parameters()["accountkey"])
	require.Equal(t, "envcontainer", config.Storage.Parameters()["container"])
	require.Equal(t, "core.chinacloudapi.cn", config.Storage.Parameters()["realm"])
}

// TestParseWithS3V2Storage validates that environment variables properly work with s3_v2 storage driver
func TestParseWithS3V2Storage(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  s3_v2:
    region: us-east-1
    bucket: mybucket
    accesskey: myaccesskey
    secretkey: mysecretkey
`
	// Test 1: Parse without environment overrides
	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "s3_v2", config.Storage.Type())
	require.Equal(t, "us-east-1", config.Storage.Parameters()["region"])
	require.Equal(t, "mybucket", config.Storage.Parameters()["bucket"])
	require.Equal(t, "myaccesskey", config.Storage.Parameters()["accesskey"])
	require.Equal(t, "mysecretkey", config.Storage.Parameters()["secretkey"])

	// Test 2: Override region via environment variable
	err = os.Setenv("REGISTRY_STORAGE_S3_V2_REGION", "eu-west-1")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_S3_V2_REGION")

	config, err = Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "s3_v2", config.Storage.Type())
	require.Equal(t, "eu-west-1", config.Storage.Parameters()["region"])
	require.Equal(t, "mybucket", config.Storage.Parameters()["bucket"])
}

// TestParseSwitchFromS3ToAzureV2 validates switching storage drivers via environment variables
func TestParseSwitchFromS3ToAzureV2(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  s3:
    region: us-east-1
    bucket: s3bucket
    accesskey: s3key
    secretkey: s3secret
`
	// Switch to azure_v2 via environment
	err := os.Setenv("REGISTRY_STORAGE", "azure_v2")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME", "azureaccount")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY", "azurekey")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_CONTAINER", "azurecontainer")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_CONTAINER")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "azure_v2", config.Storage.Type())
	require.Equal(t, "azureaccount", config.Storage.Parameters()["accountname"])
	require.Equal(t, "azurekey", config.Storage.Parameters()["accountkey"])
	require.Equal(t, "azurecontainer", config.Storage.Parameters()["container"])
	// Ensure S3 parameters are not present
	require.Nil(t, config.Storage.Parameters()["region"])
	require.Nil(t, config.Storage.Parameters()["bucket"])
}

// TestParseAzureV2NestedParameters validates nested parameter override for azure_v2
func TestParseAzureV2NestedParameters(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  azure_v2:
    accountname: myaccount
    accountkey: mykey
    container: mycontainer
    max_retries: 3
    retry_delay: 2s
`
	err := os.Setenv("REGISTRY_STORAGE_AZURE_V2_MAX_RETRIES", "5")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_MAX_RETRIES")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_RETRY_DELAY", "5s")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_RETRY_DELAY")

	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG", "true")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "azure_v2", config.Storage.Type())
	require.Equal(t, 5, config.Storage.Parameters()["max_retries"])
	require.Equal(t, "5s", config.Storage.Parameters()["retry_delay"])
	require.Equal(t, true, config.Storage.Parameters()["debug_log"])
}

// TestParseS3V2BooleanParameters validates boolean parameter parsing for s3_v2
func TestParseS3V2BooleanParameters(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  s3_v2:
    region: us-east-1
    bucket: mybucket
    encrypt: false
    secure: true
    skipverify: false
`
	err := os.Setenv("REGISTRY_STORAGE_S3_V2_ENCRYPT", "true")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_S3_V2_ENCRYPT")

	err = os.Setenv("REGISTRY_STORAGE_S3_V2_SECURE", "false")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_S3_V2_SECURE")

	err = os.Setenv("REGISTRY_STORAGE_S3_V2_SKIPVERIFY", "true")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_S3_V2_SKIPVERIFY")

	err = os.Setenv("REGISTRY_STORAGE_S3_V2_PATHSTYLE", "true")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_S3_V2_PATHSTYLE")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "s3_v2", config.Storage.Type())
	require.Equal(t, true, config.Storage.Parameters()["encrypt"])
	require.Equal(t, false, config.Storage.Parameters()["secure"])
	require.Equal(t, true, config.Storage.Parameters()["skipverify"])
	require.Equal(t, true, config.Storage.Parameters()["pathstyle"])
}

// TestCustomSplitFunction validates the customSplit function behavior
func TestCustomSplitFunction(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "azure_v2 protection",
			input:    "REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "ACCOUNTNAME"},
		},
		{
			name:     "s3_v2 protection",
			input:    "REGISTRY_STORAGE_S3_V2_REGION",
			expected: []string{"REGISTRY", "STORAGE", "S3_V2", "REGION"},
		},
		{
			name:     "regular underscore split",
			input:    "REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY",
			expected: []string{"REGISTRY", "STORAGE", "FILESYSTEM", "ROOTDIRECTORY"},
		},
		{
			name:     "mixed with azure_v2",
			input:    "REGISTRY_STORAGE_AZURE_V2_RETRY_MAX_DELAY",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "RETRY", "MAX", "DELAY"},
		},
		{
			name:     "multiple occurrences",
			input:    "AZURE_V2_STORAGE_AZURE_V2_S3_V2",
			expected: []string{"AZURE_V2", "STORAGE", "AZURE_V2", "S3_V2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := customSplit(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestParseStorageDriverCaseSensitivity validates case sensitivity in storage driver names
func TestParseStorageDriverCaseSensitivity(t *testing.T) {
	tests := []struct {
		name       string
		envValue   string
		paramEnv   string
		paramValue string
		expectType string
	}{
		{
			name:       "lowercase azure_v2",
			envValue:   "azure_v2",
			paramEnv:   "REGISTRY_STORAGE_AZURE_V2_CONTAINER",
			paramValue: "testcontainer",
			expectType: "azure_v2",
		},
		{
			name:       "lowercase s3_v2",
			envValue:   "s3_v2",
			paramEnv:   "REGISTRY_STORAGE_S3_V2_BUCKET",
			paramValue: "testbucket",
			expectType: "s3_v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configYaml := `version: 0.1`

			err := os.Setenv("REGISTRY_STORAGE", tt.envValue)
			require.NoError(t, err)
			defer os.Unsetenv("REGISTRY_STORAGE")

			err = os.Setenv(tt.paramEnv, tt.paramValue)
			require.NoError(t, err)
			defer os.Unsetenv(tt.paramEnv)

			config, err := Parse(bytes.NewReader([]byte(configYaml)))
			require.NoError(t, err)
			require.Equal(t, tt.expectType, config.Storage.Type())
		})
	}
}

// TestCustomSplitAllProtectedStrings validates all protected strings in customSplit
func TestCustomSplitAllProtectedStrings(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		// Storage drivers
		{
			name:     "azure_v2 driver",
			input:    "REGISTRY_STORAGE_AZURE_V2_CONTAINER",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "CONTAINER"},
		},
		{
			name:     "s3_v2 driver",
			input:    "REGISTRY_STORAGE_S3_V2_BUCKET",
			expected: []string{"REGISTRY", "STORAGE", "S3_V2", "BUCKET"},
		},
		// Azure v2 authentication parameters
		{
			name:     "azure credentials_type",
			input:    "REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "CREDENTIALS_TYPE"},
		},
		{
			name:     "azure tenant_id",
			input:    "REGISTRY_STORAGE_AZURE_V2_TENANT_ID",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "TENANT_ID"},
		},
		{
			name:     "azure client_id",
			input:    "REGISTRY_STORAGE_AZURE_V2_CLIENT_ID",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "CLIENT_ID"},
		},
		{
			name:     "azure client_secret",
			input:    "REGISTRY_STORAGE_AZURE_V2_CLIENT_SECRET",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "CLIENT_SECRET"},
		},
		{
			name:     "azure default_credentials",
			input:    "REGISTRY_STORAGE_AZURE_V2_DEFAULT_CREDENTIALS",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "DEFAULT_CREDENTIALS"},
		},
		{
			name:     "azure shared_key",
			input:    "REGISTRY_STORAGE_AZURE_V2_SHARED_KEY",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "SHARED_KEY"},
		},
		// Azure v2 retry parameters
		{
			name:     "azure max_retries",
			input:    "REGISTRY_STORAGE_AZURE_V2_MAX_RETRIES",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "MAX_RETRIES"},
		},
		{
			name:     "azure retry_try_timeout",
			input:    "REGISTRY_STORAGE_AZURE_V2_RETRY_TRY_TIMEOUT",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "RETRY_TRY_TIMEOUT"},
		},
		{
			name:     "azure retry_delay",
			input:    "REGISTRY_STORAGE_AZURE_V2_RETRY_DELAY",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "RETRY_DELAY"},
		},
		{
			name:     "azure max_retry_delay",
			input:    "REGISTRY_STORAGE_AZURE_V2_MAX_RETRY_DELAY",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "MAX_RETRY_DELAY"},
		},
		// Azure v2 debug parameters
		{
			name:     "azure debug_log",
			input:    "REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "DEBUG_LOG"},
		},
		{
			name:     "azure debug_log_events",
			input:    "REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG_EVENTS",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "DEBUG_LOG_EVENTS"},
		},
		// Azure v2 connection pool parameters
		{
			name:     "azure api_pool_initial_interval",
			input:    "REGISTRY_STORAGE_AZURE_V2_API_POOL_INITIAL_INTERVAL",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "API_POOL_INITIAL_INTERVAL"},
		},
		{
			name:     "azure api_pool_max_interval",
			input:    "REGISTRY_STORAGE_AZURE_V2_API_POOL_MAX_INTERVAL",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "API_POOL_MAX_INTERVAL"},
		},
		{
			name:     "azure api_pool_max_elapsed_time",
			input:    "REGISTRY_STORAGE_AZURE_V2_API_POOL_MAX_ELAPSED_TIME",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "API_POOL_MAX_ELAPSED_TIME"},
		},
		// S3 v2 checksum parameters
		{
			name:     "s3 checksum_algorithm",
			input:    "REGISTRY_STORAGE_S3_V2_CHECKSUM_ALGORITHM",
			expected: []string{"REGISTRY", "STORAGE", "S3_V2", "CHECKSUM_ALGORITHM"},
		},
		{
			name:     "s3 checksum_disabled",
			input:    "REGISTRY_STORAGE_S3_V2_CHECKSUM_DISABLED",
			expected: []string{"REGISTRY", "STORAGE", "S3_V2", "CHECKSUM_DISABLED"},
		},
		// S3 v2 storage class
		{
			name:     "s3 storage_class",
			input:    "REGISTRY_STORAGE_S3_V2_STORAGE_CLASS",
			expected: []string{"REGISTRY", "STORAGE", "S3_V2", "STORAGE_CLASS"},
		},
		// Complex cases
		{
			name:     "multiple protected strings",
			input:    "AZURE_V2_S3_V2_CLIENT_ID_TENANT_ID",
			expected: []string{"AZURE_V2", "S3_V2", "CLIENT_ID", "TENANT_ID"},
		},
		{
			name:     "protected string at end",
			input:    "REGISTRY_SOMETHING_MAX_RETRIES",
			expected: []string{"REGISTRY", "SOMETHING", "MAX_RETRIES"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := customSplit(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestParseAzureV2AuthenticationMethods tests all Azure v2 authentication parameters
func TestParseAzureV2AuthenticationMethods(t *testing.T) {
	// Test 1: Shared Key authentication
	t.Run("shared_key_auth", func(t *testing.T) {
		configYaml := `
version: 0.1
storage:
  azure_v2:
    credentials_type: shared_key
    accountname: myaccount
    accountkey: mykey
    container: mycontainer
`
		err := os.Setenv("REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE", "shared_key")
		require.NoError(t, err)
		defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE")

		config, err := Parse(bytes.NewReader([]byte(configYaml)))
		require.NoError(t, err)
		require.Equal(t, "shared_key", config.Storage.Parameters()["credentials_type"])
	})

	// Test 2: Client Secret authentication
	t.Run("client_secret_auth", func(t *testing.T) {
		configYaml := `version: 0.1`

		envVars := map[string]string{
			"REGISTRY_STORAGE":                           "azure_v2",
			"REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE": "client_secret",
			"REGISTRY_STORAGE_AZURE_V2_TENANT_ID":        "my-tenant-id",
			"REGISTRY_STORAGE_AZURE_V2_CLIENT_ID":        "my-client-id",
			"REGISTRY_STORAGE_AZURE_V2_SECRET":           "my-secret",
			"REGISTRY_STORAGE_AZURE_V2_CONTAINER":        "mycontainer",
		}

		for k, v := range envVars {
			err := os.Setenv(k, v)
			require.NoError(t, err)
			defer os.Unsetenv(k)
		}

		config, err := Parse(bytes.NewReader([]byte(configYaml)))
		require.NoError(t, err)
		require.Equal(t, "azure_v2", config.Storage.Type())
		require.Equal(t, "client_secret", config.Storage.Parameters()["credentials_type"])
		require.Equal(t, "my-tenant-id", config.Storage.Parameters()["tenant_id"])
		require.Equal(t, "my-client-id", config.Storage.Parameters()["client_id"])
		require.Equal(t, "my-secret", config.Storage.Parameters()["secret"])
	})

	// Test 3: Default Credentials
	t.Run("default_credentials_auth", func(t *testing.T) {
		configYaml := `version: 0.1`

		err := os.Setenv("REGISTRY_STORAGE", "azure_v2")
		require.NoError(t, err)
		defer os.Unsetenv("REGISTRY_STORAGE")

		err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE", "default_credentials")
		require.NoError(t, err)
		defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE")

		err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_CONTAINER", "mycontainer")
		require.NoError(t, err)
		defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_CONTAINER")

		config, err := Parse(bytes.NewReader([]byte(configYaml)))
		require.NoError(t, err)
		require.Equal(t, "default_credentials", config.Storage.Parameters()["credentials_type"])
	})
}

// TestParseAzureV2RetryConfiguration tests Azure v2 retry parameters
func TestParseAzureV2RetryConfiguration(t *testing.T) {
	configYaml := `version: 0.1`

	envVars := map[string]string{
		"REGISTRY_STORAGE":                            "azure_v2",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME":       "myaccount",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY":        "mykey",
		"REGISTRY_STORAGE_AZURE_V2_CONTAINER":         "mycontainer",
		"REGISTRY_STORAGE_AZURE_V2_MAX_RETRIES":       "10",
		"REGISTRY_STORAGE_AZURE_V2_RETRY_TRY_TIMEOUT": "30s",
		"REGISTRY_STORAGE_AZURE_V2_RETRY_DELAY":       "2s",
		"REGISTRY_STORAGE_AZURE_V2_MAX_RETRY_DELAY":   "120s",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, 10, config.Storage.Parameters()["max_retries"])
	require.Equal(t, "30s", config.Storage.Parameters()["retry_try_timeout"])
	require.Equal(t, "2s", config.Storage.Parameters()["retry_delay"])
	require.Equal(t, "120s", config.Storage.Parameters()["max_retry_delay"])
}

// TestParseAzureV2DebugConfiguration tests Azure v2 debug parameters
func TestParseAzureV2DebugConfiguration(t *testing.T) {
	configYaml := `version: 0.1`

	envVars := map[string]string{
		"REGISTRY_STORAGE":                           "azure_v2",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME":      "myaccount",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY":       "mykey",
		"REGISTRY_STORAGE_AZURE_V2_CONTAINER":        "mycontainer",
		"REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG":        "true",
		"REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG_EVENTS": "request,response,retry",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, true, config.Storage.Parameters()["debug_log"])
	require.Equal(t, "request,response,retry", config.Storage.Parameters()["debug_log_events"])
}

// TestParseAzureV2ConnectionPoolConfiguration tests Azure v2 connection pool parameters
func TestParseAzureV2ConnectionPoolConfiguration(t *testing.T) {
	configYaml := `version: 0.1`

	envVars := map[string]string{
		"REGISTRY_STORAGE":                                    "azure_v2",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTNAME":               "myaccount",
		"REGISTRY_STORAGE_AZURE_V2_ACCOUNTKEY":                "mykey",
		"REGISTRY_STORAGE_AZURE_V2_CONTAINER":                 "mycontainer",
		"REGISTRY_STORAGE_AZURE_V2_API_POOL_INITIAL_INTERVAL": "200ms",
		"REGISTRY_STORAGE_AZURE_V2_API_POOL_MAX_INTERVAL":     "2s",
		"REGISTRY_STORAGE_AZURE_V2_API_POOL_MAX_ELAPSED_TIME": "10s",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "200ms", config.Storage.Parameters()["api_pool_initial_interval"])
	require.Equal(t, "2s", config.Storage.Parameters()["api_pool_max_interval"])
	require.Equal(t, "10s", config.Storage.Parameters()["api_pool_max_elapsed_time"])
}

// TestParseS3V2ChecksumConfiguration tests S3 v2 checksum parameters
func TestParseS3V2ChecksumConfiguration(t *testing.T) {
	configYaml := `version: 0.1`

	envVars := map[string]string{
		"REGISTRY_STORAGE":                          "s3_v2",
		"REGISTRY_STORAGE_S3_V2_REGION":             "us-east-1",
		"REGISTRY_STORAGE_S3_V2_BUCKET":             "mybucket",
		"REGISTRY_STORAGE_S3_V2_ACCESSKEY":          "mykey",
		"REGISTRY_STORAGE_S3_V2_SECRETKEY":          "mysecret",
		"REGISTRY_STORAGE_S3_V2_CHECKSUM_ALGORITHM": "SHA256",
		"REGISTRY_STORAGE_S3_V2_CHECKSUM_DISABLED":  "false",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "SHA256", config.Storage.Parameters()["checksum_algorithm"])
	require.Equal(t, false, config.Storage.Parameters()["checksum_disabled"])
}

// TestParseS3V2StorageClass tests S3 v2 storage class parameter
func TestParseS3V2StorageClass(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  s3_v2:
    region: us-east-1
    bucket: mybucket
    accesskey: mykey
    secretkey: mysecret
`
	err := os.Setenv("REGISTRY_STORAGE_S3_V2_STORAGE_CLASS", "REDUCED_REDUNDANCY")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_S3_V2_STORAGE_CLASS")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "REDUCED_REDUNDANCY", config.Storage.Parameters()["storage_class"])
}

// TestParseComplexUnderscoreParameters tests complex scenarios with multiple underscore parameters
func TestParseComplexUnderscoreParameters(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  azure_v2:
    container: base-container
`
	// Set many underscore parameters at once
	envVars := map[string]string{
		"REGISTRY_STORAGE_AZURE_V2_CREDENTIALS_TYPE":      "client_secret",
		"REGISTRY_STORAGE_AZURE_V2_TENANT_ID":             "tenant123",
		"REGISTRY_STORAGE_AZURE_V2_CLIENT_ID":             "client456",
		"REGISTRY_STORAGE_AZURE_V2_SECRET":                "secret789",
		"REGISTRY_STORAGE_AZURE_V2_MAX_RETRIES":           "5",
		"REGISTRY_STORAGE_AZURE_V2_RETRY_DELAY":           "1s",
		"REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG":             "true",
		"REGISTRY_STORAGE_AZURE_V2_API_POOL_MAX_INTERVAL": "5s",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)

	// Verify all parameters were set correctly
	params := config.Storage.Parameters()
	require.Equal(t, "client_secret", params["credentials_type"])
	require.Equal(t, "tenant123", params["tenant_id"])
	require.Equal(t, "client456", params["client_id"])
	require.Equal(t, "secret789", params["secret"])
	require.Equal(t, 5, params["max_retries"])
	require.Equal(t, "1s", params["retry_delay"])
	require.Equal(t, true, params["debug_log"])
	require.Equal(t, "5s", params["api_pool_max_interval"])
	require.Equal(t, "base-container", params["container"])
}

// TestParseUnderscoreParametersInOtherSections tests that underscore parameters work in non-storage sections
func TestParseUnderscoreParametersInOtherSections(t *testing.T) {
	// While the current protectedStrings are storage-specific, this test ensures
	// the mechanism doesn't break other configuration sections
	configYaml := `
version: 0.1
storage: inmemory
http:
  debug:
    pprof:
      enabled: true
`
	// Try setting a parameter that might conflict if not handled properly
	err := os.Setenv("REGISTRY_HTTP_DEBUG_PPROF_ENABLED", "false")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_HTTP_DEBUG_PPROF_ENABLED")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.False(t, config.HTTP.Debug.Pprof.Enabled)
}

// TestParseStorageMaintenanceCache tests that storage maintenance and cache sections still work
func TestParseStorageMaintenanceCache(t *testing.T) {
	configYaml := `
version: 0.1
storage:
  s3_v2:
    region: us-east-1
    bucket: mybucket
`
	// These should still work as they're special-cased in the storage parsing logic
	err := os.Setenv("REGISTRY_STORAGE_MAINTENANCE_READONLY_ENABLED", "true")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_MAINTENANCE_READONLY_ENABLED")

	err = os.Setenv("REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR", "inmemory")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_CACHE_BLOBDESCRIPTOR")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "s3_v2", config.Storage.Type())

	// Verify maintenance and cache are set
	maintenance, ok := config.Storage["maintenance"]
	require.True(t, ok)
	require.NotNil(t, maintenance)

	// Cast maintenance to Parameters type
	maintenanceParams := maintenance

	readonly, ok := maintenanceParams["readonly"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, readonly["enabled"])

	cache, ok := config.Storage["cache"]
	require.True(t, ok)
	require.NotNil(t, cache)

	// Cast cache to Parameters type
	cacheParams := cache
	require.Equal(t, "inmemory", cacheParams["blobdescriptor"])
}

// TestCustomSplitLongestMatchFirst validates that customSplit replaces longest matches first
func TestCustomSplitLongestMatchFirst(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
		note     string
	}{
		{
			name:     "DEBUG_LOG_EVENTS vs DEBUG_LOG",
			input:    "REGISTRY_STORAGE_AZURE_V2_DEBUG_LOG_EVENTS",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "DEBUG_LOG_EVENTS"},
			note:     "Should match DEBUG_LOG_EVENTS (longer) instead of DEBUG_LOG",
		},
		{
			name:     "API_POOL_MAX_ELAPSED_TIME vs API_POOL_MAX_INTERVAL",
			input:    "REGISTRY_STORAGE_AZURE_V2_API_POOL_MAX_ELAPSED_TIME",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "API_POOL_MAX_ELAPSED_TIME"},
			note:     "Should match API_POOL_MAX_ELAPSED_TIME as complete unit",
		},
		{
			name:     "MAX_RETRY_DELAY vs MAX_RETRIES vs RETRY_DELAY",
			input:    "REGISTRY_STORAGE_AZURE_V2_MAX_RETRY_DELAY",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "MAX_RETRY_DELAY"},
			note:     "Should match MAX_RETRY_DELAY (longest) not MAX_RETRIES + DELAY",
		},
		{
			name:     "BUCKET_OWNER_FULL_CONTROL contains multiple potential matches",
			input:    "REGISTRY_STORAGE_S3_V2_BUCKET_OWNER_FULL_CONTROL",
			expected: []string{"REGISTRY", "STORAGE", "S3_V2", "BUCKET_OWNER_FULL_CONTROL"},
			note:     "Should preserve BUCKET_OWNER_FULL_CONTROL as single unit",
		},
		{
			name:     "DEFAULT_CREDENTIALS vs CREDENTIALS_TYPE",
			input:    "REGISTRY_STORAGE_AZURE_V2_DEFAULT_CREDENTIALS",
			expected: []string{"REGISTRY", "STORAGE", "AZURE_V2", "DEFAULT_CREDENTIALS"},
			note:     "Should match DEFAULT_CREDENTIALS not split into DEFAULT + CREDENTIALS_TYPE",
		},
		{
			name:     "Complex string with multiple overlapping protected strings",
			input:    "DEBUG_LOG_EVENTS_MAX_RETRY_DELAY_API_POOL_MAX_INTERVAL",
			expected: []string{"DEBUG_LOG_EVENTS", "MAX_RETRY_DELAY", "API_POOL_MAX_INTERVAL"},
			note:     "All longest matches should be preserved",
		},
		{
			name:     "Embedded protected string that shouldn't match",
			input:    "REGISTRY_MYDEBUG_LOG_CUSTOM",
			expected: []string{"REGISTRY", "MYDEBUG", "LOG", "CUSTOM"},
			note:     "DEBUG_LOG is not matched because it's not a complete token",
		},
		{
			name:     "Multiple instances of same protected string",
			input:    "CLIENT_ID_AND_CLIENT_ID_AGAIN",
			expected: []string{"CLIENT_ID", "AND", "CLIENT_ID", "AGAIN"},
			note:     "Both instances of CLIENT_ID should be preserved",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := customSplit(tt.input)
			require.Equal(t, tt.expected, result, tt.note)
		})
	}
}

// TestCustomSplitProtectedStringOrder validates the ordering of protected strings by length
func TestCustomSplitProtectedStringOrder(t *testing.T) {
	// This test verifies that our protected strings are properly handled
	// when they have overlapping parts

	// Test specific overlapping cases
	overlappingTests := []struct {
		name        string
		input       string
		expected    []string
		explanation string
	}{
		{
			name:        "DEBUG_LOG_EVENTS contains DEBUG_LOG",
			input:       "PREFIX_DEBUG_LOG_EVENTS_SUFFIX",
			expected:    []string{"PREFIX", "DEBUG_LOG_EVENTS", "SUFFIX"},
			explanation: "DEBUG_LOG_EVENTS (16 chars) should be matched before DEBUG_LOG (9 chars)",
		},
		{
			name:        "MAX_RETRY_DELAY contains MAX, RETRY, and DELAY separately",
			input:       "PREFIX_MAX_RETRY_DELAY_SUFFIX",
			expected:    []string{"PREFIX", "MAX_RETRY_DELAY", "SUFFIX"},
			explanation: "MAX_RETRY_DELAY (15 chars) should be matched as one unit",
		},
		{
			name:        "API_POOL_MAX_ELAPSED_TIME vs API_POOL_MAX_INTERVAL",
			input:       "API_POOL_MAX_ELAPSED_TIME_API_POOL_MAX_INTERVAL",
			expected:    []string{"API_POOL_MAX_ELAPSED_TIME", "API_POOL_MAX_INTERVAL"},
			explanation: "Both long strings should be preserved correctly",
		},
	}

	for _, tt := range overlappingTests {
		t.Run(tt.name, func(t *testing.T) {
			result := customSplit(tt.input)
			require.Equal(t, tt.expected, result, tt.explanation)
		})
	}
}

// TestParseProtectedStringEmbedded tests that protected strings embedded within larger strings are NOT matched
func TestParseProtectedStringEmbedded(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
		note     string
	}{
		{
			name:     "CLIENT_ID embedded at start",
			input:    "PRECLIENT_ID_FIELD",
			expected: []string{"PRECLIENT", "ID", "FIELD"},
			note:     "CLIENT_ID should not be matched when part of PRECLIENT_ID",
		},
		{
			name:     "TENANT_ID embedded at end",
			input:    "FIELD_MYTENANT_ID",
			expected: []string{"FIELD", "MYTENANT", "ID"},
			note:     "TENANT_ID should not be matched when part of MYTENANT_ID",
		},
		{
			name:     "Multiple embeddings",
			input:    "PREFIXCLIENT_IDTENANT_IDSUFFIX",
			expected: []string{"PREFIXCLIENT", "IDTENANT", "IDSUFFIX"},
			note:     "Neither CLIENT_ID nor TENANT_ID should be matched when embedded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := customSplit(tt.input)
			require.Equal(t, tt.expected, result, tt.note)
		})
	}
}

// TestParseS3V2ExtendedParameters tests additional S3 v2 parameters
func TestParseS3V2ExtendedParameters(t *testing.T) {
	configYaml := `version: 0.1`

	envVars := map[string]string{
		"REGISTRY_STORAGE":                                   "s3_v2",
		"REGISTRY_STORAGE_S3_V2_REGION":                      "us-east-1",
		"REGISTRY_STORAGE_S3_V2_BUCKET":                      "mybucket",
		"REGISTRY_STORAGE_S3_V2_ACCESSKEY":                   "mykey",
		"REGISTRY_STORAGE_S3_V2_SECRETKEY":                   "mysecret",
		"REGISTRY_STORAGE_S3_V2_SESSIONTOKEN":                "mytoken",
		"REGISTRY_STORAGE_S3_V2_REGIONENDPOINT":              "https://s3.custom.endpoint",
		"REGISTRY_STORAGE_S3_V2_FORCEPATHSTYLE":              "true",
		"REGISTRY_STORAGE_S3_V2_MAXREQUESTSPERSECOND":        "500",
		"REGISTRY_STORAGE_S3_V2_MAXRETRIES":                  "10",
		"REGISTRY_STORAGE_S3_V2_PARALLELWALK":                "true",
		"REGISTRY_STORAGE_S3_V2_MULTIPARTCOPYCHUNKSIZE":      "64M",
		"REGISTRY_STORAGE_S3_V2_MULTIPARTCOPYMAXCONCURRENCY": "200",
		"REGISTRY_STORAGE_S3_V2_MULTIPARTCOPYTHRESHOLDSIZE":  "64M",
		"REGISTRY_STORAGE_S3_V2_OBJECTACL":                   "bucket-owner-full-control",
		"REGISTRY_STORAGE_S3_V2_OBJECTOWNERSHIP":             "true",
		"REGISTRY_STORAGE_S3_V2_LOGLEVEL":                    "logretries,logrequest",
	}

	for k, v := range envVars {
		err := os.Setenv(k, v)
		require.NoError(t, err)
		defer os.Unsetenv(k) // nolint: revive // defer
	}

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)

	params := config.Storage.Parameters()
	require.Equal(t, "mytoken", params["sessiontoken"])
	require.Equal(t, "https://s3.custom.endpoint", params["regionendpoint"])
	require.Equal(t, true, params["forcepathstyle"])
	require.Equal(t, 500, params["maxrequestspersecond"])
	require.Equal(t, 10, params["maxretries"])
	require.Equal(t, true, params["parallelwalk"])
	require.Equal(t, "64M", params["multipartcopychunksize"])
	require.Equal(t, 200, params["multipartcopymaxconcurrency"])
	require.Equal(t, "64M", params["multipartcopythresholdsize"])
	require.Equal(t, "bucket-owner-full-control", params["objectacl"])
	require.Equal(t, true, params["objectownership"])
	require.Equal(t, "logretries,logrequest", params["loglevel"])
}

func TestParseParameterCaseSensitivity(t *testing.T) {
	configYaml := `version: 0.1`

	// Test with mixed case in environment variable
	err := os.Setenv("REGISTRY_STORAGE", "azure_v2")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE")

	// These should all work despite case differences in the parameter name
	err = os.Setenv("REGISTRY_STORAGE_AZURE_V2_AccountName", "myaccount")
	require.NoError(t, err)
	defer os.Unsetenv("REGISTRY_STORAGE_AZURE_V2_AccountName")

	config, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.NoError(t, err)
	require.Equal(t, "myaccount", config.Storage.Parameters()["accountname"])
}
