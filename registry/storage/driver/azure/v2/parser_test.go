package v2

import (
	"testing"
	"time"

	azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	sdriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/azure/common"
	"github.com/docker/distribution/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AzureDriverParametersTestSuite struct {
	suite.Suite
	baseParams map[string]any
}

func TestAzureDriverParametersTestSuite(t *testing.T) {
	suite.Run(t, new(AzureDriverParametersTestSuite))
}

func (s *AzureDriverParametersTestSuite) SetupTest() {
	s.baseParams = map[string]any{
		sdriver.ParamLogger:     testutil.NewTestLogger(s.T()),
		common.ParamAccountName: "testaccount",
		common.ParamAccountKey:  "testkey",
		common.ParamContainer:   "testcontainer",
	}
}

func (s *AzureDriverParametersTestSuite) TestExplicitSharedKeyCredentials() {
	params := s.baseParams
	params[common.ParamCredentialsType] = common.CredentialsTypeSharedKey
	result, err := ParseParameters(params)
	require.NoError(s.T(), err)

	driverParams := result.(*DriverParameters)
	assert.Equal(s.T(), common.CredentialsTypeSharedKey, driverParams.CredentialsType)
	assert.Equal(s.T(), "testaccount", driverParams.AccountName)
	assert.Equal(s.T(), "testkey", driverParams.AccountKey)
	assert.Equal(s.T(), "testcontainer", driverParams.Container)
}

func (s *AzureDriverParametersTestSuite) TestSharedKeyCredentialsMissingParams() {
	testCases := []struct {
		name          string
		params        map[string]any
		expectedError string
	}{
		{
			name: "Missing AccountName",
			params: map[string]any{
				common.ParamAccountKey: "testkey",
				common.ParamContainer:  "testcontainer",
			},
			expectedError: "no accountname parameter provided",
		},
		{
			name: "Empty AccountName",
			params: map[string]any{
				common.ParamAccountName: "",
				common.ParamAccountKey:  "testkey",
				common.ParamContainer:   "testcontainer",
			},
			expectedError: "no accountname parameter provided",
		},
		{
			name: "Missing AccountKey",
			params: map[string]any{
				common.ParamAccountName: "testaccount",
				common.ParamContainer:   "testcontainer",
			},
			expectedError: "no accountkey parameter provided",
		},
		{
			name: "Empty AccountKey",
			params: map[string]any{
				common.ParamAccountName: "testaccount",
				common.ParamAccountKey:  "",
				common.ParamContainer:   "testcontainer",
			},
			expectedError: "no accountkey parameter provided",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			_, err := ParseParameters(tc.params)
			require.Error(s.T(), err)
			assert.ErrorContains(s.T(), err, tc.expectedError)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestDefaultsToSharedKeyCredentials() {
	// Test that when no credentials type is specified, it defaults to shared_key
	params := s.baseParams
	result, err := ParseParameters(params)
	require.NoError(s.T(), err)

	driverParams := result.(*DriverParameters)
	assert.Equal(s.T(), common.CredentialsTypeSharedKey, driverParams.CredentialsType)
	assert.Equal(s.T(), "testaccount", driverParams.AccountName)
	assert.Equal(s.T(), "testkey", driverParams.AccountKey)
	assert.Equal(s.T(), "testcontainer", driverParams.Container)
}

func (s *AzureDriverParametersTestSuite) TestClientSecretCredentialsMissingParams() {
	testCases := []struct {
		name          string
		params        map[string]any
		expectedError string
	}{
		{
			name: "Missing TenantID",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeClientSecret,
				common.ParamClientID:        "testclient",
				common.ParamSecret:          "testsecret",
				common.ParamContainer:       "testcontainer",
			},
			expectedError: "no tenant_id parameter provided",
		},
		{
			name: "Empty TenantID",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeClientSecret,
				common.ParamTenantID:        "",
				common.ParamClientID:        "testclient",
				common.ParamSecret:          "testsecret",
				common.ParamContainer:       "testcontainer",
			},
			expectedError: "no tenant_id parameter provided",
		},
		{
			name: "Missing ClientID",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeClientSecret,
				common.ParamTenantID:        "testtenant",
				common.ParamSecret:          "testsecret",
				common.ParamContainer:       "testcontainer",
			},
			expectedError: "no client_id parameter provided",
		},
		{
			name: "Empty ClientID",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeClientSecret,
				common.ParamTenantID:        "testtenant",
				common.ParamClientID:        "",
				common.ParamSecret:          "testsecret",
				common.ParamContainer:       "testcontainer",
			},
			expectedError: "no client_id parameter provided",
		},
		{
			name: "Missing Secret",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeClientSecret,
				common.ParamTenantID:        "testtenant",
				common.ParamClientID:        "testclient",
				common.ParamContainer:       "testcontainer",
			},
			expectedError: "no secret parameter provided",
		},
		{
			name: "Empty Secret",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeClientSecret,
				common.ParamTenantID:        "testtenant",
				common.ParamClientID:        "testclient",
				common.ParamSecret:          "",
				common.ParamContainer:       "testcontainer",
			},
			expectedError: "no secret parameter provided",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			tc.params[sdriver.ParamLogger] = testutil.NewTestLogger(s.T())

			_, err := ParseParameters(tc.params)
			require.Error(s.T(), err)
			require.ErrorContains(s.T(), err, tc.expectedError)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestClientSecretCredentials() {
	params := map[string]any{
		common.ParamCredentialsType: common.CredentialsTypeClientSecret,
		common.ParamAccountName:     "testaccount",
		common.ParamTenantID:        "testtenant",
		common.ParamClientID:        "testclient",
		common.ParamSecret:          "testsecret",
		common.ParamContainer:       "testcontainer",
		sdriver.ParamLogger:         testutil.NewTestLogger(s.T()),
	}

	result, err := ParseParameters(params)
	require.NoError(s.T(), err)

	driverParams := result.(*DriverParameters)
	assert.Equal(s.T(), common.CredentialsTypeClientSecret, driverParams.CredentialsType)
	assert.Equal(s.T(), "testtenant", driverParams.TenantID)
	assert.Equal(s.T(), "testclient", driverParams.ClientID)
	assert.Equal(s.T(), "testsecret", driverParams.Secret)
}

func (s *AzureDriverParametersTestSuite) TestDefaultCredentialsMissingContainer() {
	params := map[string]any{
		common.ParamCredentialsType: common.CredentialsTypeDefaultCredentials,
		sdriver.ParamLogger:         testutil.NewTestLogger(s.T()),
	}

	_, err := ParseParameters(params)
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "no container parameter provided")
}

func (s *AzureDriverParametersTestSuite) TestDefaultCredentials() {
	params := map[string]any{
		common.ParamAccountName:     "testaccount",
		common.ParamCredentialsType: common.CredentialsTypeDefaultCredentials,
		common.ParamContainer:       "testcontainer",
		sdriver.ParamLogger:         testutil.NewTestLogger(s.T()),
	}

	result, err := ParseParameters(params)
	require.NoError(s.T(), err)

	driverParams := result.(*DriverParameters)
	require.Equal(s.T(), common.CredentialsTypeDefaultCredentials, driverParams.CredentialsType)
}

func (s *AzureDriverParametersTestSuite) TestDebugLogConfiguration() {
	testCases := []struct {
		name           string
		debugLog       any
		debugLogEvents string
		expectedEvents []azlog.Event
		expectError    bool
	}{
		{
			name:           "No Debug Log",
			debugLog:       nil,
			debugLogEvents: "",
			expectedEvents: make([]azlog.Event, 0),
		},
		{
			name:           "Debug Log Enabled No Events",
			debugLog:       true,
			debugLogEvents: "",
			expectedEvents: make([]azlog.Event, 0),
		},
		{
			name:           "Debug Log With Some Events",
			debugLog:       true,
			debugLogEvents: "Request,Response,ResponseError",
			expectedEvents: []azlog.Event{azlog.EventRequest, azlog.EventResponse, azlog.EventResponseError},
		},
		{
			name:           "Debug Log With All Events",
			debugLog:       true,
			debugLogEvents: "Request,Response,ResponseError,Retry,LongRunningOperation",
			expectedEvents: []azlog.Event{
				azlog.EventRequest,
				azlog.EventResponse,
				azlog.EventResponseError,
				azlog.EventRetryPolicy,
				azlog.EventLRO,
			},
		},
		{
			name:           "Invalid Event",
			debugLog:       true,
			debugLogEvents: "Request,InvalidEvent",
			expectError:    true,
		},
		{
			name:           "Case Insensitive Events",
			debugLog:       true,
			debugLogEvents: "request,RESPONSE,ResponseError",
			expectedEvents: []azlog.Event{azlog.EventRequest, azlog.EventResponse, azlog.EventResponseError},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			params := s.baseParams
			if tc.debugLog != nil {
				params[common.ParamDebugLog] = tc.debugLog
			}
			if tc.debugLogEvents != "" {
				params[common.ParamDebugLogEvents] = tc.debugLogEvents
			}

			result, err := ParseParameters(params)
			if tc.expectError {
				assert.Error(s.T(), err)
				return
			} else if !assert.NoError(s.T(), err) { // nolint: testifylint
				return
			}

			driverParams := result.(*DriverParameters)
			if tc.debugLog != nil {
				assert.Equal(s.T(), tc.debugLog, driverParams.DebugLog)
			} else {
				assert.False(s.T(), driverParams.DebugLog)
			}
			assert.Equal(s.T(), tc.expectedEvents, driverParams.DebugLogEvents)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestRealmAndServiceURL() {
	testCases := []struct {
		name               string
		params             map[string]any
		expectedRealm      string
		expectedServiceURL string
		expectError        bool
	}{
		{
			name:               "Default Realm",
			params:             s.baseParams,
			expectedRealm:      "core.windows.net",
			expectedServiceURL: "https://testaccount.blob.core.windows.net",
		},
		{
			name: "Custom Realm",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeDefaultCredentials,
				common.ParamAccountName:     "testaccount",
				common.ParamRealm:           "custom.realm",
				common.ParamContainer:       "testcontainer",
			},
			expectedRealm:      "custom.realm",
			expectedServiceURL: "https://testaccount.blob.custom.realm",
		},
		{
			name: "Custom Service URL",
			params: map[string]any{
				common.ParamCredentialsType: common.CredentialsTypeDefaultCredentials,
				common.ParamServiceURLKey:   "https://custom.endpoint",
				common.ParamContainer:       "testcontainer",
			},
			expectedRealm:      "core.windows.net",
			expectedServiceURL: "https://custom.endpoint",
		},
		{
			name: "Missing AccountName Without ServiceURL",
			params: map[string]any{
				common.ParamContainer: "testcontainer",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			tc.params[sdriver.ParamLogger] = testutil.NewTestLogger(s.T())

			result, err := ParseParameters(tc.params)

			if tc.expectError {
				assert.Error(s.T(), err)
				return
			} else if !assert.NoError(s.T(), err) { // nolint: testifylint
				return
			}

			driverParams := result.(*DriverParameters)
			assert.Equal(s.T(), tc.expectedRealm, driverParams.Realm)
			assert.Equal(s.T(), tc.expectedServiceURL, driverParams.ServiceURL)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestPoolConfiguration() {
	testCases := []struct {
		name                 string
		params               map[string]any
		expectedInitInterval time.Duration
		expectedMaxInterval  time.Duration
		expectedMaxElapsed   time.Duration
		expectError          bool
	}{
		{
			name:                 "Default Values",
			params:               s.baseParams,
			expectedInitInterval: DefaultPoolInitialInterval,
			expectedMaxInterval:  DefaultPoolMaxInterval,
			expectedMaxElapsed:   DefaultPoolMaxElapsedTime,
		},
		{
			name: "Custom Values",
			params: map[string]any{
				common.ParamAccountName:         "testaccount",
				common.ParamAccountKey:          "testkey",
				common.ParamContainer:           "testcontainer",
				common.ParamPoolInitialInterval: "1s",
				common.ParamPoolMaxInterval:     "10s",
				common.ParamPoolMaxElapsedTime:  "30s",
			},
			expectedInitInterval: time.Second,
			expectedMaxInterval:  10 * time.Second,
			expectedMaxElapsed:   30 * time.Second,
		},
		{
			name: "Invalid Duration",
			params: map[string]any{
				common.ParamAccountName:         "testaccount",
				common.ParamAccountKey:          "testkey",
				common.ParamContainer:           "testcontainer",
				common.ParamPoolInitialInterval: "invalid",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			tc.params[sdriver.ParamLogger] = testutil.NewTestLogger(s.T())

			result, err := ParseParameters(tc.params)
			if tc.expectError {
				assert.Error(s.T(), err)
				return
			} else if !assert.NoError(s.T(), err) { // nolint: testifylint
				return
			}

			driverParams := result.(*DriverParameters)
			assert.Equal(s.T(), tc.expectedInitInterval, driverParams.PoolInitialInterval)
			assert.Equal(s.T(), tc.expectedMaxInterval, driverParams.PoolMaxInterval)
			assert.Equal(s.T(), tc.expectedMaxElapsed, driverParams.PoolMaxElapsedTime)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestRootDirectoryConfiguration() {
	testCases := []struct {
		name         string
		rootDir      string
		expectedRoot string
	}{
		{
			name:         "Empty Root",
			rootDir:      "",
			expectedRoot: "",
		},
		{
			name:         "Root With Slashes",
			rootDir:      "/test/dir/",
			expectedRoot: "test/dir/",
		},
		{
			name:         "Root Without Slashes",
			rootDir:      "test/dir",
			expectedRoot: "test/dir/",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			params := s.baseParams
			params[common.ParamRootDirectory] = tc.rootDir

			result, err := ParseParameters(params)
			require.NoError(s.T(), err)

			driverParams := result.(*DriverParameters)
			assert.Equal(s.T(), tc.expectedRoot, driverParams.Root)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestInvalidCredentialsType() {
	params := map[string]any{
		common.ParamCredentialsType: "invalid_type",
		common.ParamAccountName:     "testaccount",
		common.ParamAccountKey:      "testkey",
		common.ParamContainer:       "testcontainer",
	}

	_, err := ParseParameters(params)
	require.Error(s.T(), err)
	assert.ErrorContains(s.T(), err, `credentials type "invalid_type" is invalid`)
}

func (s *AzureDriverParametersTestSuite) TestInvalidDebugLogOption() {
	params := map[string]any{
		common.ParamAccountName: "testaccount",
		common.ParamAccountKey:  "testkey",
		common.ParamContainer:   "testcontainer",
		common.ParamDebugLog:    "not_a_bool",
		sdriver.ParamLogger:     testutil.NewTestLogger(s.T()),
	}

	_, err := ParseParameters(params)
	require.Error(s.T(), err)
	assert.ErrorContains(s.T(), err, "parsing parameter debug_log")
}

func (s *AzureDriverParametersTestSuite) TestEmptyServiceURLAndAccountName() {
	params := map[string]any{
		common.ParamCredentialsType: common.CredentialsTypeDefaultCredentials,
		common.ParamContainer:       "testcontainer",
		common.ParamServiceURLKey:   "",
		sdriver.ParamLogger:         testutil.NewTestLogger(s.T()),
	}

	_, err := ParseParameters(params)
	require.Error(s.T(), err)
	assert.ErrorContains(s.T(), err, "unable to derive service URL")
}

func (s *AzureDriverParametersTestSuite) TestInvalidPoolDurations() {
	testCases := []struct {
		name          string
		params        map[string]any
		expectedError string
	}{
		{
			name: "Invalid MaxInterval",
			params: map[string]any{
				common.ParamAccountName:     "testaccount",
				common.ParamAccountKey:      "testkey",
				common.ParamContainer:       "testcontainer",
				common.ParamPoolMaxInterval: "invalid",
			},
			expectedError: "unable to parse parameter \"api_pool_max_interval\"",
		},
		{
			name: "Invalid MaxElapsedTime",
			params: map[string]any{
				common.ParamAccountName:        "testaccount",
				common.ParamAccountKey:         "testkey",
				common.ParamContainer:          "testcontainer",
				common.ParamPoolMaxElapsedTime: "invalid",
			},
			expectedError: "unable to parse parameter \"api_pool_max_elapsed_time\"",
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			tc.params[sdriver.ParamLogger] = testutil.NewTestLogger(s.T())

			_, err := ParseParameters(tc.params)
			require.Error(s.T(), err)
			assert.ErrorContains(s.T(), err, tc.expectedError)
		})
	}
}

func (s *AzureDriverParametersTestSuite) TestRootPrefixConfigurationError() {
	params := map[string]any{
		common.ParamAccountName:          "testaccount",
		common.ParamAccountKey:           "testkey",
		common.ParamContainer:            "testcontainer",
		common.ParamLegacyRootPrefix:     true,
		common.ParamTrimLegacyRootPrefix: true,
		sdriver.ParamLogger:              testutil.NewTestLogger(s.T()),
	}

	_, err := ParseParameters(params)
	require.Error(s.T(), err)
	assert.ErrorContains(s.T(), err, "can not both be true")
}

func (s *AzureDriverParametersTestSuite) TestRetryConfiguration() {
	testCases := []struct {
		name               string
		params             map[string]any
		expectedMaxRetries int32
		expectedTryTimeout time.Duration
		expectedRetryDelay time.Duration
		expectedMaxDelay   time.Duration
		expectError        bool
	}{
		{
			name:               "Default Values",
			params:             s.baseParams,
			expectedMaxRetries: DefaultMaxRetries,
			expectedTryTimeout: DefaultRetryTryTimeout,
			expectedRetryDelay: DefaultRetryDelay,
			expectedMaxDelay:   DefaultMaxRetryDelay,
		},
		{
			name: "Custom Values",
			params: map[string]any{
				common.ParamAccountName:     "testaccount",
				common.ParamAccountKey:      "testkey",
				common.ParamContainer:       "testcontainer",
				common.ParamMaxRetries:      5,
				common.ParamRetryTryTimeout: "30s",
				common.ParamRetryDelay:      "2s",
				common.ParamMaxRetryDelay:   "20s",
			},
			expectedMaxRetries: 5,
			expectedTryTimeout: 30 * time.Second,
			expectedRetryDelay: 2 * time.Second,
			expectedMaxDelay:   20 * time.Second,
		},
		{
			name: "Invalid MaxRetries",
			params: map[string]any{
				common.ParamAccountName: "testaccount",
				common.ParamAccountKey:  "testkey",
				common.ParamContainer:   "testcontainer",
				common.ParamMaxRetries:  "invalid",
			},
			expectError: true,
		},
		{
			name: "Invalid RetryTryTimeout",
			params: map[string]any{
				common.ParamAccountName:     "testaccount",
				common.ParamAccountKey:      "testkey",
				common.ParamContainer:       "testcontainer",
				common.ParamRetryTryTimeout: "invalid",
			},
			expectError: true,
		},
		{
			name: "Invalid RetryDelay",
			params: map[string]any{
				common.ParamAccountName: "testaccount",
				common.ParamAccountKey:  "testkey",
				common.ParamContainer:   "testcontainer",
				common.ParamRetryDelay:  "invalid",
			},
			expectError: true,
		},
		{
			name: "Invalid MaxRetryDelay",
			params: map[string]any{
				common.ParamAccountName:   "testaccount",
				common.ParamAccountKey:    "testkey",
				common.ParamContainer:     "testcontainer",
				common.ParamMaxRetryDelay: "invalid",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			tc.params[sdriver.ParamLogger] = testutil.NewTestLogger(s.T())

			result, err := ParseParameters(tc.params)
			if tc.expectError {
				assert.Error(s.T(), err)
				return
			}
			require.NoError(s.T(), err)

			driverParams := result.(*DriverParameters)
			assert.Equal(s.T(), tc.expectedMaxRetries, driverParams.MaxRetries)
			assert.Equal(s.T(), tc.expectedTryTimeout, driverParams.RetryTryTimeout)
			assert.Equal(s.T(), tc.expectedRetryDelay, driverParams.RetryDelay)
			assert.Equal(s.T(), tc.expectedMaxDelay, driverParams.MaxRetryDelay)
		})
	}
}
