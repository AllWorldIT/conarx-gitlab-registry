package configuration

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"gopkg.in/yaml.v2"
)

// configStruct is a canonical example configuration, which should map to configYamlV0_1
var configStruct = Configuration{
	Version: "0.1",
	Log: struct {
		AccessLog struct {
			Disabled  bool            `yaml:"disabled,omitempty"`
			Formatter accessLogFormat `yaml:"formatter,omitempty"`
		} `yaml:"accesslog,omitempty"`
		Level     Loglevel       `yaml:"level,omitempty"`
		Formatter logFormat      `yaml:"formatter,omitempty"`
		Output    logOutput      `yaml:"output,omitempty"`
		Fields    map[string]any `yaml:"fields,omitempty"`
	}{
		AccessLog: struct {
			Disabled  bool            `yaml:"disabled,omitempty"`
			Formatter accessLogFormat `yaml:"formatter,omitempty"`
		}{
			Formatter: "json",
		},
		Level:     "info",
		Formatter: "json",
		Output:    "stdout",
		Fields:    map[string]any{"environment": "test"},
	},
	Storage: Storage{
		"s3": Parameters{
			"region":        "us-east-1",
			"bucket":        "my-bucket",
			"rootdirectory": "/registry",
			"encrypt":       true,
			"secure":        false,
			"accesskey":     "SAMPLEACCESSKEY",
			"secretkey":     "SUPERSECRET",
			"host":          nil,
			"port":          42,
		},
	},
	Database: Database{
		Enabled:  true,
		Host:     "localhost",
		Port:     5432,
		User:     "postgres",
		Password: "",
		DBName:   "registry",
		SSLMode:  "disable",
		Primary:  "primary.record.fqdn",
		BackgroundMigrations: BackgroundMigrations{
			Enabled:       true,
			MaxJobRetries: 1,
			JobInterval:   1 * time.Minute,
		},
		LoadBalancing: DatabaseLoadBalancing{
			Enabled:              true,
			Nameserver:           "localhost",
			Port:                 8600,
			Record:               "db-replica-registry.service.consul",
			ReplicaCheckInterval: 1 * time.Minute,
		},
	},
	Auth: Auth{
		"silly": Parameters{
			"realm":   "silly",
			"service": "silly",
		},
	},
	Reporting: Reporting{
		Sentry: SentryReporting{
			Enabled: true,
			DSN:     "https://foo@12345.ingest.sentry.io/876542",
		},
	},
	Notifications: Notifications{
		FanoutTimeout: 4 * time.Second,
		Endpoints: []Endpoint{
			{
				Name: "endpoint-1",
				URL:  "http://example.com",
				Headers: http.Header{
					"Authorization": []string{"Bearer <example>"},
				},
				IgnoredMediaTypes: []string{"application/octet-stream"},
				Ignore: Ignore{
					MediaTypes: []string{"application/octet-stream"},
					Actions:    []string{"pull"},
				},
				QueuePurgeTimeout: 3 * time.Second,
			},
		},
	},
	HTTP: struct {
		Addr         string        `yaml:"addr,omitempty"`
		Net          string        `yaml:"net,omitempty"`
		Host         string        `yaml:"host,omitempty"`
		Prefix       string        `yaml:"prefix,omitempty"`
		Secret       string        `yaml:"secret,omitempty"`
		RelativeURLs bool          `yaml:"relativeurls,omitempty"`
		DrainTimeout time.Duration `yaml:"draintimeout,omitempty"`
		TLS          TLS           `yaml:"tls,omitempty"`
		Headers      http.Header   `yaml:"headers,omitempty"`
		Debug        struct {
			Addr       string   `yaml:"addr,omitempty"`
			TLS        DebugTLS `yaml:"tls,omitempty"`
			Prometheus struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Path    string `yaml:"path,omitempty"`
			} `yaml:"prometheus,omitempty"`
			Pprof struct {
				Enabled bool `yaml:"enabled,omitempty"`
			} `yaml:"pprof,omitempty"`
		} `yaml:"debug,omitempty"`
		HTTP2 struct {
			Disabled bool `yaml:"disabled,omitempty"`
		} `yaml:"http2,omitempty"`
	}{
		TLS: TLS{
			ClientCAs:    []string{"/path/to/ca.pem"},
			CipherSuites: defaultCipherSuites(),
		},
		Headers: http.Header{
			"X-Content-Type-Options": []string{"nosniff"},
		},
		HTTP2: struct {
			Disabled bool `yaml:"disabled,omitempty"`
		}{
			Disabled: false,
		},
	},
	RateLimiter: RateLimiter{
		Enabled: true,
		Limiters: []Limiter{
			{
				Name:        "test_rate_limiter",
				Description: "A rate limiter example",
				LogOnly:     false,
				Match:       Match{Type: "IP"},
				Precedence:  10,
				Limit: Limit{
					Rate:   10,
					Period: "minute",
					Burst:  100,
				},
				Action: Action{
					WarnThreshold: 0.7,
					WarnAction:    "log",
					HardAction:    "block",
				},
			},
		},
	},
}

// configYamlV0_1 is a Version 0.1 yaml document representing configStruct
var configYamlV0_1 = `
version: 0.1
log:
  level: info
  fields:
    environment: test
storage:
  s3:
    region: us-east-1
    bucket: my-bucket
    rootdirectory: /registry
    encrypt: true
    secure: false
    accesskey: SAMPLEACCESSKEY
    secretkey: SUPERSECRET
    host: ~
    port: 42
database:
  enabled: true
  host: localhost
  port: 5432
  user: postgres
  password:
  dbname: registry
  schema: public
  sslmode: disable
  primary: primary.record.fqdn
  backgroundmigrations:
    enabled: true
    maxjobretries: 1
    jobinterval: 1m
  loadbalancing:
    enabled: true
    nameserver: localhost
    port: 8600
    record: db-replica-registry.service.consul
    disconnecttimeout: 2m
    maxreplicalagtime: 1m
    maxreplicalagbytes: 8388608
    replicacheckinterval: 1m
auth:
  silly:
    realm: silly
    service: silly
notifications:
  fanouttimeout: 4s
  endpoints:
    - name: endpoint-1
      url:  http://example.com
      headers:
        Authorization: [Bearer <example>]
      ignoredmediatypes:
        - application/octet-stream
      ignore:
        mediatypes:
           - application/octet-stream
        actions:
           - pull
      queuepurgetimeout: 3s
reporting:
  sentry:
    enabled: true
    dsn: https://foo@12345.ingest.sentry.io/876542
http:
  clientcas:
    - /path/to/ca.pem
  headers:
    X-Content-Type-Options: [nosniff]
rate_limiter:
  enabled: true
  limiters:
    - name: test_rate_limiter
      description: "A rate limiter example"
      log_only: false
      match:
        type: IP
      precedence: 10
      limit:
        rate: 10
        period: "minute"
        burst: 100
      action:
        warn_threshold: 0.7
        warn_action: "log"
        hard_action: "block"
`

// inmemoryConfigYamlV0_1 is a Version 0.1 yaml document specifying an inmemory
// storage driver with no parameters
var inmemoryConfigYamlV0_1 = `
version: 0.1
log:
  level: info
storage: inmemory
auth:
  silly:
    realm: silly
    service: silly
notifications:
  fanouttimeout: 4s
  endpoints:
    - name: endpoint-1
      url:  http://example.com
      headers:
        Authorization: [Bearer <example>]
      ignoredmediatypes:
        - application/octet-stream
      ignore:
        mediatypes:
           - application/octet-stream
        actions:
           - pull
      queuepurgetimeout: 3s
http:
  headers:
    X-Content-Type-Options: [nosniff]
database:
  enabled: true
  backgroundmigrations:
    enabled: true
    maxjobretries: 1
    jobinterval: 1m
`

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigSuite))
}

type ConfigSuite struct {
	suite.Suite

	expectedConfig *Configuration
}

func (s *ConfigSuite) SetupTest() {
	os.Clearenv()
	s.expectedConfig = copyConfig(configStruct)
}

// TestMarshalRoundtrip validates that configStruct can be marshaled and
// unmarshaled without changing any parameters
func (s *ConfigSuite) TestMarshalRoundtrip() {
	configBytes, err := yaml.Marshal(s.expectedConfig)
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader(configBytes))
	s.T().Log(string(configBytes))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseSimple validates that configYamlV0_1 can be parsed into a struct
// matching configStruct
func (s *ConfigSuite) TestParseSimple() {
	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseInmemory validates that configuration yaml with storage provided as
// a string can be parsed into a Configuration struct with no storage parameters
func (s *ConfigSuite) TestParseInmemory() {
	s.expectedConfig.Storage = Storage{"inmemory": Parameters{}}
	s.expectedConfig.Database = Database{
		Enabled: true,
		BackgroundMigrations: BackgroundMigrations{
			Enabled:       true,
			MaxJobRetries: 1,
			JobInterval:   1 * time.Minute,
		},
	}
	s.expectedConfig.Reporting = Reporting{}
	s.expectedConfig.Log.Fields = nil
	s.expectedConfig.RateLimiter = RateLimiter{}

	config, err := Parse(bytes.NewReader([]byte(inmemoryConfigYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseIncomplete validates that an incomplete yaml configuration cannot
// be parsed without providing environment variables to fill in the missing
// components.
func (s *ConfigSuite) TestParseIncomplete() {
	incompleteConfigYaml := "version: 0.1"
	_, err := Parse(bytes.NewReader([]byte(incompleteConfigYaml)))
	require.Error(s.T(), err)

	s.expectedConfig.Log.Fields = nil
	s.expectedConfig.Storage = Storage{"filesystem": Parameters{"rootdirectory": "/tmp/testroot"}}
	s.expectedConfig.Database = Database{}
	s.expectedConfig.Auth = Auth{"silly": Parameters{"realm": "silly"}}
	s.expectedConfig.Reporting = Reporting{}
	s.expectedConfig.Notifications = Notifications{}
	s.expectedConfig.RateLimiter = RateLimiter{}
	s.expectedConfig.HTTP.Headers = nil

	// Note: this also tests that REGISTRY_STORAGE and
	// REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY can be used together
	err = os.Setenv("REGISTRY_STORAGE", "filesystem")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "/tmp/testroot")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_AUTH", "silly")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_AUTH_SILLY_REALM", "silly")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(incompleteConfigYaml)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseWithSameEnvStorage validates that providing environment variables
// that match the given storage type will only include environment-defined
// parameters and remove yaml-defined parameters
func (s *ConfigSuite) TestParseWithSameEnvStorage() {
	s.expectedConfig.Storage = Storage{"s3": Parameters{"region": "us-east-1"}}

	err := os.Setenv("REGISTRY_STORAGE", "s3")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_STORAGE_S3_REGION", "us-east-1")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseWithDifferentEnvStorageParams validates that providing environment variables that change
// and add to the given storage parameters will change and add parameters to the parsed
// Configuration struct
func (s *ConfigSuite) TestParseWithDifferentEnvStorageParams() {
	s.expectedConfig.Storage.setParameter("region", "us-west-1")
	s.expectedConfig.Storage.setParameter("secure", true)
	s.expectedConfig.Storage.setParameter("newparam", "some Value")

	err := os.Setenv("REGISTRY_STORAGE_S3_REGION", "us-west-1")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_STORAGE_S3_SECURE", "true")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_STORAGE_S3_NEWPARAM", "some Value")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseWithDifferentEnvStorageType validates that providing an environment variable that
// changes the storage type will be reflected in the parsed Configuration struct
func (s *ConfigSuite) TestParseWithDifferentEnvStorageType() {
	s.expectedConfig.Storage = Storage{"inmemory": Parameters{}}

	err := os.Setenv("REGISTRY_STORAGE", "inmemory")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseWithDifferentEnvStorageTypeAndParams validates that providing an environment variable
// that changes the storage type will be reflected in the parsed Configuration struct and that
// environment storage parameters will also be included
func (s *ConfigSuite) TestParseWithDifferentEnvStorageTypeAndParams() {
	s.expectedConfig.Storage = Storage{"filesystem": Parameters{}}
	s.expectedConfig.Storage.setParameter("rootdirectory", "/tmp/testroot")

	err := os.Setenv("REGISTRY_STORAGE", "filesystem")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "/tmp/testroot")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseWithSameEnvLoglevel validates that providing an environment variable defining the log
// level to the same as the one provided in the yaml will not change the parsed Configuration struct
func (s *ConfigSuite) TestParseWithSameEnvLoglevel() {
	err := os.Setenv("REGISTRY_LOGLEVEL", "info")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseWithDifferentEnvLoglevel validates that providing an environment variable defining the
// log level will override the value provided in the yaml document
func (s *ConfigSuite) TestParseWithDifferentEnvLoglevel() {
	s.expectedConfig.Log.Level = "error"

	err := os.Setenv("REGISTRY_LOG_LEVEL", "error")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseInvalidLoglevel validates that the parser will fail to parse a
// configuration if the loglevel is malformed
func (s *ConfigSuite) TestParseInvalidLoglevel() {
	invalidConfigYaml := "version: 0.1\nloglevel: derp\nstorage: inmemory"
	_, err := Parse(bytes.NewReader([]byte(invalidConfigYaml)))
	require.Error(s.T(), err)

	err = os.Setenv("REGISTRY_LOGLEVEL", "derp")
	require.NoError(s.T(), err)

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.Error(s.T(), err)
}

// TestParseWithoutStorageValidation validates that the parser will not fail to parse a configuration if a storage
// driver was not set but WithoutStorageValidation was passed as an option.
func (s *ConfigSuite) TestParseWithoutStorageValidation() {
	configYaml := "version: 0.1"

	_, err := Parse(bytes.NewReader([]byte(configYaml)))
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "no storage configuration provided")

	_, err = Parse(bytes.NewReader([]byte(configYaml)), WithoutStorageValidation())
	require.NoError(s.T(), err)
}

type parameterTest struct {
	name    string
	value   string
	want    any
	wantErr bool
	err     string
}

type parameterValidator func(t *testing.T, want any, got *Configuration)

func testParameter(t *testing.T, yml, envVar string, tests []parameterTest, fn parameterValidator) {
	t.Helper()

	testCases := []string{"yaml", "env"}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var input string

					if testCase == "env" {
						// if testing with an environment variable we need to set it and defer the unset
						require.NoError(t, os.Setenv(envVar, test.value))
						defer func() { require.NoError(t, os.Unsetenv(envVar)) }()
						// we also need to make sure to clean the YAML parameter
						input = fmt.Sprintf(yml, "")
					} else {
						input = fmt.Sprintf(yml, test.value)
					}

					got, err := Parse(bytes.NewReader([]byte(input)))
					if test.wantErr {
						require.Error(t, err)
						require.EqualError(t, err, test.err)
						require.Nil(t, got)
					} else {
						require.NoError(t, err)
						fn(t, test.want, got)
					}
				})
			}
		})
	}
}

func TestParseLogLevel(t *testing.T) {
	yml := `
version: 0.1
log:
  level: %s
storage: inmemory
`
	errTemplate := `invalid log level "%s", must be one of ` + fmt.Sprintf("%q", logLevels)

	tt := []parameterTest{
		{
			name:  "error",
			value: "error",
			want:  "error",
		},
		{
			name:  "warn",
			value: "warn",
			want:  "warn",
		},
		{
			name:  "info",
			value: "info",
			want:  "info",
		},
		{
			name:  "debug",
			value: "debug",
			want:  "debug",
		},
		{
			name:  "trace",
			value: "trace",
			want:  "trace",
		},
		{
			name: "default",
			want: "info",
		},
		{
			name:    "unknown",
			value:   "foo",
			wantErr: true,
			err:     fmt.Sprintf(errTemplate, "foo"),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Log.Level.String())
	}

	testParameter(t, yml, "REGISTRY_LOG_LEVEL", tt, validator)
}

func TestParseLogOutput(t *testing.T) {
	yml := `
version: 0.1
log:
  output: %s
storage: inmemory
`
	errTemplate := `invalid log output "%s", must be one of ` + fmt.Sprintf("%q", logOutputs)

	tt := []parameterTest{
		{
			name:  "stdout",
			value: "stdout",
			want:  "stdout",
		},
		{
			name:  "stderr",
			value: "stderr",
			want:  "stderr",
		},
		{
			name: "default",
			want: "stdout",
		},
		{
			name:    "unknown",
			value:   "foo",
			wantErr: true,
			err:     fmt.Sprintf(errTemplate, "foo"),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Log.Output.String())
	}

	testParameter(t, yml, "REGISTRY_LOG_OUTPUT", tt, validator)
}

func TestParseLogFormatter(t *testing.T) {
	yml := `
version: 0.1
log:
  formatter: %s
storage: inmemory
`
	errTemplate := `invalid log format "%s", must be one of ` + fmt.Sprintf("%q", logFormats)

	tt := []parameterTest{
		{
			name:  "text",
			value: "text",
			want:  "text",
		},
		{
			name:  "json",
			value: "json",
			want:  "json",
		},
		{
			name: "default",
			want: "json",
		},
		{
			name:    "unknown",
			value:   "foo",
			wantErr: true,
			err:     fmt.Sprintf(errTemplate, "foo"),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Log.Formatter.String())
	}

	testParameter(t, yml, "REGISTRY_LOG_FORMATTER", tt, validator)
}

func TestParseAccessLogFormatter(t *testing.T) {
	yml := `
version: 0.1
log:
  accesslog:
    formatter: %s
storage: inmemory
`
	errTemplate := `invalid access log format "%s", must be one of ` + fmt.Sprintf("%q", accessLogFormats)

	tt := []parameterTest{
		{
			name:  "text",
			value: "text",
			want:  "text",
		},
		{
			name:  "json",
			value: "json",
			want:  "json",
		},
		{
			name: "default",
			want: "json",
		},
		{
			name:    "unknown",
			value:   "foo",
			wantErr: true,
			err:     fmt.Sprintf(errTemplate, "foo"),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Log.AccessLog.Formatter.String())
	}

	testParameter(t, yml, "REGISTRY_LOG_ACCESSLOG_FORMATTER", tt, validator)
}

// TestParseWithDifferentEnvDatabase validates that environment variables properly override database parameters
func (s *ConfigSuite) TestParseWithDifferentEnvDatabase() {
	expected := Database{
		Enabled:  true,
		Host:     "127.0.0.1",
		Port:     1234,
		User:     "user",
		Password: "passwd",
		DBName:   "foo",
		SSLMode:  "allow",
		Primary:  "primary.record.fqdn",
		BackgroundMigrations: BackgroundMigrations{
			Enabled:       true,
			MaxJobRetries: 1,
			JobInterval:   1 * time.Minute,
		},
		LoadBalancing: DatabaseLoadBalancing{
			Enabled:              true,
			Nameserver:           "localhost",
			Port:                 8600,
			Record:               "db-replica-registry.service.consul",
			ReplicaCheckInterval: 1 * time.Minute,
		},
	}
	s.expectedConfig.Database = expected

	err := os.Setenv("REGISTRY_DATABASE_DISABLE", strconv.FormatBool(expected.Enabled))
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_DATABASE_HOST", expected.Host)
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_DATABASE_PORT", strconv.Itoa(expected.Port))
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_DATABASE_USER", expected.User)
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_DATABASE_PASSWORD", expected.Password)
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_DATABASE_DBNAME", expected.DBName)
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_DATABASE_SSLMODE", expected.SSLMode)
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseInvalidVersion validates that the parser will fail to parse a newer configuration
// version than the CurrentVersion
func (s *ConfigSuite) TestParseInvalidVersion() {
	s.expectedConfig.Version = MajorMinorVersion(CurrentVersion.Major(), CurrentVersion.Minor()+1)
	configBytes, err := yaml.Marshal(s.expectedConfig)
	require.NoError(s.T(), err)
	_, err = Parse(bytes.NewReader(configBytes))
	require.Error(s.T(), err)
}

// TestParseExtraneousVars validates that environment variables referring to
// nonexistent variables don't cause side effects.
func (s *ConfigSuite) TestParseExtraneousVars() {
	s.expectedConfig.Reporting.Sentry.Environment = "test"

	// A valid environment variable
	err := os.Setenv("REGISTRY_REPORTING_SENTRY_ENVIRONMENT", "test")
	require.NoError(s.T(), err)

	// Environment variables which shouldn't set config items
	err = os.Setenv("REGISTRY_DUCKS", "quack")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_REPORTING_ASDF", "ghjk")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseEnvVarImplicitMaps validates that environment variables can set
// values in maps that don't already exist.
func (s *ConfigSuite) TestParseEnvVarImplicitMaps() {
	readonly := make(map[string]any)
	readonly["enabled"] = true

	maintenance := make(map[string]any)
	maintenance["readonly"] = readonly

	s.expectedConfig.Storage["maintenance"] = maintenance

	err := os.Setenv("REGISTRY_STORAGE_MAINTENANCE_READONLY_ENABLED", "true")
	require.NoError(s.T(), err)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), s.expectedConfig, config)
}

// TestParseEnvWrongTypeMap validates that incorrectly attempting to unmarshal a
// string over existing map fails.
func (s *ConfigSuite) TestParseEnvWrongTypeMap() {
	err := os.Setenv("REGISTRY_STORAGE_S3", "somestring")
	require.NoError(s.T(), err)

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.Error(s.T(), err)
}

// TestParseEnvWrongTypeStruct validates that incorrectly attempting to
// unmarshal a string into a struct fails.
func (s *ConfigSuite) TestParseEnvWrongTypeStruct() {
	err := os.Setenv("REGISTRY_STORAGE_LOG", "somestring")
	require.NoError(s.T(), err)

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.Error(s.T(), err)
}

// TestParseEnvWrongTypeSlice validates that incorrectly attempting to
// unmarshal a string into a slice fails.
func (s *ConfigSuite) TestParseEnvWrongTypeSlice() {
	err := os.Setenv("REGISTRY_HTTP_TLS_CLIENTCAS", "somestring")
	require.NoError(s.T(), err)

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.Error(s.T(), err)
}

// TestParseEnvMany tests several environment variable overrides.
// The result is not checked - the goal of this test is to detect panics
// from misuse of reflection.
func (s *ConfigSuite) TestParseEnvMany() {
	err := os.Setenv("REGISTRY_VERSION", "0.1")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_LOG_LEVEL", "debug")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_LOG_FORMATTER", "json")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_LOG_FIELDS", "abc: xyz")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_LOGLEVEL", "debug")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_STORAGE", "s3")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_AUTH_PARAMS", "param1: value1")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_AUTH_PARAMS_VALUE2", "value2")
	require.NoError(s.T(), err)
	err = os.Setenv("REGISTRY_AUTH_PARAMS_VALUE2", "value2")
	require.NoError(s.T(), err)

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	require.NoError(s.T(), err)
}

func boolParameterTests() []parameterTest {
	return []parameterTest{
		{
			name:  "true",
			value: "true",
			want:  "true",
		},
		{
			name:  "false",
			value: "false",
			want:  "false",
		},
		{
			name: "default",
			want: strconv.FormatBool(false),
		},
	}
}

func stringParameterTests() []parameterTest {
	return []parameterTest{
		{
			name:  "string",
			value: "string",
			want:  "string",
		},
		{
			name: "default",
			want: "",
		},
	}
}

func TestParseHTTPDebugPprofEnabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
http:
  debug:
    pprof:
      enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.HTTP.Debug.Pprof.Enabled))
	}

	testParameter(t, yml, "REGISTRY_HTTP_DEBUG_PPROF_ENABLED", tt, validator)
}

func TestParseHTTPDebugTLS_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
http:
  debug:
    tls:
      enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.HTTP.Debug.TLS.Enabled))
	}

	testParameter(t, yml, "REGISTRY_HTTP_DEBUG_TLS_ENABLED", tt, validator)
}

func TestParseHTTPDebugTLS_Certificate(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
http:
  debug:
    tls:
      certificate: %s
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.HTTP.Debug.TLS.Certificate)
	}

	testParameter(t, yml, "REGISTRY_HTTP_DEBUG_TLS_CERTIFICATE", tt, validator)
}

func TestParseHTTPDebugTLS_Key(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
http:
  debug:
    tls:
      key: %s
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.HTTP.Debug.TLS.Key)
	}

	testParameter(t, yml, "REGISTRY_HTTP_DEBUG_TLS_KEY", tt, validator)
}

func TestParseHTTPDebugTLS_MinimumTLS(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
http:
  debug:
    tls:
      minimumtls: %s
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.HTTP.Debug.TLS.MinimumTLS)
	}

	testParameter(t, yml, "REGISTRY_HTTP_DEBUG_TLS_MINIMUMTLS", tt, validator)
}

func TestParseHTTPDebugTLS_ClientCAs(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
http:
  debug:
    tls:
      clientcas: %s
`
	tt := []parameterTest{
		{
			name:  "slice",
			value: `["/path/to/ca/1", "/path/to/ca/2"]`,
			want:  []string{"/path/to/ca/1", "/path/to/ca/2"},
		},
		{
			name:  "empty",
			value: "[]",
			want:  make([]string, 0),
		},
		{
			name: "default",
			want: nil,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.ElementsMatch(t, want, got.HTTP.Debug.TLS.ClientCAs)
	}

	testParameter(t, yml, "REGISTRY_HTTP_DEBUG_TLS_CLIENTCAS", tt, validator)
}

func TestParseHTTPMonitoringStackdriverEnabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
profiling:
  stackdriver:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Profiling.Stackdriver.Enabled))
	}

	testParameter(t, yml, "REGISTRY_PROFILING_STACKDRIVER_ENABLED", tt, validator)
}

func TestParseMonitoringStackdriver_Service(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
profiling:
  stackdriver:
    service: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "registry",
			want:  "registry",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Profiling.Stackdriver.Service)
	}

	testParameter(t, yml, "REGISTRY_PROFILING_STACKDRIVER_SERVICE", tt, validator)
}

func TestParseMonitoringStackdriver_ServiceVersion(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
profiling:
  stackdriver:
    serviceversion: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1.0.0",
			want:  "1.0.0",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Profiling.Stackdriver.ServiceVersion)
	}

	testParameter(t, yml, "REGISTRY_PROFILING_STACKDRIVER_SERVICEVERSION", tt, validator)
}

func TestParseMonitoringStackdriver_ProjectID(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
profiling:
  stackdriver:
    projectid: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "tBXV4hFr4QJM6oGkqzhC",
			want:  "tBXV4hFr4QJM6oGkqzhC",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Profiling.Stackdriver.ProjectID)
	}

	testParameter(t, yml, "REGISTRY_PROFILING_STACKDRIVER_PROJECTID", tt, validator)
}

func TestParseMonitoringStackdriver_KeyFile(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
profiling:
  stackdriver:
    keyfile: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "/foo/bar.json",
			want:  "/foo/bar.json",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Profiling.Stackdriver.KeyFile)
	}

	testParameter(t, yml, "REGISTRY_PROFILING_STACKDRIVER_KEYFILE", tt, validator)
}

func TestParseRedisTLS_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  tls:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.TLS.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_TLS_ENABLED", tt, validator)
}

func TestParseRedisTLS_Insecure(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  tls:
    insecure: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.TLS.Insecure))
	}

	testParameter(t, yml, "REGISTRY_REDIS_TLS_INSECURE", tt, validator)
}

func TestParseRedis_Addr(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  addr: %s
`
	tt := []parameterTest{
		{
			name:  "single",
			value: "0.0.0.0:6379",
			want:  "0.0.0.0:6379",
		},
		{
			name:  "multiple",
			value: "0.0.0.0:16379,0.0.0.0:26379",
			want:  "0.0.0.0:16379,0.0.0.0:26379",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Addr)
	}

	testParameter(t, yml, "REGISTRY_REDIS_ADDR", tt, validator)
}

func TestParseRedis_MainName(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  mainname: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "myredismainserver",
			want:  "myredismainserver",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.MainName)
	}

	testParameter(t, yml, "REGISTRY_REDIS_MAINNAME", tt, validator)
}

func TestParseRedisPool_MaxOpen(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  pool:
    size: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "empty",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Pool.Size)
	}

	testParameter(t, yml, "REGISTRY_REDIS_POOL_SIZE", tt, validator)
}

func TestParseRedisPool_MaxLifeTime(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  pool:
    maxlifetime: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1h",
			want:  1 * time.Hour,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Pool.MaxLifetime)
	}

	testParameter(t, yml, "REGISTRY_REDIS_POOL_MAXLIFETIME", tt, validator)
}

func TestParseRedisPool_IdleTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  pool:
    idletimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Pool.IdleTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_POOL_IDLETIMEOUT", tt, validator)
}

func TestDatabase_SSLMode(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  sslmode: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "disable",
			want:  "disable",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.SSLMode)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_SSLMODE", tt, validator)
}

func TestDatabase_SSLCert(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  sslcert: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "/path/to/client.crt",
			want:  "/path/to/client.crt",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.SSLCert)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_SSLCERT", tt, validator)
}

func TestDatabase_SSLKey(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  sslkey: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "/path/to/client.key",
			want:  "/path/to/client.key",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.SSLKey)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_SSLKEY", tt, validator)
}

func TestDatabase_SSLRootCert(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  sslrootcert: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "/path/to/root.crt",
			want:  "/path/to/root.crt",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.SSLRootCert)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_SSLROOTCERT", tt, validator)
}

func TestParseDatabase_PreparedStatements(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  preparedstatements: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Database.PreparedStatements))
	}

	testParameter(t, yml, "REGISTRY_DATABASE_PREPAREDSTATEMENTS", tt, validator)
}

func TestParseDatabase_DrainTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  draintimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.DrainTimeout)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_DRAINTIMEOUT", tt, validator)
}

func TestParseDatabasePool_MaxIdle(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  pool:
    maxidle: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "default",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.Pool.MaxIdle)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_POOL_MAXIDLE", tt, validator)
}

func TestParseDatabasePool_MaxOpen(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  pool:
    maxopen: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "default",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.Pool.MaxOpen)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_POOL_MAXOPEN", tt, validator)
}

func TestParseDatabasePool_MaxLifetime(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  pool:
    maxlifetime: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10s",
			want:  10 * time.Second,
		},
		{
			name: "default",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.Pool.MaxLifetime)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_POOL_MAXLIFETIME", tt, validator)
}

func TestParseDatabaseLoadBalancing_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  loadbalancing:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Database.LoadBalancing.Enabled))
	}

	testParameter(t, yml, "REGISTRY_DATABASE_LOADBALANCING_ENABLED", tt, validator)
}

func TestParseDatabaseLoadBalancing_Hosts(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  loadbalancing:
    enabled: true
    hosts: %s
`
	tt := []parameterTest{
		{
			name:  "single",
			value: `["secondary1.example.com"]`,
			want:  []string{"secondary1.example.com"},
		},
		{
			name:  "multiple",
			value: `["secondary1.example.com" ,"secondary2.example.com"]`,
			want:  []string{"secondary1.example.com", "secondary2.example.com"},
		},
		{
			name: "default",
			want: make([]string, 0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.ElementsMatch(t, want, got.Database.LoadBalancing.Hosts)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_LOADBALANCING_HOSTS", tt, validator)
}

func TestParseDatabaseLoadBalancing_Nameserver(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  loadbalancing:
    enabled: true
    nameserver: %s
`
	tt := []parameterTest{
		{name: "default", want: defaultDLBNameserver},
		{name: "custom", value: "nameserver.example.com", want: "nameserver.example.com"},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.LoadBalancing.Nameserver)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_LOADBALANCING_NAMESERVER", tt, validator)
}

func TestParseDatabaseLoadBalancing_Port(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  loadbalancing:
    enabled: true
    port: %s
`
	tt := []parameterTest{
		{
			name: "default",
			want: defaultDLBPort,
		},
		{
			name:  "custom",
			value: "1234",
			want:  1234,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.LoadBalancing.Port)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_LOADBALANCING_PORT", tt, validator)
}

func TestParseDatabaseLoadBalancing_Record(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  loadbalancing:
    enabled: true
    record: %s
`
	tt := []parameterTest{
		{name: "default", want: ""},
		{name: "custom", value: "db-replica-registry.service.consul", want: "db-replica-registry.service.consul"},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.LoadBalancing.Record)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_LOADBALANCING_RECORD", tt, validator)
}

func TestParseDatabaseLoadBalancing_ReplicaCheckInterval(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  loadbalancing:
    enabled: true
    replicacheckinterval: %s
`
	tt := []parameterTest{
		{name: "default", want: defaultDLBReplicaCheckInterval},
		{name: "custom", value: "2m", want: 2 * time.Minute},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.LoadBalancing.ReplicaCheckInterval)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_LOADBALANCING_REPLICACHECKINTERVAL", tt, validator)
}

func TestParseReportingSentry_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
reporting:
  sentry:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Reporting.Sentry.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REPORTING_SENTRY_ENABLED", tt, validator)
}

func TestParseReportingSentry_DSN(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
reporting:
  sentry:
    dsn: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "https://examplePublicKey@o0.ingest.sentry.io/0",
			want:  "https://examplePublicKey@o0.ingest.sentry.io/0",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Reporting.Sentry.DSN)
	}

	testParameter(t, yml, "REGISTRY_REPORTING_SENTRY_DSN", tt, validator)
}

func TestParseReportingSentry_Environment(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
reporting:
  sentry:
    environment: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "development",
			want:  "development",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Reporting.Sentry.Environment)
	}

	testParameter(t, yml, "REGISTRY_REPORTING_SENTRY_ENVIRONMENT", tt, validator)
}

func checkStructs(t require.TestingT, rt reflect.Type, structsChecked map[string]struct{}) {
	for rt.Kind() == reflect.Ptr || rt.Kind() == reflect.Map || rt.Kind() == reflect.Slice {
		rt = rt.Elem()
	}

	if rt.Kind() != reflect.Struct {
		return
	}
	if _, present := structsChecked[rt.String()]; present {
		// Already checked this type
		return
	}

	structsChecked[rt.String()] = struct{}{}

	byUpperCase := make(map[string]int)
	for i := 0; i < rt.NumField(); i++ {
		sf := rt.Field(i)

		upper := strings.ToUpper(sf.Name)
		require.NotContainsf(t, byUpperCase, upper, "field name collision in configuration object: %s", sf.Name)
		byUpperCase[upper] = i

		checkStructs(t, sf.Type, structsChecked)
	}
}

// TestValidateConfigStruct makes sure that the config struct has no members
// with yaml tags that would be ambiguous to the environment variable parser.
func (s *ConfigSuite) TestValidateConfigStruct() {
	structsChecked := make(map[string]struct{})
	checkStructs(s.T(), reflect.TypeOf(Configuration{}), structsChecked)
}

func copyConfig(config Configuration) *Configuration {
	configCopy := new(Configuration)

	configCopy.Version = MajorMinorVersion(config.Version.Major(), config.Version.Minor())
	configCopy.Loglevel = config.Loglevel
	configCopy.Log = config.Log
	configCopy.Log.Fields = make(map[string]any, len(config.Log.Fields))
	for k, v := range config.Log.Fields {
		configCopy.Log.Fields[k] = v
	}

	configCopy.Storage = Storage{config.Storage.Type(): Parameters{}}
	for k, v := range config.Storage.Parameters() {
		configCopy.Storage.setParameter(k, v)
	}

	configCopy.Database = config.Database

	configCopy.Reporting = Reporting{
		Sentry: SentryReporting{config.Reporting.Sentry.Enabled, config.Reporting.Sentry.DSN, config.Reporting.Sentry.Environment},
	}

	configCopy.Auth = Auth{config.Auth.Type(): Parameters{}}
	for k, v := range config.Auth.Parameters() {
		configCopy.Auth.setParameter(k, v)
	}

	configCopy.Notifications = Notifications{Endpoints: make([]Endpoint, 0)}
	configCopy.Notifications.Endpoints = append(configCopy.Notifications.Endpoints, config.Notifications.Endpoints...)
	configCopy.Notifications.FanoutTimeout = config.Notifications.FanoutTimeout

	configCopy.HTTP.Headers = make(http.Header)
	for k, v := range config.HTTP.Headers {
		configCopy.HTTP.Headers[k] = v
	}

	configCopy.HTTP.TLS.CipherSuites = make([]string, len(config.HTTP.TLS.CipherSuites))
	copy(configCopy.HTTP.TLS.CipherSuites, config.HTTP.TLS.CipherSuites)

	configCopy.RateLimiter = RateLimiter{Enabled: config.RateLimiter.Enabled, Limiters: make([]Limiter, len(config.RateLimiter.Limiters))}
	copy(configCopy.RateLimiter.Limiters, config.RateLimiter.Limiters)

	return configCopy
}

func TestParseValidation_Manifests_PayloadSizeLimit(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
validation:
  manifests:
        payloadsizelimit: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "default",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Validation.Manifests.PayloadSizeLimit)
	}

	testParameter(t, yml, "REGISTRY_VALIDATION_MANIFESTS_PAYLOADSIZELIMIT", tt, validator)
}

func TestParseRedisCache_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.Cache.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_ENABLED", tt, validator)
}

func TestParseRedisCache_TLS_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    tls:
      enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.Cache.TLS.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_TLS_ENABLED", tt, validator)
}

func TestParseRedisCache_TLS_Insecure(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    tls:
      insecure: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.Cache.TLS.Insecure))
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_TLS_INSECURE", tt, validator)
}

func TestParseRedisCache_Addr(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    addr: %s
`
	tt := []parameterTest{
		{
			name:  "single",
			value: "0.0.0.0:6379",
			want:  "0.0.0.0:6379",
		},
		{
			name:  "multiple",
			value: "0.0.0.0:16379,0.0.0.0:26379",
			want:  "0.0.0.0:16379,0.0.0.0:26379",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.Addr)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_ADDR", tt, validator)
}

func TestParseRedisCache_MainName(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    mainname: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "myredismainserver",
			want:  "myredismainserver",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.MainName)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_MAINNAME", tt, validator)
}

func TestParseRedisCache_SentinelUsername(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    mainname: default
    sentinelusername: %s
    sentinelpassword: somepass
`
	tt := []parameterTest{
		{
			name:  "default",
			value: "myuser",
			want:  "myuser",
		},
		{
			name: "empty",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.SentinelUsername)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_SENTINELUSERNAME", tt, validator)
}

func TestParseRedisCache_SentinelPassword(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    mainname: default
    sentinelpassword: %s
`
	tt := []parameterTest{
		{
			name:  "default",
			value: "mypassword",
			want:  "mypassword",
		},
		{
			name: "empty",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.SentinelPassword)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_SENTINELPASSWORD", tt, validator)
}

func TestParseRedisCache_Pool_MaxOpen(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    pool:
      size: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "empty",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.Pool.Size)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_POOL_SIZE", tt, validator)
}

func TestParseRedisCache_Pool_MaxLifeTime(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    pool:
      maxlifetime: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1h",
			want:  1 * time.Hour,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.Pool.MaxLifetime)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_POOL_MAXLIFETIME", tt, validator)
}

func TestParseRedisCache_Pool_IdleTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  cache:
    enabled: true
    pool:
      idletimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.Cache.Pool.IdleTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_CACHE_POOL_IDLETIMEOUT", tt, validator)
}

func TestParseRedisRateLimiter_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.RateLimiter.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_ENABLED", tt, validator)
}

func TestParseRedisRateLimiter_TLS_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    tls:
      enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.RateLimiter.TLS.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_TLS_ENABLED", tt, validator)
}

func TestParseRedisRateLimiter_TLS_Insecure(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    tls:
      insecure: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.RateLimiter.TLS.Insecure))
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_TLS_INSECURE", tt, validator)
}

func TestParseRedisRateLimiter_Addr(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    addr: %s
`
	tt := []parameterTest{
		{
			name:  "single",
			value: "0.0.0.0:6379",
			want:  "0.0.0.0:6379",
		},
		{
			name:  "multiple",
			value: "0.0.0.0:16379,0.0.0.0:26379",
			want:  "0.0.0.0:16379,0.0.0.0:26379",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.RateLimiter.Addr)
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_ADDR", tt, validator)
}

func TestParseRedisRateLimiter_MainName(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    mainname: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "myredismainserver",
			want:  "myredismainserver",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.RateLimiter.MainName)
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_MAINNAME", tt, validator)
}

func TestParseRedisRateLimiter_Pool_MaxOpen(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    pool:
      size: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "empty",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.RateLimiter.Pool.Size)
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_POOL_SIZE", tt, validator)
}

func TestParseRedisRateLimiter_Pool_MaxLifeTime(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    pool:
      maxlifetime: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1h",
			want:  1 * time.Hour,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.RateLimiter.Pool.MaxLifetime)
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_POOL_MAXLIFETIME", tt, validator)
}

func TestParseRedisRateLimiter_Pool_IdleTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  ratelimiter:
    enabled: true
    pool:
      idletimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.RateLimiter.Pool.IdleTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_POOL_IDLETIMEOUT", tt, validator)
}

func TestParseRedisLoadBalancing_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.LoadBalancing.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_ENABLED", tt, validator)
}

func TestParseRedisLoadBalancing_Addr(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    addr: %s
`
	tt := []parameterTest{
		{
			name:  "single",
			value: "0.0.0.0:6379",
			want:  "0.0.0.0:6379",
		},
		{
			name:  "multiple",
			value: "0.0.0.0:16379,0.0.0.0:26379",
			want:  "0.0.0.0:16379,0.0.0.0:26379",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.Addr)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_ADDR", tt, validator)
}

func TestParseRedisLoadBalancing_MainName(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    mainname: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "myredismainserver",
			want:  "myredismainserver",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.MainName)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_MAINNAME", tt, validator)
}

func TestParseRedisLoadBalancing_Username(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    username: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "myuser",
			want:  "myuser",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.Username)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_USERNAME", tt, validator)
}

func TestParseRedisLoadBalancing_Password(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    password: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "mysecret",
			want:  "mysecret",
		},
		{
			name: "default",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.Password)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_PASSWORD", tt, validator)
}

func TestParseRedisLoadBalancing_DB(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    db: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1",
			want:  1,
		},
		{
			name: "default",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.DB)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_DB", tt, validator)
}

func TestParseRedisLoadBalancing_DialTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    dialtimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.DialTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_DIALTIMEOUT", tt, validator)
}

func TestParseRedisLoadBalancing_ReadTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    readtimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.ReadTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_READTIMEOUT", tt, validator)
}

func TestParseRedisLoadBalancing_WriteTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    writetimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.WriteTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_WRITETIMEOUT", tt, validator)
}

func TestParseRedisLoadBalancing_SentinelUsername(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    mainname: default
    sentinelusername: %s
    sentinelpassword: somepass
`
	tt := []parameterTest{
		{
			name:  "default",
			value: "myuser",
			want:  "myuser",
		},
		{
			name: "empty",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.SentinelUsername)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_SENTINELUSERNAME", tt, validator)
}

func TestParseRedisLoadBalancing_SentinelPassword(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    mainname: default
    sentinelpassword: %s
`
	tt := []parameterTest{
		{
			name:  "default",
			value: "mypassword",
			want:  "mypassword",
		},
		{
			name: "empty",
			want: "",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.SentinelPassword)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_SENTINELPASSWORD", tt, validator)
}

func TestParseRedisLoadBalancing_TLS_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    tls:
      enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.LoadBalancing.TLS.Enabled))
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_TLS_ENABLED", tt, validator)
}

func TestParseRedisLoadBalancing_TLS_Insecure(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    tls:
      insecure: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Redis.LoadBalancing.TLS.Insecure))
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_TLS_INSECURE", tt, validator)
}

func TestParseRedisLoadBalancing_Pool_Size(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    pool:
      size: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "10",
			want:  10,
		},
		{
			name: "empty",
			want: 0,
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.Pool.Size)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_POOL_SIZE", tt, validator)
}

func TestParseRedisLoadBalancing_Pool_MaxLifeTime(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    pool:
      maxlifetime: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1h",
			want:  1 * time.Hour,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.Pool.MaxLifetime)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_POOL_MAXLIFETIME", tt, validator)
}

func TestParseRedisLoadBalancing_Pool_IdleTimeout(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
redis:
  loadbalancing:
    enabled: true
    pool:
      idletimeout: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "300s",
			want:  300 * time.Second,
		},
		{
			name: "empty",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Redis.LoadBalancing.Pool.IdleTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_LOADBALANCING_POOL_IDLETIMEOUT", tt, validator)
}

// TestParseBBMConfig_EnabledDefaults validates that environment variables properly override backgroundmigrations parameters
func TestParseBBMConfigEnabledDefaults(t *testing.T) {
	tests := []struct {
		name     string
		yml      string
		expected BackgroundMigrations
	}{
		{
			name: "Enabled with defaults",
			expected: BackgroundMigrations{
				Enabled: true,
				// expected default job interval when backgroundmigrations is enabled and job interval is not provided
				JobInterval: defaultBackgroundMigrationsJobInterval,
			},
			yml: `
version: 0.1
storage: inmemory
database:
  backgroundmigrations:
    enabled: true
`,
		},
		{
			name:     "Disabled",
			expected: BackgroundMigrations{},
			yml: `
version: 0.1
storage: inmemory
# backgroundmigrations section omitted, defaults to disabled
`,
		},
		{
			name: "Custom Config",
			expected: BackgroundMigrations{
				Enabled:     true,
				JobInterval: 5 * time.Second, // Custom job interval specified
			},
			yml: `
version: 0.1
storage: inmemory
database:
  backgroundmigrations:
    enabled: true
    jobinterval: 5s
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config, err := Parse(bytes.NewReader([]byte(test.yml)))
			require.NoError(t, err)
			require.Equal(t, test.expected, config.Database.BackgroundMigrations)
		})
	}
}

func TestParseRateLimiter_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.RateLimiter.Enabled))
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_ENABLED", tt, validator)
}

func TestParseRateLimiter_Limiters_Name(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "%s"
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, got.RateLimiter.Limiters[0].Name)
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_NAME", tt, validator)
}

func TestParseRateLimiter_Limiters_Name_MultipleLimiters(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "first"
    - name: "%s"
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 2)
		require.Equal(t, want, got.RateLimiter.Limiters[1].Name)
	}

	// Note that the array element is 1 and not 0
	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_1_NAME", tt, validator)
}

func TestParseRateLimiter_Limiters_Description(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      description: "%s"
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, got.RateLimiter.Limiters[0].Description)
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_DESCRIPTION", tt, validator)
}

func TestParseRateLimiter_Limiters_LogOnly(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      description: "description"
      log_only: %s

`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, strconv.FormatBool(got.RateLimiter.Limiters[0].LogOnly))
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_LOGONLY", tt, validator)
}

func TestParseRateLimiter_Limiters_MatchType(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      description: "description"
      log_only: false
      match:
        type: "%s"
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, got.RateLimiter.Limiters[0].Match.Type)
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_MATCH_TYPE", tt, validator)
}

func TestParseRateLimiter_Limiters_Precedence(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      description: "description"
      log_only: false
      precedence: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "1",
			want:  "1",
		},
		{
			name: "default",
			want: "0",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, strconv.Itoa(int(got.RateLimiter.Limiters[0].Precedence)))
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_PRECEDENCE", tt, validator)
}

func TestParseRateLimiter_Limiters_Limit_Rate(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      description: "description"
      limit:
        rate: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "100",
			want:  "100",
		},
		{
			name: "default",
			want: "0",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, strconv.Itoa(int(got.RateLimiter.Limiters[0].Limit.Rate)))
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_LIMIT_RATE", tt, validator)
}

func TestParseRateLimiter_Limiters_Limit_Burst(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      description: "description"
      limit:
        burst: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "100",
			want:  "100",
		},
		{
			name: "default",
			want: "0",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, strconv.Itoa(int(got.RateLimiter.Limiters[0].Limit.Burst)))
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_LIMIT_BURST", tt, validator)
}

func TestParseRateLimiter_Limiters_Limit_Period(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      limit:
        period: %s
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, got.RateLimiter.Limiters[0].Limit.Period)
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_LIMIT_PERIOD", tt, validator)
}

func TestParseRateLimiter_Limiters_Action_WarnThreshold(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      action:
        warn_threshold: %s
`
	tt := []parameterTest{
		{
			name:  "sample",
			value: "0.9",
			want:  "0.9",
		},
		{
			name: "default",
			want: "0.0",
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, fmt.Sprintf("%.1f", got.RateLimiter.Limiters[0].Action.WarnThreshold))
	}
	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_ACTION_WARNTHRESHOLD", tt, validator)
}

func TestParseRateLimiter_Limiters_Action_WarnAction(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      action:
        warn_action: %s
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, got.RateLimiter.Limiters[0].Action.WarnAction)
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_ACTION_WARNACTION", tt, validator)
}

func TestParseRateLimiter_Limiters_Action_HardAction(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
rate_limiter:
  enabled: true
  limiters:
    - name: "test rate limiter"
      action:
        hard_action: %s
`
	tt := stringParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Len(t, got.RateLimiter.Limiters, 1)
		require.Equal(t, want, got.RateLimiter.Limiters[0].Action.HardAction)
	}

	testParameter(t, yml, "REGISTRY_RATELIMITER_LIMITERS_0_ACTION_HARDACTION", tt, validator)
}

func TestParseDatabaseMetrics_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  metrics:
    enabled: %s
`
	tt := boolParameterTests()

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, strconv.FormatBool(got.Database.Metrics.Enabled))
	}

	testParameter(t, yml, "REGISTRY_DATABASE_METRICS_ENABLED", tt, validator)
}

// TestParseDatabaseMetrics_Interval tests parsing of collection interval.
// Note: Default value (10s) is defined in the metrics package.
func TestParseDatabaseMetrics_Interval(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  metrics:
    interval: %s
`
	tt := []parameterTest{
		{
			name:  "0s",
			value: "0s",
			want:  time.Duration(0),
		},
		{
			name:  "0",
			value: "0",
			want:  time.Duration(0),
		},
		{
			name:  "5s",
			value: "5s",
			want:  5 * time.Second,
		},
		{
			name:  "10s",
			value: "10s",
			want:  10 * time.Second,
		},
		{
			name:  "1m",
			value: "1m",
			want:  1 * time.Minute,
		},
		{
			name: "default",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.Metrics.Interval)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_METRICS_INTERVAL", tt, validator)
}

// TestParseDatabaseMetrics_LeaseDuration tests parsing of lease duration.
// Note: Default value (10s) is defined in the metrics package.
func TestParseDatabaseMetrics_LeaseDuration(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
database:
  metrics:
    leaseduration: %s
`
	tt := []parameterTest{
		{
			name:  "0s",
			value: "0s",
			want:  time.Duration(0),
		},
		{
			name:  "0",
			value: "0",
			want:  time.Duration(0),
		},
		{
			name:  "15s",
			value: "15s",
			want:  15 * time.Second,
		},
		{
			name:  "30s",
			value: "30s",
			want:  30 * time.Second,
		},
		{
			name:  "1m",
			value: "1m",
			want:  1 * time.Minute,
		},
		{
			name: "default",
			want: time.Duration(0),
		},
	}

	validator := func(t *testing.T, want any, got *Configuration) {
		require.Equal(t, want, got.Database.Metrics.LeaseDuration)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_METRICS_LEASEDURATION", tt, validator)
}
