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

	. "gopkg.in/check.v1"
	"gopkg.in/yaml.v2"
)

// Hook up gocheck into the "go test" runner
func Test(t *testing.T) { TestingT(t) }

// configStruct is a canonical example configuration, which should map to configYamlV0_1
var configStruct = Configuration{
	Version: "0.1",
	Log: struct {
		AccessLog struct {
			Disabled  bool            `yaml:"disabled,omitempty"`
			Formatter accessLogFormat `yaml:"formatter,omitempty"`
		} `yaml:"accesslog,omitempty"`
		Level     Loglevel               `yaml:"level,omitempty"`
		Formatter logFormat              `yaml:"formatter,omitempty"`
		Output    logOutput              `yaml:"output,omitempty"`
		Fields    map[string]interface{} `yaml:"fields,omitempty"`
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
		Fields:    map[string]interface{}{"environment": "test"},
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
			ClientCAs: []string{"/path/to/ca.pem"},
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
auth:
  silly:
    realm: silly
    service: silly
notifications:
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
reporting:
  sentry:
    enabled: true
    dsn: https://foo@12345.ingest.sentry.io/876542
http:
  clientcas:
    - /path/to/ca.pem
  headers:
    X-Content-Type-Options: [nosniff]
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
http:
  headers:
    X-Content-Type-Options: [nosniff]
`

type ConfigSuite struct {
	expectedConfig *Configuration
}

var _ = Suite(new(ConfigSuite))

func (suite *ConfigSuite) SetUpTest(c *C) {
	os.Clearenv()
	suite.expectedConfig = copyConfig(configStruct)
}

// TestMarshalRoundtrip validates that configStruct can be marshaled and
// unmarshaled without changing any parameters
func (suite *ConfigSuite) TestMarshalRoundtrip(c *C) {
	configBytes, err := yaml.Marshal(suite.expectedConfig)
	c.Assert(err, IsNil)
	config, err := Parse(bytes.NewReader(configBytes))
	c.Log(string(configBytes))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseSimple validates that configYamlV0_1 can be parsed into a struct
// matching configStruct
func (suite *ConfigSuite) TestParseSimple(c *C) {
	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseInmemory validates that configuration yaml with storage provided as
// a string can be parsed into a Configuration struct with no storage parameters
func (suite *ConfigSuite) TestParseInmemory(c *C) {
	suite.expectedConfig.Storage = Storage{"inmemory": Parameters{}}
	suite.expectedConfig.Database = Database{}
	suite.expectedConfig.Reporting = Reporting{}
	suite.expectedConfig.Log.Fields = nil

	config, err := Parse(bytes.NewReader([]byte(inmemoryConfigYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseIncomplete validates that an incomplete yaml configuration cannot
// be parsed without providing environment variables to fill in the missing
// components.
func (suite *ConfigSuite) TestParseIncomplete(c *C) {
	incompleteConfigYaml := "version: 0.1"
	_, err := Parse(bytes.NewReader([]byte(incompleteConfigYaml)))
	c.Assert(err, NotNil)

	suite.expectedConfig.Log.Fields = nil
	suite.expectedConfig.Storage = Storage{"filesystem": Parameters{"rootdirectory": "/tmp/testroot"}}
	suite.expectedConfig.Database = Database{}
	suite.expectedConfig.Auth = Auth{"silly": Parameters{"realm": "silly"}}
	suite.expectedConfig.Reporting = Reporting{}
	suite.expectedConfig.Notifications = Notifications{}
	suite.expectedConfig.HTTP.Headers = nil

	// Note: this also tests that REGISTRY_STORAGE and
	// REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY can be used together
	os.Setenv("REGISTRY_STORAGE", "filesystem")
	os.Setenv("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "/tmp/testroot")
	os.Setenv("REGISTRY_AUTH", "silly")
	os.Setenv("REGISTRY_AUTH_SILLY_REALM", "silly")

	config, err := Parse(bytes.NewReader([]byte(incompleteConfigYaml)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseWithSameEnvStorage validates that providing environment variables
// that match the given storage type will only include environment-defined
// parameters and remove yaml-defined parameters
func (suite *ConfigSuite) TestParseWithSameEnvStorage(c *C) {
	suite.expectedConfig.Storage = Storage{"s3": Parameters{"region": "us-east-1"}}

	os.Setenv("REGISTRY_STORAGE", "s3")
	os.Setenv("REGISTRY_STORAGE_S3_REGION", "us-east-1")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseWithDifferentEnvStorageParams validates that providing environment variables that change
// and add to the given storage parameters will change and add parameters to the parsed
// Configuration struct
func (suite *ConfigSuite) TestParseWithDifferentEnvStorageParams(c *C) {
	suite.expectedConfig.Storage.setParameter("region", "us-west-1")
	suite.expectedConfig.Storage.setParameter("secure", true)
	suite.expectedConfig.Storage.setParameter("newparam", "some Value")

	os.Setenv("REGISTRY_STORAGE_S3_REGION", "us-west-1")
	os.Setenv("REGISTRY_STORAGE_S3_SECURE", "true")
	os.Setenv("REGISTRY_STORAGE_S3_NEWPARAM", "some Value")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseWithDifferentEnvStorageType validates that providing an environment variable that
// changes the storage type will be reflected in the parsed Configuration struct
func (suite *ConfigSuite) TestParseWithDifferentEnvStorageType(c *C) {
	suite.expectedConfig.Storage = Storage{"inmemory": Parameters{}}

	os.Setenv("REGISTRY_STORAGE", "inmemory")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseWithDifferentEnvStorageTypeAndParams validates that providing an environment variable
// that changes the storage type will be reflected in the parsed Configuration struct and that
// environment storage parameters will also be included
func (suite *ConfigSuite) TestParseWithDifferentEnvStorageTypeAndParams(c *C) {
	suite.expectedConfig.Storage = Storage{"filesystem": Parameters{}}
	suite.expectedConfig.Storage.setParameter("rootdirectory", "/tmp/testroot")

	os.Setenv("REGISTRY_STORAGE", "filesystem")
	os.Setenv("REGISTRY_STORAGE_FILESYSTEM_ROOTDIRECTORY", "/tmp/testroot")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseWithSameEnvLoglevel validates that providing an environment variable defining the log
// level to the same as the one provided in the yaml will not change the parsed Configuration struct
func (suite *ConfigSuite) TestParseWithSameEnvLoglevel(c *C) {
	os.Setenv("REGISTRY_LOGLEVEL", "info")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseWithDifferentEnvLoglevel validates that providing an environment variable defining the
// log level will override the value provided in the yaml document
func (suite *ConfigSuite) TestParseWithDifferentEnvLoglevel(c *C) {
	suite.expectedConfig.Log.Level = "error"

	os.Setenv("REGISTRY_LOG_LEVEL", "error")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseInvalidLoglevel validates that the parser will fail to parse a
// configuration if the loglevel is malformed
func (suite *ConfigSuite) TestParseInvalidLoglevel(c *C) {
	invalidConfigYaml := "version: 0.1\nloglevel: derp\nstorage: inmemory"
	_, err := Parse(bytes.NewReader([]byte(invalidConfigYaml)))
	c.Assert(err, NotNil)

	os.Setenv("REGISTRY_LOGLEVEL", "derp")

	_, err = Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, NotNil)
}

// TestParseWithoutStorageValidation validates that the parser will not fail to parse a configuration if a storage
// driver was not set but WithoutStorageValidation was passed as an option.
func (suite *ConfigSuite) TestParseWithoutStorageValidation(c *C) {
	configYaml := "version: 0.1"

	_, err := Parse(bytes.NewReader([]byte(configYaml)))
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "no storage configuration provided")

	_, err = Parse(bytes.NewReader([]byte(configYaml)), WithoutStorageValidation())
	c.Assert(err, IsNil)
}

type parameterTest struct {
	name    string
	value   string
	want    interface{}
	wantErr bool
	err     string
}

type parameterValidator func(t *testing.T, want interface{}, got *Configuration)

func testParameter(t *testing.T, yml string, envVar string, tests []parameterTest, fn parameterValidator) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Log.AccessLog.Formatter.String())
	}

	testParameter(t, yml, "REGISTRY_LOG_ACCESSLOG_FORMATTER", tt, validator)
}

// TestParseWithDifferentEnvDatabase validates that environment variables properly override database parameters
func (suite *ConfigSuite) TestParseWithDifferentEnvDatabase(c *C) {
	expected := Database{
		Enabled:  true,
		Host:     "127.0.0.1",
		Port:     1234,
		User:     "user",
		Password: "passwd",
		DBName:   "foo",
		SSLMode:  "allow",
		Primary:  "primary.record.fqdn",
	}
	suite.expectedConfig.Database = expected

	os.Setenv("REGISTRY_DATABASE_DISABLE", strconv.FormatBool(expected.Enabled))
	os.Setenv("REGISTRY_DATABASE_HOST", expected.Host)
	os.Setenv("REGISTRY_DATABASE_PORT", strconv.Itoa(expected.Port))
	os.Setenv("REGISTRY_DATABASE_USER", expected.User)
	os.Setenv("REGISTRY_DATABASE_PASSWORD", expected.Password)
	os.Setenv("REGISTRY_DATABASE_DBNAME", expected.DBName)
	os.Setenv("REGISTRY_DATABASE_SSLMODE", expected.SSLMode)

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseInvalidVersion validates that the parser will fail to parse a newer configuration
// version than the CurrentVersion
func (suite *ConfigSuite) TestParseInvalidVersion(c *C) {
	suite.expectedConfig.Version = MajorMinorVersion(CurrentVersion.Major(), CurrentVersion.Minor()+1)
	configBytes, err := yaml.Marshal(suite.expectedConfig)
	c.Assert(err, IsNil)
	_, err = Parse(bytes.NewReader(configBytes))
	c.Assert(err, NotNil)
}

// TestParseExtraneousVars validates that environment variables referring to
// nonexistent variables don't cause side effects.
func (suite *ConfigSuite) TestParseExtraneousVars(c *C) {
	suite.expectedConfig.Reporting.Sentry.Environment = "test"

	// A valid environment variable
	os.Setenv("REGISTRY_REPORTING_SENTRY_ENVIRONMENT", "test")

	// Environment variables which shouldn't set config items
	os.Setenv("REGISTRY_DUCKS", "quack")
	os.Setenv("REGISTRY_REPORTING_ASDF", "ghjk")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseEnvVarImplicitMaps validates that environment variables can set
// values in maps that don't already exist.
func (suite *ConfigSuite) TestParseEnvVarImplicitMaps(c *C) {
	readonly := make(map[string]interface{})
	readonly["enabled"] = true

	maintenance := make(map[string]interface{})
	maintenance["readonly"] = readonly

	suite.expectedConfig.Storage["maintenance"] = maintenance

	os.Setenv("REGISTRY_STORAGE_MAINTENANCE_READONLY_ENABLED", "true")

	config, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
	c.Assert(config, DeepEquals, suite.expectedConfig)
}

// TestParseEnvWrongTypeMap validates that incorrectly attempting to unmarshal a
// string over existing map fails.
func (suite *ConfigSuite) TestParseEnvWrongTypeMap(c *C) {
	os.Setenv("REGISTRY_STORAGE_S3", "somestring")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, NotNil)
}

// TestParseEnvWrongTypeStruct validates that incorrectly attempting to
// unmarshal a string into a struct fails.
func (suite *ConfigSuite) TestParseEnvWrongTypeStruct(c *C) {
	os.Setenv("REGISTRY_STORAGE_LOG", "somestring")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, NotNil)
}

// TestParseEnvWrongTypeSlice validates that incorrectly attempting to
// unmarshal a string into a slice fails.
func (suite *ConfigSuite) TestParseEnvWrongTypeSlice(c *C) {
	os.Setenv("REGISTRY_HTTP_TLS_CLIENTCAS", "somestring")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, NotNil)
}

// TestParseEnvMany tests several environment variable overrides.
// The result is not checked - the goal of this test is to detect panics
// from misuse of reflection.
func (suite *ConfigSuite) TestParseEnvMany(c *C) {
	os.Setenv("REGISTRY_VERSION", "0.1")
	os.Setenv("REGISTRY_LOG_LEVEL", "debug")
	os.Setenv("REGISTRY_LOG_FORMATTER", "json")
	os.Setenv("REGISTRY_LOG_FIELDS", "abc: xyz")
	os.Setenv("REGISTRY_LOGLEVEL", "debug")
	os.Setenv("REGISTRY_STORAGE", "s3")
	os.Setenv("REGISTRY_AUTH_PARAMS", "param1: value1")
	os.Setenv("REGISTRY_AUTH_PARAMS_VALUE2", "value2")
	os.Setenv("REGISTRY_AUTH_PARAMS_VALUE2", "value2")

	_, err := Parse(bytes.NewReader([]byte(configYamlV0_1)))
	c.Assert(err, IsNil)
}

func boolParameterTests(defaultValue bool) []parameterTest {
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
			want: strconv.FormatBool(defaultValue),
		},
	}
}

func stringParameterTests(defaultValue string) []parameterTest {
	return []parameterTest{
		{
			name:  "string",
			value: "string",
			want:  "string",
		},
		{
			name: "default",
			want: defaultValue,
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := stringParameterTests("")

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := stringParameterTests("")

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := stringParameterTests("")

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
			want:  []string{},
		},
		{
			name: "default",
			want: nil,
		},
	}

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Database.Pool.MaxLifetime)
	}

	testParameter(t, yml, "REGISTRY_DATABASE_POOL_MAXLIFETIME", tt, validator)
}

func TestParseReportingSentry_Enabled(t *testing.T) {
	yml := `
version: 0.1
storage: inmemory
reporting:
  sentry:
    enabled: %s
`
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Reporting.Sentry.Environment)
	}

	testParameter(t, yml, "REGISTRY_REPORTING_SENTRY_ENVIRONMENT", tt, validator)
}

func checkStructs(c *C, t reflect.Type, structsChecked map[string]struct{}) {
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Map || t.Kind() == reflect.Slice {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return
	}
	if _, present := structsChecked[t.String()]; present {
		// Already checked this type
		return
	}

	structsChecked[t.String()] = struct{}{}

	byUpperCase := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)

		// Check that the yaml tag does not contain an _.
		yamlTag := sf.Tag.Get("yaml")
		if strings.Contains(yamlTag, "_") {
			c.Fatalf("yaml field name includes _ character: %s", yamlTag)
		}
		upper := strings.ToUpper(sf.Name)
		if _, present := byUpperCase[upper]; present {
			c.Fatalf("field name collision in configuration object: %s", sf.Name)
		}
		byUpperCase[upper] = i

		checkStructs(c, sf.Type, structsChecked)
	}
}

// TestValidateConfigStruct makes sure that the config struct has no members
// with yaml tags that would be ambiguous to the environment variable parser.
func (suite *ConfigSuite) TestValidateConfigStruct(c *C) {
	structsChecked := make(map[string]struct{})
	checkStructs(c, reflect.TypeOf(Configuration{}), structsChecked)
}

func copyConfig(config Configuration) *Configuration {
	configCopy := new(Configuration)

	configCopy.Version = MajorMinorVersion(config.Version.Major(), config.Version.Minor())
	configCopy.Loglevel = config.Loglevel
	configCopy.Log = config.Log
	configCopy.Log.Fields = make(map[string]interface{}, len(config.Log.Fields))
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

	configCopy.Notifications = Notifications{Endpoints: []Endpoint{}}
	for _, v := range config.Notifications.Endpoints {
		configCopy.Notifications.Endpoints = append(configCopy.Notifications.Endpoints, v)
	}

	configCopy.HTTP.Headers = make(http.Header)
	for k, v := range config.HTTP.Headers {
		configCopy.HTTP.Headers[k] = v
	}

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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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
	tt := boolParameterTests(false)

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
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

	validator := func(t *testing.T, want interface{}, got *Configuration) {
		require.Equal(t, want, got.Redis.RateLimiter.Pool.IdleTimeout)
	}

	testParameter(t, yml, "REGISTRY_REDIS_RATELIMITER_POOL_IDLETIMEOUT", tt, validator)
}
