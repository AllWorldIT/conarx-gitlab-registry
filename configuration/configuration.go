package configuration

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"
)

// Configuration is a versioned registry configuration, intended to be provided by a yaml file, and
// optionally modified by environment variables.
//
// Note that yaml field names should never include _ characters, since this is the separator used
// in environment variable names.
type Configuration struct {
	// Version is the version which defines the format of the rest of the configuration
	Version Version `yaml:"version"`

	// Log supports setting various parameters related to the logging
	// subsystem.
	Log struct {
		// AccessLog configures access logging.
		AccessLog struct {
			// Disabled disables access logging.
			Disabled bool `yaml:"disabled,omitempty"`

			// Formatter overrides the default formatter with another. Options include "text" and "json". The default
			// is "json".
			Formatter accessLogFormat `yaml:"formatter,omitempty"`
		} `yaml:"accesslog,omitempty"`

		// Level is the granularity at which registry operations are logged.
		// Options include "error", "warn", "info", "debug" and "trace". The
		// default is "info".
		Level Loglevel `yaml:"level,omitempty"`

		// Formatter sets the format of logging output. Options include "text" and "json". The default is "json".
		Formatter logFormat `yaml:"formatter,omitempty"`

		// Output sets the output destination. Options include "stderr" and
		// "stdout". The default is "stdout".
		Output logOutput `yaml:"output,omitempty"`

		// Fields allows users to specify static string fields to include in
		// the logger context.
		Fields map[string]interface{} `yaml:"fields,omitempty"`
	}

	// Loglevel is the level at which registry operations are logged.
	//
	// Deprecated: Use Log.Level instead.
	Loglevel Loglevel `yaml:"loglevel,omitempty"`

	// Storage is the configuration for the registry's storage driver
	Storage Storage `yaml:"storage"`

	// Database is the configuration for the registry's metadata database
	Database Database `yaml:"database"`

	// Auth allows configuration of various authorization methods that may be
	// used to gate requests.
	Auth Auth `yaml:"auth,omitempty"`

	// Middleware lists all middlewares to be used by the registry.
	Middleware map[string][]Middleware `yaml:"middleware,omitempty"`

	// Reporting is the configuration for error reporting
	Reporting Reporting `yaml:"reporting,omitempty"`

	// Profiling configures external profiling services.
	Profiling Profiling `yaml:"profiling,omitempty"`

	// HTTP contains configuration parameters for the registry's http
	// interface.
	HTTP struct {
		// Addr specifies the bind address for the registry instance.
		Addr string `yaml:"addr,omitempty"`

		// Net specifies the net portion of the bind address. A default empty value means tcp.
		Net string `yaml:"net,omitempty"`

		// Host specifies an externally-reachable address for the registry, as a fully
		// qualified URL.
		Host string `yaml:"host,omitempty"`

		Prefix string `yaml:"prefix,omitempty"`

		// Secret specifies the secret key which HMAC tokens are created with.
		Secret string `yaml:"secret,omitempty"`

		// RelativeURLs specifies that relative URLs should be returned in
		// Location headers
		RelativeURLs bool `yaml:"relativeurls,omitempty"`

		// Amount of time to wait for connection to drain before shutting down when registry
		// receives a stop signal
		DrainTimeout time.Duration `yaml:"draintimeout,omitempty"`

		// TLS instructs the http server to listen with a TLS configuration.
		// This only support simple tls configuration with a cert and key.
		// Mostly, this is useful for testing situations or simple deployments
		// that require tls. If more complex configurations are required, use
		// a proxy or make a proposal to add support here.
		TLS TLS `yaml:"tls,omitempty"`

		// Headers is a set of headers to include in HTTP responses. A common
		// use case for this would be security headers such as
		// Strict-Transport-Security. The map keys are the header names, and
		// the values are the associated header payloads.
		Headers http.Header `yaml:"headers,omitempty"`

		// Debug configures the http debug interface, if specified. This can
		// include services such as pprof, expvar and other data that should
		// not be exposed externally. Left disabled by default.
		Debug struct {
			// Addr specifies the bind address for the debug server.
			Addr string `yaml:"addr,omitempty"`
			// TLS configuration for the debug server.
			TLS DebugTLS `yaml:"tls,omitempty"`
			// Prometheus configures the Prometheus telemetry endpoint.
			Prometheus struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Path    string `yaml:"path,omitempty"`
			} `yaml:"prometheus,omitempty"`
			// Pprof configures a pprof server, which listens at `/debug/pprof`.
			Pprof struct {
				Enabled bool `yaml:"enabled,omitempty"`
			} `yaml:"pprof,omitempty"`
		} `yaml:"debug,omitempty"`

		// HTTP2 configuration options
		HTTP2 struct {
			// Specifies whether the registry should disallow clients attempting
			// to connect via http2. If set to true, only http/1.1 is supported.
			Disabled bool `yaml:"disabled,omitempty"`
		} `yaml:"http2,omitempty"`
	} `yaml:"http,omitempty"`

	// Notifications specifies configuration about various endpoint to which
	// registry events are dispatched.
	Notifications Notifications `yaml:"notifications,omitempty"`

	// Redis configures the redis instance(s) available to the application. Separate Redis instances for different
	// persistence classes (e.g. caching) can be used.
	Redis Redis `yaml:"redis,omitempty"`

	Health Health `yaml:"health,omitempty"`

	// Validation configures validation options for the registry.
	Validation struct {
		// Enabled enables the other options in this section. This field is
		// deprecated in favor of Disabled.
		Enabled bool `yaml:"enabled,omitempty"`
		// Disabled disables the other options in this section.
		Disabled bool `yaml:"disabled,omitempty"`
		// Manifests configures manifest validation.
		Manifests struct {
			// ReferenceLimit is the maximum number of blobs or manifests that manifests may reference. Set to zero to disable.
			ReferenceLimit int `yaml:"referencelimit,omitempty"`
			// PayloadSizeLimit is the maximum data size in bytes of manifest payloads. Set to zero to disable.
			PayloadSizeLimit int `yaml:"payloadsizelimit,omitempty"`
			// URLs configures validation for URLs in pushed manifests.
			URLs struct {
				// Allow specifies regular expressions (https://godoc.org/regexp/syntax)
				// that URLs in pushed manifests must match.
				Allow []string `yaml:"allow,omitempty"`
				// Deny specifies regular expressions (https://godoc.org/regexp/syntax)
				// that URLs in pushed manifests must not match.
				Deny []string `yaml:"deny,omitempty"`
			} `yaml:"urls,omitempty"`
		} `yaml:"manifests,omitempty"`
	} `yaml:"validation,omitempty"`

	// Policy configures registry policy options.
	Policy struct {
		// Repository configures policies for repositories
		Repository struct {
			// Classes is a list of repository classes which the
			// registry allows content for. This class is matched
			// against the configuration media type inside uploaded
			// manifests. When non-empty, the registry will enforce
			// the class in authorized resources.
			Classes []string `yaml:"classes"`
		} `yaml:"repository,omitempty"`
	} `yaml:"policy,omitempty"`

	GC GC `yaml:"gc,omitempty"`
}

// TLS specifies the settings for the http server to listen with a TLS configuration.
type TLS struct {
	// Certificate specifies the path to an x509 certificate file to
	// be used for TLS.
	Certificate string `yaml:"certificate,omitempty"`

	// Key specifies the path to the x509 key file, which should
	// contain the private portion for the file specified in
	// Certificate.
	Key string `yaml:"key,omitempty"`

	// Specifies the CA certs for client authentication
	// A file may contain multiple CA certificates encoded as PEM
	ClientCAs []string `yaml:"clientcas,omitempty"`

	// Specifies the lowest TLS version allowed
	MinimumTLS string `yaml:"minimumtls,omitempty"`

	// LetsEncrypt is used to configuration setting up TLS through
	// Let's Encrypt instead of manually specifying certificate and
	// key. If a TLS certificate is specified, the Let's Encrypt
	// section will not be used.
	LetsEncrypt struct {
		// CacheFile specifies cache file to use for lets encrypt
		// certificates and keys.
		CacheFile string `yaml:"cachefile,omitempty"`

		// Email is the email to use during Let's Encrypt registration
		Email string `yaml:"email,omitempty"`

		// Hosts specifies the hosts which are allowed to obtain Let's
		// Encrypt certificates.
		Hosts []string `yaml:"hosts,omitempty"`
	} `yaml:"letsencrypt,omitempty"`
}

// DebugTLS specifies the TLS settings for the HTTP Debug server
type DebugTLS struct {
	// Enabled is only used to check if TLS is enabled for the debug monitoring service
	Enabled bool `yaml:"enabled,omitempty"`
	// Certificate specifies the path to an x509 certificate file to
	// be used for TLS.
	Certificate string `yaml:"certificate,omitempty"`

	// Key specifies the path to the x509 key file, which should
	// contain the private portion for the file specified in
	// Certificate.
	Key string `yaml:"key,omitempty"`

	// Specifies the CA certs for client authentication
	// A file may contain multiple CA certificates encoded as PEM
	ClientCAs []string `yaml:"clientcas,omitempty"`

	// Specifies the lowest TLS version allowed
	MinimumTLS string `yaml:"minimumtls,omitempty"`
}

// RedisTLS specifies settings for Redis TLS connections.
type RedisTLS struct {
	// Enabled enables TLS when connecting to the server.
	Enabled bool `yaml:"enabled,omitempty"`
	// Insecure disables server name verification when connecting over TLS.
	Insecure bool `yaml:"insecure,omitempty"`
}

// RedisPool configures the behavior of the redis connection pool.
type RedisPool struct {
	// Size is the maximum number of socket connections. Default is 10 connections.
	Size int `yaml:"size,omitempty"`
	// MaxLifetime is the connection age at which client retires a connection. Default is to not close aged
	// connections.
	MaxLifetime time.Duration `yaml:"maxlifetime,omitempty"`
	// IdleTimeout sets the amount time to wait before closing inactive connections.
	IdleTimeout time.Duration `yaml:"idletimeout,omitempty"`
}

// RedisCache specifies custom settings for a Redis instance to be used for caching features.
type RedisCache struct {
	// Enabled is a simple toggle for the Redis cache. Defaults to false.
	Enabled bool `yaml:"enabled,omitempty"`
	// Addr specifies the redis instance available to the application. For Sentinel, it should be a list of
	// addresses separated by commas.
	Addr string `yaml:"addr,omitempty"`
	// MainName specifies the main server name. Only for Sentinel connections.
	MainName string `yaml:"mainname,omitempty"`
	// Password string to use when making a connection.
	Password string `yaml:"password,omitempty"`
	// DB specifies the database to connect to on the redis instance.
	DB int `yaml:"db,omitempty"`
	// DialTimeout is the timeout for establishing connections.
	DialTimeout time.Duration `yaml:"dialtimeout,omitempty"`
	// ReadTimeout is the timeout for reading data.
	ReadTimeout time.Duration `yaml:"readtimeout,omitempty"`
	// WriteTimeout is the timeout for writing data.
	WriteTimeout time.Duration `yaml:"writetimeout,omitempty"`
	// TLS specifies settings for TLS connections.
	TLS RedisTLS `yaml:"tls,omitempty"`
	// Pool configures the behavior of the redis connection pool.
	Pool RedisPool `yaml:"pool,omitempty"`
}

// Redis configures the redis instance(s) available to the application. Separate Redis instances for different
// persistence classes (e.g. caching) can be used.
type Redis struct {
	// Addr specifies the redis instance available to the application. For Sentinel it should be a list of
	// addresses separated by commas.
	Addr string `yaml:"addr,omitempty"`
	// MainName specifies the main server name. Only for Sentinel connections.
	MainName string `yaml:"mainname,omitempty"`
	// Password string to use when making a connection.
	Password string `yaml:"password,omitempty"`
	// DB specifies the database to connect to on the redis instance.
	DB int `yaml:"db,omitempty"`
	// DialTimeout is the timeout for establishing connections.
	DialTimeout time.Duration `yaml:"dialtimeout,omitempty"`
	// ReadTimeout is the timeout for reading data.
	ReadTimeout time.Duration `yaml:"readtimeout,omitempty"`
	// WriteTimeout is the timeout for writing data.
	WriteTimeout time.Duration `yaml:"writetimeout,omitempty"`
	// TLS specifies settings for TLS connections.
	TLS RedisTLS `yaml:"tls,omitempty"`
	// Pool configures the behavior of the redis connection pool.
	Pool RedisPool `yaml:"pool,omitempty"`
	// Cache specifies custom settings for a Redis instance to be used for caching features.
	Cache RedisCache `yaml:"cache,omitempty"`
}

// GC configures online Garbage Collection.
type GC struct {
	// Disabled disables the online GC workers.
	Disabled bool `yaml:"disabled,omitempty"`
	// NoIdleBackoff disables exponential backoff between worker runs when there was no task to be processed.
	NoIdleBackoff bool `yaml:"noidlebackoff,omitempty"`
	// MaxBackoff is the maximum exponential backoff duration used to sleep between worker runs when an error occurs.
	// Also applied when there are no tasks to be processed unless NoIdleBackoff is `true`. Please note that this is
	// not the absolute maximum, as a randomized jitter factor of up to 33% is always added.
	MaxBackoff time.Duration `yaml:"maxbackoff,omitempty"`
	// TransactionTimeout is the database transaction timeout for each worker run. Each worker starts a database transaction
	// at the start. The worker run is canceled if this timeout is exceeded to avoid stalled or long-running
	// transactions.
	TransactionTimeout time.Duration `yaml:"transactiontimeout,omitempty"`
	// Blobs configures the blob worker.
	Blobs GCBlobs `yaml:"blobs,omitempty"`
	// Manifests configures the manifest worker.
	Manifests GCManifests `yaml:"manifests,omitempty"`
	// ReviewAfter is the minimum amount of time after which the garbage collector should pick up a record for review.
	// -1 means no wait. Defaults to 24h.
	ReviewAfter time.Duration `yaml:"reviewafter,omitempty"`
}

// GCBlobs configures the blob worker.
type GCBlobs struct {
	// Disabled disables the blob worker.
	Disabled bool `yaml:"disabled,omitempty"`
	// Interval is the initial sleep interval between each worker run.
	Interval time.Duration `yaml:"interval,omitempty"`
	// StorageTimeout is the timeout for storage operations. Used to limit the duration of requests to delete
	// dangling blobs on the storage backend.
	StorageTimeout time.Duration `yaml:"storagetimeout,omitempty"`
}

// GCManifests configures the manifest worker.
type GCManifests struct {
	// Disabled disables the manifest worker.
	Disabled bool `yaml:"disabled,omitempty"`
	// Interval is the initial sleep interval between each worker run.
	Interval time.Duration `yaml:"interval,omitempty"`
}

// Profiling configures external profiling services.
type Profiling struct {
	Stackdriver StackdriverProfiler `yaml:"stackdriver,omitempty"`
}

// StackdriverProfiler configures the integration with the Google Stackdriver Profiler.
// See https://pkg.go.dev/cloud.google.com/go/profiler?tab=doc#Config for more details about configuration
// options.
type StackdriverProfiler struct {
	// Enabled can be set to `true` to enable the Stackdriver profiler.
	Enabled bool `yaml:"enabled,omitempty"`
	// Service is the name of the service under which the profiled data will be recorded and exposed. Defaults to the
	// value of the `GAE_SERVICE` environment variable or instance metadata.
	Service string `yaml:"service,omitempty"`
	// ServiceVersion is the version of the service. Defaults to the `GAE_VERSION` environment variable if that is set,
	// or to empty string otherwise.
	ServiceVersion string `yaml:"serviceversion,omitempty"`
	// ProjectID is the project ID. Defaults to the `GOOGLE_CLOUD_PROJECT` environment variable or instance metadata.
	ProjectID string `yaml:"projectid,omitempty"`
	// KeyFile is the path of a private service account key file in JSON format used for Service Account Authentication.
	// Defaults to the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or instance metadata.
	KeyFile string `yaml:"keyfile,omitempty"`
}

// Database is the configuration for the registry's metadata database
type Database struct {
	// Enabled can be used to enable or bypass the metadata database
	Enabled bool `yaml:"enabled"`
	// Host is the database server hostname
	Host string `yaml:"host"`
	// Port is the database server port
	Port int `yaml:"port"`
	// Username is the database username
	User string `yaml:"user"`
	// Password is the database password
	Password string `yaml:"password"`
	// Name is the database name
	DBName string `yaml:"dbname"`
	// SSLMode is the SSL mode:
	// http://www.postgresql.cn/docs/current/libpq-ssl.html#LIBPQ-SSL-SSLMODE-STATEMENTS
	SSLMode string `yaml:"sslmode"`
	// SSLCert is the PEM encoded certificate file path.
	SSLCert string `yaml:"sslcert"`
	// SSLKey is the PEM encoded key file path.
	SSLKey string `yaml:"sslkey"`
	// SSLRootCert is the PEM encoded root certificate file path.
	SSLRootCert string `yaml:"sslrootcert"`
	// Pool configures the behavior of the database connection pool.
	Pool struct {
		// MaxIdle sets the maximum number of connections in the idle connection pool. If MaxOpen is less than MaxIdle,
		// then MaxIdle is reduced to match the MaxOpen limit. Defaults to 0 (no idle connections).
		MaxIdle int `yaml:"maxidle,omitempty"`
		// MaxOpen sets the maximum number of open connections to the database. If MaxOpen is less than MaxIdle, then
		// MaxIdle is reduced to match the MaxOpen limit. Defaults to 0 (unlimited).
		MaxOpen int `yaml:"maxopen,omitempty"`
		// MaxLifetime sets the maximum amount of time a connection may be reused. Expired connections may be closed
		// lazily before reuse. Defaults to 0 (unlimited).
		MaxLifetime time.Duration `yaml:"maxlifetime,omitempty"`
		// MaxIdleTime is the maximum amount of time a connection may be idle. Expired connections may be closed lazily
		// before reuse. Defaults to 0 (unlimited).
		MaxIdleTime time.Duration `yaml:"maxidletime,omitempty"`
	} `yaml:"pool,omitempty"`
	// Maximum time to wait for a connection. Zero or not specified means waiting indefinitely.
	ConnectTimeout time.Duration `yaml:"connecttimeout,omitempty"`
	// DrainTimeout time to wait to drain all connections on shutdown. Zero or not specified means waiting indefinitely.
	DrainTimeout time.Duration `yaml:"draintimeout,omitempty"`
	// PreparedStatements can be used to enable prepared statements. Defaults to false.
	PreparedStatements bool `yaml:"preparedstatements,omitempty"`
}

// Regexp wraps regexp.Regexp to implement the encoding.TextMarshaler interface.
type Regexp struct {
	*regexp.Regexp
}

// compileRegexp wraps the standard library's regexp.Compile.
func compileRegexp(expr string) (*Regexp, error) {
	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, err
	}
	return &Regexp{re}, nil
}

// UnmarshalText implements encoding.TextMarshaler.
func (r *Regexp) UnmarshalText(text []byte) error {
	rr, err := compileRegexp(string(text))
	if err != nil {
		return fmt.Errorf("invalid regexp %q: %w", text, err)
	}
	*r = *rr
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (r *Regexp) MarshalText() ([]byte, error) {
	return []byte(r.String()), nil
}

// MailOptions provides the configuration sections to user, for specific handler.
type MailOptions struct {
	SMTP struct {
		// Addr defines smtp host address
		Addr string `yaml:"addr,omitempty"`

		// Username defines user name to smtp host
		Username string `yaml:"username,omitempty"`

		// Password defines password of login user
		Password string `yaml:"password,omitempty"`

		// Insecure defines if smtp login skips the secure certification.
		Insecure bool `yaml:"insecure,omitempty"`
	} `yaml:"smtp,omitempty"`

	// From defines mail sending address
	From string `yaml:"from,omitempty"`

	// To defines mail receiving address
	To []string `yaml:"to,omitempty"`
}

// FileChecker is a type of entry in the health section for checking files.
type FileChecker struct {
	// Interval is the duration in between checks
	Interval time.Duration `yaml:"interval,omitempty"`
	// File is the path to check
	File string `yaml:"file,omitempty"`
	// Threshold is the number of times a check must fail to trigger an
	// unhealthy state
	Threshold int `yaml:"threshold,omitempty"`
}

// HTTPChecker is a type of entry in the health section for checking HTTP URIs.
type HTTPChecker struct {
	// Timeout is the duration to wait before timing out the HTTP request
	Timeout time.Duration `yaml:"timeout,omitempty"`
	// StatusCode is the expected status code
	StatusCode int
	// Interval is the duration in between checks
	Interval time.Duration `yaml:"interval,omitempty"`
	// URI is the HTTP URI to check
	URI string `yaml:"uri,omitempty"`
	// Headers lists static headers that should be added to all requests
	Headers http.Header `yaml:"headers"`
	// Threshold is the number of times a check must fail to trigger an
	// unhealthy state
	Threshold int `yaml:"threshold,omitempty"`
}

// TCPChecker is a type of entry in the health section for checking TCP servers.
type TCPChecker struct {
	// Timeout is the duration to wait before timing out the TCP connection
	Timeout time.Duration `yaml:"timeout,omitempty"`
	// Interval is the duration in between checks
	Interval time.Duration `yaml:"interval,omitempty"`
	// Addr is the TCP address to check
	Addr string `yaml:"addr,omitempty"`
	// Threshold is the number of times a check must fail to trigger an
	// unhealthy state
	Threshold int `yaml:"threshold,omitempty"`
}

// Health provides the configuration section for health checks.
type Health struct {
	// FileCheckers is a list of paths to check
	FileCheckers []FileChecker `yaml:"file,omitempty"`
	// HTTPCheckers is a list of URIs to check
	HTTPCheckers []HTTPChecker `yaml:"http,omitempty"`
	// TCPCheckers is a list of URIs to check
	TCPCheckers []TCPChecker `yaml:"tcp,omitempty"`
	// StorageDriver configures a health check on the configured storage
	// driver
	StorageDriver struct {
		// Enabled turns on the health check for the storage driver
		Enabled bool `yaml:"enabled,omitempty"`
		// Interval is the duration in between checks
		Interval time.Duration `yaml:"interval,omitempty"`
		// Threshold is the number of times a check must fail to trigger an
		// unhealthy state
		Threshold int `yaml:"threshold,omitempty"`
	} `yaml:"storagedriver,omitempty"`
}

// v0_1Configuration is a Version 0.1 Configuration struct
// This is currently aliased to Configuration, as it is the current version
type v0_1Configuration Configuration

// UnmarshalYAML implements the yaml.Unmarshaler interface
// Unmarshals a string of the form X.Y into a Version, validating that X and Y can represent unsigned integers
func (version *Version) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var versionString string
	err := unmarshal(&versionString)
	if err != nil {
		return err
	}

	newVersion := Version(versionString)
	if _, err := newVersion.major(); err != nil {
		return err
	}

	if _, err := newVersion.minor(); err != nil {
		return err
	}

	*version = newVersion
	return nil
}

// CurrentVersion is the most recent Version that can be parsed
var CurrentVersion = MajorMinorVersion(0, 1)

// Loglevel is the level at which operations are logged. This can be "error", "warn", "info", "debug" or "trace".
type Loglevel string

const (
	LogLevelError   Loglevel = "error"
	LogLevelWarn    Loglevel = "warn"
	LogLevelInfo    Loglevel = "info"
	LogLevelDebug   Loglevel = "debug"
	LogLevelTrace   Loglevel = "trace"
	defaultLogLevel          = LogLevelInfo
)

var logLevels = []Loglevel{
	LogLevelError,
	LogLevelWarn,
	LogLevelInfo,
	LogLevelDebug,
	LogLevelTrace,
}

// String implements the Stringer interface for Loglevel.
func (l Loglevel) String() string {
	return string(l)
}

func (l Loglevel) isValid() bool {
	for _, lvl := range logLevels {
		if l == lvl {
			return true
		}
	}
	return false
}

// UnmarshalYAML implements the yaml.Umarshaler interface for Loglevel, parsing it and validating that it represents a
// valid log level.
func (l *Loglevel) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	err := unmarshal(&val)
	if err != nil {
		return err
	}

	lvl := Loglevel(strings.ToLower(val))
	if !lvl.isValid() {
		return fmt.Errorf("invalid log level %q, must be one of %q", val, logLevels)
	}

	*l = lvl
	return nil
}

// logOutput is the output destination for logs. This can be either "stdout" or "stderr".
type logOutput string

const (
	LogOutputStdout  logOutput = "stdout"
	LogOutputStderr  logOutput = "stderr"
	LogOutputDiscard logOutput = "discard"
	defaultLogOutput           = LogOutputStdout
)

var logOutputs = []logOutput{LogOutputStdout, LogOutputStderr}

// String implements the Stringer interface for logOutput.
func (out logOutput) String() string {
	return string(out)
}

// Descriptor returns the os file descriptor of a log output.
func (out logOutput) Descriptor() io.Writer {
	switch out {
	case LogOutputStderr:
		return os.Stderr
	case LogOutputDiscard:
		return io.Discard
	default:
		return os.Stdout
	}
}

func (out logOutput) isValid() bool {
	for _, output := range logOutputs {
		if out == output {
			return true
		}
	}
	return false
}

// UnmarshalYAML implements the yaml.Umarshaler interface for logOutput, parsing it and validating that it represents a
// valid log output destination.
func (out *logOutput) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	lo := logOutput(strings.ToLower(val))
	if !lo.isValid() {
		return fmt.Errorf("invalid log output %q, must be one of %q", lo, logOutputs)
	}

	*out = lo
	return nil
}

// logFormat is the format of the application logs output. This can be either "text" or "json".
type logFormat string

const (
	LogFormatText    logFormat = "text"
	LogFormatJSON    logFormat = "json"
	defaultLogFormat           = LogFormatJSON
)

var logFormats = []logFormat{
	LogFormatText,
	LogFormatJSON,
}

// String implements the Stringer interface for logFormat.
func (ft logFormat) String() string {
	return string(ft)
}

func (ft logFormat) isValid() bool {
	for _, formatter := range logFormats {
		if ft == formatter {
			return true
		}
	}
	return false
}

// UnmarshalYAML implements the yaml.Umarshaler interface for logFormat, parsing it and validating that it
// represents a valid application log output format.
func (ft *logFormat) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	format := logFormat(strings.ToLower(val))
	if !format.isValid() {
		return fmt.Errorf("invalid log format %q, must be one of %q", format, logFormats)
	}

	*ft = format
	return nil
}

// accessLogFormat is the format of the access logs output. This can be either "text" or "json".
type accessLogFormat string

const (
	AccessLogFormatText    accessLogFormat = "text"
	AccessLogFormatJSON    accessLogFormat = "json"
	defaultAccessLogFormat                 = AccessLogFormatJSON
)

var accessLogFormats = []accessLogFormat{
	AccessLogFormatText,
	AccessLogFormatJSON,
}

// String implements the Stringer interface for accessLogFormat.
func (ft accessLogFormat) String() string {
	return string(ft)
}

func (ft accessLogFormat) isValid() bool {
	for _, formatter := range accessLogFormats {
		if ft == formatter {
			return true
		}
	}
	return false
}

// UnmarshalYAML implements the yaml.Umarshaler interface for accessLogFormat, parsing it and validating that it
// represents a valid access log output format.
func (ft *accessLogFormat) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var val string
	if err := unmarshal(&val); err != nil {
		return err
	}

	format := accessLogFormat(strings.ToLower(val))
	if !format.isValid() {
		return fmt.Errorf("invalid access log format %q, must be one of %q", format, accessLogFormats)
	}

	*ft = format
	return nil
}

// Parameters defines a key-value parameters mapping
type Parameters map[string]interface{}

// Storage defines the configuration for registry object storage
type Storage map[string]Parameters

// Type returns the storage driver type, such as filesystem or s3
func (storage Storage) Type() string {
	var storageType []string

	// Return only key in this map
	for k := range storage {
		switch k {
		case "maintenance":
			// allow configuration of maintenance
		case "cache":
			// allow configuration of caching
		case "delete":
			// allow configuration of delete
		case "redirect":
			// allow configuration of redirect
		default:
			storageType = append(storageType, k)
		}
	}
	if len(storageType) > 1 {
		panic("multiple storage drivers specified in configuration or environment: " + strings.Join(storageType, ", "))
	}
	if len(storageType) == 1 {
		return storageType[0]
	}
	return ""
}

// Parameters returns the Parameters map for a Storage configuration
func (storage Storage) Parameters() Parameters {
	return storage[storage.Type()]
}

// setParameter changes the parameter at the provided key to the new value
func (storage Storage) setParameter(key string, value interface{}) {
	storage[storage.Type()][key] = value
}

// UnmarshalYAML implements the yaml.Unmarshaler interface
// Unmarshals a single item map into a Storage or a string into a Storage type with no parameters
func (storage *Storage) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var storageMap map[string]Parameters
	err := unmarshal(&storageMap)
	if err == nil {
		if len(storageMap) > 1 {
			types := make([]string, 0, len(storageMap))
			for k := range storageMap {
				switch k {
				case "maintenance":
					// allow for configuration of maintenance
				case "cache":
					// allow configuration of caching
				case "delete":
					// allow configuration of delete
				case "redirect":
					// allow configuration of redirect
				default:
					types = append(types, k)
				}
			}

			if len(types) > 1 {
				return fmt.Errorf("must provide exactly one storage type. Provided: %v", types)
			}
		}
		*storage = storageMap
		return nil
	}

	var storageType string
	err = unmarshal(&storageType)
	if err == nil {
		*storage = Storage{storageType: Parameters{}}
		return nil
	}

	return err
}

// MarshalYAML implements the yaml.Marshaler interface
func (storage Storage) MarshalYAML() (interface{}, error) {
	if storage.Parameters() == nil {
		return storage.Type(), nil
	}
	return map[string]Parameters(storage), nil
}

// Auth defines the configuration for registry authorization.
type Auth map[string]Parameters

// Type returns the auth type, such as token
func (auth Auth) Type() string {
	// Return only key in this map
	for k := range auth {
		return k
	}
	return ""
}

// Parameters returns the Parameters map for an Auth configuration
func (auth Auth) Parameters() Parameters {
	return auth[auth.Type()]
}

// setParameter changes the parameter at the provided key to the new value
func (auth Auth) setParameter(key string, value interface{}) {
	auth[auth.Type()][key] = value
}

// UnmarshalYAML implements the yaml.Unmarshaler interface
// Unmarshals a single item map into a Storage or a string into a Storage type with no parameters
func (auth *Auth) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var m map[string]Parameters
	err := unmarshal(&m)
	if err == nil {
		if len(m) > 1 {
			types := make([]string, 0, len(m))
			for k := range m {
				types = append(types, k)
			}

			return fmt.Errorf("must provide exactly one type. Provided: %v", types)
		}
		*auth = m
		return nil
	}

	var authType string
	err = unmarshal(&authType)
	if err == nil {
		*auth = Auth{authType: Parameters{}}
		return nil
	}

	return err
}

// MarshalYAML implements the yaml.Marshaler interface
func (auth Auth) MarshalYAML() (interface{}, error) {
	if auth.Parameters() == nil {
		return auth.Type(), nil
	}
	return map[string]Parameters(auth), nil
}

// Notifications configures multiple http endpoints.
type Notifications struct {
	// EventConfig is the configuration for the event format that is sent to each Endpoint.
	EventConfig Events `yaml:"events,omitempty"`
	// Endpoints is a list of http configurations for endpoints that
	// respond to webhook notifications. In the future, we may allow other
	// kinds of endpoints, such as external queues.
	Endpoints []Endpoint `yaml:"endpoints,omitempty"`
}

// Endpoint describes the configuration of an http webhook notification
// endpoint.
type Endpoint struct {
	Name              string        `yaml:"name"`              // identifies the endpoint in the registry instance.
	Disabled          bool          `yaml:"disabled"`          // disables the endpoint
	URL               string        `yaml:"url"`               // post url for the endpoint.
	Headers           http.Header   `yaml:"headers"`           // static headers that should be added to all requests
	Timeout           time.Duration `yaml:"timeout"`           // HTTP timeout
	Threshold         int           `yaml:"threshold"`         // circuit breaker threshold before backing off on failure
	Backoff           time.Duration `yaml:"backoff"`           // backoff duration
	IgnoredMediaTypes []string      `yaml:"ignoredmediatypes"` // target media types to ignore
	Ignore            Ignore        `yaml:"ignore"`            // ignore event types
}

// Events configures notification events.
type Events struct {
	IncludeReferences bool `yaml:"includereferences"` // include reference data in manifest events
}

// Ignore configures mediaTypes and actions of the event, that it won't be propagated
type Ignore struct {
	MediaTypes []string `yaml:"mediatypes"` // target media types to ignore
	Actions    []string `yaml:"actions"`    // ignore action types
}

// Reporting defines error reporting methods.
type Reporting struct {
	// Sentry configures error reporting for Sentry (sentry.io).
	Sentry SentryReporting `yaml:"sentry,omitempty"`
}

// SentryReporting configures error reporting for Sentry (sentry.io).
type SentryReporting struct {
	// Enabled can be set to `true` to enable the Sentry error reporting.
	Enabled bool `yaml:"enabled,omitempty"`
	// DSN is the Sentry DSN.
	DSN string `yaml:"dsn,omitempty"`
	// Environment is the Sentry environment.
	Environment string `yaml:"environment,omitempty"`
}

// Middleware configures named middlewares to be applied at injection points.
type Middleware struct {
	// Name the middleware registers itself as
	Name string `yaml:"name"`
	// Flag to disable middleware easily
	Disabled bool `yaml:"disabled,omitempty"`
	// Map of parameters that will be passed to the middleware's initialization function
	Options Parameters `yaml:"options"`
}

type parseOpts struct {
	noStorageRequired bool
}

// ParseOption is used to pass options to Parse.
type ParseOption func(*parseOpts)

// WithoutStorageValidation configures Parse to disable the storage parameters validation.
func WithoutStorageValidation() ParseOption {
	return func(opts *parseOpts) {
		opts.noStorageRequired = true
	}
}

// Parse parses an input configuration yaml document into a Configuration struct
// This should generally be capable of handling old configuration format versions
//
// Environment variables may be used to override configuration parameters other than version,
// following the scheme below:
// Configuration.Abc may be replaced by the value of REGISTRY_ABC,
// Configuration.Abc.Xyz may be replaced by the value of REGISTRY_ABC_XYZ, and so forth
func Parse(rd io.Reader, opts ...ParseOption) (*Configuration, error) {
	options := parseOpts{}
	for _, v := range opts {
		v(&options)
	}

	in, err := io.ReadAll(rd)
	if err != nil {
		return nil, err
	}

	p := NewParser("registry", []VersionedParseInfo{
		{
			Version: MajorMinorVersion(0, 1),
			ParseAs: reflect.TypeOf(v0_1Configuration{}),
			ConversionFunc: func(c interface{}) (interface{}, error) {
				if v0_1, ok := c.(*v0_1Configuration); ok {
					if v0_1.Log.Level == Loglevel("") {
						if v0_1.Loglevel != Loglevel("") {
							v0_1.Log.Level = v0_1.Loglevel
						}
					}
					if v0_1.Loglevel != Loglevel("") {
						v0_1.Loglevel = Loglevel("")
					}
					if !options.noStorageRequired {
						if v0_1.Storage.Type() == "" {
							return nil, errors.New("no storage configuration provided")
						}
					}
					return (*Configuration)(v0_1), nil
				}
				return nil, fmt.Errorf("expected *v0_1Configuration, received %#v", c)
			},
		},
	})

	config := new(Configuration)
	err = p.Parse(in, config)
	if err != nil {
		return nil, err
	}

	ApplyDefaults(config)

	return config, nil
}

func ApplyDefaults(config *Configuration) {
	if config.Log.Level == "" {
		config.Log.Level = defaultLogLevel
	}
	if config.Log.Output == "" {
		config.Log.Output = defaultLogOutput
	}
	if config.Log.Formatter == "" {
		config.Log.Formatter = defaultLogFormat
	}
	if !config.Log.AccessLog.Disabled && config.Log.AccessLog.Formatter == "" {
		config.Log.AccessLog.Formatter = defaultAccessLogFormat
	}
	if config.HTTP.Debug.Prometheus.Enabled && config.HTTP.Debug.Prometheus.Path == "" {
		config.HTTP.Debug.Prometheus.Path = "/metrics"
	}
	if config.Redis.Addr != "" && config.Redis.Pool.Size == 0 {
		config.Redis.Pool.Size = 10
	}

	// copy TLS config to debug server when enabled and debug TLS certificate is empty
	if config.HTTP.Debug.TLS.Enabled {
		if config.HTTP.Debug.TLS.Certificate == "" {
			config.HTTP.Debug.TLS.Certificate = config.HTTP.TLS.Certificate
			config.HTTP.Debug.TLS.Key = config.HTTP.TLS.Key
		}
		// Only replace if the debug section is empty which allows finer configuration settings for the
		// debug server, for example allowing only certain clients to access it.
		if len(config.HTTP.Debug.TLS.ClientCAs) == 0 {
			config.HTTP.Debug.TLS.ClientCAs = config.HTTP.TLS.ClientCAs
		}
		if config.HTTP.Debug.TLS.MinimumTLS == "" {
			config.HTTP.Debug.TLS.MinimumTLS = config.HTTP.TLS.MinimumTLS
		}
	}
}
