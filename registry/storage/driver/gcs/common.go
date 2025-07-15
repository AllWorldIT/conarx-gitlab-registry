package gcs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"time"

	// nolint: revive,gosec // imports-blocklist

	"cloud.google.com/go/storage"
	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/internal"
	dstorage "github.com/docker/distribution/registry/storage"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/registry/storage/driver/internal/parse"
	"github.com/docker/distribution/registry/storage/internal/metrics"
	"github.com/docker/distribution/version"
	sloglogrus "github.com/samber/slog-logrus/v2"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

const (
	driverName = "gcs"

	uploadSessionContentType       = "application/x-docker-upload-session"
	minChunkSize             int64 = 256 * 1024
	defaultChunkSize               = 20 * minChunkSize
	defaultMaxConcurrency          = 50
	minConcurrency                 = 25
	maxDeleteConcurrency           = 150
	maxWalkConcurrency             = 100
	maxTries                       = 5
)

// customGitlabGoogle... are the query params appended to gcs signed redirect url
const (
	customGitlabGoogleNamespaceIdParam = "x-goog-custom-audit-gitlab-namespace-id"
	customGitlabGoogleProjectIdParam   = "x-goog-custom-audit-gitlab-project-id"
	customGitlabGoogleAuthTypeParam    = "x-goog-custom-audit-gitlab-auth-type"
	customGitlabGoogleObjectSizeParam  = "x-goog-custom-audit-gitlab-size-bytes"
)

const registryGCSDriverEnv = "REGISTRY_GCS_DRIVER"

var rangeHeader = regexp.MustCompile(`^bytes=([0-9])+-([0-9]+)$`)

// customParamKeys is the mapping between gitlab keys to gcs signed-redirect-url query parameter keys
var customParamKeys = map[string]string{
	dstorage.NamespaceIdKey: customGitlabGoogleNamespaceIdParam,
	dstorage.ProjectIdKey:   customGitlabGoogleProjectIdParam,
	dstorage.AuthTypeKey:    customGitlabGoogleAuthTypeParam,
	dstorage.SizeBytesKey:   customGitlabGoogleObjectSizeParam,
}

// for testing purposes
var systemClock internal.Clock = clock.New()

func init() {
	factory.Register(driverName, &gcsDriverFactory{})
}

// driverParameters is a struct that encapsulates all the driver parameters after all values have been set
type driverParameters struct {
	bucket        string
	email         string
	privateKey    []byte
	client        *http.Client
	storageClient *storage.Client
	rootDirectory string
	chunkSize     int64

	// maxConcurrency limits the number of concurrent driver operations
	// to GCS, which ultimately increases reliability of many simultaneous
	// pushes by ensuring we aren't DoSing our own server with many
	// connections.
	maxConcurrency uint64

	// parallelWalk enables or disables concurrently walking the filesystem.
	parallelWalk bool
}

// gcsDriverFactory implements the factory.StorageDriverFactory interface
type gcsDriverFactory struct{}

// Create StorageDriver from parameters
func (*gcsDriverFactory) Create(parameters map[string]any) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

// Wrapper wraps `driver` with a throttler, ensuring that no more than N
// GCS actions can occur concurrently. The default limit is 75.
type Wrapper struct {
	baseEmbed
}

// GCSBucketKey returns the GCS bucket key for the given storage driver path.
func (d *Wrapper) GCSBucketKey(path string) string {
	// This is currently used exclusively by the Google Cloud CDN middleware.
	// During an online migration we have to maintain two separate storage
	// drivers, each with a different root directory. Because of that we have
	// no other option than hand over the object full path construction to the
	// underlying GCS driver, instead of manually concatenating the CDN
	// endpoint with the object path.
	baseDriver := d.StorageDriver.(*base.Regulator).StorageDriver

	// Type assertion to handle both driver types
	switch typedDriver := baseDriver.(type) {
	case *driverNext:
		return typedDriver.pathToKey(path)
	case *driver:
		return typedDriver.pathToKey(path)
	default:
		// Handle unexpected driver type or fail gracefully. Depending on
		// requirements, we might want to log this or handle differently
		return path
	}
}

type baseEmbed struct {
	base.Base
}

type request func() error

func ShouldRetry(err error) bool {
	return shouldRetryImpl(true, err)
}

// shouldRetryImpl function determines if the request to GCS should be retried.
// The interface used by ShouldRetry func is determined by 3rd party package,
// so we wrap the function in wrapper that passes correct retry type.
func shouldRetryImpl(nativeRetry bool, err error) bool {
	metrics.StorageBackendRetry(nativeRetry)

	// Context cancelation/expiry is fatal, do not try to retry:
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// NOTE(prozlach): http2 stream errors are also retryable
	var streamError http2.StreamError
	if errors.As(err, &streamError) {
		return true
	}

	return storage.ShouldRetry(err)
}

func retry(req request) error {
	backoff := time.Second
	var err error
	for i := 0; i < maxTries; i++ {
		err = req()
		if err == nil {
			return nil
		}

		if !shouldRetryImpl(false, err) {
			return err
		}

		gErr := new(googleapi.Error)
		if errors.As(err, &gErr) && gErr.Code == http.StatusTooManyRequests {
			metrics.StorageRatelimit()
		}

		// nolint:gosec // this is just a random number for rety backoff
		time.Sleep(backoff - time.Second + (time.Duration(rand.Int32N(1000)) * time.Millisecond))
		if i <= 4 {
			backoff *= 2
		}
	}

	var gerr *googleapi.Error
	if ok := errors.As(err, &gerr); ok && gerr.Code == http.StatusTooManyRequests {
		return storagedriver.TooManyRequestsError{Cause: err}
	}

	return err
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - bucket
func FromParameters(parameters map[string]any) (storagedriver.StorageDriver, error) {
	useNext := os.Getenv(registryGCSDriverEnv) == "next"

	params, err := parseParameters(parameters, useNext)
	if err != nil {
		return nil, err
	}

	if useNext {
		logrus.Warn("Using next-gen GCS driver")
		return NewNext(params)
	}
	logrus.Warn("Using legacy GCS driver")
	return New(params)
}

func parseParameters(parameters map[string]any, useNext bool) (*driverParameters, error) {
	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("no bucket parameter provided")
	}

	rootDirectory, ok := parameters["rootdirectory"]
	if !ok {
		rootDirectory = ""
	}

	chunkSize := defaultChunkSize
	chunkSizeParam, ok := parameters["chunksize"]
	if ok {
		switch v := chunkSizeParam.(type) {
		case string:
			vv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("chunksize parameter must be an integer, %v invalid", chunkSizeParam)
			}
			chunkSize = vv
		case int, uint, int32, uint32, uint64, int64:
			chunkSize = reflect.ValueOf(v).Convert(reflect.TypeOf(chunkSize)).Int()
		default:
			return nil, fmt.Errorf("invalid value for chunksize: %#v", chunkSizeParam)
		}

		if chunkSize < minChunkSize {
			return nil, fmt.Errorf("the chunksize %#v parameter should be a number that is larger than or equal to %d", chunkSize, minChunkSize)
		}

		if chunkSize%minChunkSize != 0 {
			return nil, fmt.Errorf("chunksize should be a multiple of %d", minChunkSize)
		}
	}

	var ts oauth2.TokenSource
	jwtConf := new(jwt.Config)
	if keyfile, ok := parameters["keyfile"]; ok {
		jsonKey, err := os.ReadFile(fmt.Sprint(keyfile))
		if err != nil {
			return nil, err
		}
		jwtConf, err = google.JWTConfigFromJSON(jsonKey, storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
		ts = jwtConf.TokenSource(context.Background())
	} else if credentials, ok := parameters["credentials"]; ok {
		credentialMap, ok := credentials.(map[any]any)
		if !ok {
			return nil, fmt.Errorf("the credentials were not specified in the correct format")
		}

		stringMap := make(map[string]any, 0)
		for k, v := range credentialMap {
			key, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("one of the credential keys was not a string: %s", fmt.Sprint(k))
			}
			stringMap[key] = v
		}

		data, err := json.Marshal(stringMap)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal gcs credentials to json")
		}

		jwtConf, err = google.JWTConfigFromJSON(data, storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
		ts = jwtConf.TokenSource(context.Background())
	} else {
		var err error
		ts, err = google.DefaultTokenSource(context.Background(), storage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
	}

	maxConcurrency, err := base.GetLimitFromParameter(parameters["maxconcurrency"], minConcurrency, defaultMaxConcurrency)
	if err != nil {
		return nil, fmt.Errorf("maxconcurrency config error: %s", err)
	}

	opts := []option.ClientOption{option.WithTokenSource(ts)}
	if useNext {
		debugLogging := false

		if _, ok = parameters["debug_log"]; ok {
			debugLogging, err = parse.Bool(parameters, "debug_log", false)
			if err != nil {
				return nil, fmt.Errorf("parsing parameter %s: %w", "debug_log", err)
			}
		}
		if debugLogging {
			// NOTE(prozlach): Casting directly to logrus.Entry is a shortcut
			// here, as we require Logrus logger for the adapter. In theory we
			// should be using the log.Logger interface instead of peeking into
			// the implementation, but this requies a deeper refactoring.
			//
			// Hopefully we will switch to slog/zap at some point and this will
			// become non-issue.
			logrusEntry, err := log.ToLogrusEntry(
				log.GetLogger().WithFields(log.Fields{
					"component": "registry.storage.gcs_next.internal",
				}),
			)
			if err != nil {
				return nil, fmt.Errorf("converting logger to logrus.Entry: %w", err)
			}
			slogger := slog.New(
				sloglogrus.Option{
					Level:  slog.LevelDebug,
					Logger: logrusEntry.Logger,
				}.NewLogrusHandler(),
			)
			// NOTE(prozlach): This does not work really, see https://github.com/googleapis/google-cloud-go/issues/12475
			// As a workaround we set the env variable as well, but this only
			// makes GCS print logs to stdout using JSON format. Not ideal.
			//
			// Only set the environment variable if it's not already set
			// nolint: revive // max-control-nesting
			if existingLevel := os.Getenv("GOOGLE_SDK_GO_LOGGING_LEVEL"); existingLevel != "" {
				log.GetLogger().WithFields(logrus.Fields{
					"existing_value":  existingLevel,
					"requested_value": "debug",
				}).Warn("GOOGLE_SDK_GO_LOGGING_LEVEL environment variable is already set, not overriding")
			} else {
				err = os.Setenv("GOOGLE_SDK_GO_LOGGING_LEVEL", "debug")
				if err != nil {
					return nil, fmt.Errorf("setting `GOOGLE_SDK_GO_LOGGING_LEVEL` env var: %w", err)
				}
			}
			opts = append(opts, option.WithLogger(slogger))
		}

		// NOTE(prozlach): By default, reads are made using the Cloud Storage XML
		// API. GCS SDK recommends using the JSON API instead, which is done
		// here by setting WithJSONReads. This ensures consistency with other
		// client operations, which all use JSON. JSON will become the default
		// in a future release of GCS SDK. We only enable it for GCS next.
		// https://cloud.google.com/go/docs/reference/cloud.google.com/go/storage/latest#cloud_google_com_go_storage_WithJSONReads
		opts = append(opts, storage.WithJSONReads())

		if userAgent, ok := parameters["useragent"]; ok {
			if ua, ok := userAgent.(string); ok && ua != "" {
				opts = append(opts, option.WithUserAgent(ua))
			} else {
				userAgent := fmt.Sprintf("container-registry %s (%s)", version.Version, runtime.Version())
				opts = append(opts, option.WithUserAgent(userAgent))
			}
		}
	}
	storageClient, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("storage client error: %s", err)
	}

	parallelWalkBool, err := parse.Bool(parameters, "parallelwalk", false)
	if err != nil {
		return nil, err
	}

	return &driverParameters{
		bucket:         fmt.Sprint(bucket),
		rootDirectory:  fmt.Sprint(rootDirectory),
		email:          jwtConf.Email,
		privateKey:     jwtConf.PrivateKey,
		client:         oauth2.NewClient(context.Background(), ts),
		storageClient:  storageClient,
		chunkSize:      chunkSize,
		maxConcurrency: maxConcurrency,
		parallelWalk:   parallelWalkBool,
	}, nil
}
