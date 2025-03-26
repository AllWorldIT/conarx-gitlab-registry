package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"expvar"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/health"
	dlog "github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/listener"
	"github.com/docker/distribution/uuid"
	"github.com/docker/distribution/version"

	"github.com/hashicorp/go-multierror"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/errortracking"
	"gitlab.com/gitlab-org/labkit/fips"
	logkit "gitlab.com/gitlab-org/labkit/log"
	"gitlab.com/gitlab-org/labkit/monitoring"
	"golang.org/x/crypto/acme"
	"golang.org/x/crypto/acme/autocert"
)

var errSkipTLSConfig = errors.New("no TLS config found")

var tlsLookup = map[string]uint16{
	"":       tls.VersionTLS12,
	"tls1.2": tls.VersionTLS12,
	"tls1.3": tls.VersionTLS13,
}

// a map of TLS cipher suite names to constants in https://golang.org/pkg/crypto/tls/#pkg-constants
var cipherSuites = map[string]uint16{
	// These are insecure:
	// "TLS_RSA_WITH_RC4_128_SHA",
	// "TLS_RSA_WITH_AES_128_CBC_SHA256",
	// "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA",
	// "TLS_ECDHE_RSA_WITH_RC4_128_SHA",
	// "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
	// "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",

	// TLS 1.0 - 1.2 cipher suites
	"TLS_RSA_WITH_3DES_EDE_CBC_SHA":                 tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA,
	"TLS_RSA_WITH_AES_128_CBC_SHA":                  tls.TLS_RSA_WITH_AES_128_CBC_SHA,
	"TLS_RSA_WITH_AES_256_CBC_SHA":                  tls.TLS_RSA_WITH_AES_256_CBC_SHA,
	"TLS_RSA_WITH_AES_128_GCM_SHA256":               tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_RSA_WITH_AES_256_GCM_SHA384":               tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA":          tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
	"TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA":          tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA":           tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA":            tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA":            tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
	"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256":         tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256":       tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384":         tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384":       tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	"TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256":   tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	"TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256": tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
}

// ServeCmd is a cobra command for running the registry.
var ServeCmd = &cobra.Command{
	Use:   "serve <config>",
	Short: "`serve` stores and distributes Docker images",
	Long:  "`serve` stores and distributes Docker images.",
	RunE: func(_ *cobra.Command, args []string) error {
		// setup context
		ctx := dcontext.WithVersion(dcontext.Background(), version.Version)

		config, err := resolveConfiguration(args)
		if err != nil {
			return fmt.Errorf("configuration error: %w", err)
		}

		registry, err := NewRegistry(ctx, config)
		if err != nil {
			return fmt.Errorf("creating new registry instance: %w", err)
		}

		go func() {
			opts, err := configureMonitoring(ctx, config)
			if err != nil {
				log.WithError(err).Error("failed to configure monitoring service, skipping")
				return
			}

			if err := monitoring.Start(opts...); err != nil {
				log.WithError(err).Error("unable to start monitoring service")
			}
		}()

		// if running in FIPS mode, this emits an info log message saying so
		fips.Check()

		if err = registry.ListenAndServe(); err != nil {
			return fmt.Errorf("running http server: %w", err)
		}
		return nil
	},
}

// A Registry represents a complete instance of the registry.
type Registry struct {
	config *configuration.Configuration
	app    *handlers.App
	server *http.Server
}

// NewRegistry creates a new registry from a context and configuration struct.
func NewRegistry(ctx context.Context, config *configuration.Configuration) (*Registry, error) {
	var err error
	ctx, err = configureLogging(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("configuring logger: %w", err)
	}

	// inject a logger into the uuid library. warns us if there is a problem
	// with uuid generation under low entropy.
	uuid.Loggerf = dcontext.GetLogger(ctx).Warnf

	app, err := handlers.NewApp(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("configuring application: %w", err)
	}

	// aaronl: The global scope of the health checks means NewRegistry
	// can only be called once per process.
	if err := app.RegisterHealthChecks(); err != nil {
		return nil, err
	}

	handler := panicHandler(app)
	if handler, err = configureReporting(config, handler); err != nil {
		return nil, fmt.Errorf("configuring reporting services: %w", err)
	}
	handler = alive("/", handler)
	handler = health.Handler(handler)
	if handler, err = configureAccessLogging(config, handler); err != nil {
		return nil, fmt.Errorf("configuring access logger: %w", err)
	}
	handler = correlation.InjectCorrelationID(handler, correlation.WithPropagation())

	server := &http.Server{
		// NOTE(prozlach): prevent slow-loris attack, but be conservative at it
		ReadHeaderTimeout: 60 * time.Second,
		Handler:           handler,
	}

	return &Registry{
		app:    app,
		config: config,
		server: server,
	}, nil
}

// Channel to capture signals used to gracefully shutdown the registry.
// It is global to ease unit testing
var quit = make(chan os.Signal, 1)

// ListenAndServe runs the registry's HTTP server.
func (registry *Registry) ListenAndServe() error {
	config := registry.config

	ln, err := listener.NewListener(config.HTTP.Net, config.HTTP.Addr)
	if err != nil {
		return err
	}

	tlsConf, err := getTLSConfig(registry.app.Context, config.HTTP.TLS, config.HTTP.HTTP2.Disabled)
	if err != nil && !errors.Is(err, errSkipTLSConfig) {
		return err
	}

	if tlsConf != nil {
		ln = tls.NewListener(ln, tlsConf)

		dcontext.GetLogger(registry.app).Infof("listening on %v, tls", ln.Addr())
	} else {
		dcontext.GetLogger(registry.app).Infof("listening on %v", ln.Addr())
	}

	// Setup channel to get notified on SIGTERM and interrupt signals.
	signal.Notify(quit, syscall.SIGTERM, os.Interrupt)
	serveErr := make(chan error)

	// Start serving in goroutine and listen for stop signal in main thread
	go func() {
		serveErr <- registry.server.Serve(ln)
	}()

	select {
	case err := <-serveErr:
		return err
	case s := <-quit:
		l := log.WithFields(log.Fields{
			"quit_signal":            s.String(),
			"http_drain_timeout":     registry.config.HTTP.DrainTimeout,
			"database_drain_timeout": registry.config.Database.DrainTimeout,
		})
		l.Info("attempting to stop server gracefully...")

		// shutdown the server with a grace period of configured timeout
		if registry.config.HTTP.DrainTimeout != 0 {
			l.Info("draining http connections")
			ctx, cancel := context.WithTimeout(context.Background(), registry.config.HTTP.DrainTimeout)
			defer cancel()
			if err := registry.server.Shutdown(ctx); err != nil {
				return err
			}
		}

		if registry.config.Database.Enabled {
			l.Info("closing database connections")

			ctx := context.Background()
			var cancel context.CancelFunc

			// Drain database with grace period, rather than waiting indefinitely.
			if registry.config.Database.DrainTimeout != 0 {
				ctx, cancel = context.WithTimeout(ctx, registry.config.Database.DrainTimeout)
				defer cancel()
			}

			if err := registry.app.GracefulShutdown(ctx); err != nil {
				return err
			}
		}

		l.Info("graceful shutdown successful")
		return nil
	}
}

func getTLSConfig(ctx context.Context, config configuration.TLS, http2Disabled bool) (*tls.Config, error) {
	if config.Certificate == "" && config.LetsEncrypt.CacheFile == "" {
		return nil, errSkipTLSConfig
	}

	tlsMinVersion, ok := tlsLookup[config.MinimumTLS]
	if !ok {
		return nil, fmt.Errorf("unknown minimum TLS level %q specified for http.tls.minimumtls", config.MinimumTLS)
	}

	if config.MinimumTLS != "" {
		dcontext.GetLogger(ctx).WithFields(log.Fields{"minimum_tls": config.MinimumTLS}).Info("restricting minimum TLS version")
	}

	var tlsCipherSuites []uint16
	// configuring cipher suites are no longer supported after the tls1.3.
	if tlsMinVersion > tls.VersionTLS12 {
		dcontext.GetLogger(ctx).Warnf("ignoring configured TLS cipher suites as they are not configurable in %s", config.MinimumTLS)
	} else {
		tlsCipherSuites, err := getCipherSuites(config.CipherSuites)
		if err != nil {
			return nil, fmt.Errorf("parsing cipher suites: %w", err)
		}
		dcontext.GetLogger(ctx).Infof("restricting TLS cipher suites to: %s", strings.Join(getCipherSuiteNames(tlsCipherSuites), ","))
	}

	// nolint gosec,G402
	tlsConf := &tls.Config{
		ClientAuth:               tls.NoClientCert,
		NextProtos:               nextProtos(http2Disabled),
		MinVersion:               tlsMinVersion,
		PreferServerCipherSuites: true,
		CipherSuites:             tlsCipherSuites,
	}

	if config.LetsEncrypt.CacheFile != "" {
		if config.Certificate != "" {
			return nil, fmt.Errorf("cannot specify both certificate and Let's Encrypt")
		}
		m := &autocert.Manager{
			HostPolicy: autocert.HostWhitelist(config.LetsEncrypt.Hosts...),
			Cache:      autocert.DirCache(config.LetsEncrypt.CacheFile),
			Email:      config.LetsEncrypt.Email,
			Prompt:     autocert.AcceptTOS,
		}
		tlsConf.GetCertificate = m.GetCertificate
		tlsConf.NextProtos = append(tlsConf.NextProtos, acme.ALPNProto)
	} else {
		var err error
		tlsConf.Certificates = make([]tls.Certificate, 1)
		tlsConf.Certificates[0], err = tls.LoadX509KeyPair(config.Certificate, config.Key)
		if err != nil {
			return nil, err
		}
	}

	if len(config.ClientCAs) != 0 {
		pool := x509.NewCertPool()

		for _, ca := range config.ClientCAs {
			// nolint: gosec
			caPem, err := os.ReadFile(ca)
			if err != nil {
				return nil, err
			}

			if ok := pool.AppendCertsFromPEM(caPem); !ok {
				return nil, fmt.Errorf("could not add CA to pool")
			}
		}

		for _, subj := range pool.Subjects() {
			dcontext.GetLogger(ctx).WithFields(log.Fields{"ca_subject": string(subj)}).Debug("client CA subject")
		}

		tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConf.ClientCAs = pool
	}

	return tlsConf, nil
}

func configureReporting(config *configuration.Configuration, h http.Handler) (http.Handler, error) {
	handler := h

	if config.Reporting.Sentry.Enabled {
		if err := errortracking.Initialize(
			errortracking.WithSentryDSN(config.Reporting.Sentry.DSN),
			errortracking.WithSentryEnvironment(config.Reporting.Sentry.Environment),
			errortracking.WithVersion(version.Version),
		); err != nil {
			return nil, fmt.Errorf("failed to configure Sentry: %w", err)
		}

		handler = errortracking.NewHandler(handler)
	}

	return handler, nil
}

// configureLogging prepares the context with a logger using the configuration.
func configureLogging(ctx context.Context, config *configuration.Configuration) (context.Context, error) {
	// We need to set the GITLAB_ISO8601_LOG_TIMESTAMP env var so that LabKit will use ISO 8601 timestamps with
	// millisecond precision instead of the logrus default format (RFC3339).
	envVar := "GITLAB_ISO8601_LOG_TIMESTAMP"
	if err := os.Setenv(envVar, "true"); err != nil {
		return nil, fmt.Errorf("unable to set environment variable %q: %w", envVar, err)
	}

	// the registry doesn't log to a file, so we can ignore the io.Closer (noop) returned by LabKit (we could also
	// ignore the error, but keeping it for future proofing)
	if _, err := logkit.Initialize(
		logkit.WithFormatter(config.Log.Formatter.String()),
		logkit.WithLogLevel(config.Log.Level.String()),
		logkit.WithOutputName(config.Log.Output.String()),
	); err != nil {
		return nil, err
	}

	if len(config.Log.Fields) > 0 {
		// build up the static fields, if present.
		var fields []any
		for k := range config.Log.Fields {
			fields = append(fields, k)
		}

		ctx = dcontext.WithValues(ctx, config.Log.Fields)
		ctx = dcontext.WithLogger(ctx, dcontext.GetLogger(ctx, fields...))
	}

	return ctx, nil
}

func configureAccessLogging(config *configuration.Configuration, h http.Handler) (http.Handler, error) {
	if config.Log.AccessLog.Disabled {
		return h, nil
	}

	logger := log.New()
	// the registry doesn't log to a file, so we can ignore the io.Closer (noop) returned by LabKit (we could also
	// ignore the error, but keeping it for future proofing)
	if _, err := logkit.Initialize(
		logkit.WithLogger(logger),
		logkit.WithFormatter(config.Log.AccessLog.Formatter.String()),
		logkit.WithOutputName(config.Log.Output.String()),
	); err != nil {
		return nil, err
	}

	// This func is used by WithExtraFields to add additional fields to the logger
	extraFieldGenerator := func(r *http.Request) log.Fields {
		fields := make(log.Fields)
		// Set the `CF-ray` header to the log field only if it exists in the request
		if rv, ok := r.Header[http.CanonicalHeaderKey(dcontext.CFRayIDHeader)]; ok && rv != nil {
			fields[string(dcontext.CFRayIDLogKey)] = r.Header.Get(dcontext.CFRayIDHeader)
		}
		return fields
	}

	return logkit.AccessLogger(h, logkit.WithAccessLogger(logger), logkit.WithExtraFields(extraFieldGenerator)), nil
}

func configureMonitoring(ctx context.Context, config *configuration.Configuration) ([]monitoring.Option, error) {
	l := dcontext.GetLogger(ctx)

	var opts []monitoring.Option
	addr := config.HTTP.Debug.Addr

	ln, err := listener.NewListener("tcp", addr)
	if err != nil {
		return nil, err
	}

	if addr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/vars", expvar.Handler().ServeHTTP)
		mux.HandleFunc("/debug/health", health.StatusHandler)
		l.WithFields(log.Fields{"address": addr, "path": "/debug/health"}).Info("starting health checker")

		opts = []monitoring.Option{
			monitoring.WithServeMux(mux),
		}

		if config.HTTP.Debug.Prometheus.Enabled {
			opts = append(opts, monitoring.WithMetricsHandlerPattern(config.HTTP.Debug.Prometheus.Path))
			opts = append(opts, monitoring.WithBuildInformation(version.Version, version.BuildTime))
			opts = append(opts, monitoring.WithBuildExtraLabels(map[string]string{
				"package":  version.Package,
				"revision": version.Revision,
			}))
			l.WithFields(log.Fields{"address": addr, "path": config.HTTP.Debug.Prometheus.Path}).Info("starting Prometheus listener")
		} else {
			opts = append(opts, monitoring.WithoutMetrics())
		}

		if config.HTTP.Debug.Pprof.Enabled {
			l.WithFields(log.Fields{"address": addr, "path": "/debug/pprof/"}).Info("starting pprof listener")
		} else {
			opts = append(opts, monitoring.WithoutPprof())
		}

		if config.HTTP.Debug.TLS.Enabled {
			tlsConf, err := getTLSConfig(ctx, configuration.TLS{
				Certificate: config.HTTP.Debug.TLS.Certificate,
				Key:         config.HTTP.Debug.TLS.Key,
				ClientCAs:   config.HTTP.Debug.TLS.ClientCAs,
				MinimumTLS:  config.HTTP.Debug.TLS.MinimumTLS,
			}, config.HTTP.HTTP2.Disabled)
			if err != nil {
				l.WithError(err).Warn("failed to configure TLS for debug server")
			} else {
				ln = tls.NewListener(ln, tlsConf)
				l.Info("configured TLS for debug server")
			}
		}

		// set listener here so that TLS is configured properly if enabled
		opts = append(opts, monitoring.WithListener(ln))
	} else {
		opts = []monitoring.Option{
			monitoring.WithoutMetrics(),
			monitoring.WithoutPprof(),
		}
	}

	if config.Profiling.Stackdriver.Enabled {
		opts = append(opts, monitoring.WithProfilerCredentialsFile(config.Profiling.Stackdriver.KeyFile))
		if err := configureStackdriver(config); err != nil {
			log.WithError(err).Error("failed to configure Stackdriver profiler")
			return opts, nil
		}
		log.Info("starting Stackdriver profiler")
	} else {
		opts = append(opts, monitoring.WithoutContinuousProfiling())
	}

	return opts, nil
}

func configureStackdriver(config *configuration.Configuration) error {
	if !config.Profiling.Stackdriver.Enabled {
		return nil
	}

	// the GITLAB_CONTINUOUS_PROFILING env var (as per the LabKit spec) takes precedence over any application
	// configuration settings and is required to configure the Stackdriver service.
	envVar := "GITLAB_CONTINUOUS_PROFILING"
	var service, serviceVersion, projectID string

	// if it's not set then we must set it based on the registry settings, with URL encoded settings for Stackdriver,
	// see https://pkg.go.dev/gitlab.com/gitlab-org/labkit/monitoring?tab=doc for details.
	if _, ok := os.LookupEnv(envVar); !ok {
		service = config.Profiling.Stackdriver.Service
		serviceVersion = config.Profiling.Stackdriver.ServiceVersion
		projectID = config.Profiling.Stackdriver.ProjectID

		u, err := url.Parse("stackdriver")
		if err != nil {
			// this should never happen
			return fmt.Errorf("failed to parse base URL: %w", err)
		}

		q := u.Query()
		if service != "" {
			q.Add("service", service)
		}
		if serviceVersion != "" {
			q.Add("service_version", serviceVersion)
		}
		if projectID != "" {
			q.Add("project_id", projectID)
		}
		u.RawQuery = q.Encode()

		log.WithFields(log.Fields{"name": envVar, "value": u.String()}).Debug("setting environment variable")
		if err := os.Setenv(envVar, u.String()); err != nil {
			return fmt.Errorf("unable to set environment variable %q: %w", envVar, err)
		}
	}

	return nil
}

// panicHandler add an HTTP handler to web app. The handler recover the happening
// panic. logrus.Panic transmits panic message to pre-config log hooks, which is
// defined in config.yml.
func panicHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// nolint: revive // deep-exit
				log.WithField("stack", string(debug.Stack())).Panic(fmt.Sprintf("%v", err))
			}
		}()
		handler.ServeHTTP(w, r)
	})
}

// alive simply wraps the handler with a route that always returns an http 200
// response when the path is matched. If the path is not matched, the request
// is passed to the provided handler. There is no guarantee of anything but
// that the server is up. Wrap with other handlers (such as health.Handler)
// for greater affect.
func alive(path string, handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
			w.Header().Set("Cache-Control", "no-cache")
			w.WriteHeader(http.StatusOK)
			return
		}

		handler.ServeHTTP(w, r)
	})
}

func resolveConfiguration(args []string, opts ...configuration.ParseOption) (*configuration.Configuration, error) {
	var configurationPath string

	if len(args) > 0 {
		configurationPath = args[0]
	} else if os.Getenv("REGISTRY_CONFIGURATION_PATH") != "" {
		configurationPath = os.Getenv("REGISTRY_CONFIGURATION_PATH")
	}

	if configurationPath == "" {
		return nil, fmt.Errorf("configuration path unspecified")
	}

	// nolint: gosec
	fp, err := os.Open(configurationPath)
	if err != nil {
		return nil, err
	}

	defer fp.Close()

	config, err := configuration.Parse(fp, opts...)
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", configurationPath, err)
	}

	if err := validate(config); err != nil {
		return nil, fmt.Errorf("validation: %w", err)
	}

	return config, nil
}

func validate(config *configuration.Configuration) error {
	var errs *multierror.Error

	// Validate redirect section.
	if redirectConfig, ok := config.Storage["redirect"]; ok {
		if v, ok := redirectConfig["disable"]; ok {
			if _, ok := v.(bool); !ok {
				errs = multierror.Append(fmt.Errorf("invalid type %[1]T for 'storage.redirect.disable' (boolean)", v))
			}
		}
		if v, ok := redirectConfig["expirydelay"]; ok {
			switch val := v.(type) {
			case time.Duration:
			case string:
				// nolint: revive // max-control-nesting
				if _, err := time.ParseDuration(val); err != nil {
					errs = multierror.Append(fmt.Errorf("%q value for 'storage.redirect.expirydelay' is not a valid duration", val))
				}
			default:
				errs = multierror.Append(fmt.Errorf("invalid type %[1]T for 'storage.redirect.expirydelay' (duration)", v))
			}
		}
	}

	//  Validate and/or Log potential issues with azure `trimlegacyrootprefix` and `legacyrootprefix` configuration options.
	if ac, ok := config.Storage["azure"]; ok {
		var legacyPrefix, legacyPrefixIsBool, trimLegacyPrefix, trimLegacyPrefixIsBool bool

		// assert `trimlegacyrootprefix` can be represented as a boolean
		trimLegacyPrefixI, trimLegacyPrefixExist := ac["trimlegacyrootprefix"]
		if trimLegacyPrefixExist {
			if trimLegacyPrefix, trimLegacyPrefixIsBool = trimLegacyPrefixI.(bool); !trimLegacyPrefixIsBool {
				errs = multierror.Append(fmt.Errorf("invalid type %[1]T for 'storage.azure.trimlegacyrootprefix' (boolean)", trimLegacyPrefix))
			}
		}

		// assert `legacyrootprefix` can be represented as a boolean
		legacyPrefixI, legacyPrefixExist := ac["legacyrootprefix"]
		if legacyPrefixExist {
			if legacyPrefix, legacyPrefixIsBool = legacyPrefixI.(bool); !legacyPrefixIsBool {
				errs = multierror.Append(fmt.Errorf("invalid type %[1]T for 'storage.azure.legacyrootprefix' (boolean)", legacyPrefix))
			}
		}

		switch {
		// both parameters exist, check for conflict:
		case trimLegacyPrefixExist && legacyPrefixExist:
			// while it is allowed to set both configs (as long as they do not conflict), setting only one is sufficient.
			dlog.GetLogger().Warn("Both 'storage.azure.legacyrootprefix' and 'storage.azure.trimlegacyrootprefix' are set. It is recommended to set one or the other, rather than both.")

			// conflict: user explicitly enabled legacyrootprefix but also enabled trimlegacyrootprefix (or disabled both).
			if legacyPrefixIsBool && trimLegacyPrefixIsBool {
				// nolint: revive // max-control-nesting
				if legacyPrefix == trimLegacyPrefix {
					errs = multierror.Append(fmt.Errorf("storage.azure.trimlegacyrootprefix' and  'storage.azure.trimlegacyrootprefix' can not both be %v", legacyPrefix))
				}
			}

		// both parameters do not exist, warn user that we will be using the default (i.e storage.azure.legacyrootprefix=false aka storage.azure.trimlegacyrootprefix=true):
		case !trimLegacyPrefixExist && !legacyPrefixExist:
			dlog.GetLogger().Warn("A configuration parameter for 'storage.azure.legacyrootprefix' or 'storage.azure.trimlegacyrootprefix' was not specified. The azure driver will default to using the standard root prefix: \"/\" ")

		// one parameter does not exist, while the other does
		case !trimLegacyPrefixExist || !legacyPrefixExist:
			// nothing to do here
		}
	}

	return errs.ErrorOrNil()
}

func nextProtos(http2Disabled bool) []string {
	switch http2Disabled {
	case true:
		return []string{"http/1.1"}
	default:
		return []string{"h2", "http/1.1"}
	}
}

func dbFromConfig(config *configuration.Configuration) (*datastore.DB, error) {
	return datastore.NewConnector().Open(context.Background(), &datastore.DSN{
		Host:        config.Database.Host,
		Port:        config.Database.Port,
		User:        config.Database.User,
		Password:    config.Database.Password,
		DBName:      config.Database.DBName,
		SSLMode:     config.Database.SSLMode,
		SSLCert:     config.Database.SSLCert,
		SSLKey:      config.Database.SSLKey,
		SSLRootCert: config.Database.SSLRootCert,
	},
		datastore.WithLogger(log.WithFields(log.Fields{"database": config.Database.DBName})),
		datastore.WithLogLevel(config.Log.Level),
		datastore.WithPoolConfig(&datastore.PoolConfig{
			MaxIdle:     config.Database.Pool.MaxIdle,
			MaxOpen:     config.Database.Pool.MaxOpen,
			MaxLifetime: config.Database.Pool.MaxLifetime,
			MaxIdleTime: config.Database.Pool.MaxIdleTime,
		}),
		datastore.WithPreparedStatements(config.Database.PreparedStatements),
	)
}

// migrationDBFromConfig returns a DB instance specifically configured for running database migrations.
// The returned DB instance targets the primary database (when specified) and uses only a single connection.
func migrationDBFromConfig(config *configuration.Configuration) (*datastore.DB, error) {
	// deep copy the config object so we don't mistakenly modify the pointer
	primaryConfig := *config

	if config.Database.Primary != "" {
		primaryConfig.Database.Host = config.Database.Primary
		log.WithFields(log.Fields{"host": config.Database.Primary}).Info("database connection to primary host")

		// Override database.pool.maxopen to 1 when applying migrations.
		// This is not guaranteed to work forever, but for now, it is an effective workaround to enforce a single connection.
		// https://gitlab.com/gitlab-org/container-registry/-/issues/922
		primaryConfig.Database.Pool.MaxOpen = 1
	}

	return dbFromConfig(&primaryConfig)
}

// takes a list of cipher suites and converts it to a list of respective tls constants
// if an empty list is provided, then the defaults will be used
func getCipherSuites(names []string) ([]uint16, error) {
	cipherSuiteConsts := make([]uint16, len(names))
	for i, name := range names {
		cipherSuiteConst, ok := cipherSuites[name]
		if !ok {
			return nil, fmt.Errorf("unknown TLS cipher suite %q specified for http.tls.ciphersuites", name)
		}
		cipherSuiteConsts[i] = cipherSuiteConst
	}
	return cipherSuiteConsts, nil
}

// takes a list of cipher suite ids and converts it to a list of respective names
func getCipherSuiteNames(ids []uint16) []string {
	if len(ids) == 0 {
		return nil
	}
	names := make([]string, len(ids))
	for i, id := range ids {
		names[i] = tls.CipherSuiteName(id)
	}
	return names
}
