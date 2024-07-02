package handlers

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/health"
	"github.com/docker/distribution/health/checks"
	"github.com/docker/distribution/internal/feature"
	dlog "github.com/docker/distribution/log"
	prometheus "github.com/docker/distribution/metrics"
	"github.com/docker/distribution/notifications"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	"github.com/docker/distribution/registry/api/urls"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/bbm"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/gc"
	"github.com/docker/distribution/registry/gc/worker"
	"github.com/docker/distribution/registry/internal"
	redismetrics "github.com/docker/distribution/registry/internal/metrics/redis"
	registrymiddleware "github.com/docker/distribution/registry/middleware/registry"
	repositorymiddleware "github.com/docker/distribution/registry/middleware/repository"
	"github.com/docker/distribution/registry/storage"
	memorycache "github.com/docker/distribution/registry/storage/cache/memory"
	rediscache "github.com/docker/distribution/registry/storage/cache/redis"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	storagemiddleware "github.com/docker/distribution/registry/storage/driver/middleware"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/docker/distribution/version"
	gocache "github.com/eko/gocache/lib/v4/cache"
	libstore "github.com/eko/gocache/lib/v4/store"
	redisstore "github.com/eko/gocache/store/redis/v4"
	"github.com/getsentry/sentry-go"
	"github.com/gorilla/mux"
	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/errortracking"
	metricskit "gitlab.com/gitlab-org/labkit/metrics"
	"gitlab.com/gitlab-org/labkit/metrics/sqlmetrics"
)

// randomSecretSize is the number of random bytes to generate if no secret
// was specified.
const randomSecretSize = 32

// defaultCheckInterval is the default time in between health checks
const defaultCheckInterval = 10 * time.Second

// redisCacheTTL is the global expiry duration for objects cached in Redis.
const redisCacheTTL = 6 * time.Hour

// App is a global registry application object. Shared resources can be placed
// on this object that will be accessible from all requests. Any writable
// fields should be protected.
type App struct {
	context.Context

	Config *configuration.Configuration

	router   *metaRouter                 // router dispatcher while we consolidate into the new router
	driver   storagedriver.StorageDriver // driver maintains the app global storage driver instance.
	db       datastore.LoadBalancer      // db is the global database handle used across the app.
	registry distribution.Namespace      // registry is the primary registry backend for the app instance.

	repoRemover      distribution.RepositoryRemover // repoRemover provides ability to delete repos
	accessController auth.AccessController          // main access controller for application

	// httpHost is a parsed representation of the http.host parameter from
	// the configuration. Only the Scheme and Host fields are used.
	httpHost url.URL

	// events contains notification related configuration.
	events struct {
		sink   notifications.Sink
		source notifications.SourceRecord
	}

	redis redis.UniversalClient

	// readOnly is true if the registry is in a read-only maintenance mode
	readOnly bool

	manifestURLs validation.ManifestURLs

	manifestRefLimit         int
	manifestPayloadSizeLimit int

	// redisCache is the interface for manipulating cached data on Redis.
	redisCache *gocache.Cache[any]
}

// NewApp takes a configuration and returns a configured app, ready to serve
// requests. The app only implements ServeHTTP and can be wrapped in other
// handlers accordingly.
func NewApp(ctx context.Context, config *configuration.Configuration) (*App, error) {
	app := &App{
		Config:  config,
		Context: ctx,
	}

	if err := app.initMetaRouter(); err != nil {
		return nil, fmt.Errorf("initing metaRouter: %w", err)
	}

	storageParams := config.Storage.Parameters()
	if storageParams == nil {
		storageParams = make(configuration.Parameters)
	}

	var err error
	app.driver, err = factory.Create(config.Storage.Type(), storageParams)
	if err != nil {
		return nil, err
	}

	log := dcontext.GetLogger(app)

	purgeConfig := uploadPurgeDefaultConfig()
	if mc, ok := config.Storage["maintenance"]; ok {
		if v, ok := mc["uploadpurging"]; ok {
			purgeConfig, ok = v.(map[interface{}]interface{})
			if !ok {
				return nil, fmt.Errorf("uploadpurging config key must contain additional keys")
			}
		}
		if v, ok := mc["readonly"]; ok {
			readOnly, ok := v.(map[interface{}]interface{})
			if !ok {
				return nil, fmt.Errorf("readonly config key must contain additional keys")
			}
			if readOnlyEnabled, ok := readOnly["enabled"]; ok {
				app.readOnly, ok = readOnlyEnabled.(bool)
				if !ok {
					return nil, fmt.Errorf("readonly's enabled config key must have a boolean value")
				}

				if app.readOnly {
					if enabled, ok := purgeConfig["enabled"].(bool); ok && enabled {
						log.Info("disabled upload purging in readonly mode")
						purgeConfig["enabled"] = false
					}
				}
			}
		}
	}

	if err := startUploadPurger(app, app.driver, log, purgeConfig); err != nil {
		return nil, err
	}

	app.driver, err = applyStorageMiddleware(app.driver, config.Middleware["storage"])
	if err != nil {
		return nil, err
	}

	if err := app.configureSecret(config); err != nil {
		return nil, err
	}
	app.configureEvents(config)
	app.configureRedis(config)

	if err := app.configureRedisCache(ctx, config); err != nil {
		// Because the Redis cache is not a strictly required dependency (data will be served from the metadata DB if
		// we're unable to serve or find it in cache) we simply log and report a failure here and proceed to not prevent
		// the app from starting.
		log.WithError(err).Error("failed configuring Redis cache")
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
	}

	if err := app.configureRedisRateLimiter(ctx, config); err != nil {
		// Redis rate-limiter is not a strictly required dependency, we simply log and report a failure here
		// and proceed to not prevent the app from starting.
		log.WithError(err).Error("failed configuring Redis rate-limiter")
		errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
	}

	options := registrymiddleware.GetRegistryOptions()

	// TODO: Once schema1 code is removed throughout the registry, we will not
	// need to explicitly configure this.
	options = append(options, storage.DisableSchema1Pulls)

	if config.HTTP.Host != "" {
		u, err := url.Parse(config.HTTP.Host)
		if err != nil {
			return nil, fmt.Errorf(`could not parse http "host" parameter: %w`, err)
		}
		app.httpHost = *u
	}

	// configure deletion
	if d, ok := config.Storage["delete"]; ok {
		e, ok := d["enabled"]
		if ok {
			if deleteEnabled, ok := e.(bool); ok && deleteEnabled {
				options = append(options, storage.EnableDelete)
			}
		}
	}

	// configure redirects
	var redirectDisabled bool
	if redirectConfig, ok := config.Storage["redirect"]; ok {
		v := redirectConfig["disable"]
		switch v := v.(type) {
		case bool:
			redirectDisabled = v
		case nil:
			// disable is not mandatory as we default to false, so do nothing if it doesn't exist
		default:
			return nil, fmt.Errorf("invalid type %T for 'storage.redirect.disable' (boolean)", v)
		}
	}
	if redirectDisabled {
		log.Info("backend redirection disabled")
	} else {
		l := log
		exceptions := config.Storage["redirect"]["exceptions"]
		if exceptions, ok := exceptions.([]interface{}); ok && len(exceptions) > 0 {
			s := make([]string, len(exceptions))
			for i, v := range exceptions {
				s[i] = fmt.Sprint(v)
			}

			l.WithField("exceptions", s)

			options = append(options, storage.EnableRedirectWithExceptions(s))
		} else {
			options = append(options, storage.EnableRedirect)
		}

		// expiry delay
		delay := config.Storage["redirect"]["expirydelay"]
		var d time.Duration

		switch v := delay.(type) {
		case time.Duration:
			d = v
		case string:
			if d, err = time.ParseDuration(v); err != nil {
				return nil, fmt.Errorf("%q value for 'storage.redirect.expirydelay' is not a valid duration", v)
			}
		case nil:
		default:
			return nil, fmt.Errorf("invalid type %[1]T for 'storage.redirect.expirydelay' (duration)", delay)
		}
		if d > 0 {
			l = l.WithField("expiry_delay_s", d.Seconds())
			options = append(options, storage.WithRedirectExpiryDelay(d))
		}

		l.Info("storage backend redirection enabled")
	}

	if !config.Validation.Enabled {
		config.Validation.Enabled = !config.Validation.Disabled
	}

	// configure validation
	if config.Validation.Enabled {
		if len(config.Validation.Manifests.URLs.Allow) == 0 && len(config.Validation.Manifests.URLs.Deny) == 0 {
			// If Allow and Deny are empty, allow nothing.
			app.manifestURLs.Allow = regexp.MustCompile("^$")
			options = append(options, storage.ManifestURLsAllowRegexp(app.manifestURLs.Allow))
		} else {
			if len(config.Validation.Manifests.URLs.Allow) > 0 {
				for i, s := range config.Validation.Manifests.URLs.Allow {
					// Validate via compilation.
					if _, err := regexp.Compile(s); err != nil {
						return nil, fmt.Errorf("validation.manifests.urls.allow: %w", err)
					}
					// Wrap with non-capturing group.
					config.Validation.Manifests.URLs.Allow[i] = fmt.Sprintf("(?:%s)", s)
				}
				app.manifestURLs.Allow = regexp.MustCompile(strings.Join(config.Validation.Manifests.URLs.Allow, "|"))
				options = append(options, storage.ManifestURLsAllowRegexp(app.manifestURLs.Allow))
			}
			if len(config.Validation.Manifests.URLs.Deny) > 0 {
				for i, s := range config.Validation.Manifests.URLs.Deny {
					// Validate via compilation.
					if _, err := regexp.Compile(s); err != nil {
						return nil, fmt.Errorf("validation.manifests.urls.deny: %w", err)
					}
					// Wrap with non-capturing group.
					config.Validation.Manifests.URLs.Deny[i] = fmt.Sprintf("(?:%s)", s)
				}
				app.manifestURLs.Deny = regexp.MustCompile(strings.Join(config.Validation.Manifests.URLs.Deny, "|"))
				options = append(options, storage.ManifestURLsDenyRegexp(app.manifestURLs.Deny))
			}
		}

		app.manifestRefLimit = config.Validation.Manifests.ReferenceLimit
		options = append(options, storage.ManifestReferenceLimit(app.manifestRefLimit))

		app.manifestPayloadSizeLimit = config.Validation.Manifests.PayloadSizeLimit
		options = append(options, storage.ManifestPayloadSizeLimit(app.manifestPayloadSizeLimit))
	}

	// Connect to the metadata database, if enabled.
	if config.Database.Enabled {
		log.Warn("the metadata database is a beta feature, please carefully review the documentation before enabling it in production")

		if config.GC.Disabled {
			log.Warn("garbage collection is disabled")
		}

		// Do not write or check for repository layer link metadata on the filesystem when the database is enabled.
		options = append(options, storage.UseDatabase)

		dsn := &datastore.DSN{
			Host:           config.Database.Host,
			Port:           config.Database.Port,
			User:           config.Database.User,
			Password:       config.Database.Password,
			DBName:         config.Database.DBName,
			SSLMode:        config.Database.SSLMode,
			SSLCert:        config.Database.SSLCert,
			SSLKey:         config.Database.SSLKey,
			SSLRootCert:    config.Database.SSLRootCert,
			ConnectTimeout: config.Database.ConnectTimeout,
		}

		dbOpts := []datastore.Option{
			datastore.WithLogger(log.WithFields(logrus.Fields{"database": config.Database.DBName})),
			datastore.WithLogLevel(config.Log.Level),
			datastore.WithPreparedStatements(config.Database.PreparedStatements),
			datastore.WithPoolConfig(&datastore.PoolConfig{
				MaxIdle:     config.Database.Pool.MaxIdle,
				MaxOpen:     config.Database.Pool.MaxOpen,
				MaxLifetime: config.Database.Pool.MaxLifetime,
				MaxIdleTime: config.Database.Pool.MaxIdleTime,
			}),
		}

		if config.Database.LoadBalancing.Enabled {
			if len(config.Database.LoadBalancing.Hosts) > 0 {
				hosts := config.Database.LoadBalancing.Hosts
				log.WithField("hosts", hosts).Info("enabling database load balancing")
				dbOpts = append(dbOpts, datastore.WithLoadBalancingHosts(hosts))
			}
		}

		db, err := datastore.NewDBLoadBalancer(dsn, dbOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize database connections: %w", err)
		}

		// Skip postdeployment migrations to prevent pending post deployment
		// migrations from preventing the registry from starting.
		m := migrations.NewMigrator(db.Primary().DB, migrations.SkipPostDeployment)
		pending, err := m.HasPending()
		if err != nil {
			return nil, fmt.Errorf("failed to check database migrations status: %w", err)
		}
		if pending {
			return nil, fmt.Errorf("there are pending database migrations, use the 'registry database migrate' CLI " +
				"command to check and apply them")
		}

		app.db = db
		options = append(options, storage.Database(app.db))

		if config.HTTP.Debug.Prometheus.Enabled {
			// Expose database metrics to prometheus.
			// TODO(dlb): collect metrics for all hosts
			collector := sqlmetrics.NewDBStatsCollector(config.Database.DBName, db.Primary())
			promclient.MustRegister(collector)
		}

		// update online GC settings (if needed) in the background to avoid delaying the app start
		go func() {
			if err := updateOnlineGCSettings(app.Context, app.db.Primary(), config); err != nil {
				errortracking.Capture(err, errortracking.WithContext(app.Context), errortracking.WithStackTrace())
				log.WithError(err).Error("failed to update online GC settings")
			}
		}()

		startOnlineGC(app.Context, app.db.Primary(), app.driver, config)

		// Now that we've started the database successfully, lock the filesystem
		// to signal that this object storage needs to be managed by the database.
		dbLock := storage.DatabaseInUseLocker{Driver: app.driver}
		if err := dbLock.Lock(app.Context); err != nil {
			// Right now, the server doesn't make use of this lock, only the offline
			// garbage collector reads this file, so we are fee to log a warning
			// and move on with spinning up the application.
			log.WithError(err).Warn("failed to mark filesystem for database only usage, continuing")
		}
		if config.Database.BackgroundMigrations.Enabled && feature.BBMProcess.Enabled() {
			startBackgroundMigrations(app.Context, app.db.Primary(), config)
		}
	}

	// configure storage caches
	// It's possible that the metadata database will fill the same original need
	// as the blob descriptor cache (avoiding slow and/or expensive calls to
	// external storage) and enable the cache to be removed long term.
	//
	// For now, disabling concurrent use of the metadata database and cache
	// decreases the surface area which we are testing during database development.
	if cc, ok := config.Storage["cache"]; ok && !config.Database.Enabled {
		v, ok := cc["blobdescriptor"]
		if !ok {
			// Backwards compatible: "layerinfo" == "blobdescriptor"
			v = cc["layerinfo"]
		}

		switch v {
		case "redis":
			if app.redis == nil {
				return nil, fmt.Errorf("redis configuration required to use for layerinfo cache")
			}
			cacheProvider := rediscache.NewRedisBlobDescriptorCacheProvider(app.redis)
			localOptions := append(options, storage.BlobDescriptorCacheProvider(cacheProvider))
			app.registry, err = storage.NewRegistry(app, app.driver, localOptions...)
			if err != nil {
				return nil, fmt.Errorf("could not create registry: %w", err)
			}
			log.Info("using redis blob descriptor cache")
		case "inmemory":
			cacheProvider := memorycache.NewInMemoryBlobDescriptorCacheProvider()
			localOptions := append(options, storage.BlobDescriptorCacheProvider(cacheProvider))
			app.registry, err = storage.NewRegistry(app, app.driver, localOptions...)
			if err != nil {
				return nil, fmt.Errorf("could not create registry: %w", err)
			}
			log.Info("using inmemory blob descriptor cache")
		default:
			if v != "" {
				log.WithField("type", config.Storage["cache"]).Warn("unknown cache type, caching disabled")
			}
		}
	} else if ok && config.Database.Enabled {
		log.Warn("blob descriptor cache is not compatible with metadata database, caching disabled")
	}

	if app.registry == nil {
		// configure the registry if no cache section is available.
		app.registry, err = storage.NewRegistry(app.Context, app.driver, options...)
		if err != nil {
			return nil, fmt.Errorf("could not create registry: %w", err)
		}
	}

	app.registry, err = applyRegistryMiddleware(app, app.registry, config.Middleware["registry"])
	if err != nil {
		return nil, err
	}

	authType := config.Auth.Type()

	if authType != "" && !strings.EqualFold(authType, "none") {
		accessController, err := auth.GetAccessController(config.Auth.Type(), config.Auth.Parameters())
		if err != nil {
			return nil, fmt.Errorf("unable to configure authorization (%s): %w", authType, err)
		}
		app.accessController = accessController
		log.WithField("auth_type", authType).Debug("configured access controller")
	}

	var ok bool
	app.repoRemover, ok = app.registry.(distribution.RepositoryRemover)
	if !ok {
		log.Warn("registry does not implement RepositoryRemover. Will not be able to delete repos and tags")
	}

	return app, nil
}

var (
	onlineGCUpdateJitterMaxSeconds = 60
	onlineGCUpdateTimeout          = 2 * time.Second
	// for testing purposes (mocks)
	systemClock                internal.Clock = clock.New()
	gcSettingsStoreConstructor                = datastore.NewGCSettingsStore
)

func updateOnlineGCSettings(ctx context.Context, db datastore.Queryer, config *configuration.Configuration) error {
	if !config.Database.Enabled || config.GC.Disabled || (config.GC.Blobs.Disabled && config.GC.Manifests.Disabled) {
		return nil
	}
	if config.GC.ReviewAfter == 0 {
		return nil
	}

	d := config.GC.ReviewAfter
	// -1 means no review delay, so set it to 0 here
	if d == -1 {
		d = 0
	}

	log := dcontext.GetLogger(ctx)

	// execute DB update after a randomized jitter of up to 60 seconds to ease concurrency in clustered environments
	rand.Seed(systemClock.Now().UnixNano())
	/* #nosec G404 */
	jitter := time.Duration(rand.Intn(onlineGCUpdateJitterMaxSeconds)) * time.Second

	log.WithField("jitter_s", jitter.Seconds()).Info("preparing to update online GC settings")
	systemClock.Sleep(jitter)

	// set a tight timeout to avoid delaying the app start for too long, another instance is likely to succeed
	start := systemClock.Now()
	ctx2, cancel := context.WithDeadline(ctx, start.Add(onlineGCUpdateTimeout))
	defer cancel()

	// for now we use the same value for all events, so we simply update all rows in `gc_review_after_defaults`
	s := gcSettingsStoreConstructor(db)
	updated, err := s.UpdateAllReviewAfterDefaults(ctx2, d)
	if err != nil {
		return err
	}

	elapsed := systemClock.Since(start).Seconds()
	if updated {
		log.WithField("duration_s", elapsed).Info("online GC settings updated successfully")
	} else {
		log.WithField("duration_s", elapsed).Info("online GC settings are up to date")
	}

	return nil
}

func startOnlineGC(ctx context.Context, db *datastore.DB, storageDriver storagedriver.StorageDriver, config *configuration.Configuration) {
	if !config.Database.Enabled || config.GC.Disabled || (config.GC.Blobs.Disabled && config.GC.Manifests.Disabled) {
		return
	}

	l := dlog.GetLogger(dlog.WithContext(ctx))

	aOpts := []gc.AgentOption{
		gc.WithLogger(l),
	}
	if config.GC.NoIdleBackoff {
		aOpts = append(aOpts, gc.WithoutIdleBackoff())
	}
	if config.GC.MaxBackoff > 0 {
		aOpts = append(aOpts, gc.WithMaxBackoff(config.GC.MaxBackoff))
	}
	if config.GC.ErrorCooldownPeriod > 0 {
		aOpts = append(aOpts, gc.WithErrorCooldown(config.GC.ErrorCooldownPeriod))
	}

	var agents []*gc.Agent

	if !config.GC.Blobs.Disabled {
		bwOpts := []worker.BlobWorkerOption{
			worker.WithBlobLogger(l),
		}
		if config.GC.TransactionTimeout > 0 {
			bwOpts = append(bwOpts, worker.WithBlobTxTimeout(config.GC.TransactionTimeout))
		}
		if config.GC.Blobs.StorageTimeout > 0 {
			bwOpts = append(bwOpts, worker.WithBlobStorageTimeout(config.GC.Blobs.StorageTimeout))
		}
		bw := worker.NewBlobWorker(db, storageDriver, bwOpts...)

		baOpts := aOpts
		if config.GC.Blobs.Interval > 0 {
			baOpts = append(baOpts, gc.WithInitialInterval(config.GC.Blobs.Interval))
		}
		ba := gc.NewAgent(bw, baOpts...)
		agents = append(agents, ba)
	}

	if !config.GC.Manifests.Disabled {
		mwOpts := []worker.ManifestWorkerOption{
			worker.WithManifestLogger(l),
		}
		if config.GC.TransactionTimeout > 0 {
			mwOpts = append(mwOpts, worker.WithManifestTxTimeout(config.GC.TransactionTimeout))
		}
		mw := worker.NewManifestWorker(db, mwOpts...)

		maOpts := aOpts
		if config.GC.Manifests.Interval > 0 {
			maOpts = append(maOpts, gc.WithInitialInterval(config.GC.Manifests.Interval))
		}
		ma := gc.NewAgent(mw, maOpts...)
		agents = append(agents, ma)
	}

	for _, a := range agents {
		go func(a *gc.Agent) {
			// This function can only end in two situations: panic or context cancellation. If a panic occurs we should
			// log, report to Sentry and then re-panic, as the instance would be in an inconsistent/unknown state. In
			// case of context cancellation, the app is shutting down, so there is nothing to worry about.
			defer func() {
				if err := recover(); err != nil {
					l.WithFields(dlog.Fields{"error": err}).Error("online GC agent stopped with panic")
					sentry.CurrentHub().Recover(err)
					sentry.Flush(5 * time.Second)
					panic(err)
				}
			}()
			if err := a.Start(ctx); err != nil {
				if errors.Is(err, context.Canceled) {
					// leaving this here for now for additional confidence and improved observability
					l.Warn("shutting down online GC agent due due to context cancellation")
				} else {
					// this should never happen, but leaving it here for future proofing against bugs within Agent.Start
					errortracking.Capture(fmt.Errorf("online GC agent stopped with error: %w", err), errortracking.WithStackTrace())
					l.WithError(err).Error("online GC agent stopped")
				}
			}
		}(a)
	}
}

// RegisterHealthChecks is an awful hack to defer health check registration
// control to callers. This should only ever be called once per registry
// process, typically in a main function. The correct way would be register
// health checks outside of app, since multiple apps may exist in the same
// process. Because the configuration and app are tightly coupled,
// implementing this properly will require a refactor. This method may panic
// if called twice in the same process.
func (app *App) RegisterHealthChecks(healthRegistries ...*health.Registry) error {
	if len(healthRegistries) > 1 {
		return fmt.Errorf("RegisterHealthChecks called with more than one registry")
	}
	healthRegistry := health.DefaultRegistry
	if len(healthRegistries) == 1 {
		healthRegistry = healthRegistries[0]
	}

	if app.Config.Health.StorageDriver.Enabled {
		interval := app.Config.Health.StorageDriver.Interval
		if interval == 0 {
			interval = defaultCheckInterval
		}

		storageDriverCheck := func() error {
			_, err := app.driver.Stat(app, "/") // "/" should always exist
			if _, ok := err.(storagedriver.PathNotFoundError); ok {
				err = nil // pass this through, backend is responding, but this path doesn't exist.
			}
			return err
		}

		if app.Config.Health.StorageDriver.Threshold != 0 {
			healthRegistry.RegisterPeriodicThresholdFunc("storagedriver_"+app.Config.Storage.Type(), interval, app.Config.Health.StorageDriver.Threshold, storageDriverCheck)
		} else {
			healthRegistry.RegisterPeriodicFunc("storagedriver_"+app.Config.Storage.Type(), interval, storageDriverCheck)
		}
	}

	for _, fileChecker := range app.Config.Health.FileCheckers {
		interval := fileChecker.Interval
		if interval == 0 {
			interval = defaultCheckInterval
		}
		dcontext.GetLogger(app).Infof("configuring file health check path=%s, interval=%d", fileChecker.File, interval/time.Second)
		healthRegistry.Register(fileChecker.File, health.PeriodicChecker(checks.FileChecker(fileChecker.File), interval))
	}

	for _, httpChecker := range app.Config.Health.HTTPCheckers {
		interval := httpChecker.Interval
		if interval == 0 {
			interval = defaultCheckInterval
		}

		statusCode := httpChecker.StatusCode
		if statusCode == 0 {
			statusCode = 200
		}

		checker := checks.HTTPChecker(httpChecker.URI, statusCode, httpChecker.Timeout, httpChecker.Headers)

		if httpChecker.Threshold != 0 {
			dcontext.GetLogger(app).Infof("configuring HTTP health check uri=%s, interval=%d, threshold=%d", httpChecker.URI, interval/time.Second, httpChecker.Threshold)
			healthRegistry.Register(httpChecker.URI, health.PeriodicThresholdChecker(checker, interval, httpChecker.Threshold))
		} else {
			dcontext.GetLogger(app).Infof("configuring HTTP health check uri=%s, interval=%d", httpChecker.URI, interval/time.Second)
			healthRegistry.Register(httpChecker.URI, health.PeriodicChecker(checker, interval))
		}
	}

	for _, tcpChecker := range app.Config.Health.TCPCheckers {
		interval := tcpChecker.Interval
		if interval == 0 {
			interval = defaultCheckInterval
		}

		checker := checks.TCPChecker(tcpChecker.Addr, tcpChecker.Timeout)

		if tcpChecker.Threshold != 0 {
			dcontext.GetLogger(app).Infof("configuring TCP health check addr=%s, interval=%d, threshold=%d", tcpChecker.Addr, interval/time.Second, tcpChecker.Threshold)
			healthRegistry.Register(tcpChecker.Addr, health.PeriodicThresholdChecker(checker, interval, tcpChecker.Threshold))
		} else {
			dcontext.GetLogger(app).Infof("configuring TCP health check addr=%s, interval=%d", tcpChecker.Addr, interval/time.Second)
			healthRegistry.Register(tcpChecker.Addr, health.PeriodicChecker(checker, interval))
		}
	}
	return nil
}

var routeMetricsMiddleware = metricskit.NewHandlerFactory(
	metricskit.WithNamespace(prometheus.NamespacePrefix),
	metricskit.WithLabels("route"),
	// Keeping the same buckets used before LabKit, as defined in
	// https://github.com/docker/go-metrics/blob/b619b3592b65de4f087d9f16863a7e6ff905973c/handler.go#L31:L32
	metricskit.WithRequestDurationBuckets([]float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 25, 60}),
	metricskit.WithByteSizeBuckets(promclient.ExponentialBuckets(1024, 2, 22)), //1K to 4G
)

// register a handler with the application, by route name. The handler will be
// passed through the application filters and context will be constructed at
// request time.
func (app *App) registerDistribution(routeName string, dispatch dispatchFunc) {
	handler := app.dispatcher(dispatch)

	// Chain the handler with prometheus instrumented handler
	if app.Config.HTTP.Debug.Prometheus.Enabled {
		handler = routeMetricsMiddleware(
			handler,
			metricskit.WithLabelValues(map[string]string{"route": v2.RoutePath(routeName)}),
		)
	}

	app.router.distribution.GetRoute(routeName).Handler(handler)
}

func (app *App) registerGitlab(route v1.Route, dispatch dispatchFunc) {
	handler := app.dispatcherGitlab(dispatch)

	// Chain the handler with prometheus instrumented handler
	if app.Config.HTTP.Debug.Prometheus.Enabled {
		handler = routeMetricsMiddleware(
			handler,
			metricskit.WithLabelValues(map[string]string{"route": route.ID}),
		)
	}

	app.router.gitlab.GetRoute(route.Name).Handler(handler)
}

// configureEvents prepares the event sink for action.
func (app *App) configureEvents(configuration *configuration.Configuration) {
	// Configure all of the endpoint sinks.
	var sinks []notifications.Sink
	for _, endpoint := range configuration.Notifications.Endpoints {
		if endpoint.Disabled {
			dcontext.GetLogger(app).Infof("endpoint %s disabled, skipping", endpoint.Name)
			continue
		}

		dcontext.GetLogger(app).Infof("configuring endpoint %v (%v), timeout=%s, headers=%v", endpoint.Name, endpoint.URL, endpoint.Timeout, endpoint.Headers)
		endpoint := notifications.NewEndpoint(endpoint.Name, endpoint.URL, notifications.EndpointConfig{
			Timeout:           endpoint.Timeout,
			Threshold:         endpoint.Threshold,
			MaxRetries:        endpoint.MaxRetries,
			Backoff:           endpoint.Backoff,
			Headers:           endpoint.Headers,
			IgnoredMediaTypes: endpoint.IgnoredMediaTypes,
			Ignore:            endpoint.Ignore,
		})

		sinks = append(sinks, endpoint)

	}

	// TODO: replace broadcaster with a new worker that will consume events from the queue
	// https://gitlab.com/gitlab-org/container-registry/-/issues/765
	app.events.sink = notifications.NewBroadcaster(sinks...)

	// Populate registry event source
	hostname, err := os.Hostname()
	if err != nil {
		hostname = configuration.HTTP.Addr
	} else {
		// try to pick the port off the config
		_, port, err := net.SplitHostPort(configuration.HTTP.Addr)
		if err == nil {
			hostname = net.JoinHostPort(hostname, port)
		}
	}

	app.events.source = notifications.SourceRecord{
		Addr:       hostname,
		InstanceID: dcontext.GetStringValue(app, "instance.id"),
	}
}

func (app *App) configureRedisRateLimiter(ctx context.Context, config *configuration.Configuration) error {
	if !config.Redis.RateLimiter.Enabled {
		return nil
	}

	opts := &redis.UniversalOptions{
		Username:        config.Redis.RateLimiter.Username,
		Addrs:           strings.Split(config.Redis.RateLimiter.Addr, ","),
		DB:              config.Redis.RateLimiter.DB,
		Password:        config.Redis.RateLimiter.Password,
		DialTimeout:     config.Redis.RateLimiter.DialTimeout,
		ReadTimeout:     config.Redis.RateLimiter.ReadTimeout,
		WriteTimeout:    config.Redis.RateLimiter.WriteTimeout,
		PoolSize:        config.Redis.RateLimiter.Pool.Size,
		ConnMaxLifetime: config.Redis.RateLimiter.Pool.MaxLifetime,
		MasterName:      config.Redis.RateLimiter.MainName,
	}
	if config.Redis.RateLimiter.TLS.Enabled {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: config.Redis.RateLimiter.TLS.Insecure,
		}
	}
	if config.Redis.RateLimiter.Pool.IdleTimeout > 0 {
		opts.ConnMaxIdleTime = config.Redis.RateLimiter.Pool.IdleTimeout
	}

	// redis.NewUniversalClient will take care of returning the appropriate client type (single, cluster or sentinel)
	// depending on the configuration options. See https://pkg.go.dev/github.com/go-redis/redis/v9#NewUniversalClient.
	redisClient := redis.NewUniversalClient(opts)

	if config.HTTP.Debug.Prometheus.Enabled {
		redismetrics.InstrumentClient(
			redisClient,
			redismetrics.WithInstanceName("ratelimiting"),
			redismetrics.WithMaxConns(opts.PoolSize),
		)
	}

	// Ensure the client is correctly configured and the server is reachable. We use a new local context here with a
	// tight timeout to avoid blocking the application start for too long.
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if cmd := redisClient.Ping(pingCtx); cmd.Err() != nil {
		return cmd.Err()
	}

	// TODO: add rate-limiter instance to the app
	// https://gitlab.com/gitlab-org/container-registry/-/issues/1225

	dlog.GetLogger(dlog.WithContext(app.Context)).Info("redis rate-limiter configured successfully")

	return nil
}

func (app *App) configureRedisCache(ctx context.Context, config *configuration.Configuration) error {
	if !config.Redis.Cache.Enabled {
		return nil
	}

	opts := &redis.UniversalOptions{
		Addrs:            strings.Split(config.Redis.Cache.Addr, ","),
		DB:               config.Redis.Cache.DB,
		Password:         config.Redis.Cache.Password,
		DialTimeout:      config.Redis.Cache.DialTimeout,
		ReadTimeout:      config.Redis.Cache.ReadTimeout,
		WriteTimeout:     config.Redis.Cache.WriteTimeout,
		PoolSize:         config.Redis.Cache.Pool.Size,
		ConnMaxLifetime:  config.Redis.Cache.Pool.MaxLifetime,
		MasterName:       config.Redis.Cache.MainName,
		SentinelUsername: config.Redis.Cache.SentinelUsername,
		SentinelPassword: config.Redis.Cache.SentinelPassword,
	}
	if config.Redis.Cache.TLS.Enabled {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: config.Redis.Cache.TLS.Insecure,
		}
	}
	if config.Redis.Cache.Pool.IdleTimeout > 0 {
		opts.ConnMaxIdleTime = config.Redis.Cache.Pool.IdleTimeout
	}

	// redis.NewUniversalClient will take care of returning the appropriate client type (single, cluster or sentinel)
	// depending on the configuration options. See https://pkg.go.dev/github.com/go-redis/redis/v9#NewUniversalClient.
	redisClient := redis.NewUniversalClient(opts)

	if config.HTTP.Debug.Prometheus.Enabled {
		redismetrics.InstrumentClient(
			redisClient,
			redismetrics.WithInstanceName("cache"),
			redismetrics.WithMaxConns(opts.PoolSize),
		)
	}

	// Ensure the client is correctly configured and the server is reachable. We use a new local context here with a
	// tight timeout to avoid blocking the application start for too long.
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	if cmd := redisClient.Ping(pingCtx); cmd.Err() != nil {
		return cmd.Err()
	}

	redisStore := redisstore.NewRedis(redisClient, libstore.WithExpiration(redisCacheTTL))
	app.redisCache = gocache.New[any](redisStore)

	dlog.GetLogger(dlog.WithContext(app.Context)).Info("redis cache configured successfully")

	return nil
}

func (app *App) configureRedis(configuration *configuration.Configuration) {
	if configuration.Redis.Addr == "" {
		return
	}

	opts := &redis.UniversalOptions{
		Addrs:           strings.Split(configuration.Redis.Addr, ","),
		DB:              configuration.Redis.DB,
		Password:        configuration.Redis.Password,
		DialTimeout:     configuration.Redis.DialTimeout,
		ReadTimeout:     configuration.Redis.ReadTimeout,
		WriteTimeout:    configuration.Redis.WriteTimeout,
		PoolSize:        configuration.Redis.Pool.Size,
		ConnMaxLifetime: configuration.Redis.Pool.MaxLifetime,
		MasterName:      configuration.Redis.MainName,
	}
	if configuration.Redis.TLS.Enabled {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: configuration.Redis.TLS.Insecure,
		}
	}
	if configuration.Redis.Pool.IdleTimeout > 0 {
		opts.ConnMaxIdleTime = configuration.Redis.Pool.IdleTimeout
	}
	// NewUniversalClient will take care of returning the appropriate client type (simple or sentinel) depending on the
	// configuration options. See https://pkg.go.dev/github.com/go-redis/redis/v9#NewUniversalClient.
	app.redis = redis.NewUniversalClient(opts)

	// setup expvar
	registry := expvar.Get("registry")
	if registry == nil {
		registry = expvar.NewMap("registry")
	}

	registry.(*expvar.Map).Set("redis", expvar.Func(func() interface{} {
		poolStats := app.redis.PoolStats()
		return map[string]interface{}{
			"Config": configuration.Redis,
			"Active": poolStats.TotalConns - poolStats.IdleConns,
		}
	}))

	dlog.GetLogger(dlog.WithContext(app.Context)).Info("main redis configured successfully")
}

// configureSecret creates a random secret if a secret wasn't included in the
// configuration.
func (app *App) configureSecret(configuration *configuration.Configuration) error {
	if configuration.HTTP.Secret == "" {
		var secretBytes [randomSecretSize]byte
		if _, err := cryptorand.Read(secretBytes[:]); err != nil {
			return fmt.Errorf("could not generate random bytes for HTTP secret: %w", err)
		}
		configuration.HTTP.Secret = string(secretBytes[:])
		dcontext.GetLogger(app).Warn("No HTTP secret provided - generated random secret. This may cause problems with uploads if multiple registries are behind a load-balancer. To provide a shared secret, fill in http.secret in the configuration file or set the REGISTRY_HTTP_SECRET environment variable.")
	}
	return nil
}

func (app *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close() // ensure that request body is always closed.

	// Prepare the context with our own little decorations.
	ctx := r.Context()
	ctx = dcontext.WithRequest(ctx, r)
	ctx, w = dcontext.WithResponseWriter(ctx, w)
	ctx = dcontext.WithLogger(ctx, dcontext.GetRequestCorrelationLogger(ctx))
	r = r.WithContext(ctx)

	if app.Config.Log.AccessLog.Disabled {
		defer func() {
			status, ok := ctx.Value("http.response.status").(int)
			if ok && status >= 200 && status <= 399 {
				dcontext.GetResponseLogger(r.Context()).Infof("response completed")
			}
		}()
	}

	app.router.ServeHTTP(w, r)
}

// metaRouter determines which router is appropreate for a given route. This
// is a temporary measure while we consolidate all routes into a single chi router.
// See: https://gitlab.com/groups/gitlab-org/-/epics/9467
type metaRouter struct {
	distribution *mux.Router // main application router, configured with dispatchers
	gitlab       *mux.Router // gitlab specific router
	v1RouteRegex *regexp.Regexp
}

// initMetaRouter constructs a new metaRouter and attaches it to the app.
func (app *App) initMetaRouter() error {
	app.router = &metaRouter{
		distribution: v2.RouterWithPrefix(app.Config.HTTP.Prefix),
		gitlab:       v1.RouterWithPrefix(app.Config.HTTP.Prefix),
	}

	// Register middleware.
	app.router.distribution.Use(app.gorillaLogMiddleware)
	app.router.distribution.Use(distributionAPIVersionMiddleware)

	app.router.gitlab.Use(app.gorillaLogMiddleware)

	// Register the handler dispatchers.
	app.registerDistribution(v2.RouteNameBase, func(ctx *Context, r *http.Request) http.Handler {
		return distributionAPIBase(app.Config.Database.Enabled)
	})
	app.registerDistribution(v2.RouteNameManifest, manifestDispatcher)
	app.registerDistribution(v2.RouteNameCatalog, catalogDispatcher)
	app.registerDistribution(v2.RouteNameTags, tagsDispatcher)
	app.registerDistribution(v2.RouteNameBlob, blobDispatcher)
	app.registerDistribution(v2.RouteNameBlobUpload, blobUploadDispatcher)
	app.registerDistribution(v2.RouteNameBlobUploadChunk, blobUploadDispatcher)

	// Register Gitlab handlers dispatchers.

	h := dbAssertionhandler{dbEnabled: app.Config.Database.Enabled}

	app.registerGitlab(v1.Base, h.wrap(func(ctx *Context, r *http.Request) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiBase(w, r)
		})
	}))
	app.registerGitlab(v1.RepositoryTags, h.wrap(repositoryTagsDispatcher))
	app.registerGitlab(v1.Repositories, h.wrap(repositoryDispatcher))
	app.registerGitlab(v1.SubRepositories, h.wrap(subRepositoriesDispatcher))

	var err error
	v1PathWithPrefix := fmt.Sprintf("^%s%s.*", strings.TrimSuffix(app.Config.HTTP.Prefix, "/"), v1.Base.Path)
	app.router.v1RouteRegex, err = regexp.Compile(v1PathWithPrefix)
	if err != nil {
		return fmt.Errorf("compiling v1 route prefix: %w", err)
	}

	return nil
}

// ServeHTTP delegates urls to the appropriate router.
func (m *metaRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.v1RouteRegex.MatchString(r.URL.Path) {
		m.gitlab.ServeHTTP(w, r)
		return
	}

	m.distribution.ServeHTTP(w, r)
}

// Temporary middleware to add router and http configuration information
// while we switch over to a new router away from gorilla/mux. See:
// https://gitlab.com/groups/gitlab-org/-/epics/9467
func (app *App) gorillaLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := app.context(w, r)

		c := app.Config.HTTP

		dlog.GetLogger(dlog.WithContext(ctx)).WithFields(dlog.Fields{
			"router":                    "gorilla/mux",
			"method":                    r.Method,
			"path":                      r.URL.Path,
			"config_http_host":          c.Host,
			"config_http_addr":          c.Addr,
			"config_http_net":           c.Net,
			"config_http_prefix":        c.Prefix,
			"config_http_relative_urls": c.RelativeURLs,
		}).Info("router info")

		next.ServeHTTP(w, r)
	})
}

type dbAssertionhandler struct{ dbEnabled bool }

func (h dbAssertionhandler) wrap(child func(ctx *Context, r *http.Request) http.Handler) func(ctx *Context, r *http.Request) http.Handler {
	// Return a 404, signaling that the database is disabled and GitLab v1 API features are not available.
	if !h.dbEnabled {
		return func(ctx *Context, r *http.Request) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Gitlab-Container-Registry-Version", strings.TrimPrefix(version.Version, "v"))
				w.WriteHeader(http.StatusNotFound)
			})
		}
	}

	return child
}

// distributionAPIVersionMiddleware sets a header with the Docker Distribution
// API Version for distribution API responses.
func distributionAPIVersionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Docker-Distribution-API-Version", "registry/2.0")

		next.ServeHTTP(w, r)
	})
}

// dispatchFunc takes a context and request and returns a constructed handler
// for the route. The dispatcher will use this to dynamically create request
// specific handlers for each endpoint without creating a new router for each
// request.
type dispatchFunc func(ctx *Context, r *http.Request) http.Handler

// dispatcher returns a handler that constructs a request specific context and
// handler, using the dispatch factory function.
func (app *App) dispatcher(dispatch dispatchFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for headerName, headerValues := range app.Config.HTTP.Headers {
			for _, value := range headerValues {
				w.Header().Add(headerName, value)
			}
		}

		ctx := app.context(w, r)

		// attach CF-RayID header to context and pass the key to the logger
		ctx.Context = dcontext.WithCFRayID(ctx.Context, r)
		ctx.Context = dcontext.WithLogger(ctx.Context, dcontext.GetLogger(ctx.Context, dcontext.CFRayIDLogKey))

		if err := app.authorized(w, r, ctx); err != nil {
			var authErr auth.Challenge
			if !errors.As(err, &authErr) {
				dcontext.GetLogger(ctx).WithError(err).Warn("error authorizing context")
			}
			return
		}

		// Add extra context to request logging
		ctx.Context = dcontext.WithLogger(ctx.Context, dcontext.GetLogger(ctx.Context, auth.UserNameKey, auth.UserTypeKey, auth.ResourceProjectPathsKey))
		// sync up context on the request.
		r = r.WithContext(ctx)

		// get all metadata either from the database or from the filesystem
		if app.Config.Database.Enabled {
			ctx.useDatabase = true
		}

		if app.nameRequired(r) {
			bp, ok := app.registry.Blobs().(distribution.BlobProvider)
			if !ok {
				err := fmt.Errorf("unable to convert BlobEnumerator into BlobProvider")
				dcontext.GetLogger(ctx).Error(err)
				ctx.Errors = append(ctx.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			}
			ctx.blobProvider = bp

			repository, err := app.repositoryFromContext(ctx, w)
			if err != nil {
				return
			}

			ctx.queueBridge = app.queueBridge(ctx, r)

			// assign and decorate the authorized repository with an event bridge.
			ctx.Repository, ctx.RepositoryRemover = notifications.Listen(
				repository,
				ctx.App.repoRemover,
				app.eventBridge(ctx, r),
				ctx.useDatabase)

			ctx.Repository, err = applyRepoMiddleware(app, ctx.Repository, app.Config.Middleware["repository"])
			if err != nil {
				dcontext.GetLogger(ctx).Errorf("error initializing repository middleware: %v", err)
				ctx.Errors = append(ctx.Errors, errcode.ErrorCodeUnknown.WithDetail(err))

				if err := errcode.ServeJSON(w, ctx.Errors); err != nil {
					dcontext.GetLogger(ctx).Errorf("error serving error json: %v (from %v)", err, ctx.Errors)
				}
				return
			}
		}

		if ctx.useDatabase {
			ctx.repoCache = datastore.NewSingleRepositoryCache()
		}

		dispatch(ctx, r).ServeHTTP(w, r)

		// Automated error response handling here. Handlers may return their
		// own errors if they need different behavior (such as range errors
		// for layer upload).
		if ctx.Errors.Len() > 0 {
			if err := errcode.ServeJSON(w, ctx.Errors); err != nil {
				dcontext.GetLogger(ctx).Errorf("error serving error json: %v (from %v)", err, ctx.Errors)
			}

			app.logError(ctx, r, ctx.Errors)
		}
	})
}

func (app *App) dispatcherGitlab(dispatch dispatchFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := &Context{
			App:     app,
			Context: dcontext.WithVars(r.Context(), r),
		}
		// attach CF-RayID header to context and pass the key to the logger
		ctx.Context = dcontext.WithCFRayID(ctx.Context, r)
		ctx.Context = dcontext.WithLogger(ctx.Context, dcontext.GetLogger(ctx.Context, dcontext.CFRayIDLogKey))

		if err := app.authorized(w, r, ctx); err != nil {
			var authErr auth.Challenge
			if !errors.As(err, &authErr) {
				dcontext.GetLogger(ctx).WithError(err).Warn("error authorizing context")
			}
			return
		}

		// Add extra context to request logging
		ctx.Context = dcontext.WithLogger(ctx.Context, dcontext.GetLogger(ctx.Context, auth.UserNameKey, auth.UserTypeKey, auth.ResourceProjectPathsKey))
		// sync up context on the request.
		r = r.WithContext(ctx)

		if app.nameRequired(r) {
			repository, err := app.repositoryFromContext(ctx, w)
			if err != nil {
				return
			}

			ctx.Repository = repository
		}

		dispatch(ctx, r).ServeHTTP(w, r)

		// Automated error response handling here. Handlers may return their
		// own errors if they need different behavior (such as range errors
		// for layer upload).
		if ctx.Errors.Len() > 0 {
			if err := errcode.ServeJSON(w, ctx.Errors); err != nil {
				dcontext.GetLogger(ctx).Errorf("error serving error json: %v (from %v)", err, ctx.Errors)
			}

			app.logError(ctx, r, ctx.Errors)
		}
	})
}

func (app *App) logError(ctx context.Context, r *http.Request, errors errcode.Errors) {
	for _, e := range errors {
		var code errcode.ErrorCode
		var message, detail string

		switch ex := e.(type) {
		case errcode.Error:
			code = ex.Code
			message = ex.Message
			detail = fmt.Sprintf("%+v", ex.Detail)
		case errcode.ErrorCode:
			code = ex
			message = ex.Message()
		default:
			// just normal go 'error'
			code = errcode.ErrorCodeUnknown
			message = ex.Error()
		}

		// inject request specifc fields into the error logs
		l := dcontext.GetMappedRequestLogger(ctx).WithField("code", code.String())
		if detail != "" {
			l = l.WithField("detail", detail)
		}

		// HEAD requests check for manifests and blobs that are often not present as
		// part of normal request flow, so logging these errors is superfluous.
		if r.Method == http.MethodHead &&
			(code == v2.ErrorCodeBlobUnknown || code == v2.ErrorCodeManifestUnknown) {
			l.WithError(e).Debug(message)
		} else {
			l.WithError(e).Error(message)
		}

		// only report 500 errors to Sentry
		if code == errcode.ErrorCodeUnknown {
			// Encode detail in error message so that it shows up in Sentry. This is a hack until we refactor error
			// handling across the whole application to enforce consistent behaviour and formatting.
			// see https://gitlab.com/gitlab-org/container-registry/-/issues/198
			detailSuffix := ""
			if detail != "" {
				detailSuffix = fmt.Sprintf(": %s", detail)
			}
			err := errcode.ErrorCodeUnknown.WithMessage(fmt.Sprintf("%s%s", message, detailSuffix))
			errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithRequest(r), errortracking.WithStackTrace())
		}
	}
}

// context constructs the context object for the application. This only be
// called once per request.
func (app *App) context(w http.ResponseWriter, r *http.Request) *Context {
	ctx := r.Context()
	ctx = dcontext.WithVars(ctx, r)
	name := dcontext.GetStringValue(ctx, "vars.name")
	ctx = context.WithValue(ctx, "root_repo", strings.Split(name, "/")[0])
	ctx = dcontext.WithLogger(ctx, dcontext.GetLogger(ctx,
		"root_repo",
		"vars.name",
		"vars.reference",
		"vars.digest",
		"vars.uuid"))

	context := &Context{
		App:     app,
		Context: ctx,
	}

	if app.httpHost.Scheme != "" && app.httpHost.Host != "" {
		// A "host" item in the configuration takes precedence over
		// X-Forwarded-Proto and X-Forwarded-Host headers, and the
		// hostname in the request.
		context.urlBuilder = urls.NewBuilder(&app.httpHost, false)
	} else {
		context.urlBuilder = urls.NewBuilderFromRequest(r, app.Config.HTTP.RelativeURLs)
	}

	return context
}

// authorized checks if the request can proceed with access to the requested
// repository. If it succeeds, the context may access the requested
// repository. An error will be returned if access is not available.
func (app *App) authorized(w http.ResponseWriter, r *http.Request, context *Context) error {
	dcontext.GetLogger(context).Debug("authorizing request")
	repo := getName(context)

	if app.accessController == nil {
		return nil // access controller is not enabled.
	}

	var (
		accessRecords []auth.Access
		err           error
		errCode       error
	)

	if repo != "" {
		accessRecords = appendAccessRecords(accessRecords, r.Method, repo)
		accessRecords = appendRepositoryDetailsAccessRecords(accessRecords, r, repo)
		accessRecords, err, errCode = appendRepositoryNamespaceAccessRecords(accessRecords, r)
		if err != nil {
			if err := errcode.ServeJSON(w, errCode); err != nil {
				dcontext.GetLogger(context).Errorf("error serving error json: %v (from %v)", err, context.Errors)
			}
			return fmt.Errorf("error creating access records: %w", err)
		}

		if fromRepo := r.FormValue("from"); fromRepo != "" {
			// mounting a blob from one repository to another requires pull (GET)
			// access to the source repository.
			accessRecords = appendAccessRecords(accessRecords, http.MethodGet, fromRepo)
		}
	} else {
		// Only allow the name not to be set on the base route.
		if app.nameRequired(r) {
			// For this to be properly secured, repo must always be set for a
			// resource that may make a modification. The only condition under
			// which name is not set and we still allow access is when the
			// base route is accessed. This section prevents us from making
			// that mistake elsewhere in the code, allowing any operation to
			// proceed.
			if err := errcode.ServeJSON(w, errcode.ErrorCodeUnauthorized); err != nil {
				dcontext.GetLogger(context).Errorf("error serving error json: %v (from %v)", err, context.Errors)
			}
			return fmt.Errorf("forbidden: no repository name")
		}
		accessRecords = appendCatalogAccessRecord(accessRecords, r)
	}

	ctx, err := app.accessController.Authorized(context.Context, accessRecords...)
	if err != nil {
		switch err := err.(type) {
		case auth.Challenge:
			// Add the appropriate WWW-Auth header
			err.SetHeaders(r, w)

			if err := errcode.ServeJSON(w, errcode.ErrorCodeUnauthorized.WithDetail(accessRecords)); err != nil {
				dcontext.GetLogger(context).Errorf("error serving error json: %v (from %v)", err, context.Errors)
			}
		default:
			// This condition is a potential security problem either in
			// the configuration or whatever is backing the access
			// controller. Just return a bad request with no information
			// to avoid exposure. The request should not proceed.
			dcontext.GetLogger(context).Errorf("error checking authorization: %v", err)
			w.WriteHeader(http.StatusBadRequest)
		}

		return err
	}

	dcontext.GetLogger(ctx, auth.UserNameKey, auth.UserTypeKey, auth.ResourceProjectPathsKey).Info("authorized request")
	context.Context = ctx
	return nil
}

// eventBridge returns a bridge for the current request, configured with the
// correct actor and source.
func (app *App) eventBridge(ctx *Context, r *http.Request) notifications.Listener {
	actor := notifications.ActorRecord{
		Name:     getUserName(ctx, r),
		UserType: getUserType(ctx),
	}
	request := notifications.NewRequestRecord(dcontext.GetRequestID(ctx), r)

	return notifications.NewBridge(ctx.urlBuilder, app.events.source, actor, request, app.events.sink, app.Config.Notifications.EventConfig.IncludeReferences)
}

func (app *App) queueBridge(ctx *Context, r *http.Request) *notifications.QueueBridge {
	actor := notifications.ActorRecord{
		Name:     getUserName(ctx, r),
		UserType: getUserType(ctx),
		User:     getUserJWT(ctx),
	}
	request := notifications.NewRequestRecord(dcontext.GetRequestID(ctx), r)

	return notifications.NewQueueBridge(ctx.urlBuilder, app.events.source, actor, request, app.events.sink, app.Config.Notifications.EventConfig.IncludeReferences)
}

// nameRequired returns true if the route requires a name.
func (app *App) nameRequired(r *http.Request) bool {
	route := mux.CurrentRoute(r)
	if route == nil {
		return true
	}
	routeName := route.GetName()

	switch routeName {
	case v2.RouteNameBase, v2.RouteNameCatalog, v1.Base.Name:
		return false
	}

	return true
}

// distributionAPIBase provides clients with extra information about extended
// features the distribution API via the Gitlab-Container-Registry-Features header.
func distributionAPIBase(dbEnabled bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Gitlab-Container-Registry-Features", version.ExtFeatures)
		w.Header().Set("Gitlab-Container-Registry-Database-Enabled", strconv.FormatBool(dbEnabled))
		apiBase(w, r)
	}
}

// apiBase implements a simple yes-man for doing overall checks against the
// api. This can support auth roundtrips to support docker login.
func apiBase(w http.ResponseWriter, r *http.Request) {
	const emptyJSON = "{}"

	// Provide a simple /v2/ 200 OK response with empty json response.
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(emptyJSON)))

	w.Header().Set("Gitlab-Container-Registry-Version", strings.TrimPrefix(version.Version, "v"))

	fmt.Fprint(w, emptyJSON)
}

// appendAccessRecords checks the method and adds the appropriate Access records to the records list.
func appendAccessRecords(records []auth.Access, method string, repo string) []auth.Access {
	resource := auth.Resource{
		Type: "repository",
		Name: repo,
	}

	switch method {
	case http.MethodGet, http.MethodHead:
		records = append(records,
			auth.Access{
				Resource: resource,
				Action:   "pull",
			})
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		records = append(records,
			auth.Access{
				Resource: resource,
				Action:   "pull",
			},
			auth.Access{
				Resource: resource,
				Action:   "push",
			})
	case http.MethodDelete:
		records = append(records,
			auth.Access{
				Resource: resource,
				Action:   "delete",
			})
	}
	return records
}

// Add the access record for the catalog if it's our current route
func appendCatalogAccessRecord(accessRecords []auth.Access, r *http.Request) []auth.Access {
	route := mux.CurrentRoute(r)
	routeName := route.GetName()

	if routeName == v2.RouteNameCatalog {
		resource := auth.Resource{
			Type: "registry",
			Name: "catalog",
		}

		accessRecords = append(accessRecords,
			auth.Access{
				Resource: resource,
				Action:   "*",
			})
	}
	return accessRecords
}

func appendRepositoryDetailsAccessRecords(accessRecords []auth.Access, r *http.Request, repo string) []auth.Access {
	route := mux.CurrentRoute(r)
	routeName := route.GetName()

	// For now, we only have three operations requiring a custom access record, which are:
	// 1. for returning the size of a repository including its descendants.
	// 2. for returning all the repositories under a given repository base path (including the base repository)
	// 3. renaming a base repository (name and path) and updating the sub-repositories (path) accordingly
	// These three operations require an access record of type `repository` and name `<name>/*`
	// (to grant access on all descendants), in addition to the standard access record of type `repository` and
	// name `<name>` (to grant read access to the base repository), which was appended in the preceding call to
	// `appendAccessRecords`.
	if routeName == v1.SubRepositories.Name ||
		(routeName == v1.Repositories.Name &&
			(sizeQueryParamValue(r) == sizeQueryParamSelfWithDescendantsValue || r.Method == http.MethodPatch)) {
		accessRecords = append(accessRecords, auth.Access{
			Resource: auth.Resource{
				Type: "repository",
				Name: fmt.Sprintf("%s/*", repo),
			},
			Action: "pull",
		})
	}

	return accessRecords
}

// appendRepositoryNamespaceAccessRecords adds the needed access records for moving a project's repositories
// from one namespace to another as facilitated by the PATCH repository request.
// Once it detects that the current request is a request to move repositories,
// it adds the correct access records to the list of access records that must be present in the token.
func appendRepositoryNamespaceAccessRecords(accessRecords []auth.Access, r *http.Request) ([]auth.Access, error, error) {
	route := mux.CurrentRoute(r)
	routeName := route.GetName()

	if r.Method == http.MethodPatch && routeName == v1.Repositories.Name {
		// Read the request body
		buf := new(bytes.Buffer)

		// Rread from r.Body and write to buf simultaneously
		teeReader := io.TeeReader(r.Body, buf)

		// Read the body from the TeeReader
		body, err := io.ReadAll(teeReader)
		if err != nil {
			return accessRecords, err, v1.ErrorCodeInvalidJSONBody.WithDetail("invalid json")
		}

		// Rewind the request body reader to its original position to allow downstream handlers to read it if needed.
		r.Body = io.NopCloser(buf)

		// Parse the request body into a struct.
		var data RenameRepositoryAPIRequest
		if err := json.Unmarshal(body, &data); err != nil {
			return accessRecords, err, v1.ErrorCodeInvalidJSONBody.WithDetail("invalid json")
		}
		if data.Namespace != "" {
			accessRecords = append(accessRecords, auth.Access{
				Resource: auth.Resource{
					Type: "repository",
					Name: fmt.Sprintf("%s/*", data.Namespace),
				},
				Action: "push",
			})
		}
	}

	return accessRecords, nil, nil
}

// applyRegistryMiddleware wraps a registry instance with the configured middlewares
func applyRegistryMiddleware(ctx context.Context, registry distribution.Namespace, middlewares []configuration.Middleware) (distribution.Namespace, error) {
	for _, mw := range middlewares {
		rmw, err := registrymiddleware.Get(ctx, mw.Name, mw.Options, registry)
		if err != nil {
			return nil, fmt.Errorf("unable to configure registry middleware (%s): %s", mw.Name, err)
		}
		registry = rmw
	}
	return registry, nil
}

// applyRepoMiddleware wraps a repository with the configured middlewares
func applyRepoMiddleware(ctx context.Context, repository distribution.Repository, middlewares []configuration.Middleware) (distribution.Repository, error) {
	for _, mw := range middlewares {
		rmw, err := repositorymiddleware.Get(ctx, mw.Name, mw.Options, repository)
		if err != nil {
			return nil, err
		}
		repository = rmw
	}
	return repository, nil
}

// applyStorageMiddleware wraps a storage driver with the configured middlewares
func applyStorageMiddleware(driver storagedriver.StorageDriver, middlewares []configuration.Middleware) (storagedriver.StorageDriver, error) {
	for _, mw := range middlewares {
		smw, err := storagemiddleware.Get(mw.Name, mw.Options, driver)
		if err != nil {
			return nil, fmt.Errorf("unable to configure storage middleware (%s): %v", mw.Name, err)
		}
		driver = smw
	}
	return driver, nil
}

// uploadPurgeDefaultConfig provides a default configuration for upload
// purging to be used in the absence of configuration in the
// configuration file
func uploadPurgeDefaultConfig() map[interface{}]interface{} {
	config := map[interface{}]interface{}{}
	config["enabled"] = true
	config["age"] = "168h"
	config["interval"] = "24h"
	config["dryrun"] = false
	return config
}

func badPurgeUploadConfig(reason string) error {
	return fmt.Errorf("Unable to parse upload purge configuration: %s", reason)
}

// startUploadPurger schedules a goroutine which will periodically
// check upload directories for old files and delete them
func startUploadPurger(ctx context.Context, storageDriver storagedriver.StorageDriver, log dcontext.Logger, config map[interface{}]interface{}) error {
	if config["enabled"] == false {
		return nil
	}

	var purgeAgeDuration time.Duration
	var err error
	purgeAge, ok := config["age"]
	if ok {
		ageStr, ok := purgeAge.(string)
		if !ok {
			return badPurgeUploadConfig("age is not a string")
		}
		purgeAgeDuration, err = time.ParseDuration(ageStr)
		if err != nil {
			return badPurgeUploadConfig(fmt.Sprintf("Cannot parse duration: %s", err.Error()))
		}
	} else {
		return badPurgeUploadConfig("age missing")
	}

	var intervalDuration time.Duration
	interval, ok := config["interval"]
	if ok {
		intervalStr, ok := interval.(string)
		if !ok {
			return badPurgeUploadConfig("interval is not a string")
		}

		intervalDuration, err = time.ParseDuration(intervalStr)
		if err != nil {
			return badPurgeUploadConfig(fmt.Sprintf("Cannot parse interval: %s", err.Error()))
		}
	} else {
		return badPurgeUploadConfig("interval missing")
	}

	var dryRunBool bool
	dryRun, ok := config["dryrun"]
	if ok {
		dryRunBool, ok = dryRun.(bool)
		if !ok {
			return badPurgeUploadConfig("cannot parse dryrun")
		}
	} else {
		return badPurgeUploadConfig("dryrun missing")
	}

	go func() {
		rand.Seed(time.Now().Unix())
		/* #nosec G404 */
		jitter := time.Duration(rand.Int()%60) * time.Minute
		log.Infof("Starting upload purge in %s", jitter)
		time.Sleep(jitter)

		for {
			storage.PurgeUploads(ctx, storageDriver, time.Now().Add(-purgeAgeDuration), !dryRunBool)
			log.Infof("Starting upload purge in %s", intervalDuration)
			time.Sleep(intervalDuration)
		}
	}()

	return nil
}

// GracefulShutdown allows the app to free any resources before shutdown.
func (app *App) GracefulShutdown(ctx context.Context) error {
	errors := make(chan error)

	go func() {
		errors <- app.db.Close()
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("app shutdown failed: %w", ctx.Err())
	case err := <-errors:
		if err != nil {
			return fmt.Errorf("app shutdown failed: %w", err)
		}
		return nil
	}
}

// DBStats returns the sql.DBStats for the metadata database connection handle.
func (app *App) DBStats() sql.DBStats {
	// TODO(dlb): collect metrics for all hosts
	return app.db.Primary().Stats()
}

func (app *App) repositoryFromContext(ctx *Context, w http.ResponseWriter) (distribution.Repository, error) {
	return repositoryFromContextWithRegistry(ctx, w, app.registry)
}

func repositoryFromContextWithRegistry(ctx *Context, w http.ResponseWriter, registry distribution.Namespace) (distribution.Repository, error) {
	nameRef, err := reference.WithName(getName(ctx))
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error parsing reference from context: %v", err)
		ctx.Errors = append(ctx.Errors, distribution.ErrRepositoryNameInvalid{
			Name:   getName(ctx),
			Reason: err,
		})
		if err := errcode.ServeJSON(w, ctx.Errors); err != nil {
			dcontext.GetLogger(ctx).Errorf("error serving error json: %v (from %v)", err, ctx.Errors)
		}
		return nil, err
	}

	repository, err := registry.Repository(ctx, nameRef)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error resolving repository: %v", err)

		switch err := err.(type) {
		case distribution.ErrRepositoryUnknown:
			ctx.Errors = append(ctx.Errors, v2.ErrorCodeNameUnknown.WithDetail(err))
		case distribution.ErrRepositoryNameInvalid:
			ctx.Errors = append(ctx.Errors, v2.ErrorCodeNameInvalid.WithDetail(err))
		case errcode.Error:
			ctx.Errors = append(ctx.Errors, err)
		}

		if err := errcode.ServeJSON(w, ctx.Errors); err != nil {
			dcontext.GetLogger(ctx).Errorf("error serving error json: %v (from %v)", err, ctx.Errors)
		}
		return nil, err
	}

	return repository, nil
}

func startBackgroundMigrations(ctx context.Context, db *datastore.DB, config *configuration.Configuration) {
	l := dlog.GetLogger(dlog.WithContext(ctx))

	// register all work functions with worker
	worker, err := bbm.RegisterWork(bbm.AllWork(),
		bbm.WithDB(db),
		bbm.WithLogger(dlog.GetLogger(dlog.WithContext(ctx))),
		bbm.WithJobInterval(config.Database.BackgroundMigrations.JobInterval),
		bbm.WithMaxJobAttempt(config.Database.BackgroundMigrations.MaxJobRetries),
	)
	if err != nil {
		l.WithError(err).Error("background migration worker could not start")
		return
	}

	doneCh := make(chan struct{})
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)
	// listen for registry process exit signal in the background.
	go bbmListenForShutdown(ctx, sigChan, doneCh)

	// start looking for background migrations
	gracefulFinish, err := worker.ListenForBackgroundMigration(ctx, doneCh)
	if err != nil {
		l.WithError(err).Error("background migration worker exited abruptly")
	}
	l.Info("Background migration worker is running")

	// wait for the worker to acknowledge that it is safe to exit.
	<-gracefulFinish
	l.Info("Background migration worker stopped")
}

func bbmListenForShutdown(ctx context.Context, c chan os.Signal, doneCh chan struct{}) {
	<-c
	// signal to worker that the program is exiting
	dlog.GetLogger(dlog.WithContext(ctx)).Info("Background migration worker is shutting down")
	close(doneCh)
}
