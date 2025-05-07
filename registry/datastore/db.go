//go:generate mockgen -package mocks -destination mocks/db.go . Handler,Transactor,LoadBalancer,Connector,DNSResolver

package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/errortracking"
	"gitlab.com/gitlab-org/labkit/metrics/sqlmetrics"
)

const (
	driverName                  = "pgx"
	defaultReplicaCheckInterval = 1 * time.Minute

	HostTypePrimary = "primary"
	HostTypeReplica = "replica"
	HostTypeUnknown = "unknown"

	// upToDateReplicaTimeout establishes the maximum amount of time we're willing to wait for an up-to-date database
	// replica to be identified during load balancing. If a replica is not identified within this threshold, then it's
	// likely that there is a performance degradation going on, in which case we want to gracefully fall back to the
	// primary database to avoid further processing delays. The current 100ms value is a starting point/educated guess
	// that matches the one used in GitLab Rails (https://gitlab.com/gitlab-org/gitlab/-/merge_requests/159633).
	upToDateReplicaTimeout = 100 * time.Millisecond

	// ReplicaResolveTimeout sets a global limit on wait time for resolving replicas in load balancing, covering both DNS
	// lookups and connection attempts for all identified replicas.
	ReplicaResolveTimeout = 2 * time.Second

	// InitReplicaResolveTimeout is a stricter limit used only during startup in NewDBLoadBalancer, taking precedence over
	// ReplicaResolveTimeout. A quick failure here prevents startup delays, allowing asynchronous retries in
	// StartReplicaChecking. While these timeouts are currently similar, they remain separate to allow independent tuning.
	InitReplicaResolveTimeout = 1 * time.Second

	// minLivenessProbeInterval is the minimum time between replica host liveness probes during load balancing.
	minLivenessProbeInterval = 1 * time.Second
	// minResolveReplicasInterval is the default minimum time between replicas resolution calls during load balancing.
	minResolveReplicasInterval = 10 * time.Second
	// livenessProbeTimeout is the maximum time for a replica liveness probe to run.
	livenessProbeTimeout = 100 * time.Millisecond

	// replicaLagCheckTimeout is the default timeout for checking replica lag.
	replicaLagCheckTimeout = 100 * time.Millisecond
	// MaxReplicaLagTime is the default maximum replication lag time
	MaxReplicaLagTime = 30 * time.Second
	// MaxReplicaLagBytes is the default maximum replication lag in bytes. This matches the Rails default, see
	// https://gitlab.com/gitlab-org/gitlab/blob/5c68653ce8e982e255277551becb3270a92f5e9e/lib/gitlab/database/load_balancing/configuration.rb#L48-48
	MaxReplicaLagBytes = 8 * 1024 * 1024
)

// Queryer is the common interface to execute queries on a database.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Handler represents a database connection handler.
type Handler interface {
	Queryer
	Stats() sql.DBStats
	Close() error
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transactor, error)
}

// Transactor represents a database transaction.
type Transactor interface {
	Queryer
	Commit() error
	Rollback() error
}

// QueryErrorProcessor defines the interface for handling database query errors.
type QueryErrorProcessor interface {
	ProcessQueryError(ctx context.Context, db *DB, query string, err error)
}

// DB implements Handler.
type DB struct {
	*sql.DB
	DSN            *DSN
	errorProcessor QueryErrorProcessor
}

// BeginTx wraps sql.Tx from the innner sql.DB within a datastore.Tx.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transactor, error) {
	tx, err := db.DB.BeginTx(ctx, opts)

	return &Tx{tx, db, db.errorProcessor}, err
}

// Begin wraps sql.Tx from the inner sql.DB within a datastore.Tx.
func (db *DB) Begin() (Transactor, error) {
	return db.BeginTx(context.Background(), nil)
}

// Address returns the database host network address.
func (db *DB) Address() string {
	if db.DSN == nil {
		return ""
	}
	return db.DSN.Address()
}

// QueryContext wraps the underlying QueryContext.
func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := db.DB.QueryContext(ctx, query, args...)
	if err != nil && db.errorProcessor != nil {
		db.errorProcessor.ProcessQueryError(ctx, db, query, err)
	}
	return rows, err
}

// ExecContext wraps the underlying ExecContext.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := db.DB.ExecContext(ctx, query, args...)
	if err != nil && db.errorProcessor != nil {
		db.errorProcessor.ProcessQueryError(ctx, db, query, err)
	}
	return res, err
}

// Row is a wrapper around sql.Row that allows us to intercept errors during the rows' Scan.
type Row struct {
	*sql.Row
	db             *DB
	errorProcessor QueryErrorProcessor
	query          string
	ctx            context.Context
}

// QueryRowContext wraps the underlying QueryRowContext with our custom Row.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	row := db.DB.QueryRowContext(ctx, query, args...)
	return &Row{
		Row:            row,
		db:             db,
		errorProcessor: db.errorProcessor,
		query:          query,
		ctx:            ctx,
	}
}

// Scan implements the sql.Row.Scan method and processes errors.
func (r *Row) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)

	if err != nil && r.errorProcessor != nil {
		r.errorProcessor.ProcessQueryError(r.ctx, r.db, r.query, err)
	}

	return err
}

// Tx implements Transactor.
type Tx struct {
	*sql.Tx
	db             *DB
	errorProcessor QueryErrorProcessor
}

// QueryContext wraps the underlying QueryContext.
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := tx.Tx.QueryContext(ctx, query, args...)
	if err != nil && tx.errorProcessor != nil {
		tx.errorProcessor.ProcessQueryError(ctx, tx.db, query, err)
	}
	return rows, err
}

// QueryRowContext wraps the underlying Tx.QueryRowContext with our custom Row.
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	row := tx.Tx.QueryRowContext(ctx, query, args...)
	return &Row{
		Row:            row,
		db:             tx.db,
		errorProcessor: tx.errorProcessor,
		query:          query,
		ctx:            ctx,
	}
}

// ExecContext wraps the underlying ExecContext.
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := tx.Tx.ExecContext(ctx, query, args...)
	if err != nil && tx.errorProcessor != nil {
		tx.errorProcessor.ProcessQueryError(ctx, tx.db, query, err)
	}
	return res, err
}

// DSN represents the Data Source Name parameters for a DB connection.
type DSN struct {
	Host           string
	Port           int
	User           string
	Password       string
	DBName         string
	SSLMode        string
	SSLCert        string
	SSLKey         string
	SSLRootCert    string
	ConnectTimeout time.Duration
}

// String builds the string representation of a DSN.
func (dsn *DSN) String() string {
	var params []string

	port := ""
	if dsn.Port > 0 {
		port = strconv.Itoa(dsn.Port)
	}
	connectTimeout := ""
	if dsn.ConnectTimeout > 0 {
		connectTimeout = fmt.Sprintf("%.0f", dsn.ConnectTimeout.Seconds())
	}

	for _, param := range []struct{ k, v string }{
		{"host", dsn.Host},
		{"port", port},
		{"user", dsn.User},
		{"password", dsn.Password},
		{"dbname", dsn.DBName},
		{"sslmode", dsn.SSLMode},
		{"sslcert", dsn.SSLCert},
		{"sslkey", dsn.SSLKey},
		{"sslrootcert", dsn.SSLRootCert},
		{"connect_timeout", connectTimeout},
	} {
		if len(param.v) == 0 {
			continue
		}

		param.v = strings.ReplaceAll(param.v, "'", `\'`)
		param.v = strings.ReplaceAll(param.v, " ", `\ `)

		params = append(params, param.k+"="+param.v)
	}

	return strings.Join(params, " ")
}

// Address returns the host:port segment of a DSN.
func (dsn *DSN) Address() string {
	return net.JoinHostPort(dsn.Host, strconv.Itoa(dsn.Port))
}

type opts struct {
	logger               *logrus.Entry
	logLevel             tracelog.LogLevel
	pool                 *PoolConfig
	preferSimpleProtocol bool
	loadBalancing        *LoadBalancingConfig
	metricsEnabled       bool
	promRegisterer       prometheus.Registerer
}

type PoolConfig struct {
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

// LoadBalancingConfig represents the database load balancing configuration.
type LoadBalancingConfig struct {
	active                     bool
	hosts                      []string
	resolver                   DNSResolver
	connector                  Connector
	replicaCheckInterval       time.Duration
	lsnStore                   RepositoryCache
	minResolveReplicasInterval time.Duration
}

// Option is used to configure the database connections.
type Option func(*opts)

// WithLogger configures the logger for the database connection driver.
func WithLogger(l *logrus.Entry) Option {
	return func(opts *opts) {
		opts.logger = l
	}
}

// WithLogLevel configures the logger level for the database connection driver.
func WithLogLevel(l configuration.Loglevel) Option {
	var lvl tracelog.LogLevel
	switch l {
	case configuration.LogLevelTrace:
		lvl = tracelog.LogLevelTrace
	case configuration.LogLevelDebug:
		lvl = tracelog.LogLevelDebug
	case configuration.LogLevelInfo:
		lvl = tracelog.LogLevelInfo
	case configuration.LogLevelWarn:
		lvl = tracelog.LogLevelWarn
	default:
		lvl = tracelog.LogLevelError
	}

	return func(opts *opts) {
		opts.logLevel = lvl
	}
}

// WithPoolConfig configures the settings for the database connection pool.
func WithPoolConfig(c *PoolConfig) Option {
	return func(opts *opts) {
		opts.pool = c
	}
}

// WithPoolMaxIdle configures the maximum number of idle pool connections.
func WithPoolMaxIdle(c int) Option {
	return func(opts *opts) {
		if opts.pool == nil {
			opts.pool = &PoolConfig{}
		}

		opts.pool.MaxIdle = c
	}
}

// WithPoolMaxOpen configures the maximum number of open pool connections.
func WithPoolMaxOpen(c int) Option {
	return func(opts *opts) {
		if opts.pool == nil {
			opts.pool = &PoolConfig{}
		}

		opts.pool.MaxOpen = c
	}
}

// WithPreparedStatements configures the settings to allow the database
// driver to use prepared statements.
func WithPreparedStatements(b bool) Option {
	return func(opts *opts) {
		// Registry configuration uses opposite semantics as pgx for prepared statements.
		opts.preferSimpleProtocol = !b
	}
}

func applyOptions(input []Option) opts {
	l := logrus.New()
	l.SetOutput(io.Discard)

	config := opts{
		logger: logrus.NewEntry(l),
		pool:   &PoolConfig{},
		loadBalancing: &LoadBalancingConfig{
			connector:            NewConnector(),
			replicaCheckInterval: defaultReplicaCheckInterval,
		},
		promRegisterer: prometheus.DefaultRegisterer,
	}

	for _, v := range input {
		v(&config)
	}

	return config
}

type logger struct {
	*logrus.Entry
}

// used to minify SQL statements on log entries by removing multiple spaces, tabs and new lines.
var logMinifyPattern = regexp.MustCompile(`\s+|\t+|\n+`)

// Log implements the tracelog.Logger interface.
func (l *logger) Log(_ context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	// silence if debug level is not enabled, unless it's a warn or error
	if !l.Logger.IsLevelEnabled(logrus.DebugLevel) && level != tracelog.LogLevelWarn && level != tracelog.LogLevelError {
		return
	}
	var configuredLogger *logrus.Entry
	if data != nil {
		// minify SQL statement, if any
		if _, ok := data["sql"]; ok {
			raw := fmt.Sprintf("%v", data["sql"])
			data["sql"] = logMinifyPattern.ReplaceAllString(raw, " ")
		}
		// use milliseconds for query duration
		if _, ok := data["time"]; ok {
			raw := fmt.Sprintf("%v", data["time"])
			d, err := time.ParseDuration(raw)
			if err == nil { // this should never happen, but lets make sure to avoid panics and missing log entries
				data["duration_ms"] = d.Milliseconds()
				delete(data, "time")
			}
		}
		// convert known keys to snake_case notation for consistency
		if _, ok := data["rowCount"]; ok {
			data["row_count"] = data["rowCount"]
			delete(data, "rowCount")
		}
		configuredLogger = l.WithFields(data)
	} else {
		configuredLogger = l.Entry
	}

	switch level {
	case tracelog.LogLevelTrace:
		configuredLogger.Trace(msg)
	case tracelog.LogLevelDebug:
		configuredLogger.Debug(msg)
	case tracelog.LogLevelInfo:
		configuredLogger.Info(msg)
	case tracelog.LogLevelWarn:
		configuredLogger.Warn(msg)
	case tracelog.LogLevelError:
		configuredLogger.Error(msg)
	default:
		// this should never happen, but if it does, something went wrong and we need to notice it
		configuredLogger.WithField("invalid_log_level", level).Error(msg)
	}
}

// Connector is an interface for opening database connections. This enabled low-level testing for how connections are
// established during load balancing.
type Connector interface {
	Open(ctx context.Context, dsn *DSN, opts ...Option) (*DB, error)
}

// sqlConnector is the default implementation of Connector using sql.Open.
type sqlConnector struct{}

// NewConnector creates a new sqlConnector.
func NewConnector() Connector {
	return &sqlConnector{}
}

// Open opens a new database connection with the given DSN and options.
func (*sqlConnector) Open(ctx context.Context, dsn *DSN, opts ...Option) (*DB, error) {
	config := applyOptions(opts)
	pgxConfig, err := pgx.ParseConfig(dsn.String())
	if err != nil {
		return nil, fmt.Errorf("parsing connection string failed: %w", err)
	}

	pgxConfig.Tracer = &tracelog.TraceLog{
		Logger:   &logger{config.logger},
		LogLevel: config.logLevel,
	}

	if config.preferSimpleProtocol {
		// TODO: there are more query execution modes that we may want to consider in the future
		// https://pkg.go.dev/github.com/jackc/pgx/v5#QueryExecMode
		pgxConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	connStr := stdlib.RegisterConnConfig(pgxConfig)
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		return nil, fmt.Errorf("open connection handle failed: %w", err)
	}

	db.SetMaxOpenConns(config.pool.MaxOpen)
	db.SetMaxIdleConns(config.pool.MaxIdle)
	db.SetConnMaxLifetime(config.pool.MaxLifetime)
	db.SetConnMaxIdleTime(config.pool.MaxIdleTime)

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("verification failed: %w", err)
	}

	return &DB{DB: db, DSN: dsn}, nil
}

// LoadBalancer represents a database load balancer.
type LoadBalancer interface {
	Primary() *DB
	Replica(context.Context) *DB
	UpToDateReplica(context.Context, *models.Repository) *DB
	Replicas() []*DB
	Close() error
	RecordLSN(context.Context, *models.Repository) error
	StartReplicaChecking(context.Context) error
	TypeOf(*DB) string
}

// DBLoadBalancer manages connections to a primary database and multiple replicas.
type DBLoadBalancer struct {
	active   bool
	primary  *DB
	replicas []*DB

	lsnCache   RepositoryCache
	connector  Connector
	resolver   DNSResolver
	fixedHosts []string

	// replicaIndex and replicaMutex are used to implement a round-robin selection of replicas.
	replicaIndex int
	replicaMutex sync.Mutex

	replicaOpenOpts      []Option
	replicaCheckInterval time.Duration

	// primaryDSN is stored separately to ensure we can derive replicas DSNs, even if the initial connection to the
	// primary database fails. This is necessary as DB.DSN is only set after successfully establishing a connection.
	primaryDSN *DSN

	metricsEnabled        bool
	promRegisterer        prometheus.Registerer
	replicaPromCollectors map[string]prometheus.Collector

	// For controlling replicas liveness probing
	livenessProber *LivenessProber

	// For controlling concurrent replicas pool resolution
	throttledPoolResolver *ThrottledPoolResolver

	// For tracking replication lag
	lagTracker *ReplicaLagTracker
}

// WithFixedHosts configures the list of static hosts to use for read replicas during database load balancing.
func WithFixedHosts(hosts []string) Option {
	return func(opts *opts) {
		opts.loadBalancing.hosts = hosts
		opts.loadBalancing.active = true
	}
}

// WithServiceDiscovery enables and configures service discovery for read replicas during database load balancing.
func WithServiceDiscovery(resolver DNSResolver) Option {
	return func(opts *opts) {
		opts.loadBalancing.resolver = resolver
		opts.loadBalancing.active = true
	}
}

// WithConnector allows specifying a custom database Connector implementation to be used to establish connections,
// otherwise sql.Open is used.
func WithConnector(connector Connector) Option {
	return func(opts *opts) {
		opts.loadBalancing.connector = connector
	}
}

// WithReplicaCheckInterval configures a custom refresh interval for the replica list when using service discovery.
// Defaults to 1 minute.
func WithReplicaCheckInterval(interval time.Duration) Option {
	return func(opts *opts) {
		opts.loadBalancing.replicaCheckInterval = interval
	}
}

// WithLSNCache allows providing a RepositoryCache implementation to be used for recording WAL insert Log Sequence
// Numbers (LSNs) that are used to enable primary sticking during database load balancing.
func WithLSNCache(cache RepositoryCache) Option {
	return func(opts *opts) {
		opts.loadBalancing.lsnStore = cache
	}
}

// WithMinResolveReplicasInterval configures the minimum time between resolve replicas calls. This prevents excessive
// replica resolution operations during periods of connection instability.
func WithMinResolveReplicasInterval(interval time.Duration) Option {
	return func(opts *opts) {
		opts.loadBalancing.minResolveReplicasInterval = interval
	}
}

// WithMetricsCollection enables metrics collection.
func WithMetricsCollection() Option {
	return func(opts *opts) {
		opts.metricsEnabled = true
	}
}

// WithPrometheusRegisterer allows specifying a custom Prometheus Registerer for metrics registration.
func WithPrometheusRegisterer(r prometheus.Registerer) Option {
	return func(opts *opts) {
		opts.promRegisterer = r
	}
}

// DNSResolver is an interface for DNS resolution operations. This enabled low-level testing for how connections are
// established during load balancing.
type DNSResolver interface {
	// LookupSRV looks up SRV records.
	LookupSRV(ctx context.Context) ([]*net.SRV, error)
	// LookupHost looks up IP addresses for a given host.
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// dnsResolver is the default implementation of DNSResolver using net.Resolver.
type dnsResolver struct {
	resolver *net.Resolver
	record   string
}

// LookupSRV performs an SRV record lookup.
func (r *dnsResolver) LookupSRV(ctx context.Context) ([]*net.SRV, error) {
	report := metrics.SRVLookup()
	_, addrs, err := r.resolver.LookupSRV(ctx, "", "", r.record)
	report(err)
	return addrs, err
}

// LookupHost performs an IP address lookup for the given host.
func (r *dnsResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	report := metrics.HostLookup()
	addrs, err := r.resolver.LookupHost(ctx, host)
	report(err)
	return addrs, err
}

// NewDNSResolver creates a new dnsResolver for the specified nameserver, port, and record.
func NewDNSResolver(nameserver string, port int, record string) DNSResolver {
	dialer := &net.Dialer{}

	return &dnsResolver{
		resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", nameserver, port))
			},
		},
		record: record,
	}
}

func resolveHosts(ctx context.Context, resolver DNSResolver) ([]*net.TCPAddr, error) {
	srvs, err := resolver.LookupSRV(ctx)
	if err != nil {
		return nil, fmt.Errorf("error resolving DNS SRV record: %w", err)
	}

	var result *multierror.Error
	var addrs []*net.TCPAddr
	for _, srv := range srvs {
		// TODO: consider allowing partial successes where only a subset of replicas is reachable
		ips, err := resolver.LookupHost(ctx, srv.Target)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("error resolving host %q address: %v", srv.Target, err))
			continue
		}
		for _, ip := range ips {
			addr := &net.TCPAddr{
				IP:   net.ParseIP(ip),
				Port: int(srv.Port),
			}
			addrs = append(addrs, addr)
		}
	}

	if result.ErrorOrNil() != nil {
		return nil, result
	}

	return addrs, nil
}

// logger returns a log.Logger decorated with a key/value pair that uniquely identifies all entries as being emitted
// by the database load balancer component. Instead of relying on a fixed log.Logger instance, this method allows
// retrieving and extending a base logger embedded in the input context (if any) to preserve relevant key/value
// pairs introduced upstream (such as a correlation ID, present when calling from the API handlers).
func (*DBLoadBalancer) logger(ctx context.Context) log.Logger {
	return log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"component": "registry.datastore.DBLoadBalancer",
	})
}

func (lb *DBLoadBalancer) metricsCollector(db *DB, hostType string) *sqlmetrics.DBStatsCollector {
	return sqlmetrics.NewDBStatsCollector(
		lb.primaryDSN.DBName,
		db,
		sqlmetrics.WithExtraLabels(map[string]string{
			"host_type": hostType,
			"host_addr": db.Address(),
		}),
	)
}

// LivenessProber manages liveness probes for database hosts.
type LivenessProber struct {
	sync.Mutex
	inProgress  map[string]time.Time       // Maps host address to probe start time
	minInterval time.Duration              // Minimum time between probes for the same host
	timeout     time.Duration              // Maximum time for a probe to run
	onUnhealthy func(context.Context, *DB) // Callback for unhealthy hosts
}

// LivenessProberOption configures a LivenessProber.
type LivenessProberOption func(*LivenessProber)

// WithMinProbeInterval sets the minimum interval between probes for the same host.
func WithMinProbeInterval(interval time.Duration) LivenessProberOption {
	return func(p *LivenessProber) {
		p.minInterval = interval
	}
}

// WithProbeTimeout sets the timeout for a probe operation.
func WithProbeTimeout(timeout time.Duration) LivenessProberOption {
	return func(p *LivenessProber) {
		p.timeout = timeout
	}
}

// WithUnhealthyCallback sets the callback to be invoked when a host is determined to be unhealthy.
func WithUnhealthyCallback(callback func(context.Context, *DB)) LivenessProberOption {
	return func(p *LivenessProber) {
		p.onUnhealthy = callback
	}
}

// NewLivenessProber creates a new LivenessProber with the given options.
func NewLivenessProber(opts ...LivenessProberOption) *LivenessProber {
	prober := &LivenessProber{
		inProgress:  make(map[string]time.Time),
		minInterval: minLivenessProbeInterval,
		timeout:     livenessProbeTimeout,
	}

	for _, opt := range opts {
		opt(prober)
	}

	return prober
}

// BeginCheck checks if a probe is allowed for the given host. It returns false if another probe for this host is in
// progress or if a probe was completed too recently (within minInterval). If the probe is allowed, it
// marks the host as being probed and returns true.
func (p *LivenessProber) BeginCheck(hostAddr string) bool {
	p.Lock()
	defer p.Unlock()

	lastProbe, exists := p.inProgress[hostAddr]
	if exists && time.Since(lastProbe) < p.minInterval {
		return false
	}

	p.inProgress[hostAddr] = time.Now()
	return true
}

// EndCheck marks a probe as completed by removing its entry from the in-progress tracking map,
// allowing future probes for this host to proceed (subject to timing constraints).
func (p *LivenessProber) EndCheck(hostAddr string) {
	p.Lock()
	defer p.Unlock()
	delete(p.inProgress, hostAddr)
}

// Probe performs a health check on a database host. The probe rate is limited to prevent excessive
// concurrent probing of the same host. If the probe fails, the onUnhealthy callback is invoked.
func (p *LivenessProber) Probe(ctx context.Context, db *DB) {
	addr := db.Address()
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"db_host_addr": addr})

	// Check if this host is already being probed and mark it if not
	if !p.BeginCheck(addr) {
		l.Info("skipping liveness probe, already in progress or too recent")
		return
	}

	// When we're done with this function, remove the in-progress marker
	defer p.EndCheck(addr)

	// Perform a lightweight health check with a short timeout
	probeCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	l.Info("performing liveness probe")
	start := time.Now()
	err := db.PingContext(probeCtx)
	duration := time.Since(start).Seconds()
	if err == nil {
		l.WithFields(log.Fields{"duration_s": duration}).Info("host passed liveness probe")
		return
	}

	// If we get here, the liveness probe failed
	l.WithFields(log.Fields{"duration_s": duration}).WithError(err).Warn("host failed liveness probe; invoking callback")
	if p.onUnhealthy != nil {
		p.onUnhealthy(ctx, db)
	}
}

// ThrottledPoolResolver manages resolution of database replicas with throttling to prevent excessive operations.
type ThrottledPoolResolver struct {
	sync.Mutex
	inProgress   bool
	lastComplete time.Time
	minInterval  time.Duration
	resolveFn    func(context.Context) error
}

// ThrottledPoolResolverOption configures a ThrottledPoolResolver.
type ThrottledPoolResolverOption func(*ThrottledPoolResolver)

// WithMinInterval sets the minimum interval between replica resolutions.
func WithMinInterval(interval time.Duration) ThrottledPoolResolverOption {
	return func(r *ThrottledPoolResolver) {
		r.minInterval = interval
	}
}

// WithResolveFunction sets the function that performs the actual resolution.
func WithResolveFunction(fn func(context.Context) error) ThrottledPoolResolverOption {
	return func(r *ThrottledPoolResolver) {
		r.resolveFn = fn
	}
}

// NewThrottledPoolResolver creates a new ThrottledPoolResolver with the given options.
func NewThrottledPoolResolver(opts ...ThrottledPoolResolverOption) *ThrottledPoolResolver {
	resolver := &ThrottledPoolResolver{
		minInterval: minResolveReplicasInterval,
	}

	for _, opt := range opts {
		opt(resolver)
	}

	return resolver
}

// Begin checks if a replica pool resolution operation is currently allowed. It returns false if another resolution is
// already in progress or if one was completed too recently (within minInterval). If resolution is allowed, it marks
// the operation as in progress and returns true.
func (r *ThrottledPoolResolver) Begin() bool {
	r.Lock()
	defer r.Unlock()

	if r.inProgress || (time.Since(r.lastComplete) < r.minInterval) {
		return false
	}

	r.inProgress = true
	return true
}

// Complete marks a replicas resolution operation as complete and updates the timestamp of the last completed operation
// to enforce the minimum interval between operations.
func (r *ThrottledPoolResolver) Complete() {
	r.Lock()
	defer r.Unlock()

	r.inProgress = false
	r.lastComplete = time.Now()
}

// Resolve triggers a resolution of the replica pool if allowed by throttling constraints.
// It returns true if the resolution was performed, false if it was skipped due to throttling.
func (r *ThrottledPoolResolver) Resolve(ctx context.Context) bool {
	l := log.GetLogger(log.WithContext(ctx))

	// Check if another resolution is already in progress or happened too recently
	if !r.Begin() {
		l.Info("skipping replica pool resolution, already in progress or too recent")
		return false
	}

	// When we're done, mark the operation as complete
	defer r.Complete()

	// Perform the resolution
	l.Info("resolving replicas")
	if err := r.resolveFn(ctx); err != nil {
		l.WithError(err).Error("failed to resolve replicas")
	} else {
		l.Info("successfully resolved replicas")
	}

	return true
}

// isConnectivityError determines if the given error is related to database connectivity issues. It checks for specific
// PostgreSQL error classes that may indicate severe connection problems:
// - Class 08: Connection Exception (connection failures, protocol violations)
// - Class 57: Operator Intervention (server shutdown, crash)
// - Class 53: Insufficient Resources (self-explanatory)
// - Class 58: System Error (errors external to PostgreSQL itself)
// - Class XX: Internal Error (self-explanatory)
// Note: We intentionally don't check for raw network timeout as those can lead to a high rate of false positives. The
// list of watched errors should be fine-tuned as we experience connectivity issues in production.
// See https://www.postgresql.org/docs/current/errcodes-appendix.html
func isConnectivityError(err error) bool {
	if err == nil {
		return false
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgerrcode.IsConnectionException(pgErr.Code) ||
			pgerrcode.IsOperatorIntervention(pgErr.Code) ||
			pgerrcode.IsInsufficientResources(pgErr.Code) ||
			pgerrcode.IsSystemError(pgErr.Code) ||
			pgerrcode.IsInternalError(pgErr.Code) {
			return true
		}
	}

	return false
}

// ProcessQueryError handles database connectivity errors during query executions by triggering appropriate responses:
// - For primary database errors, it initiates a full replica resolution to refresh the pool
// - For replica database errors, it initiates an individual liveness probe that may retire the (faulty?) replica
func (lb *DBLoadBalancer) ProcessQueryError(ctx context.Context, db *DB, query string, err error) {
	if err != nil && isConnectivityError(err) {
		hostType := lb.TypeOf(db)
		l := lb.logger(ctx).WithError(err).WithFields(log.Fields{
			"db_host_type": hostType,
			"db_host_addr": db.Address(),
			"query":        query,
		})

		switch hostType {
		case HostTypePrimary:
			// If the primary connection fails (a failover event is possible), proactively refresh all replicas
			l.Warn("primary database connection error during query execution; initiating replica resolution")
			go lb.throttledPoolResolver.Resolve(context.WithoutCancel(ctx)) // detach from outer context to avoid external cancellation
		case HostTypeReplica:
			// For a replica, run a liveness probe and retire it if necessary
			l.Warn("replica database connection error during query execution; initiating liveness probe")
			go lb.livenessProber.Probe(context.WithoutCancel(ctx), db)
		default:
			// This is not supposed to happen, log and report
			err := fmt.Errorf("unknown database host type: %w", err)
			l.Error(err)
			errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		}
	}
}

// unregisterReplicaMetricsCollector removes the Prometheus metrics collector associated with a database replica.
// This should be called when a replica is retired from the pool to ensure metrics are properly cleaned up.
// If metrics collection is disabled or if no collector exists for the given replica, this is a no-op.
func (lb *DBLoadBalancer) unregisterReplicaMetricsCollector(r *DB) {
	if lb.metricsEnabled {
		if collector, exists := lb.replicaPromCollectors[r.Address()]; exists {
			lb.promRegisterer.Unregister(collector)
			delete(lb.replicaPromCollectors, r.Address())
		}
	}
}

// ResolveReplicas initializes or updates the list of available replicas atomically by resolving the provided hosts
// either through service discovery or using a fixed hosts list. As result, the load balancer replica pool will be
// up-to-date. Replicas for which we failed to establish a connection to are not included in the pool.
func (lb *DBLoadBalancer) ResolveReplicas(ctx context.Context) error {
	lb.replicaMutex.Lock()
	defer lb.replicaMutex.Unlock()

	ctx, cancel := context.WithTimeout(ctx, ReplicaResolveTimeout)
	defer cancel()

	var result *multierror.Error
	l := lb.logger(ctx)

	// Resolve replica DSNs
	var resolvedDSNs []DSN
	if lb.resolver != nil {
		l.Info("resolving replicas with service discovery")
		addrs, err := resolveHosts(ctx, lb.resolver)
		if err != nil {
			return fmt.Errorf("failed to resolve replica hosts: %w", err)
		}
		for _, addr := range addrs {
			dsn := *lb.primaryDSN
			dsn.Host = addr.IP.String()
			dsn.Port = addr.Port
			resolvedDSNs = append(resolvedDSNs, dsn)
		}
	} else if len(lb.fixedHosts) > 0 {
		l.Info("resolving replicas with fixed hosts list")
		for _, host := range lb.fixedHosts {
			dsn := *lb.primaryDSN
			dsn.Host = host
			resolvedDSNs = append(resolvedDSNs, dsn)
		}
	}

	// Open connections for _added_ replicas
	var outputReplicas []*DB
	var added, removed []string
	for i := range resolvedDSNs {
		var err error
		dsn := &resolvedDSNs[i]
		l = l.WithFields(logrus.Fields{"db_replica_addr": dsn.Address()})

		r := dbByAddress(lb.replicas, dsn.Address())
		if r != nil {
			// check if connection to existing replica is still usable
			if err := r.PingContext(ctx); err != nil {
				l.WithError(err).Warn("replica is known but connection is stale, attempting to reconnect")
				r, err = lb.connector.Open(ctx, dsn, lb.replicaOpenOpts...)
				if err != nil {
					result = multierror.Append(result, fmt.Errorf("reopening replica %q database connection: %w", dsn.Address(), err))
					continue
				}
			} else {
				l.Info("replica is known and healthy, reusing connection")
			}
		} else {
			l.Info("replica is new, opening connection")
			if r, err = lb.connector.Open(ctx, dsn, lb.replicaOpenOpts...); err != nil {
				result = multierror.Append(result, fmt.Errorf("failed to open replica %q database connection: %w", dsn.Address(), err))
				continue
			}
			added = append(added, r.Address())
			metrics.ReplicaAdded()

			// Register metrics collector for the added replica
			if lb.metricsEnabled {
				collector := lb.metricsCollector(r, HostTypeReplica)
				// Unlike the primary host metrics collector, replica collectors wil be registered in the background
				// whenever the pool changes. We don't want to cause a panic here, so we'll rely on prometheus.Register
				// instead of prometheus.MustRegister and gracefully handle an error by logging and reporting it.
				if err := lb.promRegisterer.Register(collector); err != nil {
					l.WithError(err).WithFields(log.Fields{"db_replica_addr": r.Address()}).
						Error("failed to register collector for database replica metrics")
					errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
				}
				lb.replicaPromCollectors[r.Address()] = collector
			}
		}
		r.errorProcessor = lb
		outputReplicas = append(outputReplicas, r)
	}

	// Identify removed replicas
	for _, r := range lb.replicas {
		if dbByAddress(outputReplicas, r.Address()) == nil {
			removed = append(removed, r.Address())
			metrics.ReplicaRemoved()

			// Unregister the metrics collector for the removed replica
			lb.unregisterReplicaMetricsCollector(r)

			// Close handlers for retired replicas
			l.WithFields(log.Fields{"db_replica_addr": r.Address()}).Info("closing connection handler for retired replica")
			if err := r.Close(); err != nil {
				err = fmt.Errorf("failed to close retired replica %q connection: %w", r.Address(), err)
				result = multierror.Append(result, err)
				errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
			}
		}
	}

	l.WithFields(logrus.Fields{
		"added_hosts":   strings.Join(added, ","),
		"removed_hosts": strings.Join(removed, ","),
	}).Info("updating replicas list")
	metrics.ReplicaPoolSize(len(outputReplicas))
	lb.replicas = outputReplicas

	return result.ErrorOrNil()
}

func dbByAddress(dbs []*DB, addr string) *DB {
	for _, r := range dbs {
		if r.Address() == addr {
			return r
		}
	}
	return nil
}

// StartReplicaChecking synchronously refreshes the list of replicas in the configured interval.
func (lb *DBLoadBalancer) StartReplicaChecking(ctx context.Context) error {
	// If the check interval was set to zero (no recurring checks) or the resolver is not set (service discovery
	// was not enabled), then exit early as there is nothing to do
	if lb.replicaCheckInterval == 0 || lb.resolver == nil {
		return nil
	}

	l := lb.logger(ctx)
	t := time.NewTicker(lb.replicaCheckInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			l.WithFields(log.Fields{"interval_ms": lb.replicaCheckInterval.Milliseconds()}).
				Info("scheduled refresh of replicas list")
			lb.throttledPoolResolver.Resolve(ctx)
		}
	}
}

// removeReplica removes a replica from the pool and closes its connection.
func (lb *DBLoadBalancer) removeReplica(ctx context.Context, r *DB) {
	replicaAddr := r.Address()
	l := lb.logger(ctx).WithFields(log.Fields{"db_replica_addr": replicaAddr})

	lb.replicaMutex.Lock()
	defer lb.replicaMutex.Unlock()

	for i, replica := range lb.replicas {
		if replica.Address() == replicaAddr {
			l.Warn("removing replica from pool")
			lb.replicas = append(lb.replicas[:i], lb.replicas[i+1:]...)
			lb.unregisterReplicaMetricsCollector(r)
			metrics.ReplicaRemoved()
			metrics.ReplicaPoolSize(len(lb.replicas))

			if err := r.Close(); err != nil {
				l.WithError(err).Error("error closing retired replica connection")
			}
			break
		}
	}
}

// NewDBLoadBalancer initializes a DBLoadBalancer with primary and replica connections. An error is returned if failed
// to connect to the primary server. Failures to connect to replica server(s) are handled gracefully, that is, logged,
// reported and ignored. This is to prevent halting the app start, as it can function with the primary server only.
// DBLoadBalancer.StartReplicaChecking can be used to periodically refresh the list of replicas, potentially leading to
// the self-healing of transient connection failures during this initialization.
func NewDBLoadBalancer(ctx context.Context, primaryDSN *DSN, opts ...Option) (*DBLoadBalancer, error) {
	config := applyOptions(opts)

	lb := &DBLoadBalancer{
		active:                config.loadBalancing.active,
		primaryDSN:            primaryDSN,
		connector:             config.loadBalancing.connector,
		resolver:              config.loadBalancing.resolver,
		fixedHosts:            config.loadBalancing.hosts,
		replicaOpenOpts:       opts,
		replicaCheckInterval:  config.loadBalancing.replicaCheckInterval,
		lsnCache:              config.loadBalancing.lsnStore,
		metricsEnabled:        config.metricsEnabled,
		promRegisterer:        config.promRegisterer,
		replicaPromCollectors: make(map[string]prometheus.Collector),
	}

	// Initialize the replicas liveness prober with a callback to retire unhealthy hosts
	lb.livenessProber = NewLivenessProber(WithUnhealthyCallback(lb.removeReplica))

	// Initialize the throttled replica pool resolver
	lb.throttledPoolResolver = NewThrottledPoolResolver(
		WithMinInterval(minResolveReplicasInterval),
		WithResolveFunction(lb.ResolveReplicas),
	)

	// Initialize the replica lag tracker using the same interval as replica checking
	lb.lagTracker = NewReplicaLagTracker(
		WithLagCheckInterval(config.loadBalancing.replicaCheckInterval),
	)

	primary, err := lb.connector.Open(ctx, primaryDSN, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to open primary database connection: %w", err)
	}
	primary.errorProcessor = lb
	lb.primary = primary

	// Conditionally register metrics for the primary database handle
	if lb.metricsEnabled {
		lb.promRegisterer.MustRegister(lb.metricsCollector(primary, HostTypePrimary))
	}

	if lb.active {
		ctx, cancel := context.WithTimeout(ctx, InitReplicaResolveTimeout)
		defer cancel()

		if err := lb.ResolveReplicas(ctx); err != nil {
			lb.logger(ctx).WithError(err).Error("failed to resolve database load balancing replicas")
			errortracking.Capture(err, errortracking.WithContext(ctx), errortracking.WithStackTrace())
		}
	}

	return lb, nil
}

// Primary returns the primary database handler.
func (lb *DBLoadBalancer) Primary() *DB {
	return lb.primary
}

// Replica returns a round-robin elected replica database handler. If no replicas are configured, then the primary
// database handler is returned.
func (lb *DBLoadBalancer) Replica(ctx context.Context) *DB {
	lb.replicaMutex.Lock()
	defer lb.replicaMutex.Unlock()

	if len(lb.replicas) == 0 {
		lb.logger(ctx).Info("no replicas available, falling back to primary")
		return lb.primary
	}

	replica := lb.replicas[lb.replicaIndex]
	lb.replicaIndex = (lb.replicaIndex + 1) % len(lb.replicas)

	return replica
}

// Replicas returns all replica database handlers currently in the pool.
func (lb *DBLoadBalancer) Replicas() []*DB {
	return lb.replicas
}

// Close closes all database connections managed by the DBLoadBalancer.
func (lb *DBLoadBalancer) Close() error {
	var result *multierror.Error

	if err := lb.primary.Close(); err != nil {
		result = multierror.Append(result, fmt.Errorf("failed closing primary connection: %w", err))
	}

	for _, replica := range lb.replicas {
		if err := replica.Close(); err != nil {
			result = multierror.Append(result, fmt.Errorf("failed closing replica %q connection: %w", replica.Address(), err))
		}
	}

	return result.ErrorOrNil()
}

// RecordLSN queries the current primary database WAL insert Log Sequence Number (LSN) and records it in the LSN cache
// in association with a given models.Repository.
// See https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs/spec/gitlab/database-load-balancing.md?ref_type=heads#primary-sticking
func (lb *DBLoadBalancer) RecordLSN(ctx context.Context, r *models.Repository) error {
	if lb.lsnCache == nil {
		return fmt.Errorf("LSN cache is not configured")
	}

	lsn, err := lb.primaryLSN(ctx)
	if err != nil {
		return fmt.Errorf("failed to query current WAL insert LSN: %w", err)
	}

	if err := lb.lsnCache.SetLSN(ctx, r, lsn); err != nil {
		return fmt.Errorf("failed to cache WAL insert LSN: %w", err)
	}

	lb.logger(ctx).WithFields(log.Fields{"repository": r.Path, "lsn": lsn}).Info("current WAL insert LSN recorded")

	return nil
}

// UpToDateReplica returns the most suitable database connection handle for serving a read request for a given
// models.Repository based on the last recorded primary Log Sequence Number (LSN) for that same repository. All errors
// during this method execution are handled gracefully with a fallback to the primary connection handle.
// Relevant errors during query execution should be handled _explicitly_ by the caller. For example, if the caller
// obtained a connection handle for replica `R` at time `T`, and `R` is retired from the load balancer pool at `T+1`,
// then any queries attempted against `R` after `T+1` would result in a `sql: database is closed` error raised by the
// `database/sql` package. In such case, it is the caller's responsibility to fall back to Primary and retry.
func (lb *DBLoadBalancer) UpToDateReplica(ctx context.Context, r *models.Repository) *DB {
	primary := lb.primary
	primary.errorProcessor = lb

	if !lb.active {
		return lb.primary
	}

	l := lb.logger(ctx).WithFields(log.Fields{"repository": r.Path})

	if lb.lsnCache == nil {
		l.Info("no LSN cache configured, falling back to primary")
		metrics.PrimaryFallbackNoCache()
		return lb.primary
	}

	// Do not let the LSN cache lookup and subsequent DB comparison (total) take more than upToDateReplicaTimeout,
	// effectively enforcing a graceful fallback to the primary database if so.
	ctx, cancel := context.WithTimeout(ctx, upToDateReplicaTimeout)
	defer cancel()

	// Get the next replica using round-robin. For simplicity, on the first iteration of DLB, we simply check against
	// the first returned replica candidate, not against (potentially) all replicas in the pool. If the elected replica
	// candidate is behind the previously recorded primary LSN, then we simply fall back to the connection handle for
	// the primary database.
	replica := lb.Replica(ctx)
	if replica == lb.primary {
		metrics.PrimaryFallbackNoReplica()
		return lb.primary
	}
	l = l.WithFields(log.Fields{"db_replica_addr": replica.Address()})

	// Fetch the primary LSN from cache
	primaryLSN, err := lb.lsnCache.GetLSN(ctx, r)
	if err != nil {
		l.WithError(err).Error("failed to fetch primary LSN from cache, falling back to primary")
		metrics.PrimaryFallbackError()
		return lb.primary
	}
	// If the record does not exist in cache, the replica is considered suitable
	if primaryLSN == "" {
		metrics.LSNCacheMiss()
		l.Info("no primary LSN found in cache, replica is eligible")
		metrics.ReplicaTarget()
		return replica
	}

	metrics.LSNCacheHit()
	l = l.WithFields(log.Fields{"primary_lsn": primaryLSN})

	// Query to check if the candidate replica is up-to-date with the primary LSN
	query := `
        WITH replica_lsn AS (
			SELECT pg_last_wal_replay_lsn () AS lsn
		)
		SELECT
			pg_wal_lsn_diff ($1::pg_lsn, lsn) <= 0
		FROM
			replica_lsn`

	var upToDate bool
	if err := replica.QueryRowContext(ctx, query, primaryLSN).Scan(&upToDate); err != nil {
		l.WithError(err).Error("failed to calculate LSN diff, falling back to primary")
		metrics.PrimaryFallbackError()
		return lb.primary
	}

	if upToDate {
		l.Info("replica is up-to-date")
		metrics.ReplicaTarget()
		return replica
	}

	l.Info("replica is not up-to-date, falling back to primary")
	metrics.PrimaryFallbackNotUpToDate()
	return lb.primary
}

// TypeOf returns the type of the provided *DB instance: HostTypePrimary, HostTypeReplica or HostTypeUnknown.
func (lb *DBLoadBalancer) TypeOf(db *DB) string {
	lb.replicaMutex.Lock()
	defer lb.replicaMutex.Unlock()

	if db == lb.primary {
		return HostTypePrimary
	}
	for _, replica := range lb.replicas {
		if db == replica {
			return HostTypeReplica
		}
	}
	return HostTypeUnknown
}

// primaryLSN returns the primary database's current write location
func (lb *DBLoadBalancer) primaryLSN(ctx context.Context) (string, error) {
	var lsn string
	query := "SELECT pg_current_wal_insert_lsn()::text AS location"
	if err := lb.primary.QueryRowContext(ctx, query).Scan(&lsn); err != nil {
		return "", err
	}

	return lsn, nil
}

// QueryBuilder helps in building SQL queries with parameters.
type QueryBuilder struct {
	sql     strings.Builder
	params  []any
	newLine bool
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{
		params: make([]any, 0),
	}
}

// Build takes the given sql string replaces any ? with the equivalent indexed
// parameter and appends elems to the args slice.
func (qb *QueryBuilder) Build(q string, qArgs ...any) error {
	placeholderCount := strings.Count(q, "?")
	if placeholderCount != len(qArgs) {
		return fmt.Errorf(
			"number of placeholders (%d) in query %q does not match the number of arguments (%d) passed",
			placeholderCount, q, len(qArgs),
		)
	}

	if q == "" {
		return nil
	}

	for _, elem := range qArgs {
		qb.params = append(qb.params, elem)
		paramName := fmt.Sprintf("$%d", len(qb.params))

		q = strings.Replace(q, "?", paramName, 1)
	}

	q = strings.Trim(q, " \t")
	newLine := q[len(q)-1] == '\n'

	// If the query ends in a newline, don't add a space before it. Adding a
	// space is a convenience for chaining expressions.
	switch {
	case qb.newLine, qb.sql.Len() == 0, q == "\n":
		_, _ = qb.sql.WriteString(q)
	default:
		_, _ = fmt.Fprintf(&qb.sql, " %s", q)
	}

	qb.newLine = newLine
	return nil
}

// WrapIntoSubqueryOf wraps existing query as a subquery of the given query.
// The outerQuery param needs to have a single %s where the current query will
// be copied into.
func (qb *QueryBuilder) WrapIntoSubqueryOf(outerQuery string) error {
	if !strings.Contains(outerQuery, "%s") || strings.Count(outerQuery, "%s") != 1 {
		return fmt.Errorf("outerQuery must contain exactly one %%s placeholder. Query: %v", outerQuery)
	}

	newSQL := strings.Builder{}
	_, _ = fmt.Fprintf(&newSQL, outerQuery, qb.sql.String())
	qb.sql = newSQL

	return nil
}

// SQL returns the rendered SQL query.
func (qb *QueryBuilder) SQL() string {
	if qb.newLine {
		return strings.TrimRight(qb.sql.String(), "\n")
	}
	return qb.sql.String()
}

// Params returns the slice of query literals to be used in the SQL query.
func (qb *QueryBuilder) Params() []any {
	ret := make([]any, len(qb.params))
	copy(ret, qb.params)
	return ret
}

// IsInRecovery checks if a provided database is in read-only mode.
func IsInRecovery(ctx context.Context, db *DB) (bool, error) {
	var inRecovery bool

	// https://www.postgresql.org/docs/9.0/functions-admin.html#:~:text=Table%209%2D58.%20Recovery%20Information%20Functions
	query := `SELECT pg_is_in_recovery()`
	err := db.QueryRowContext(ctx, query).Scan(&inRecovery)

	return inRecovery, err
}

// ReplicaLagInfo stores lag information for a replica
type ReplicaLagInfo struct {
	Address     string
	TimeLag     time.Duration
	BytesLag    int64
	LastChecked time.Time
}

// ReplicaLagTracker manages replication lag tracking
type ReplicaLagTracker struct {
	sync.Mutex
	lagInfo       map[string]*ReplicaLagInfo
	checkInterval time.Duration
}

// ReplicaLagTrackerOption configures a ReplicaLagTracker.
type ReplicaLagTrackerOption func(*ReplicaLagTracker)

// WithLagCheckInterval sets the interval for checking replication lag.
func WithLagCheckInterval(interval time.Duration) ReplicaLagTrackerOption {
	return func(t *ReplicaLagTracker) {
		if interval > 0 {
			t.checkInterval = interval
		}
	}
}

// NewReplicaLagTracker creates a new ReplicaLagTracker with the given options.
func NewReplicaLagTracker(opts ...ReplicaLagTrackerOption) *ReplicaLagTracker {
	tracker := &ReplicaLagTracker{
		lagInfo:       make(map[string]*ReplicaLagInfo),
		checkInterval: defaultReplicaCheckInterval,
	}

	for _, opt := range opts {
		opt(tracker)
	}

	return tracker
}

// Get returns the replication lag info for a replica
func (t *ReplicaLagTracker) Get(replicaAddr string) *ReplicaLagInfo {
	t.Lock()
	defer t.Unlock()

	info, exists := t.lagInfo[replicaAddr]
	if !exists {
		return nil
	}

	// Return a copy to avoid race conditions
	lagInfo := *info
	return &lagInfo
}

// set updates the replication lag information for a replica
func (t *ReplicaLagTracker) set(_ context.Context, db *DB, timeLag time.Duration, bytesLag int64) {
	t.Lock()
	defer t.Unlock()

	addr := db.Address()
	now := time.Now()

	info, exists := t.lagInfo[addr]
	if !exists {
		info = &ReplicaLagInfo{
			Address: addr,
		}
		t.lagInfo[addr] = info
	}

	info.TimeLag = timeLag
	info.BytesLag = bytesLag
	info.LastChecked = now

	// TODO: collect metrics
}

// CheckBytesLag retrieves the data-based replication lag for a replica
func (*ReplicaLagTracker) CheckBytesLag(ctx context.Context, primaryLSN string, replica *DB) (int64, error) {
	queryCtx, cancel := context.WithTimeout(ctx, replicaLagCheckTimeout)
	defer cancel()

	// Calculate bytes lag on the replica using the provided primary LSN
	var bytesLag int64
	query := `SELECT pg_wal_lsn_diff($1, pg_last_wal_replay_lsn())::bigint AS diff`
	err := replica.QueryRowContext(queryCtx, query, primaryLSN).Scan(&bytesLag)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate replica bytes lag: %w", err)
	}

	return bytesLag, nil
}

// CheckTimeLag retrieves the time-based replication lag for a replica
func (*ReplicaLagTracker) CheckTimeLag(ctx context.Context, replica *DB) (time.Duration, error) {
	queryCtx, cancel := context.WithTimeout(ctx, replicaLagCheckTimeout)
	defer cancel()

	query := `SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::float AS lag`

	var timeLagSeconds float64
	err := replica.QueryRowContext(queryCtx, query).Scan(&timeLagSeconds)
	if err != nil {
		return 0, fmt.Errorf("failed to check replica time lag: %w", err)
	}

	return time.Duration(timeLagSeconds * float64(time.Second)), nil
}

// Check checks replication lag for a specific replica and stores it.
func (t *ReplicaLagTracker) Check(ctx context.Context, primaryLSN string, replica *DB) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"db_replica_addr": replica.Address(),
	})

	timeLag, err := t.CheckTimeLag(ctx, replica)
	if err != nil {
		l.WithError(err).Error("failed to check time-based replication lag")
		return err
	}

	bytesLag, err := t.CheckBytesLag(ctx, primaryLSN, replica)
	if err != nil {
		l.WithError(err).Error("failed to check data-based replication lag")
		return err
	}

	// Log at appropriate level based on max thresholds
	l = l.WithFields(log.Fields{"lag_time_s": timeLag.Seconds(), "lag_bytes": bytesLag})

	if timeLag > MaxReplicaLagTime {
		l.Warn("replica time-based replication lag above max threshold")
	}
	if bytesLag > MaxReplicaLagBytes {
		l.Warn("replica data-based replication lag above max threshold")
	}
	if timeLag <= MaxReplicaLagTime && bytesLag <= MaxReplicaLagBytes {
		l.Info("replica replication lag below max thresholds")
	}

	t.set(ctx, replica, timeLag, bytesLag)

	return nil
}
