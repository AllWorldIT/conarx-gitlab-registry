//go:generate mockgen -package mocks -destination mocks/db.go . Handler,Transactor,LoadBalancer,Connector,DNSResolver

package datastore

import (
	"context"
	"database/sql"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/sirupsen/logrus"
)

const (
	driverName                  = "pgx"
	dnsTimeout                  = 2 * time.Second
	defaultReplicaCheckInterval = 1 * time.Minute

	HostTypePrimary = "primary"
	HostTypeReplica = "replica"
	HostTypeUnknown = "unknown"
)

// Queryer is the common interface to execute queries on a database.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
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

// DB implements Handler.
type DB struct {
	*sql.DB
	DSN *DSN
}

// BeginTx wraps sql.Tx from the innner sql.DB within a datastore.Tx.
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transactor, error) {
	tx, err := db.DB.BeginTx(ctx, opts)

	return &Tx{tx}, err
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

// Tx implements Transactor.
type Tx struct {
	*sql.Tx
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
}

type PoolConfig struct {
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

// LoadBalancingConfig represents the database load balancing configuration.
type LoadBalancingConfig struct {
	hosts                []string
	resolver             DNSResolver
	connector            Connector
	replicaCheckInterval time.Duration
	lsnStore             RepositoryCache
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
	log := logrus.New()
	log.SetOutput(io.Discard)

	config := opts{
		logger: logrus.NewEntry(log),
		pool:   &PoolConfig{},
		loadBalancing: &LoadBalancingConfig{
			connector:            NewConnector(),
			replicaCheckInterval: defaultReplicaCheckInterval,
		},
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
func (l *logger) Log(_ context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
	// silence if debug level is not enabled, unless it's a warn or error
	if !l.Logger.IsLevelEnabled(logrus.DebugLevel) && level != tracelog.LogLevelWarn && level != tracelog.LogLevelError {
		return
	}
	var log *logrus.Entry
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
		log = l.WithFields(data)
	} else {
		log = l.Entry
	}

	switch level {
	case tracelog.LogLevelTrace:
		log.Trace(msg)
	case tracelog.LogLevelDebug:
		log.Debug(msg)
	case tracelog.LogLevelInfo:
		log.Info(msg)
	case tracelog.LogLevelWarn:
		log.Warn(msg)
	case tracelog.LogLevelError:
		log.Error(msg)
	default:
		// this should never happen, but if it does, something went wrong and we need to notice it
		log.WithField("invalid_log_level", level).Error(msg)
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
func (c *sqlConnector) Open(ctx context.Context, dsn *DSN, opts ...Option) (*DB, error) {
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

	return &DB{db, dsn}, nil
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
}

// WithFixedHosts configures the list of static hosts to use for read replicas during database load balancing.
func WithFixedHosts(hosts []string) Option {
	return func(opts *opts) {
		opts.loadBalancing.hosts = hosts
	}
}

// WithServiceDiscovery enables and configures service discovery for read replicas during database load balancing.
func WithServiceDiscovery(resolver DNSResolver) Option {
	return func(opts *opts) {
		opts.loadBalancing.resolver = resolver
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
	return &dnsResolver{
		resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				return net.DialTimeout("tcp", fmt.Sprintf("%s:%d", nameserver, port), dnsTimeout)
			},
		},
		record: record,
	}
}

func resolveHosts(ctx context.Context, resolver DNSResolver) ([]*net.TCPAddr, error) {
	ctx, cancel := context.WithTimeout(ctx, dnsTimeout)
	defer cancel()

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
func (lb *DBLoadBalancer) logger(ctx context.Context) log.Logger {
	return log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"component": "registry.datastore.DBLoadBalancer",
	})
}

// ResolveReplicas initializes or updates the list of available replicas atomically by resolving the provided hosts
// either through service discovery or using a fixed hosts list. As result, the load balancer replica pool will be
// up-to-date. Replicas for which we failed to establish a connection to are not included in the pool.
func (lb *DBLoadBalancer) ResolveReplicas(ctx context.Context) *multierror.Error {
	var replicas []*DB
	var result *multierror.Error

	l := lb.logger(ctx)
	tmpDSN := *lb.primaryDSN
	var replicaDSNs []DSN

	if lb.resolver != nil {
		l.Info("resolving replicas with service discovery")
		addrs, err := resolveHosts(ctx, lb.resolver)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("failed to resolve replica hosts: %w", err))
		} else {
			for _, addr := range addrs {

				tmpDSN.Host = addr.IP.String()
				tmpDSN.Port = addr.Port
				replicaDSNs = append(replicaDSNs, tmpDSN)
			}
		}
	} else if len(lb.fixedHosts) > 0 {
		l.Info("resolving replicas with fixed hosts list")
		for _, host := range lb.fixedHosts {
			tmpDSN.Host = host
			replicaDSNs = append(replicaDSNs, tmpDSN)
		}
	}

	for i := range replicaDSNs {
		dsn := replicaDSNs[i]
		l := l.WithFields(logrus.Fields{
			"index":   i,
			"total":   len(replicaDSNs),
			"address": dsn.Address(),
		})
		l.Info("opening replica connection")
		replica, err := lb.connector.Open(ctx, &dsn, lb.replicaOpenOpts...)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("failed to open replica %q database connection: %w", dsn.Address(), err))
		} else {
			replicas = append(replicas, replica)
		}
	}

	// TODO(dlb): use disconnecttimeout to close handlers from retired replicas
	lb.replicaMutex.Lock()
	defer lb.replicaMutex.Unlock()

	var added, removed []string
	for _, r := range lb.replicas {
		if !containsReplica(replicas, r.Address()) {
			removed = append(removed, r.Address())
			metrics.ReplicaRemoved()
		}
	}
	for _, r := range replicas {
		if !containsReplica(lb.replicas, r.Address()) {
			added = append(added, r.Address())
			metrics.ReplicaAdded()
		}
	}

	l.WithFields(logrus.Fields{"added": added, "removed": removed}).Info("updating replicas list")
	metrics.ReplicaPoolSize(len(replicas))
	lb.replicas = replicas

	return result
}

func containsReplica(replicas []*DB, addr string) bool {
	for _, r := range replicas {
		if r.Address() == addr {
			return true
		}
	}
	return false
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
				Info("refreshing replicas list")
			if err := lb.ResolveReplicas(ctx); err != nil {
				l.WithError(err).Error("failed to refresh replicas list")
			}
		}
	}
}

// NewDBLoadBalancer initializes a DBLoadBalancer with primary and replica connections.
func NewDBLoadBalancer(ctx context.Context, primaryDSN *DSN, opts ...Option) (*DBLoadBalancer, error) {
	config := applyOptions(opts)
	var result *multierror.Error

	lb := &DBLoadBalancer{
		primaryDSN:           primaryDSN,
		connector:            config.loadBalancing.connector,
		resolver:             config.loadBalancing.resolver,
		fixedHosts:           config.loadBalancing.hosts,
		replicaOpenOpts:      opts,
		replicaCheckInterval: config.loadBalancing.replicaCheckInterval,
		lsnCache:             config.loadBalancing.lsnStore,
	}

	primary, err := lb.connector.Open(ctx, primaryDSN, opts...)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("failed to open primary database connection: %w", err))
	}
	lb.primary = primary

	if err := lb.ResolveReplicas(ctx); err.ErrorOrNil() != nil {
		result = multierror.Append(result, err)
	}

	if result.ErrorOrNil() != nil {
		return nil, result.ErrorOrNil()
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
	if len(lb.replicas) == 0 {
		lb.logger(ctx).Info("no replicas available, falling back to primary")
		return lb.primary
	}

	lb.replicaMutex.Lock()
	defer lb.replicaMutex.Unlock()

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

	var lsn string
	if err := lb.Primary().QueryRowContext(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&lsn); err != nil {
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
// are handled gracefully with a fallback to the primary connection handle.
func (lb *DBLoadBalancer) UpToDateReplica(ctx context.Context, r *models.Repository) *DB {
	l := lb.logger(ctx).WithFields(log.Fields{"repository": r.Path})

	if lb.lsnCache == nil {
		l.Info("no LSN cache configured, falling back to primary")
		metrics.PrimaryFallbackNoCache()
		return lb.primary
	}

	// Get the next replica using round-robin. For simplicity, on the first iteration of DLB, we simply check against
	// the first returned replica candidate, not against (potentially) all replicas in the pool. If the elected replica
	// candidate is behind the previously recorded primary LSN, then we simply fall back to the connection handle for
	// the primary database.
	replica := lb.Replica(ctx)
	if replica == lb.primary {
		metrics.PrimaryFallbackNoReplica()
		return lb.primary
	}

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

	newSql := strings.Builder{}
	_, _ = fmt.Fprintf(&newSql, outerQuery, qb.sql.String())
	qb.sql = newSql

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
