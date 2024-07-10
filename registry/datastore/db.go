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
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/sirupsen/logrus"
)

const (
	driverName = "pgx"
	dnsTimeout = 2 * time.Second
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
	dsn *DSN
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
	hosts    []string
	resolver DNSResolver
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
		logger:        logrus.NewEntry(log),
		pool:          &PoolConfig{},
		loadBalancing: &LoadBalancingConfig{},
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
	Replica() *DB
	Replicas() []*DB
	Close() error
}

// DBLoadBalancer manages connections to a primary database and multiple replicas.
type DBLoadBalancer struct {
	primary  *DB
	replicas []*DB

	// replicaIndex and replicaMutex are used to implement a round-robin selection of replicas.
	replicaIndex int
	replicaMutex sync.Mutex
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
	_, addrs, err := r.resolver.LookupSRV(ctx, "", "", r.record)
	return addrs, err
}

// LookupHost performs an IP address lookup for the given host.
func (r *dnsResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return r.resolver.LookupHost(ctx, host)
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
		return nil, result.ErrorOrNil()
	}

	return addrs, nil
}

// NewDBLoadBalancer initializes a DBLoadBalancer with primary and replica connections. A custom database Connector
// implementation can be used to establish connections, otherwise sql.Open is used.
func NewDBLoadBalancer(ctx context.Context, primaryDSN *DSN, connector Connector, opts ...Option) (*DBLoadBalancer, error) {
	config := applyOptions(opts)
	var result *multierror.Error

	if connector == nil {
		connector = NewConnector()
	}

	primary, err := connector.Open(ctx, primaryDSN, opts...)
	if err != nil {
		result = multierror.Append(result, fmt.Errorf("failed to open primary database connection: %w", err))
	}

	var replicas []*DB
	if config.loadBalancing.resolver != nil {
		addrs, err := resolveHosts(ctx, config.loadBalancing.resolver)
		if err != nil {
			result = multierror.Append(result, fmt.Errorf("failed to fetch replica hosts: %w", err))
		} else {
			for _, addr := range addrs {
				dsn := *primaryDSN
				dsn.Host = addr.IP.String()
				dsn.Port = addr.Port
				replica, err := connector.Open(ctx, &dsn, opts...)
				if err != nil {
					// TODO: consider allowing partial successes where only a subset of replicas is reachable
					result = multierror.Append(result, fmt.Errorf("failed to open replica %q database connection: %w", dsn.Host, err))
				} else {
					replicas = append(replicas, replica)
				}
			}
		}
	} else if len(config.loadBalancing.hosts) > 0 {
		for _, host := range config.loadBalancing.hosts {
			dsn := *primaryDSN
			dsn.Host = host
			replica, err := connector.Open(ctx, &dsn, opts...)
			if err != nil {
				result = multierror.Append(result, fmt.Errorf("failed to open replica %q database connection: %w", dsn.Host, err))
			} else {
				replicas = append(replicas, replica)
			}
		}
	}

	if result.ErrorOrNil() != nil {
		return nil, result
	}

	return &DBLoadBalancer{primary: primary, replicas: replicas}, nil
}

// Primary returns the primary database handler.
func (lb *DBLoadBalancer) Primary() *DB {
	return lb.primary
}

// Replica returns a round-robin elected replica database handler. If no replicas are configured, then the primary
// database handler is returned.
func (lb *DBLoadBalancer) Replica() *DB {
	if len(lb.replicas) == 0 {
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
			result = multierror.Append(result, fmt.Errorf("failed closing replica %q connection: %w", replica.dsn.Host, err))
		}
	}

	return result.ErrorOrNil()
}
