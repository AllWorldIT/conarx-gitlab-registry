//go:generate mockgen -package mocks -destination mocks/db.go . Handler,Transactor,Connector

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
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const driverName = "pgx"

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

// versionToServerVersionNum converts a PostgreSQL version string (e.g., "14.5")
// to the server_version_num format for PostgreSQL 10+
func versionToServerVersionNum(version string) (int, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid version format: %s", version)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	return major*10000 + minor, nil
}

// IsDBSupported checks if a provided database has a version that is supported
func IsDBSupported(ctx context.Context, db *DB) (bool, error) {
	var inSupported bool

	minVersion, err := versionToServerVersionNum(MinPostgresqlVersion)
	if err != nil {
		return false, fmt.Errorf("the minimum PostgreSQL version string %q is invalid: %w", MinPostgresqlVersion, err)
	}

	defer metrics.InstrumentQuery("server_version_check")()
	query := fmt.Sprintf("SELECT current_setting('server_version_num')::integer >= %d", minVersion)
	err = db.QueryRowContext(ctx, query).Scan(&inSupported)

	return inSupported, err
}
