//go:generate mockgen -package mocks -destination mocks/db.go . Handler,Transactor

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
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/sirupsen/logrus"
)

const driverName = "pgx"

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

type openOpts struct {
	logger               *logrus.Entry
	logLevel             tracelog.LogLevel
	pool                 *PoolConfig
	preferSimpleProtocol bool
}

type PoolConfig struct {
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

// OpenOption is used to pass options to Open.
type OpenOption func(*openOpts)

// WithLogger configures the logger for the database connection driver.
func WithLogger(l *logrus.Entry) OpenOption {
	return func(opts *openOpts) {
		opts.logger = l
	}
}

// WithLogLevel configures the logger level for the database connection driver.
func WithLogLevel(l configuration.Loglevel) OpenOption {
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

	return func(opts *openOpts) {
		opts.logLevel = lvl
	}
}

// WithPoolConfig configures the settings for the database connection pool.
func WithPoolConfig(c *PoolConfig) OpenOption {
	return func(opts *openOpts) {
		opts.pool = c
	}
}

// WithPoolMaxIdle configures the maximum number of idle pool connections.
func WithPoolMaxIdle(c int) OpenOption {
	return func(opts *openOpts) {
		if opts.pool == nil {
			opts.pool = &PoolConfig{}
		}

		opts.pool.MaxIdle = c
	}
}

// WithPoolMaxOpen configures the maximum number of open pool connections.
func WithPoolMaxOpen(c int) OpenOption {
	return func(opts *openOpts) {
		if opts.pool == nil {
			opts.pool = &PoolConfig{}
		}

		opts.pool.MaxOpen = c
	}
}

// WithPreparedStatements configures the settings to allow the database
// driver to use prepared statements.
func WithPreparedStatements(b bool) OpenOption {
	return func(opts *openOpts) {
		// Registry configuration uses opposite semantics as pgx for prepared statements.
		opts.preferSimpleProtocol = !b
	}
}

func applyOptions(opts []OpenOption) openOpts {
	log := logrus.New()
	log.SetOutput(io.Discard)

	config := openOpts{
		logger: logrus.NewEntry(log),
		pool:   &PoolConfig{},
	}

	for _, v := range opts {
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

// Open creates a database connection handler.
func Open(dsn *DSN, opts ...OpenOption) (*DB, error) {
	config := applyOptions(opts)
	pgxConfig, err := pgx.ParseConfig(dsn.String())
	if err != nil {
		return nil, fmt.Errorf("datastore: parse config: %w", err)
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

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("verification failed: %w", err)
	}

	return &DB{db, dsn}, nil
}
