//go:generate mockgen -package mocks -destination mocks/worker.go . Worker

package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/internal"
	"github.com/getsentry/sentry-go"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/errortracking"
)

const (
	componentKey     = "component"
	defaultTxTimeout = 10 * time.Second
)

// Worker represents an online GC worker, which is responsible for processing review tasks, determining eligibility
// for deletion and deleting artifacts from the backend if eligible.
type Worker interface {
	// Name returns the worker name for observability purposes.
	Name() string
	// Run executes the worker once, processing the next available GC task.
	Run(context.Context) RunResult
}

// for test purposes (mocking)
var systemClock internal.Clock = clock.New()

type baseWorker struct {
	name      string
	db        datastore.Handler
	logger    log.Logger
	txTimeout time.Duration
}

// Name implements Worker.
func (w *baseWorker) Name() string {
	return w.name
}

func (w *baseWorker) applyDefaults() {
	if w.logger == nil {
		defaultLogger := logrus.New()
		defaultLogger.SetOutput(io.Discard)
		w.logger = log.FromLogrusLogger(defaultLogger)
	}
	if w.txTimeout == 0 {
		w.txTimeout = defaultTxTimeout
	}
}

// RunResult represents the result of a worker run.
type RunResult struct {
	// Found indicates whether there was a task available or not, regardless if processing it succeeded or not.
	Found bool
	// Dangling indicates whether the reviewed artifact was dangling.
	Dangling bool
	// Event indicates the event that last led to the task creation.
	Event string
	// Err indicates if there was an error during the run.
	Err error
}

type processor interface {
	processTask(context.Context) RunResult
}

func (w *baseWorker) run(ctx context.Context, p processor) RunResult {
	ctx = injectCorrelationID(ctx, w.logger)

	res := p.processTask(ctx)
	if res.Err != nil {
		res.Err = fmt.Errorf("processing task: %w", res.Err)
		w.logAndReportErr(ctx, res.Err)
	}

	return res
}

func (w *baseWorker) logAndReportErr(ctx context.Context, err error) {
	errortracking.Capture(
		err,
		errortracking.WithContext(ctx),
		errortracking.WithField(componentKey, w.name),
		errortracking.WithStackTrace(),
	)
	log.GetLogger(log.WithContext(ctx)).WithError(err).Error(err.Error())
}

// rollbackOnExit can be used to ensure that no transaction is left open without an explicit commit/rollback. Such can
// happen if we (humans) miss an explicit commit/rollback call somewhere, or the program has faced a panic and therefore
// escaped the error handling path.
// The database/sql standard library keeps track of the state of each transaction. After the *first* call of Commit and
// Rollback methods, subsequent calls are bypassed (not sent to the database server), and the unique sql.ErrTxDone error
// is returned to the caller. This method leverages this behavior to guarantee that we always roll back the transaction
// here for safety if no commit/rollback was executed in the appropriate place. For visibility, an error is logged and
// reported. This error carries a correlation ID that will help us track down the execution path where a transaction was
// not subject to an explicit commit/rollback as expected. Additionally, it is advisable to configure metrics/alerts on
// the database server to monitor rollback commands. This should aid in ensuring that rolled back transactions are
// detected as misbehavior or an error that should be investigated.
func (w *baseWorker) rollbackOnExit(ctx context.Context, tx datastore.Transactor) {
	rollback := func() {
		if err := tx.Rollback(); err != nil {
			switch {
			case errors.Is(err, sql.ErrTxDone):
				// the transaction was already committed or rolled back (good!), ignore
			case pgconn.SafeToRetry(err) && err.Error() == "conn closed":
				// The transaction context likely timed out and the connection is closed, ignore.
			default:
				// the transaction wasn't committed or rolled back yet (bad!), and we failed to rollback here (2x bad!)
				w.logAndReportErr(ctx, fmt.Errorf("error rolling back database transaction on exit: %w", err))
			}
		} else {
			// There was no sql.ErrTxDone error when "rolling back" the transaction, which means it was not committed or
			// rolled back before getting here (bad!). Log and report that we're missing a commit/rollback somewhere!
			w.logAndReportErr(ctx, errors.New("database transaction not explicitly committed or rolled back"))
		}
	}
	// in case of panic we want to rollback the transaction straight away, notify Sentry, and then re-panic
	if err := recover(); err != nil {
		rollback()
		sentry.CurrentHub().Recover(err)
		sentry.Flush(5 * time.Second)
		panic(err)
	}
	rollback()
}

func injectCorrelationID(ctx context.Context, logger log.Logger) context.Context {
	id := correlation.ExtractFromContextOrGenerate(ctx)

	l := logger.WithFields(log.Fields{correlation.FieldName: id})
	ctx = log.WithLogger(ctx, l)

	return ctx
}

func exponentialBackoff(i int) time.Duration {
	base := 5 * time.Minute
	maximum := 24 * time.Hour

	// Protect against unit underflow.
	if i < 0 {
		return base
	}

	// avoid int64 overflow
	if i > 24 {
		return maximum
	}

	backoff := base * time.Duration(1<<uint(i))

	switch {
	case backoff < base:
		return base
	case backoff > maximum:
		return maximum
	default:
		return backoff
	}
}
