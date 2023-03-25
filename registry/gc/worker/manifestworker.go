package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/gc/internal/metrics"
	"github.com/hashicorp/go-multierror"
	"github.com/jackc/pgconn"
)

var (
	// for test purposes (mocking)
	manifestTaskStoreConstructor = datastore.NewGCManifestTaskStore
	manifestStoreConstructor     = datastore.NewManifestStore
)

var _ Worker = (*ManifestWorker)(nil)

// ManifestWorker is the online GC worker responsible for processing tasks related with manifests. It consumes tasks
// from the manifest review queue, identifies if the corresponding manifest is eligible for deletion, and if so,
// deletes it from the database.
type ManifestWorker struct {
	*baseWorker
}

// ManifestWorkerOption provides functional options for NewManifestWorker.
type ManifestWorkerOption func(*ManifestWorker)

// WithManifestLogger sets the logger.
func WithManifestLogger(l log.Logger) ManifestWorkerOption {
	return func(w *ManifestWorker) {
		w.logger = l
	}
}

// WithManifestTxTimeout sets the database transaction timeout for each run. Defaults to 10 seconds.
func WithManifestTxTimeout(d time.Duration) ManifestWorkerOption {
	return func(w *ManifestWorker) {
		w.txTimeout = d
	}
}

// NewManifestWorker creates a new BlobWorker.
func NewManifestWorker(db datastore.Handler, opts ...ManifestWorkerOption) *ManifestWorker {
	w := &ManifestWorker{baseWorker: &baseWorker{db: db}}
	w.name = "registry.gc.worker.ManifestWorker"
	w.applyDefaults()
	for _, opt := range opts {
		opt(w)
	}
	w.logger = w.logger.WithFields(log.Fields{componentKey: w.name})

	return w
}

// Run implements Worker.
func (w *ManifestWorker) Run(ctx context.Context) RunResult {
	ctx = log.WithLogger(ctx, w.logger)
	return w.run(ctx, w)
}

func (w *ManifestWorker) processTask(ctx context.Context) RunResult {
	l := log.GetLogger(log.WithContext(ctx))

	// don't let the database transaction run for longer than w.txTimeout
	ctx, cancel := context.WithDeadline(ctx, systemClock.Now().Add(w.txTimeout))
	defer cancel()

	var res RunResult
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		res.Err = fmt.Errorf("creating database transaction: %w", err)
		return res
	}
	defer w.rollbackOnExit(ctx, tx)

	mts := manifestTaskStoreConstructor(tx)
	t, err := mts.Next(ctx)
	if err != nil {
		res.Err = err
		return res
	}
	if t == nil {
		l.Info("no task available")
		if err := tx.Commit(); err != nil {
			res.Err = fmt.Errorf("committing database transaction: %w", err)
			return res
		}
		return res
	}

	res.Found = true
	res.Event = t.Event

	l.WithFields(log.Fields{
		"review_after":  t.ReviewAfter.UTC(),
		"review_count":  t.ReviewCount,
		"repository_id": t.RepositoryID,
		"manifest_id":   t.ManifestID,
		"created_at":    t.CreatedAt.UTC(),
		"event":         t.Event,
	}).Info("processing task")

	dangling, err := mts.IsDangling(ctx, t)
	if err != nil {
		res.Err = w.handleDBError(ctx, t, err)
		return res
	}

	res.Dangling = dangling
	if dangling {
		l.Info("the manifest is dangling, deleting")
		// deleting the manifest cascades to the review queue, so we don't need to delete the task directly here
		if err := w.deleteManifest(ctx, tx, t); err != nil {
			res.Err = w.handleDBError(ctx, t, err)
			return res
		}
	} else {
		l.Info("the manifest is not dangling, deleting task")
		if err := mts.Delete(ctx, t); err != nil {
			res.Err = w.handleDBError(ctx, t, err)
			return res
		}
	}

	if err := tx.Commit(); err != nil {
		res.Err = fmt.Errorf("committing database transaction: %w", err)
		return res
	}

	return res
}

func (w *ManifestWorker) deleteManifest(ctx context.Context, tx datastore.Transactor, t *models.GCManifestTask) error {
	l := log.GetLogger(log.WithContext(ctx))

	ms := manifestStoreConstructor(tx)

	report := metrics.ManifestDelete()
	dgst, err := ms.Delete(ctx, t.NamespaceID, t.RepositoryID, t.ManifestID)
	if err != nil {
		report(err)
		return err
	}
	if dgst == nil {
		// this should never happen because deleting a manifest cascades to the review queue, nevertheless...
		l.Warn("manifest no longer exists on database, deleting task")
		mts := manifestTaskStoreConstructor(tx)
		return mts.Delete(ctx, t)
	}
	l.WithFields(log.Fields{"digest": dgst, "repository_id": t.RepositoryID}).Info("manifest deleted")

	report(nil)
	return nil
}

// postponeTask will postpone the next review of a GC task by applying an exponential delay based on the amount of times
// a task has been reviewed/retried. A row lock is used to avoid having other GC workers picking this task "at the same
// time". To guard against long-running transactions in case another worker already got a lock for this task, the caller
// of this method should set a timeout for the provided context. This is already done when called from processTask where
// we enforce a global processing deadline of w.txTimeout (configurable).
func (w *ManifestWorker) postponeTask(ctx context.Context, t *models.GCManifestTask) error {
	// create transaction
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("creating database transaction: %w", err)
	}
	defer tx.Rollback()

	// find and lock task for update
	mts := manifestTaskStoreConstructor(tx)
	t2, err := mts.FindAndLock(ctx, t.NamespaceID, t.RepositoryID, t.ManifestID)
	if err != nil {
		return err
	}
	if t2 == nil {
		// if the task no longer exists it means it was reprocessed successfully by another worker and then deleted
		return nil
	}

	// If the value of review_after retrieved from the DB is ahead of the one we have, it means that this task was
	// already postponed by another worker.
	l := log.GetLogger(log.WithContext(ctx))
	if t2.ReviewAfter.After(t.ReviewAfter) {
		l.WithFields(log.Fields{"review_after": t2.ReviewAfter.String()}).Info("task already postponed, skipping")
		return nil
	}

	// otherwise, calculate what should be the next review delay and update the task
	d := exponentialBackoff(t.ReviewCount)
	log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"backoff_duration": d.String()}).Info("postponing next review")
	if err := mts.Postpone(ctx, t, d); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing database transaction: %w", err)
	}

	metrics.ReviewPostpone(w.name)
	return nil
}

func (w *ManifestWorker) handleDBError(ctx context.Context, t *models.GCManifestTask, err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded), pgconn.Timeout(err):
		// this error is likely temporary. Do not postpone the task's next review as it is likely to succeed
		log.GetLogger(log.WithContext(ctx)).WithError(err).Warn("skipping next review postpone as error is likely temporary")
	default:
		// If this is not a temporary error and/or we're not sure how to handle it, then we should err on the safe side
		// and try to postpone this task's next review so that we have time to debug and fix the underlying cause.
		if innerErr := w.postponeTask(ctx, t); innerErr != nil {
			return multierror.Append(err, innerErr)
		}
	}

	return err
}
