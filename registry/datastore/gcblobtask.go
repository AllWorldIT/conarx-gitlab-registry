//go:generate mockgen -package mocks -destination mocks/gcblobtask.go . GCBlobTaskStore

package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
)

type GCBlobTaskStore interface {
	FindAll(ctx context.Context) ([]*models.GCBlobTask, error)
	Count(ctx context.Context) (int, error)
	Next(ctx context.Context) (*models.GCBlobTask, error)
	Postpone(ctx context.Context, b *models.GCBlobTask, d time.Duration) error
	IsDangling(ctx context.Context, b *models.GCBlobTask) (bool, error)
	Delete(ctx context.Context, b *models.GCBlobTask) error
}

type gcBlobTaskStore struct {
	db Queryer
}

// NewGCBlobTaskStore builds a new gcBlobTaskStore.
func NewGCBlobTaskStore(db Queryer) GCBlobTaskStore {
	return &gcBlobTaskStore{db: db}
}

func scanFullGCBlobTasks(rows *sql.Rows) ([]*models.GCBlobTask, error) {
	rr := make([]*models.GCBlobTask, 0)
	defer rows.Close()

	for rows.Next() {
		var dgst Digest
		r := new(models.GCBlobTask)

		err := rows.Scan(&r.ReviewAfter, &r.ReviewCount, &dgst, &r.CreatedAt, &r.Event)
		if err != nil {
			return nil, fmt.Errorf("scanning GC blob task: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return nil, err
		}
		r.Digest = d

		rr = append(rr, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning GC blob tasks: %w", err)
	}

	return rr, nil
}

func scanFullGCBlobTask(row *Row) (*models.GCBlobTask, error) {
	b := new(models.GCBlobTask)
	var dgst Digest

	err := row.Scan(&b.ReviewAfter, &b.ReviewCount, &dgst, &b.CreatedAt, &b.Event)
	if err != nil {
		return nil, fmt.Errorf("scanning GC blob task: %w", err)
	}

	d, err := dgst.Parse()
	if err != nil {
		return nil, err
	}
	b.Digest = d

	return b, nil
}

// FindAll finds all GC blob tasks.
func (s *gcBlobTaskStore) FindAll(ctx context.Context) ([]*models.GCBlobTask, error) {
	defer metrics.InstrumentQuery("gc_blob_task_find_all")()

	q := `SELECT
			review_after,
			review_count,
			encode(digest, 'hex') as digest,
			created_at,
			event
		FROM
			gc_blob_review_queue`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding GC blob tasks: %w", err)
	}

	return scanFullGCBlobTasks(rows)
}

// Count counts all GC blob tasks.
func (s *gcBlobTaskStore) Count(ctx context.Context) (int, error) {
	defer metrics.InstrumentQuery("gc_blob_task_count")()

	q := "SELECT COUNT(*) FROM gc_blob_review_queue"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting GC blob tasks: %w", err)
	}

	return count, nil
}

// Next reads and locks the blob review queue row with the oldest review_after before the current date. In case of a
// draw (multiple unlocked records with the same review_after) the returned row is the one that was first inserted.
// This method may be called safely from multiple concurrent goroutines or processes. A `SELECT FOR UPDATE` is used to
// ensure that callers don't get the same record. The operation does not block, and no error is returned if there are
// no rows or none is available (i.e., all locked by other processes). A `nil` record is returned in this situation.
func (s *gcBlobTaskStore) Next(ctx context.Context) (*models.GCBlobTask, error) {
	defer metrics.InstrumentQuery("gc_blob_task_next")()

	q := `SELECT
			review_after,
			review_count,
			encode(digest, 'hex') AS digest,
			created_at,
			event
		FROM
			gc_blob_review_queue
		WHERE
			review_after < NOW()
		ORDER BY
    		review_after
		FOR UPDATE
			SKIP LOCKED
		LIMIT 1`

	row := s.db.QueryRowContext(ctx, q)
	b, err := scanFullGCBlobTask(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("fetching next GC blob task: %w", err)
	}

	return b, nil
}

// Postpone moves the review_after of a blob task forward by a given amount of time. The review_count is automatically
// incremented.
func (s *gcBlobTaskStore) Postpone(ctx context.Context, b *models.GCBlobTask, d time.Duration) error {
	defer metrics.InstrumentQuery("gc_blob_task_postpone")()

	q := `UPDATE
			gc_blob_review_queue
		SET
			review_after = $1,
			review_count = $2
		WHERE
			digest = decode($3, 'hex')`

	ra := b.ReviewAfter.Add(d)
	rc := b.ReviewCount + 1
	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return err
	}

	res, err := s.db.ExecContext(ctx, q, ra, rc, dgst)
	if err != nil {
		return fmt.Errorf("postponing GC blob task: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("postponing GC blob task: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("GC blob task not found")
	}

	b.ReviewAfter = ra
	b.ReviewCount = rc

	return nil
}

// Delete deletes a blob task from the blob review queue.
func (s *gcBlobTaskStore) Delete(ctx context.Context, b *models.GCBlobTask) error {
	defer metrics.InstrumentQuery("gc_blob_task_delete")()

	q := "DELETE FROM gc_blob_review_queue WHERE digest = decode($1, 'hex')"
	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, q, dgst)
	if err != nil {
		return fmt.Errorf("deleting GC blob task: %w", err)
	}
	count, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("deleting GC blob task: %w", err)
	}
	if count == 0 {
		return fmt.Errorf("GC blob task not found")
	}

	return nil
}

// IsDangling determines if the blob referenced by the GC blob task is eligible for deletion or not.
func (s *gcBlobTaskStore) IsDangling(ctx context.Context, b *models.GCBlobTask) (bool, error) {
	defer metrics.InstrumentQuery("gc_blob_task_is_dangling")()

	q := `SELECT
			EXISTS (
				SELECT
					1
				FROM
					gc_blobs_configurations
				WHERE
					digest = decode($1, 'hex')
				UNION
				SELECT
					1
				FROM
					gc_blobs_layers
				WHERE
					digest = decode($1, 'hex'))`

	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return false, err
	}

	var referenced bool
	if err := s.db.QueryRowContext(ctx, q, dgst).Scan(&referenced); err != nil {
		return false, fmt.Errorf("determining blob eligibily for deletion: %w", err)
	}

	return !referenced, nil
}
