//go:generate mockgen -package mocks -destination mocks/blob.go . BlobStore

package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/opencontainers/go-digest"
)

// BlobReader is the interface that defines read operations for a blob store.
type BlobReader interface {
	FindAll(ctx context.Context) (models.Blobs, error)
	FindByDigest(ctx context.Context, d digest.Digest) (*models.Blob, error)
	Count(ctx context.Context) (int, error)
}

// BlobWriter is the interface that defines write operations for a blob store.
type BlobWriter interface {
	Create(ctx context.Context, b *models.Blob) error
	CreateOrFind(ctx context.Context, b *models.Blob) error
	Delete(ctx context.Context, d digest.Digest) error
}

// BlobStore is the interface that a blob store should conform to.
type BlobStore interface {
	BlobReader
	BlobWriter
}

// blobStore is the concrete implementation of a BlobStore.
type blobStore struct {
	db Queryer
}

// NewBlobStore builds a new blobStore.
func NewBlobStore(db Queryer) BlobStore {
	return &blobStore{db: db}
}

func scanFullBlob(row *Row) (*models.Blob, error) {
	var dgst Digest
	b := new(models.Blob)

	if err := row.Scan(&b.MediaType, &dgst, &b.Size, &b.CreatedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning blob: %w", err)
		}
		return nil, nil
	}

	d, err := dgst.Parse()
	if err != nil {
		return nil, err
	}
	b.Digest = d

	return b, nil
}

func scanFullBlobs(rows *sql.Rows) (models.Blobs, error) {
	bb := make(models.Blobs, 0)
	defer rows.Close()

	for rows.Next() {
		var dgst Digest
		b := new(models.Blob)

		err := rows.Scan(&b.MediaType, &dgst, &b.Size, &b.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scanning blob: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return nil, err
		}
		b.Digest = d

		bb = append(bb, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning blobs: %w", err)
	}

	return bb, nil
}

// FindByDigest finds a blob by digest.
func (s *blobStore) FindByDigest(ctx context.Context, d digest.Digest) (*models.Blob, error) {
	defer metrics.InstrumentQuery("blob_find_by_digest")()
	q := `SELECT
			mt.media_type,
			encode(b.digest, 'hex') as digest,
			b.size,
			b.created_at
		FROM
			blobs AS b
			JOIN media_types AS mt ON b.media_type_id = mt.id
		WHERE
			b.digest = decode($1, 'hex')`

	dgst, err := NewDigest(d)
	if err != nil {
		return nil, err
	}
	row := s.db.QueryRowContext(ctx, q, dgst)

	return scanFullBlob(row)
}

// FindAll finds all blobs.
func (s *blobStore) FindAll(ctx context.Context) (models.Blobs, error) {
	defer metrics.InstrumentQuery("blob_find_all")()
	q := `SELECT
			mt.media_type,
			encode(b.digest, 'hex') as digest,
			b.size,
			b.created_at
		FROM
			blobs AS b
			JOIN media_types AS mt ON b.media_type_id = mt.id`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding blobs: %w", err)
	}

	return scanFullBlobs(rows)
}

// Count counts all blobs.
func (s *blobStore) Count(ctx context.Context) (int, error) {
	defer metrics.InstrumentQuery("blob_count")()
	q := "SELECT COUNT(*) FROM blobs"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting blobs: %w", err)
	}

	return count, nil
}

// Create saves a new blob.
func (s *blobStore) Create(ctx context.Context, b *models.Blob) error {
	defer metrics.InstrumentQuery("blob_create")()
	q := `INSERT INTO blobs (digest, media_type_id, size)
			VALUES (decode($1, 'hex'), $2, $3)
		RETURNING
			created_at`

	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return err
	}

	mtStore := NewMediaTypeStore(s.db)

	mediaTypeID, err := mtStore.FindID(ctx, b.MediaType)
	if err != nil {
		return err
	}
	row := s.db.QueryRowContext(ctx, q, dgst, mediaTypeID, b.Size)
	if err := row.Scan(&b.CreatedAt); err != nil {
		return fmt.Errorf("creating blob: %w", err)
	}

	return nil
}

// CreateOrFind attempts to create a blob. If the blob already exists (same digest_hex) that record is loaded from the
// database into b. This is similar to a FindByDigest followed by a Create, but without being prone to race conditions
// on write operations between the corresponding read (FindByDigest) and write (Create) operations. Separate Find* and
// Create method calls should be preferred to this when race conditions are not a concern.
func (s *blobStore) CreateOrFind(ctx context.Context, b *models.Blob) error {
	defer metrics.InstrumentQuery("blob_create_or_find")()
	q := `INSERT INTO blobs (digest, media_type_id, size)
			VALUES (decode($1, 'hex'), $2, $3)
		ON CONFLICT (digest)
			DO NOTHING
		RETURNING
			created_at`

	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return err
	}

	mtStore := NewMediaTypeStore(s.db)

	mediaTypeID, err := mtStore.FindID(ctx, b.MediaType)
	if err != nil {
		return err
	}

	row := s.db.QueryRowContext(ctx, q, dgst, mediaTypeID, b.Size)
	if err := row.Scan(&b.CreatedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("creating blob: %w", err)
		}
		// if the result set has no rows, then the blob already exists
		tmp, err := s.FindByDigest(ctx, b.Digest)
		if err != nil {
			return err
		}
		*b = *tmp
	}

	return nil
}

// Delete deletes a blob.
func (s *blobStore) Delete(ctx context.Context, d digest.Digest) error {
	defer metrics.InstrumentQuery("blob_delete")()
	q := "DELETE FROM blobs WHERE digest = decode($1, 'hex')"

	dgst, err := NewDigest(d)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx, q, dgst)
	if err != nil {
		return fmt.Errorf("deleting blob: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("deleting blob: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}

	return nil
}
