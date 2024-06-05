package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore/metrics"
)

// MediaTypeReader is the interface that defines read operations for a media type store.
type MediaTypeReader interface {
	Exists(ctx context.Context, mt string) (bool, error)
	FindID(ctx context.Context, mt string) (int, error)
}

// MediaTypeWriter is the interface that defines write operations for a media type store.
type MediaTypeWriter interface {
	SafeFindOrCreateID(ctx context.Context, mt string) (int, error)
}

// MediaTypeStore is the interface that a media type store should conform to.
type MediaTypeStore interface {
	MediaTypeReader
	MediaTypeWriter
}

// mediaTypeStore is a concrete implementation of a media type store.
type mediaTypeStore struct {
	// db can be either a *sql.DB or *sql.Tx
	db Queryer
}

// NewMediaTypeStore builds a new media type store.
func NewMediaTypeStore(db Queryer) *mediaTypeStore {
	return &mediaTypeStore{db: db}
}

func (s *mediaTypeStore) Exists(ctx context.Context, mt string) (bool, error) {
	defer metrics.InstrumentQuery("media_type_exists")()
	// Query returns "t" or "f", the subquery is only evaluated based on whether a
	// row is returned or not, so the fields from the media_types table are ignored.
	q := `SELECT
    EXISTS (
        SELECT
            1
        FROM
            media_types
        WHERE
            media_type = $1)`

	var exists string

	if err := s.db.QueryRowContext(ctx, q, mt).Scan(&exists); err != nil {
		return false, fmt.Errorf("scanning media type row: %w", err)
	}

	return strconv.ParseBool(exists)
}

func (s *mediaTypeStore) FindID(ctx context.Context, mediaType string) (int, error) {
	defer metrics.InstrumentQuery("media_type_find_id")()

	q := `SELECT
			id
		FROM
			media_types
		WHERE
			media_type = $1`

	var id int
	row := s.db.QueryRowContext(ctx, q, mediaType)
	if err := row.Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrUnknownMediaType{MediaType: mediaType}
		}
		return 0, fmt.Errorf("unable to map media type: %w", err)
	}

	return id, nil
}

// SafeFindOrCreateID provides a concurrency safe way to find or create a media
// type record. This is optimized for the fact that 1) for the vast majority of
// requests the target media type will already exist (low/moderate creation
// rate) and 2) we never delete media type records from the database. This
// method works by 1) find media type record 2) if found return otherwise
// perform an upsert.
func (s *mediaTypeStore) SafeFindOrCreateID(ctx context.Context, mediaType string) (int, error) {
	defer metrics.InstrumentQuery("media_type_safe_find_or_create_id")()

	foundID, err := s.FindID(ctx, mediaType)
	if err == nil ||
		!feature.DynamicMediaTypes.Enabled() ||
		!errors.As(err, &ErrUnknownMediaType{}) {
		return foundID, err
	}

	log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"media_type": mediaType}).Info("creating new media type")

	q := `INSERT INTO media_types (media_type)
			VALUES ($1)
		ON CONFLICT (media_type)
			DO NOTHING
		RETURNING
			id`

	var id int

	row := s.db.QueryRowContext(ctx, q, mediaType)
	if err := row.Scan(&id); err != nil {
		if err != sql.ErrNoRows {
			return 0, fmt.Errorf("creating media type: %w", err)
		}
		// If the result set has no rows, then the media type already exists.
		return s.FindID(ctx, mediaType)
	}

	return id, nil
}
