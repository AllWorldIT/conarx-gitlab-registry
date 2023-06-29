package datastore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/docker/distribution/registry/datastore/metrics"
)

// MediaTypeReader is the interface that defines read operations for a media type store.
type MediaTypeReader interface {
	Exists(ctx context.Context, mt string) (bool, error)
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
