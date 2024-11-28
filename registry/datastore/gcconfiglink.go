package datastore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/docker/distribution/registry/datastore/models"
)

type GCConfigLinkStore struct {
	db Queryer
}

// NewGCConfigLinkStore builds a new GCConfigLinkStore.
func NewGCConfigLinkStore(db Queryer) *GCConfigLinkStore {
	return &GCConfigLinkStore{db: db}
}

func scanFullGCConfigLinks(rows *sql.Rows) ([]*models.GCConfigLink, error) {
	rr := make([]*models.GCConfigLink, 0)
	defer rows.Close()

	for rows.Next() {
		var dgst Digest
		r := new(models.GCConfigLink)

		err := rows.Scan(&r.ID, &r.NamespaceID, &r.RepositoryID, &r.ManifestID, &dgst)
		if err != nil {
			return nil, fmt.Errorf("scanning GC configuration link: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return nil, err
		}
		r.Digest = d

		rr = append(rr, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning GC configuration links: %w", err)
	}

	return rr, nil
}

// FindAll finds all GC configuration links.
func (s *GCConfigLinkStore) FindAll(ctx context.Context) ([]*models.GCConfigLink, error) {
	q := `SELECT
			id,
			top_level_namespace_id,
			repository_id,
			manifest_id,
			encode(digest, 'hex') as digest
		FROM
			gc_blobs_configurations`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding GC configuration links: %w", err)
	}

	return scanFullGCConfigLinks(rows)
}

// Count counts all GC configuration links.
func (s *GCConfigLinkStore) Count(ctx context.Context) (int, error) {
	q := "SELECT COUNT(*) FROM gc_blobs_configurations"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting GC configuration links: %w", err)
	}

	return count, nil
}
