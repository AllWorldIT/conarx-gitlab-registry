package datastore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/docker/distribution/registry/datastore/models"
)

type GCLayerLinkStore struct {
	db Queryer
}

// NewGCLayerLinkStore builds a new GCLayerLinkStore.
func NewGCLayerLinkStore(db Queryer) *GCLayerLinkStore {
	return &GCLayerLinkStore{db: db}
}

func scanFullGCLayerLinks(rows *sql.Rows) ([]*models.GCLayerLink, error) {
	rr := make([]*models.GCLayerLink, 0)
	defer rows.Close()

	for rows.Next() {
		var dgst Digest
		r := new(models.GCLayerLink)

		err := rows.Scan(&r.ID, &r.NamespaceID, &r.RepositoryID, &r.LayerID, &dgst)
		if err != nil {
			return nil, fmt.Errorf("scanning GC layer link: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return nil, err
		}
		r.Digest = d

		rr = append(rr, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning GC layer links: %w", err)
	}

	return rr, nil
}

// FindAll finds all GC layer links.
func (s *GCLayerLinkStore) FindAll(ctx context.Context) ([]*models.GCLayerLink, error) {
	q := `SELECT
			id,
			top_level_namespace_id,
			repository_id,
			layer_id,
			encode(digest, 'hex') as digest
		FROM
			gc_blobs_layers`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding GC layer links: %w", err)
	}

	return scanFullGCLayerLinks(rows)
}

// Count counts all GC layer links.
func (s *GCLayerLinkStore) Count(ctx context.Context) (int, error) {
	q := "SELECT COUNT(*) FROM gc_blobs_layers"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting GC layer links: %w", err)
	}

	return count, nil
}
