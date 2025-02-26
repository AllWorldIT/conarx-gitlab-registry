package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

// TagReader is the interface that defines read operations for a tag store.
type TagReader interface {
	FindAll(ctx context.Context) (models.Tags, error)
	FindByID(ctx context.Context, id int64) (*models.Tag, error)
	Count(ctx context.Context) (int, error)
	Repository(ctx context.Context, t *models.Tag) (*models.Repository, error)
	Manifest(ctx context.Context, t *models.Tag) (*models.Manifest, error)
}

// TagWriter is the interface that defines write operations for a tag store.
type TagWriter interface {
	CreateOrUpdate(ctx context.Context, t *models.Tag) error
}

// TagStore is the interface that a tag store should conform to.
type TagStore interface {
	TagReader
	TagWriter
}

// tagStore is the concrete implementation of a TagStore.
type tagStore struct {
	db Queryer
}

// NewTagStore builds a new tag store.
func NewTagStore(db Queryer) TagStore {
	return &tagStore{db: db}
}

func scanFullTag(row *sql.Row) (*models.Tag, error) {
	t := new(models.Tag)

	if err := row.Scan(&t.ID, &t.NamespaceID, &t.Name, &t.RepositoryID, &t.ManifestID, &t.CreatedAt, &t.UpdatedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning tag: %w", err)
		}
		return nil, nil
	}

	return t, nil
}

func scanFullTags(rows *sql.Rows) (models.Tags, error) {
	tt := make(models.Tags, 0)
	defer rows.Close()

	for rows.Next() {
		t := new(models.Tag)
		if err := rows.Scan(&t.ID, &t.NamespaceID, &t.Name, &t.RepositoryID, &t.ManifestID, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scanning tag: %w", err)
		}
		tt = append(tt, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning tags: %w", err)
	}

	return tt, nil
}

// FindByID finds a Tag by ID.
func (s *tagStore) FindByID(ctx context.Context, id int64) (*models.Tag, error) {
	defer metrics.InstrumentQuery("tag_find_by_id")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			repository_id,
			manifest_id,
			created_at,
			updated_at
		FROM
			tags
		WHERE
			id = $1`
	row := s.db.QueryRowContext(ctx, q, id)

	return scanFullTag(row)
}

// FindAll finds all tags.
func (s *tagStore) FindAll(ctx context.Context) (models.Tags, error) {
	defer metrics.InstrumentQuery("tag_find_all")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			repository_id,
			manifest_id,
			created_at,
			updated_at
		FROM
			tags`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding tags: %w", err)
	}

	return scanFullTags(rows)
}

// Count counts all tags.
func (s *tagStore) Count(ctx context.Context) (int, error) {
	defer metrics.InstrumentQuery("tag_count")()
	q := "SELECT COUNT(*) FROM tags"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting tags: %w", err)
	}

	return count, nil
}

// Repository finds a tag repository.
func (s *tagStore) Repository(ctx context.Context, t *models.Tag) (*models.Repository, error) {
	defer metrics.InstrumentQuery("tag_repository")()
	q := `SELECT
			id,
			top_level_namespace_id,
			name,
			path,
			parent_id,
			created_at,
			updated_at,
			last_published_at
		FROM
			repositories
		WHERE
			top_level_namespace_id = $1
			AND id = $2`
	row := s.db.QueryRowContext(ctx, q, t.NamespaceID, t.RepositoryID)

	return scanFullRepository(row)
}

// Manifest finds a tag manifest. A tag can be associated with either a manifest or a manifest list.
func (s *tagStore) Manifest(ctx context.Context, t *models.Tag) (*models.Manifest, error) {
	defer metrics.InstrumentQuery("tag_manifest")()
	q := `SELECT
			m.id,
			m.top_level_namespace_id,
			m.repository_id,
			m.total_size,
			m.schema_version,
			mt.media_type,
			at.media_type as artifact_type,
			encode(m.digest, 'hex') as digest,
			m.payload,
			mtc.media_type as configuration_media_type,
			encode(m.configuration_blob_digest, 'hex') as configuration_blob_digest,
			m.configuration_payload,
			m.non_conformant,
			m.non_distributable_layers,
			m.subject_id,
			m.created_at
		FROM
			manifests AS m
			JOIN media_types AS mt ON mt.id = m.media_type_id
			LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
			LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
		WHERE
			m.top_level_namespace_id = $1
			AND m.repository_id = $2
			AND m.id = $3`
	row := s.db.QueryRowContext(ctx, q, t.NamespaceID, t.RepositoryID, t.ManifestID)

	return scanFullManifest(row)
}

// CreateOrUpdate upsert a tag. A tag with a given name on a given repository may not exist (in which case it should be
// inserted), already exist and point to the same manifest (in which case nothing needs to be done) or already exist but
// points to a different manifest (in which case it should be updated).
func (s *tagStore) CreateOrUpdate(ctx context.Context, t *models.Tag) error {
	defer metrics.InstrumentQuery("tag_create_or_update")()
	q := `INSERT INTO tags (top_level_namespace_id, repository_id, manifest_id, name)
		   VALUES ($1, $2, $3, $4)
	   ON CONFLICT (top_level_namespace_id, repository_id, name)
		   DO UPDATE SET
			   manifest_id = EXCLUDED.manifest_id, updated_at = now()
		   WHERE
			   tags.manifest_id <> excluded.manifest_id
	   RETURNING
		   id, created_at, updated_at`

	row := s.db.QueryRowContext(ctx, q, t.NamespaceID, t.RepositoryID, t.ManifestID, t.Name)
	if err := row.Scan(&t.ID, &t.CreatedAt, &t.UpdatedAt); err != nil && !errors.Is(err, sql.ErrNoRows) {
		var pgErr *pgconn.PgError
		// this can happen if the manifest is deleted by the online GC while attempting to tag an untagged manifest
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation {
			return ErrManifestNotFound
		}
		return fmt.Errorf("creating tag: %w", err)
	}

	return nil
}
