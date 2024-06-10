//go:generate mockgen -package mocks -destination mocks/manifest.go . ManifestStore

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
	"github.com/opencontainers/go-digest"
)

// ManifestReader is the interface that defines read operations for a Manifest store.
type ManifestReader interface {
	FindAll(ctx context.Context) (models.Manifests, error)
	Count(ctx context.Context) (int, error)
	LayerBlobs(ctx context.Context, m *models.Manifest) (models.Blobs, error)
	References(ctx context.Context, m *models.Manifest) (models.Manifests, error)
}

// ManifestWriter is the interface that defines write operations for a Manifest store.
type ManifestWriter interface {
	Create(ctx context.Context, m *models.Manifest) error
	CreateOrFind(ctx context.Context, m *models.Manifest) error
	AssociateManifest(ctx context.Context, ml *models.Manifest, m *models.Manifest) error
	DissociateManifest(ctx context.Context, ml *models.Manifest, m *models.Manifest) error
	AssociateLayerBlob(ctx context.Context, m *models.Manifest, b *models.Blob) error
	DissociateLayerBlob(ctx context.Context, m *models.Manifest, b *models.Blob) error
	Delete(ctx context.Context, namespaceID, repositoryID, id int64) (*digest.Digest, error)
}

// ManifestStore is the interface that a Manifest store should conform to.
type ManifestStore interface {
	ManifestReader
	ManifestWriter
}

// manifestStore is the concrete implementation of a ManifestStore.
type manifestStore struct {
	db Queryer
}

// NewManifestStore builds a new manifest store.
func NewManifestStore(db Queryer) ManifestStore {
	return &manifestStore{db: db}
}

func scanFullManifest(row *sql.Row) (*models.Manifest, error) {
	var dgst Digest
	var cfgDigest, cfgMediaType sql.NullString
	var cfgPayload *models.Payload
	m := new(models.Manifest)

	err := row.Scan(&m.ID, &m.NamespaceID, &m.RepositoryID, &m.TotalSize, &m.SchemaVersion,
		&m.MediaType, &m.ArtifactType, &dgst, &m.Payload, &cfgMediaType, &cfgDigest, &cfgPayload,
		&m.NonConformant, &m.NonDistributableLayers, &m.SubjectID, &m.CreatedAt)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("scanning manifest: %w", err)
		}
		return nil, nil
	}

	d, err := dgst.Parse()
	if err != nil {
		return nil, err
	}
	m.Digest = d

	if cfgDigest.Valid {
		d, err := Digest(cfgDigest.String).Parse()
		if err != nil {
			return nil, err
		}

		m.Configuration = &models.Configuration{
			MediaType: cfgMediaType.String,
			Digest:    d,
			Payload:   *cfgPayload,
		}
	}

	return m, nil
}

func scanFullManifests(rows *sql.Rows) (models.Manifests, error) {
	mm := make(models.Manifests, 0)
	defer rows.Close()

	for rows.Next() {
		var dgst Digest
		var cfgDigest, cfgMediaType sql.NullString
		var cfgPayload *models.Payload
		m := new(models.Manifest)

		err := rows.Scan(&m.ID, &m.NamespaceID, &m.RepositoryID, &m.TotalSize, &m.SchemaVersion,
			&m.MediaType, &m.ArtifactType, &dgst, &m.Payload, &cfgMediaType, &cfgDigest, &cfgPayload,
			&m.NonConformant, &m.NonDistributableLayers, &m.SubjectID, &m.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scanning manifest: %w", err)
		}

		d, err := dgst.Parse()
		if err != nil {
			return nil, err
		}
		m.Digest = d

		if cfgDigest.Valid {
			d, err := Digest(cfgDigest.String).Parse()
			if err != nil {
				return nil, err
			}

			m.Configuration = &models.Configuration{
				MediaType: cfgMediaType.String,
				Digest:    d,
				Payload:   *cfgPayload,
			}
		}

		mm = append(mm, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanning manifests: %w", err)
	}

	return mm, nil
}

// FindAll finds all manifests.
func (s *manifestStore) FindAll(ctx context.Context) (models.Manifests, error) {
	defer metrics.InstrumentQuery("manifest_find_all")()
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
		ORDER BY
			id`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("finding manifests: %w", err)
	}

	return scanFullManifests(rows)
}

// Count counts all manifests.
func (s *manifestStore) Count(ctx context.Context) (int, error) {
	defer metrics.InstrumentQuery("manifest_count")()
	q := "SELECT COUNT(*) FROM manifests"
	var count int

	if err := s.db.QueryRowContext(ctx, q).Scan(&count); err != nil {
		return count, fmt.Errorf("counting manifests: %w", err)
	}

	return count, nil
}

// LayerBlobs finds layer blobs associated with a manifest, through the `layers` relationship entity.
func (s *manifestStore) LayerBlobs(ctx context.Context, m *models.Manifest) (models.Blobs, error) {
	defer metrics.InstrumentQuery("manifest_layer_blobs")()
	q := `SELECT
			mt.media_type,
			encode(b.digest, 'hex') as digest,
			b.size,
			b.created_at
		FROM
			layers AS l
			INNER JOIN blobs AS b ON l.digest = b.digest
			JOIN media_types AS mt ON mt.id = l.media_type_id
		WHERE
			l.manifest_id = $1
			AND l.repository_id = $2
			AND l.top_level_namespace_id = $3`

	rows, err := s.db.QueryContext(ctx, q, m.ID, m.RepositoryID, m.NamespaceID)
	if err != nil {
		return nil, fmt.Errorf("finding blobs: %w", err)
	}

	return scanFullBlobs(rows)
}

// References finds all manifests directly referenced by a manifest (if any).
func (s *manifestStore) References(ctx context.Context, m *models.Manifest) (models.Manifests, error) {
	defer metrics.InstrumentQuery("manifest_references")()
	q := `SELECT DISTINCT
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
			JOIN manifest_references AS mr ON mr.top_level_namespace_id = m.top_level_namespace_id
				AND mr.child_id = m.id
			JOIN media_types AS mt ON mt.id = m.media_type_id
			LEFT JOIN media_types AS mtc ON mtc.id = m.configuration_media_type_id
			LEFT JOIN media_types AS at ON at.id = m.artifact_media_type_id
		WHERE
			m.top_level_namespace_id = $1
			AND mr.repository_id = $2
			AND mr.parent_id = $3`

	rows, err := s.db.QueryContext(ctx, q, m.NamespaceID, m.RepositoryID, m.ID)
	if err != nil {
		return nil, fmt.Errorf("finding referenced manifests: %w", err)
	}

	return scanFullManifests(rows)
}

// Create saves a new Manifest.
func (s *manifestStore) Create(ctx context.Context, m *models.Manifest) error {
	defer metrics.InstrumentQuery("manifest_create")()
	q := `INSERT INTO manifests (top_level_namespace_id, repository_id, total_size, schema_version,
				media_type_id, artifact_media_type_id, digest, payload, configuration_media_type_id,
				configuration_blob_digest, configuration_payload, non_conformant, non_distributable_layers, subject_id)
			VALUES ($1, $2, $3, $4, $5, $6, decode($7, 'hex'), $8, $9, decode($10, 'hex'), $11, $12, $13, $14)
		RETURNING
			id, created_at`

	dgst, err := NewDigest(m.Digest)
	if err != nil {
		return err
	}

	mtStore := NewMediaTypeStore(s.db)

	mediaTypeID, err := mtStore.SafeFindOrCreateID(ctx, m.MediaType)
	if err != nil {
		return fmt.Errorf("mapping manifest media type: %w", err)
	}

	var artifactTypeID sql.NullInt64
	if m.ArtifactType.Valid {
		aid, err := mtStore.SafeFindOrCreateID(ctx, m.ArtifactType.String)
		if err != nil {
			return fmt.Errorf("mapping manifest artifact type: %w", err)
		}
		artifactTypeID.Valid = true
		artifactTypeID.Int64 = int64(aid)
	}

	var configDgst sql.NullString
	var configMediaTypeID sql.NullInt32
	var configPayload *models.Payload
	if m.Configuration != nil {
		dgst, err := NewDigest(m.Configuration.Digest)
		if err != nil {
			return err
		}
		configDgst.Valid = true
		configDgst.String = dgst.String()
		id, err := mtStore.SafeFindOrCreateID(ctx, m.Configuration.MediaType)
		if err != nil {
			return fmt.Errorf("mapping config media type: %w", err)
		}
		configMediaTypeID.Valid = true
		configMediaTypeID.Int32 = int32(id)
		configPayload = &m.Configuration.Payload
	}

	row := s.db.QueryRowContext(ctx, q, m.NamespaceID, m.RepositoryID, m.TotalSize, m.SchemaVersion,
		mediaTypeID, artifactTypeID, dgst, m.Payload, configMediaTypeID, configDgst, configPayload,
		m.NonConformant, m.NonDistributableLayers, m.SubjectID)
	if err := row.Scan(&m.ID, &m.CreatedAt); err != nil {
		return fmt.Errorf("creating manifest: %w", err)
	}

	return nil
}

// CreateOrFind attempts to create a manifest. If the manifest already exists (same digest in the scope of a given repository)
// that record is loaded from the database into m. This is similar to a repositoryStore.FindManifestByDigest followed by
// a Create, but without being  prone to race conditions on write operations between the corresponding read (FindManifestByDigest)
// and write (Create) operations.
// Separate Find* and Create method calls should be preferred to this when race conditions are not a concern.
func (s *manifestStore) CreateOrFind(ctx context.Context, m *models.Manifest) error {
	defer metrics.InstrumentQuery("manifest_create_or_find")()
	q := `INSERT INTO manifests (top_level_namespace_id, repository_id, total_size, schema_version,
				media_type_id, artifact_media_type_id, digest, payload, configuration_media_type_id,
				configuration_blob_digest, configuration_payload, non_conformant, non_distributable_layers, subject_id)
			VALUES ($1, $2, $3, $4, $5, $6, decode($7, 'hex'), $8, $9, decode($10, 'hex'), $11, $12, $13, $14)
			ON CONFLICT (top_level_namespace_id, repository_id, digest) DO NOTHING
		RETURNING
			id, created_at`

	dgst, err := NewDigest(m.Digest)
	if err != nil {
		return err
	}

	mtStore := NewMediaTypeStore(s.db)

	mediaTypeID, err := mtStore.SafeFindOrCreateID(ctx, m.MediaType)
	if err != nil {
		return fmt.Errorf("mapping manifest media type: %w", err)
	}

	var artifactTypeID sql.NullInt64
	if m.ArtifactType.Valid {
		aid, err := mtStore.SafeFindOrCreateID(ctx, m.ArtifactType.String)
		if err != nil {
			return fmt.Errorf("mapping manifest artifact type: %w", err)
		}
		artifactTypeID.Valid = true
		artifactTypeID.Int64 = int64(aid)
	}

	var configDgst sql.NullString
	var configMediaTypeID sql.NullInt32
	var configPayload *models.Payload
	if m.Configuration != nil {
		dgst, err := NewDigest(m.Configuration.Digest)
		if err != nil {
			return err
		}
		configDgst.Valid = true
		configDgst.String = dgst.String()
		id, err := mtStore.SafeFindOrCreateID(ctx, m.Configuration.MediaType)
		if err != nil {
			return fmt.Errorf("mapping config media type: %w", err)
		}
		configMediaTypeID.Valid = true
		configMediaTypeID.Int32 = int32(id)
		configPayload = &m.Configuration.Payload
	}

	row := s.db.QueryRowContext(ctx, q, m.NamespaceID, m.RepositoryID, m.TotalSize, m.SchemaVersion,
		mediaTypeID, artifactTypeID, dgst, m.Payload, configMediaTypeID, configDgst, configPayload,
		m.NonConformant, m.NonDistributableLayers, m.SubjectID)
	if err := row.Scan(&m.ID, &m.CreatedAt); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("creating manifest: %w", err)
		}
		tmp, err := findManifestByDigest(ctx, s.db, m.NamespaceID, m.RepositoryID, dgst)
		if err != nil {
			return err
		}
		*m = *tmp
	}

	return nil
}

// AssociateManifest associates a manifest with a manifest list. It does nothing if already associated.
func (s *manifestStore) AssociateManifest(ctx context.Context, ml *models.Manifest, m *models.Manifest) error {
	defer metrics.InstrumentQuery("manifest_associate_manifest")()
	if ml.ID == m.ID {
		return fmt.Errorf("cannot associate a manifest with itself")
	}

	q := `INSERT INTO manifest_references (top_level_namespace_id, repository_id, parent_id, child_id)
			VALUES ($1, $2, $3, $4)
		ON CONFLICT (top_level_namespace_id, repository_id, parent_id, child_id)
			DO NOTHING`

	if _, err := s.db.ExecContext(ctx, q, ml.NamespaceID, ml.RepositoryID, ml.ID, m.ID); err != nil {
		var pgErr *pgconn.PgError
		// this can happen if the child manifest is deleted by the online GC while attempting to create the list
		if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation {
			return ErrRefManifestNotFound
		}
		return fmt.Errorf("associating manifest: %w", err)
	}

	return nil
}

// DissociateManifest dissociates a manifest and a manifest list. It does nothing if not associated.
func (s *manifestStore) DissociateManifest(ctx context.Context, ml *models.Manifest, m *models.Manifest) error {
	defer metrics.InstrumentQuery("manifest_dissociate_manifest")()
	q := `DELETE FROM manifest_references
		WHERE top_level_namespace_id = $1
			AND repository_id = $2
			AND parent_id = $3
			AND child_id = $4`

	res, err := s.db.ExecContext(ctx, q, ml.NamespaceID, ml.RepositoryID, ml.ID, m.ID)
	if err != nil {
		return fmt.Errorf("dissociating manifest: %w", err)
	}

	if _, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("dissociating manifest: %w", err)
	}

	return nil
}

// AssociateLayerBlob associates a layer blob and a manifest. It does nothing if already associated.
func (s *manifestStore) AssociateLayerBlob(ctx context.Context, m *models.Manifest, b *models.Blob) error {
	defer metrics.InstrumentQuery("manifest_associate_layer_blob")()
	q := `INSERT INTO layers (top_level_namespace_id, repository_id, manifest_id, digest, media_type_id, size)
			VALUES ($1, $2, $3, decode($4, 'hex'), $5, $6)
		ON CONFLICT (top_level_namespace_id, repository_id, manifest_id, digest)
			DO NOTHING`

	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return err
	}

	mtStore := NewMediaTypeStore(s.db)

	mediaTypeID, err := mtStore.SafeFindOrCreateID(ctx, b.MediaType)
	if err != nil {
		return err
	}

	if _, err := s.db.ExecContext(ctx, q, m.NamespaceID, m.RepositoryID, m.ID, dgst, mediaTypeID, b.Size); err != nil {
		return fmt.Errorf("associating layer blob: %w", err)
	}

	return nil
}

// DissociateLayerBlob dissociates a layer blob and a manifest. It does nothing if not associated.
func (s *manifestStore) DissociateLayerBlob(ctx context.Context, m *models.Manifest, b *models.Blob) error {
	defer metrics.InstrumentQuery("manifest_dissociate_layer_blob")()
	q := `DELETE FROM layers
		WHERE top_level_namespace_id = $1
			AND repository_id = $2
			AND manifest_id = $3
			AND digest = decode($4, 'hex')`

	dgst, err := NewDigest(b.Digest)
	if err != nil {
		return err
	}

	res, err := s.db.ExecContext(ctx, q, m.NamespaceID, m.RepositoryID, m.ID, dgst)
	if err != nil {
		return fmt.Errorf("dissociating layer blob: %w", err)
	}

	if _, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("dissociating layer blob: %w", err)
	}

	return nil
}

// Delete deletes a manifest. The digest is returned to denote whether the manifest was deleted or not. This avoids the
// need for a separate preceding `SELECT` to find if it exists. A manifest cannot be deleted if it is referenced by a
// manifest list.
func (s *manifestStore) Delete(ctx context.Context, namespaceID, repositoryID, id int64) (*digest.Digest, error) {
	defer metrics.InstrumentQuery("manifest_delete")()
	q := `DELETE FROM manifests
		WHERE top_level_namespace_id = $1
			AND repository_id = $2
			AND id = $3
		RETURNING
			encode(digest, 'hex')`

	var tmp Digest
	row := s.db.QueryRowContext(ctx, q, namespaceID, repositoryID, id)
	if err := row.Scan(&tmp); err != nil {
		var pgErr *pgconn.PgError
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, nil
		case errors.As(err, &pgErr) && pgErr.Code == pgerrcode.ForeignKeyViolation && pgErr.TableName == "manifest_references":
			return nil, fmt.Errorf("deleting manifest: %w", ErrManifestReferencedInList)
		default:
			return nil, fmt.Errorf("deleting manifest: %w", err)
		}
	}

	dgst, err := tmp.Parse()
	if err != nil {
		return nil, err
	}
	return &dgst, nil
}

// findManifestByDigest finds a manifest by digest, repository and top level namespace ID
func findManifestByDigest(ctx context.Context, db Queryer, namespaceID, repositoryID int64, dgst Digest) (*models.Manifest, error) {
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
			AND m.digest = decode($3, 'hex')`

	row := db.QueryRowContext(ctx, q, namespaceID, repositoryID, dgst)

	return scanFullManifest(row)
}
