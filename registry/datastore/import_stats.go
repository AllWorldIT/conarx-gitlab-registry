package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/docker/distribution/registry/datastore/metrics"
	"github.com/docker/distribution/registry/datastore/models"
)

// ImportStatisticsStore provides basic CRUD operations for import statistics
type ImportStatisticsStore interface {
	Create(ctx context.Context, stats *models.ImportStatistics) error
	FindByID(ctx context.Context, id int64) (*models.ImportStatistics, error)
	FindAll(ctx context.Context) ([]*models.ImportStatistics, error)
}

type importStatisticsStore struct {
	db Queryer
}

// NewImportStatisticsStore creates a new import statistics store
func NewImportStatisticsStore(db Queryer) ImportStatisticsStore {
	return &importStatisticsStore{db: db}
}

func scanFullImportStatistics(row *Row) (*models.ImportStatistics, error) {
	stats := new(models.ImportStatistics)

	err := row.Scan(
		&stats.ID,
		&stats.CreatedAt,
		&stats.StartedAt,
		&stats.FinishedAt,
		&stats.PreImport,
		&stats.PreImportStartedAt,
		&stats.PreImportFinishedAt,
		&stats.PreImportError,
		&stats.TagImport,
		&stats.TagImportStartedAt,
		&stats.TagImportFinishedAt,
		&stats.TagImportError,
		&stats.BlobImport,
		&stats.BlobImportStartedAt,
		&stats.BlobImportFinishedAt,
		&stats.BlobImportError,
		&stats.RepositoriesCount,
		&stats.TagsCount,
		&stats.ManifestsCount,
		&stats.BlobsCount,
		&stats.BlobsSizeBytes,
		&stats.StorageDriver,
	)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("scanning import statistics: %w", err)
		}
		return nil, nil
	}

	return stats, nil
}

func (s *importStatisticsStore) Create(ctx context.Context, stats *models.ImportStatistics) error {
	defer metrics.InstrumentQuery("import_statistics_create")()

	q := `INSERT INTO import_statistics (
			started_at,
			finished_at,
			pre_import,
			pre_import_started_at,
			pre_import_finished_at,
			pre_import_error,
			tag_import,
			tag_import_started_at,
			tag_import_finished_at,
			tag_import_error,
			blob_import,
			blob_import_started_at,
			blob_import_finished_at,
			blob_import_error,
			repositories_count,
			tags_count,
			manifests_count,
			blobs_count,
			blobs_size_bytes,
			storage_driver
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
		RETURNING
			id, created_at`

	row := s.db.QueryRowContext(ctx, q,
		stats.StartedAt,
		stats.FinishedAt,
		stats.PreImport,
		stats.PreImportStartedAt,
		stats.PreImportFinishedAt,
		stats.PreImportError,
		stats.TagImport,
		stats.TagImportStartedAt,
		stats.TagImportFinishedAt,
		stats.TagImportError,
		stats.BlobImport,
		stats.BlobImportStartedAt,
		stats.BlobImportFinishedAt,
		stats.BlobImportError,
		stats.RepositoriesCount,
		stats.TagsCount,
		stats.ManifestsCount,
		stats.BlobsCount,
		stats.BlobsSizeBytes,
		stats.StorageDriver,
	)

	if err := row.Scan(&stats.ID, &stats.CreatedAt); err != nil {
		return fmt.Errorf("creating import statistics: %w", err)
	}

	return nil
}

func (s *importStatisticsStore) FindByID(ctx context.Context, id int64) (*models.ImportStatistics, error) {
	defer metrics.InstrumentQuery("import_statistics_find_by_id")()

	q := `SELECT
			id,
			created_at,
			started_at,
			finished_at,
			pre_import,
			pre_import_started_at,
			pre_import_finished_at,
			pre_import_error,
			tag_import,
			tag_import_started_at,
			tag_import_finished_at,
			tag_import_error,
			blob_import,
			blob_import_started_at,
			blob_import_finished_at,
			blob_import_error,
			repositories_count,
			tags_count,
			manifests_count,
			blobs_count,
			blobs_size_bytes,
			storage_driver
		FROM
			import_statistics
		WHERE
			id = $1`

	row := s.db.QueryRowContext(ctx, q, id)
	return scanFullImportStatistics(row)
}

func (s *importStatisticsStore) FindAll(ctx context.Context) ([]*models.ImportStatistics, error) {
	defer metrics.InstrumentQuery("import_statistics_find_all")()

	q := `SELECT
			id,
			created_at,
			started_at,
			finished_at,
			pre_import,
			pre_import_started_at,
			pre_import_finished_at,
			pre_import_error,
			tag_import,
			tag_import_started_at,
			tag_import_finished_at,
			tag_import_error,
			blob_import,
			blob_import_started_at,
			blob_import_finished_at,
			blob_import_error,
			repositories_count,
			tags_count,
			manifests_count,
			blobs_count,
			blobs_size_bytes,
			storage_driver
		FROM
			import_statistics
		ORDER BY
			id`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("querying import statistics: %w", err)
	}
	defer rows.Close()

	var allStats []*models.ImportStatistics
	for rows.Next() {
		stats := new(models.ImportStatistics)

		err := rows.Scan(
			&stats.ID,
			&stats.CreatedAt,
			&stats.StartedAt,
			&stats.FinishedAt,
			&stats.PreImport,
			&stats.PreImportStartedAt,
			&stats.PreImportFinishedAt,
			&stats.PreImportError,
			&stats.TagImport,
			&stats.TagImportStartedAt,
			&stats.TagImportFinishedAt,
			&stats.TagImportError,
			&stats.BlobImport,
			&stats.BlobImportStartedAt,
			&stats.BlobImportFinishedAt,
			&stats.BlobImportError,
			&stats.RepositoriesCount,
			&stats.TagsCount,
			&stats.ManifestsCount,
			&stats.BlobsCount,
			&stats.BlobsSizeBytes,
			&stats.StorageDriver,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning import statistics: %w", err)
		}

		allStats = append(allStats, stats)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating import statistics rows: %w", err)
	}

	return allStats, nil
}
