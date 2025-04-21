package premigrations

import (
	"fmt"

	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func softDeleteEmptyRepositoriesBatchQuery(from, to int) string {
	tmpl := `UPDATE
		repositories AS r
	SET
		deleted_at = now()
	WHERE
		r.id BETWEEN %d AND %d
		AND NOT EXISTS (
			SELECT
			FROM
				manifests AS m
			WHERE
				m.top_level_namespace_id = r.top_level_namespace_id
				AND m.repository_id = r.id)
		AND NOT EXISTS (
			SELECT
			FROM
				repository_blobs AS rb
			WHERE
				rb.top_level_namespace_id = r.top_level_namespace_id
				AND rb.repository_id = r.id)`

	return fmt.Sprintf(tmpl, from, to)
}

func undoSoftDeleteEmptyRepositoriesBatchQuery(from, to int) string {
	tmpl := `UPDATE
		repositories AS r
	SET
		deleted_at = NULL
	WHERE
		r.id BETWEEN %d AND %d
		AND deleted_at IS NOT NULL`

	return fmt.Sprintf(tmpl, from, to)
}

func init() {
	// Related to https://gitlab.com/gitlab-org/container-registry/-/issues/570.
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220215130020_soft_delete_emtpy_repositories_batch_1",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(1, 1000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(1, 1000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
