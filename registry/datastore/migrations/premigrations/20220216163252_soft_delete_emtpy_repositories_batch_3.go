package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220216163252_soft_delete_emtpy_repositories_batch_3",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(40001, 50000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(40001, 50000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
