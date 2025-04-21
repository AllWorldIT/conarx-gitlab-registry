package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220216163242_soft_delete_emtpy_repositories_batch_3",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(20001, 30000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(20001, 30000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
