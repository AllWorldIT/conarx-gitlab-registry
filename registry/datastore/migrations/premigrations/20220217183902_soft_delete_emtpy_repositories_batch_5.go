package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220217183902_soft_delete_emtpy_repositories_batch_5",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(110001, 120000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(110001, 120000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
