package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220217184751_soft_delete_emtpy_repositories_batch_6",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(170001, 180000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(170001, 180000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
