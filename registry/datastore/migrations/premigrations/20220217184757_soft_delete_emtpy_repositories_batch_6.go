package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220217184757_soft_delete_emtpy_repositories_batch_6",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(200001, 210000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(200001, 210000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
