package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220217095717_soft_delete_emtpy_repositories_batch_4",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(60001, 70000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(60001, 70000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
