package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220217095720_soft_delete_emtpy_repositories_batch_4",
			Up: []string{
				softDeleteEmptyRepositoriesBatchQuery(80001, 90000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(80001, 90000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
