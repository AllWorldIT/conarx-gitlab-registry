package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	// Related to https://gitlab.com/gitlab-org/container-registry/-/issues/570.
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220216122704_soft_delete_emtpy_repositories_batch_2",
			Up: []string{
				// The schema migrations tool will use a single transaction to execute all Up/Down statements within a
				// given migration. This is why we're spreading updates across multiple migrations (this and
				// 20220216124355_soft_delete_emtpy_repositories_batch_2 for batch 2) to decrease the time and the
				// number of locked `repositories` rows per migration.
				softDeleteEmptyRepositoriesBatchQuery(1001, 10000),
			},
			Down: []string{
				undoSoftDeleteEmptyRepositoriesBatchQuery(1001, 10000),
			},
		},
	}

	migrations.AppendPreMigration(m)
}
