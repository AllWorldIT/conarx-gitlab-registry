package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220210115916_drop_repositories_parent_id_constraint",
			Up: []string{
				"ALTER TABLE repositories DROP CONSTRAINT IF EXISTS fk_repositories_top_lvl_namespace_id_and_parent_id_repositories",
			},
			Down: []string{
				"ALTER TABLE repositories ADD CONSTRAINT fk_repositories_top_lvl_namespace_id_and_parent_id_repositories FOREIGN KEY (top_level_namespace_id, parent_id) REFERENCES repositories (top_level_namespace_id, id) ON DELETE CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
