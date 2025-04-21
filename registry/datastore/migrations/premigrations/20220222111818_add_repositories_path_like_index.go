package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220222111818_add_repositories_path_like_index",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_repositories_on_top_level_namespace_id_and_path_and_id ON repositories USING btree (top_level_namespace_id, path text_pattern_ops, id)",
			},
			Down: []string{
				"DROP INDEX CONCURRENTLY IF EXISTS index_repositories_on_top_level_namespace_id_and_path_and_id",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}
	migrations.AppendPreMigration(m)
}
