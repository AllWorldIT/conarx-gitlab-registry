package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220308164158_add_repositories_on_id_where_deleted_at_not_null_index",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_repositories_on_id_where_deleted_at_not_null ON repositories USING btree (id) WHERE deleted_at IS NOT NULL",
			},
			Down: []string{
				"DROP INDEX CONCURRENTLY IF EXISTS index_repositories_on_id_where_deleted_at_not_null",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
