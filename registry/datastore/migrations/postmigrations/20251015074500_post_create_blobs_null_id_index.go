package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251015074500_post_create_blobs_null_id_index",
			Up: []string{
				"CREATE INDEX index_blobs_on_null_id ON public.blobs USING btree (id) WHERE id IS NULL",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_blobs_on_null_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
