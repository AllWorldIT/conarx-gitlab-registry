package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251218074500_post_create_layers_id_index",
			Up: []string{
				"CREATE INDEX index_layers_on_id ON public.layers USING btree (id)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_layers_on_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
