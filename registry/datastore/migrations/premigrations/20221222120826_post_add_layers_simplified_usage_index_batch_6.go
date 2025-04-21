package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20221222120826_post_add_layers_simplified_usage_index_batch_6",
			Up: []string{
				"CREATE INDEX index_layers_on_top_level_namespace_id_and_digest_and_size ON public.layers USING btree (top_level_namespace_id, digest, size)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_layers_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
