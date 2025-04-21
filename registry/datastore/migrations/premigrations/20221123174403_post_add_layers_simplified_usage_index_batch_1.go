package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20221123174403_post_add_layers_simplified_usage_index_batch_1",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_0_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_0 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_1_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_1 USING btree (top_level_namespace_id, digest, size)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_layers_p_0_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_1_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
