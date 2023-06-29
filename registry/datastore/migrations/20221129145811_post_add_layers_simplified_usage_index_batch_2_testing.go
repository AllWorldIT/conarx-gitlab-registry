//go:build integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20221129145811_post_add_layers_simplified_usage_index_batch_2_testing",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_2_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_2 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_3_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_3 USING btree (top_level_namespace_id, digest, size)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_layers_p_2_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_3_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
