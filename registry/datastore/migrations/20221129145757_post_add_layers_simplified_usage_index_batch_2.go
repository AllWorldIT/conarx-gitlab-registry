//go:build !integration
// +build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20221129145757_post_add_layers_simplified_usage_index_batch_2",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_2_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_2 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_3_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_3 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_4_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_4 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_5_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_5 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_6_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_6 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_7_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_7 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_8_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_8 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_9_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_9 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_10_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_10 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_11_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_11 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_12_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_12 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_13_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_13 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_14_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_14 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_15_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_15 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_16_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_16 USING btree (top_level_namespace_id, digest, size)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_layers_p_2_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_3_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_4_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_5_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_6_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_7_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_8_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_9_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_10_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_11_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_12_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_13_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_14_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_15_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_16_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
