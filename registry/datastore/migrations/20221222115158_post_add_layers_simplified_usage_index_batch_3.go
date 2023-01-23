//go:build !integration
// +build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20221222115158_post_add_layers_simplified_usage_index_batch_3",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_17_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_17 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_18_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_18 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_19_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_19 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_20_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_20 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_21_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_21 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_22_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_22 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_23_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_23 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_24_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_24 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_25_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_25 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_26_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_26 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_27_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_27 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_28_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_28 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_29_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_29 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_30_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_30 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_31_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_31 USING btree (top_level_namespace_id, digest, size)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_layers_p_17_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_18_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_19_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_20_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_21_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_22_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_23_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_24_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_25_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_26_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_27_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_28_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_29_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_30_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_31_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
