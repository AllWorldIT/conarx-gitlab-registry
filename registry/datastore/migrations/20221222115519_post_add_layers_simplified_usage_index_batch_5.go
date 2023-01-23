//go:build !integration
// +build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20221222115519_post_add_layers_simplified_usage_index_batch_5",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_48_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_48 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_49_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_49 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_50_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_50 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_51_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_51 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_52_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_52 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_53_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_53 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_54_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_54 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_55_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_55 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_56_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_56 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_57_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_57 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_58_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_58 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_59_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_59 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_60_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_60 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_61_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_61 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_62_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_62 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_63_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_63 USING btree (top_level_namespace_id, digest, size)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_layers_p_48_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_49_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_50_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_51_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_52_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_53_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_54_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_55_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_56_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_57_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_58_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_59_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_60_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_61_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_62_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_63_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
