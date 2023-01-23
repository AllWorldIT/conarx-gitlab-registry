//go:build !integration
// +build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20221222115318_post_add_layers_simplified_usage_index_batch_4",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_32_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_32 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_33_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_33 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_34_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_34 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_35_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_35 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_36_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_36 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_37_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_37 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_38_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_38 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_39_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_39 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_40_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_40 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_41_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_41 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_42_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_42 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_43_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_43 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_44_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_44 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_45_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_45 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_46_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_46 USING btree (top_level_namespace_id, digest, size)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_layers_p_47_on_top_level_namespace_id_and_digest_and_size ON partitions.layers_p_47 USING btree (top_level_namespace_id, digest, size)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_layers_p_32_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_33_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_34_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_35_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_36_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_37_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_38_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_39_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_40_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_41_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_42_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_43_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_44_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_45_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_46_on_top_level_namespace_id_and_digest_and_size CASCADE",
				"DROP INDEX IF EXISTS partitions.index_layers_p_47_on_top_level_namespace_id_and_digest_and_size CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
