//go:build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230301135802_post_create_tags_name_index_batch_1",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_0_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_0 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_1_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_1 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_2_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_2 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_3_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_3 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_4_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_4 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_5_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_5 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_6_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_6 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_7_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_7 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_8_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_8 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_9_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_9 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_10_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_10 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_11_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_11 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_12_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_12 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_13_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_13 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_14_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_14 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_15_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_15 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_tags_p_0_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_1_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_2_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_3_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_4_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_5_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_6_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_7_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_8_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_9_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_10_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_11_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_12_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_13_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_14_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_15_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
