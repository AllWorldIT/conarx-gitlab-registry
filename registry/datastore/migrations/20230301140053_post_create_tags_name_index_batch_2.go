//go:build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230301140053_post_create_tags_name_index_batch_2",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_16_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_16 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_17_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_17 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_18_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_18 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_19_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_19 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_20_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_20 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_21_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_21 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_22_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_22 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_23_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_23 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_24_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_24 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_25_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_25 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_26_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_26 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_27_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_27 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_28_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_28 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_29_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_29 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_30_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_30 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_31_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_31 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_tags_p_16_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_17_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_18_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_19_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_20_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_21_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_22_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_23_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_24_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_25_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_26_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_27_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_28_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_29_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_30_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_31_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
