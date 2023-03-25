//go:build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230301140423_post_create_tags_name_index_batch_4",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_48_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_48 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_49_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_49 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_50_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_50 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_51_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_51 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_52_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_52 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_53_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_53 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_54_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_54 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_55_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_55 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_56_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_56 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_57_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_57 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_58_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_58 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_59_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_59 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_60_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_60 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_61_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_61 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_62_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_62 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_63_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_63 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_tags_p_48_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_49_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_50_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_51_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_52_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_53_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_54_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_55_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_56_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_57_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_58_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_59_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_60_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_61_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_62_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_63_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
