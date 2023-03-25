//go:build integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230301132550_post_create_tags_name_index_testing",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_0_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_0 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_1_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_1 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_2_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_2 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_tags_p_3_on_ns_id_and_repo_id_and_manifest_id_and_name ON partitions.tags_p_3 USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
				"CREATE INDEX index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name ON public.tags USING btree (top_level_namespace_id, repository_id, manifest_id, name)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS index_tags_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_0_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_1_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_2_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
				"DROP INDEX IF EXISTS partitions.index_tags_p_3_on_ns_id_and_repo_id_and_manifest_id_and_name CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
