//go:build integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20250224143824_post_create_manifests_partitions_subject_id_index_testing",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_0_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_0 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_1_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_1 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_2_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_2 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_3_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_3 USING btree (top_level_namespace_id, repository_id, subject_id)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_0_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_1_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_2_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_3_on_ns_id_and_repo_id_and_subject_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
