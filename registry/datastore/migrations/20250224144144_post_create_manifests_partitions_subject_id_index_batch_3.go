//go:build !integration

package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20250224144144_post_create_manifests_partitions_subject_id_index_batch_3",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_32_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_32 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_33_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_33 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_34_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_34 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_35_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_35 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_36_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_36 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_37_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_37 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_38_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_38 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_39_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_39 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_40_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_40 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_41_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_41 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_42_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_42 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_43_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_43 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_44_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_44 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_45_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_45 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_46_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_46 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_47_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_47 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_32_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_33_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_34_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_35_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_36_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_37_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_38_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_39_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_40_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_41_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_42_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_43_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_44_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_45_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_46_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_47_on_ns_id_and_repo_id_and_subject_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
		PostDeployment: true,
	}

	allMigrations = append(allMigrations, m)
}
