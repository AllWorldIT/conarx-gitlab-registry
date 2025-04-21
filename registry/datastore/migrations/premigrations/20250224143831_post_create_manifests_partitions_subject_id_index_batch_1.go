//go:build !integration

package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250224143831_post_create_manifests_partitions_subject_id_index_batch_1",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_0_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_0 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_1_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_1 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_2_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_2 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_3_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_3 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_4_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_4 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_5_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_5 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_6_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_6 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_7_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_7 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_8_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_8 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_9_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_9 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_10_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_10 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_11_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_11 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_12_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_12 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_13_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_13 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_14_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_14 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_15_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_15 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_0_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_1_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_2_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_3_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_4_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_5_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_6_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_7_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_8_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_9_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_10_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_11_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_12_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_13_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_14_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_15_on_ns_id_and_repo_id_and_subject_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
