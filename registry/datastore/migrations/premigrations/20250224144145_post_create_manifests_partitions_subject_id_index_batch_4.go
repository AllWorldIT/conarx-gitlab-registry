//go:build !integration

package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250224144145_post_create_manifests_partitions_subject_id_index_batch_4",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_48_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_48 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_49_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_49 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_50_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_50 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_51_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_51 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_52_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_52 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_53_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_53 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_54_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_54 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_55_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_55 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_56_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_56 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_57_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_57 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_58_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_58 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_59_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_59 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_60_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_60 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_61_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_61 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_62_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_62 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_63_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_63 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_48_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_49_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_50_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_51_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_52_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_53_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_54_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_55_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_56_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_57_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_58_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_59_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_60_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_61_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_62_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_63_on_ns_id_and_repo_id_and_subject_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
