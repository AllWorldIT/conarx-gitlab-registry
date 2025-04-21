//go:build !integration

package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250224144143_post_create_manifests_partitions_subject_id_index_batch_2",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_16_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_16 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_17_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_17 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_18_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_18 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_19_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_19 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_20_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_20 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_21_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_21 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_22_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_22 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_23_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_23 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_24_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_24 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_25_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_25 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_26_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_26 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_27_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_27 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_28_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_28 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_29_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_29 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_30_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_30 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_31_on_ns_id_and_repo_id_and_subject_id ON partitions.manifests_p_31 USING btree (top_level_namespace_id, repository_id, subject_id)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_16_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_17_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_18_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_19_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_20_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_21_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_22_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_23_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_24_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_25_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_26_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_27_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_28_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_29_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_30_on_ns_id_and_repo_id_and_subject_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_31_on_ns_id_and_repo_id_and_subject_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPreMigration(m)
}
