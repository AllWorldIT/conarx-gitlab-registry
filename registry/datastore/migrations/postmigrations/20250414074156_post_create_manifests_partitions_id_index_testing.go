//go:build integration

package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250414074156_post_create_manifests_partitions_id_index_testing",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_0_on_id ON partitions.manifests_p_0 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_1_on_id ON partitions.manifests_p_1 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_2_on_id ON partitions.manifests_p_2 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_3_on_id ON partitions.manifests_p_3 USING btree (id)",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_0_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_1_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_2_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_3_on_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
