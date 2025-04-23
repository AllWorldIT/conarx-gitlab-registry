//go:build !integration

package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250414074415_post_create_manifests_partitions_id_index_batch_2",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_16_on_id ON partitions.manifests_p_16 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_17_on_id ON partitions.manifests_p_17 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_18_on_id ON partitions.manifests_p_18 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_19_on_id ON partitions.manifests_p_19 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_20_on_id ON partitions.manifests_p_20 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_21_on_id ON partitions.manifests_p_21 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_22_on_id ON partitions.manifests_p_22 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_23_on_id ON partitions.manifests_p_23 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_24_on_id ON partitions.manifests_p_24 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_25_on_id ON partitions.manifests_p_25 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_26_on_id ON partitions.manifests_p_26 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_27_on_id ON partitions.manifests_p_27 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_28_on_id ON partitions.manifests_p_28 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_29_on_id ON partitions.manifests_p_29 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_30_on_id ON partitions.manifests_p_30 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_31_on_id ON partitions.manifests_p_31 USING btree (id)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_16_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_17_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_18_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_19_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_20_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_21_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_22_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_23_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_24_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_25_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_26_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_27_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_28_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_29_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_30_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_31_on_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
