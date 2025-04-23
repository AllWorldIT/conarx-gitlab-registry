//go:build !integration

package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250414074422_post_create_manifests_partitions_id_index_batch_4",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_48_on_id ON partitions.manifests_p_48 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_49_on_id ON partitions.manifests_p_49 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_50_on_id ON partitions.manifests_p_50 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_51_on_id ON partitions.manifests_p_51 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_52_on_id ON partitions.manifests_p_52 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_53_on_id ON partitions.manifests_p_53 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_54_on_id ON partitions.manifests_p_54 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_55_on_id ON partitions.manifests_p_55 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_56_on_id ON partitions.manifests_p_56 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_57_on_id ON partitions.manifests_p_57 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_58_on_id ON partitions.manifests_p_58 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_59_on_id ON partitions.manifests_p_59 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_60_on_id ON partitions.manifests_p_60 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_61_on_id ON partitions.manifests_p_61 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_62_on_id ON partitions.manifests_p_62 USING btree (id)",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_manifests_p_63_on_id ON partitions.manifests_p_63 USING btree (id)",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_manifests_p_48_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_49_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_50_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_51_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_52_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_53_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_54_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_55_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_56_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_57_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_58_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_59_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_60_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_61_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_62_on_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_manifests_p_63_on_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
