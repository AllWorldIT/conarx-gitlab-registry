//go:build !integration

package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251015074419_post_create_blobs_partitions_null_id_index_batch_3",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_32_on_null_id ON partitions.blobs_p_32 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_33_on_null_id ON partitions.blobs_p_33 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_34_on_null_id ON partitions.blobs_p_34 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_35_on_null_id ON partitions.blobs_p_35 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_36_on_null_id ON partitions.blobs_p_36 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_37_on_null_id ON partitions.blobs_p_37 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_38_on_null_id ON partitions.blobs_p_38 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_39_on_null_id ON partitions.blobs_p_39 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_40_on_null_id ON partitions.blobs_p_40 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_41_on_null_id ON partitions.blobs_p_41 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_42_on_null_id ON partitions.blobs_p_42 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_43_on_null_id ON partitions.blobs_p_43 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_44_on_null_id ON partitions.blobs_p_44 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_45_on_null_id ON partitions.blobs_p_45 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_46_on_null_id ON partitions.blobs_p_46 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_47_on_null_id ON partitions.blobs_p_47 USING btree (id) WHERE id IS NULL",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_blobs_p_32_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_33_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_34_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_35_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_36_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_37_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_38_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_39_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_40_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_41_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_42_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_43_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_44_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_45_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_46_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_47_on_null_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
