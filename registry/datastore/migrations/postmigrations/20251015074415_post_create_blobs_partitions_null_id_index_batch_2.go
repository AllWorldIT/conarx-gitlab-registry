//go:build !integration

package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251015074415_post_create_blobs_partitions_null_id_index_batch_2",
			Up: []string{
				"SET statement_timeout TO 0",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_16_on_null_id ON partitions.blobs_p_16 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_17_on_null_id ON partitions.blobs_p_17 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_18_on_null_id ON partitions.blobs_p_18 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_19_on_null_id ON partitions.blobs_p_19 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_20_on_null_id ON partitions.blobs_p_20 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_21_on_null_id ON partitions.blobs_p_21 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_22_on_null_id ON partitions.blobs_p_22 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_23_on_null_id ON partitions.blobs_p_23 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_24_on_null_id ON partitions.blobs_p_24 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_25_on_null_id ON partitions.blobs_p_25 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_26_on_null_id ON partitions.blobs_p_26 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_27_on_null_id ON partitions.blobs_p_27 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_28_on_null_id ON partitions.blobs_p_28 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_29_on_null_id ON partitions.blobs_p_29 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_30_on_null_id ON partitions.blobs_p_30 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_31_on_null_id ON partitions.blobs_p_31 USING btree (id) WHERE id IS NULL",
				"RESET statement_timeout",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_blobs_p_16_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_17_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_18_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_19_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_20_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_21_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_22_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_23_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_24_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_25_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_26_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_27_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_28_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_29_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_30_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_31_on_null_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
