//go:build integration

package postmigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251015074156_post_create_blobs_partitions_null_id_index_testing",
			Up: []string{
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_0_on_null_id ON partitions.blobs_p_0 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_1_on_null_id ON partitions.blobs_p_1 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_2_on_null_id ON partitions.blobs_p_2 USING btree (id) WHERE id IS NULL",
				"CREATE INDEX CONCURRENTLY IF NOT EXISTS index_blobs_p_3_on_null_id ON partitions.blobs_p_3 USING btree (id) WHERE id IS NULL",
			},
			Down: []string{
				"DROP INDEX IF EXISTS partitions.index_blobs_p_0_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_1_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_2_on_null_id CASCADE",
				"DROP INDEX IF EXISTS partitions.index_blobs_p_3_on_null_id CASCADE",
			},
			DisableTransactionUp:   true,
			DisableTransactionDown: true,
		},
	}

	migrations.AppendPostMigration(m)
}
