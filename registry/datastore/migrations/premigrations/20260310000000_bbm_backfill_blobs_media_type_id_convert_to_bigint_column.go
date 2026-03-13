package premigrations

import (
	"fmt"

	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	const numPartitions = 64

	upStatements := make([]string, 0, numPartitions)
	downStatements := make([]string, 0, numPartitions)

	for i := 0; i < numPartitions; i++ {
		partitionName := fmt.Sprintf("partitions.blobs_p_%d", i)
		migrationName := fmt.Sprintf("copy_blobs_p_%d_media_type_id_column_to_media_type_id_convert_to_bigint_column", i)

		upStatements = append(upStatements, fmt.Sprintf(`
			INSERT INTO batched_background_migrations ("name", "min_value", "max_value", "batch_size", "sub_batch_size", "status", "job_signature_name", "table_name", "column_name")
				VALUES ('%s', 1, -- Default BIGINT Undershoot minimum for IDENTITY
					9223372036854775807, -- BIGINT maximum (overshoot)
					100000, 20000, 1, -- Active status
					'copyBlobMediaTypeIDToNewBigIntColumn', '%s', 'id')`,
			migrationName,
			partitionName,
		))

		downStatements = append(downStatements, fmt.Sprintf(`
			DELETE FROM batched_background_migrations WHERE "name" = '%s'`,
			migrationName,
		))
	}

	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id:   "20260310000000_bbm_backfill_blobs_media_type_id_convert_to_bigint_column",
			Up:   upStatements,
			Down: downStatements,
		},
	}

	migrations.AppendPreMigration(m)
}
