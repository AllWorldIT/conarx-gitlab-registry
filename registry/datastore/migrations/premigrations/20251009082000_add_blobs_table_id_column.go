package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251009082000_add_blobs_table_id_column",
			Up: []string{
				`CREATE SEQUENCE blobs_id_seq`,
				// The new column is nullable to allow backfilling existing records using BBM:
				// https://gitlab.com/gitlab-org/container-registry/-/issues/1248#note_1877479379

				// Add the new column without a default so existing records are NULL
				`ALTER TABLE blobs ADD COLUMN id BIGINT`,
				// Set the default for future inserts only
				`ALTER TABLE blobs ALTER COLUMN id SET DEFAULT nextval('blobs_id_seq')`,
			},
			Down: []string{
				`ALTER TABLE blobs DROP COLUMN IF EXISTS id`,
				`DROP SEQUENCE IF EXISTS blobs_id_seq`,
			},
		},
	}
	migrations.AppendPreMigration(m)
}
