package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20260223191423_add_bbm_sub_batch_size_column",
			Up: []string{
				"ALTER TABLE batched_background_migrations ADD COLUMN IF NOT EXISTS sub_batch_size integer NOT NULL DEFAULT 0",
			},
			Down: []string{
				"ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS sub_batch_size",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
