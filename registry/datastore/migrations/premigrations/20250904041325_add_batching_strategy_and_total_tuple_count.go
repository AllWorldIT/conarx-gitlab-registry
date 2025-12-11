package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250904041325_add_batching_strategy_and_total_tuple_count",
			Up: []string{
				"ALTER TABLE batched_background_migrations ADD COLUMN IF NOT EXISTS batching_strategy text",
				"ALTER TABLE batched_background_migrations ADD COLUMN IF NOT EXISTS total_tuple_count bigint",
			},
			Down: []string{
				"ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS batching_strategy",
				"ALTER TABLE batched_background_migrations DROP COLUMN IF EXISTS total_tuple_count",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
