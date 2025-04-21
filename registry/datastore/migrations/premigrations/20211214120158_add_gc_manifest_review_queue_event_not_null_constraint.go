package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20211214120158_add_gc_manifest_review_queue_event_not_null_constraint",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							information_schema.constraint_column_usage
						WHERE
							table_name = 'gc_manifest_review_queue'
							AND column_name = 'event'
							AND constraint_name = 'check_gc_manifest_review_queue_event_not_null') THEN
						ALTER TABLE public.gc_manifest_review_queue
							ADD CONSTRAINT check_gc_manifest_review_queue_event_not_null CHECK (event IS NOT NULL) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE gc_manifest_review_queue VALIDATE CONSTRAINT check_gc_manifest_review_queue_event_not_null",
			},
			Down: []string{
				"ALTER TABLE gc_manifest_review_queue DROP CONSTRAINT IF EXISTS check_gc_manifest_review_queue_event_not_null",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
