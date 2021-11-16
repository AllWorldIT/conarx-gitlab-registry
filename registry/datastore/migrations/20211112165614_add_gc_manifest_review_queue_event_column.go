package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20211112165614_add_gc_manifest_review_queue_event_column",
			Up: []string{
				"ALTER TABLE gc_manifest_review_queue ADD COLUMN IF NOT EXISTS event text",
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
							AND constraint_name = 'check_gc_manifest_review_queue_event_length') THEN
						ALTER TABLE public.gc_manifest_review_queue
							ADD CONSTRAINT check_gc_manifest_review_queue_event_length CHECK ((char_length(event) <= 255)) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE gc_manifest_review_queue VALIDATE CONSTRAINT check_gc_manifest_review_queue_event_length",
			},
			Down: []string{
				"ALTER TABLE gc_manifest_review_queue DROP CONSTRAINT IF EXISTS check_gc_manifest_review_queue_event_length",
				"ALTER TABLE gc_manifest_review_queue DROP COLUMN IF EXISTS event",
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
