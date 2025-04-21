package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220729143447_update_gc_review_after_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_review_after (e text)
					RETURNS timestamp WITH time zone VOLATILE
					AS $$
				DECLARE
					result timestamp WITH time zone;
					jitter_s interval;
				BEGIN
					SELECT
						(random() * (60 - 5 + 1) + 5) * INTERVAL '1 second' INTO jitter_s;
					SELECT
						(now() + value) INTO result
					FROM
						gc_review_after_defaults
					WHERE
						event = e;
					IF result IS NULL THEN
						RETURN now() + interval '1 day' + jitter_s;
					ELSE
						RETURN result + jitter_s;
					END IF;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				// restore previous version
				`CREATE OR REPLACE FUNCTION gc_review_after (e text)
					RETURNS timestamp WITH time zone VOLATILE
					AS $$
				DECLARE
					result timestamp WITH time zone;
				BEGIN
					SELECT
						(now() + value) INTO result
					FROM
						gc_review_after_defaults
					WHERE
						event = e;
					IF result IS NULL THEN
						RETURN now() + interval '1 day';
					ELSE
						RETURN result;
					END IF;
				END;
				$$
				LANGUAGE plpgsql`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
