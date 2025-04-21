package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20240530090526_add_media_type_not_empty_string_constraint",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							information_schema.constraint_column_usage
						WHERE
							table_name = 'media_types'
							AND column_name = 'media_type'
							AND constraint_name = 'check_media_types_media_type_not_empty_string') THEN
						ALTER TABLE public.media_types
							ADD CONSTRAINT check_media_types_media_type_not_empty_string CHECK (LENGTH(TRIM(media_type)) > 0) NOT VALID;
					END IF;
				END;
				$$`,
				"ALTER TABLE media_types VALIDATE CONSTRAINT check_media_types_media_type_not_empty_string",
			},
			Down: []string{
				"ALTER TABLE media_types DROP CONSTRAINT IF EXISTS check_media_types_media_type_not_empty_string",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
