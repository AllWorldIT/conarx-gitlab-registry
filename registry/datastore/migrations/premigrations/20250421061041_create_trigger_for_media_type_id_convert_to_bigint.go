package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250421061041_create_trigger_for_media_type_id_convert_to_bigint",
			Up: []string{
				// Create the trigger function to set media_type_id_convert_to_bigint
				`CREATE OR REPLACE FUNCTION set_media_type_id_convert_to_bigint ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					NEW.media_type_id_convert_to_bigint := NEW.media_type_id;
					RETURN NEW;
				END;
				$$
				LANGUAGE plpgsql;`,

				// Create the trigger to fire on INSERT and UPDATE
				`CREATE TRIGGER set_media_type_id_convert_to_bigint
    				BEFORE INSERT OR UPDATE ON manifests
    				FOR EACH ROW
    				EXECUTE PROCEDURE set_media_type_id_convert_to_bigint ()`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS set_media_type_id_convert_to_bigint ON manifests",
				"DROP FUNCTION IF EXISTS set_media_type_id_convert_to_bigint",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
