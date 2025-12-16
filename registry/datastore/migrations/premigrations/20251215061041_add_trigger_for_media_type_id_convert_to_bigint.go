package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20251215061041_add_trigger_for_media_type_id_convert_to_bigint",
			Up: []string{
				// We already have a media_type_id_convert_to_bigint function in 20250421061041_create_trigger_for_media_type_id_convert_to_bigint to re-use for blobs and layers table.
				// Now we need to create a new function and trigger for manifest.configuration_media_type_id_convert_to_bigint.
				`CREATE OR REPLACE FUNCTION set_config_media_type_id_convert_to_bigint ()
                    RETURNS TRIGGER
                    AS $$
                BEGIN
                    NEW.configuration_media_type_id_convert_to_bigint := NEW.configuration_media_type_id;
                    RETURN NEW;
                END;
                $$
                LANGUAGE plpgsql;`,
				// Create the manifests table trigger to fire on INSERT and UPDATE
				`DO $$
				BEGIN
					-- Scope trigger check to this table so other tables can reuse the same trigger name safely.
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'set_config_media_type_id_convert_to_bigint'
							AND tgrelid = 'manifests'::regclass) THEN
						CREATE TRIGGER set_config_media_type_id_convert_to_bigint
							BEFORE INSERT OR UPDATE ON manifests
							FOR EACH ROW
							EXECUTE PROCEDURE set_config_media_type_id_convert_to_bigint ();
					END IF;
				END
				$$`,
				// Create the layers table trigger to fire on INSERT and UPDATE
				`DO $$
				BEGIN
					-- Same trigger name as manifests; check tgrelid to avoid blocking creation on layers.
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'set_media_type_id_convert_to_bigint'
							AND tgrelid = 'layers'::regclass) THEN
						CREATE TRIGGER set_media_type_id_convert_to_bigint
							BEFORE INSERT OR UPDATE ON layers
							FOR EACH ROW
							EXECUTE PROCEDURE set_media_type_id_convert_to_bigint ();
					END IF;
				END
				$$`,
				// Create the blobs table trigger to fire on INSERT and UPDATE
				`DO $$
				BEGIN
					-- Same trigger name as manifests/layers; check tgrelid to avoid blocking creation on blobs.
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'set_media_type_id_convert_to_bigint'
							AND tgrelid = 'blobs'::regclass) THEN
						CREATE TRIGGER set_media_type_id_convert_to_bigint
							BEFORE INSERT OR UPDATE ON blobs
							FOR EACH ROW
							EXECUTE PROCEDURE set_media_type_id_convert_to_bigint ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS set_config_media_type_id_convert_to_bigint ON manifests",
				"DROP FUNCTION IF EXISTS set_config_media_type_id_convert_to_bigint",
				"DROP TRIGGER IF EXISTS set_media_type_id_convert_to_bigint ON layers",
				"DROP TRIGGER IF EXISTS set_media_type_id_convert_to_bigint ON blobs",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
