package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161355_create_gc_track_configuration_blobs_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_configuration_blobs ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					IF NEW.configuration_blob_digest IS NOT NULL THEN
						INSERT INTO gc_blobs_configurations (top_level_namespace_id, repository_id, manifest_id, digest)
							VALUES (NEW.top_level_namespace_id, NEW.repository_id, NEW.id, NEW.configuration_blob_digest)
						ON CONFLICT (digest, manifest_id)
							DO NOTHING;
					END IF;
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_configuration_blobs CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
