package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161629_create_gc_track_layer_blobs_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_layer_blobs ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					INSERT INTO gc_blobs_layers (top_level_namespace_id, repository_id, layer_id, digest)
						VALUES (NEW.top_level_namespace_id, NEW.repository_id, NEW.id, NEW.digest)
					ON CONFLICT (digest, layer_id)
						DO NOTHING;
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_layer_blobs CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
