package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161831_create_gc_track_tmp_blobs_manifests_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_tmp_blobs_manifests ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					INSERT INTO gc_tmp_blobs_manifests (digest)
						VALUES (NEW.digest)
					ON CONFLICT (digest)
						DO NOTHING;
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_tmp_blobs_manifests CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
