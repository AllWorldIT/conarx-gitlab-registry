package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161027_create_gc_track_blob_uploads_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_blob_uploads ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					INSERT INTO gc_blob_review_queue (digest, review_after)
						VALUES (NEW.digest, gc_review_after ('blob_upload'))
					ON CONFLICT (digest)
						DO UPDATE SET
							review_after = gc_review_after ('blob_upload');
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_blob_uploads CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
