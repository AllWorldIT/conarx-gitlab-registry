package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161121_create_gc_track_blob_uploads_trigger",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'gc_track_blob_uploads_trigger') THEN
						CREATE TRIGGER gc_track_blob_uploads_trigger
							AFTER INSERT ON blobs
							FOR EACH ROW
							EXECUTE PROCEDURE gc_track_blob_uploads ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS gc_track_blob_uploads_trigger ON blobs CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
