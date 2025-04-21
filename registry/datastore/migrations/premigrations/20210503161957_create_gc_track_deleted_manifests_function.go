package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161957_create_gc_track_deleted_manifests_function",
			Up: []string{
				`CREATE OR REPLACE FUNCTION gc_track_deleted_manifests ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					IF OLD.configuration_blob_digest IS NOT NULL THEN
						INSERT INTO gc_blob_review_queue (digest, review_after)
							VALUES (OLD.configuration_blob_digest, gc_review_after ('manifest_delete'))
						ON CONFLICT (digest)
							DO UPDATE SET
								review_after = gc_review_after ('manifest_delete');
					END IF;
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_deleted_manifests CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
