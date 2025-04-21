package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20211119110903_update_gc_track_deleted_manifests_function",
			Up: []string{
				// updates function definition to start filling the new `event` column
				`CREATE OR REPLACE FUNCTION gc_track_deleted_manifests ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					IF OLD.configuration_blob_digest IS NOT NULL THEN
						INSERT INTO gc_blob_review_queue (digest, review_after, event)
							VALUES (OLD.configuration_blob_digest, gc_review_after ('manifest_delete'), 'manifest_delete')
						ON CONFLICT (digest)
							DO UPDATE SET
								review_after = gc_review_after ('manifest_delete'), event = 'manifest_delete';
					END IF;
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				// restore previous function definition
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
		},
	}

	migrations.AppendPreMigration(m)
}
