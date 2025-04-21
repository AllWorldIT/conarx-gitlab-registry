package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250410113134_update_gc_track_deleted_manifests_function",
			Up: []string{
				// updates function definition to start tracking subject ID references
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
					IF OLD.subject_id IS NOT NULL THEN
						INSERT INTO gc_manifest_review_queue (top_level_namespace_id, repository_id, manifest_id, review_after, event)
							VALUES (OLD.top_level_namespace_id, OLD.repository_id, OLD.subject_id, gc_review_after ('manifest_delete'), 'manifest_delete')
						ON CONFLICT (top_level_namespace_id, repository_id, manifest_id)
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
		},
	}

	migrations.AppendPreMigration(m)
}
