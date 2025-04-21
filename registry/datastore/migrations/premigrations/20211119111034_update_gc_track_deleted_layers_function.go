package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20211119111034_update_gc_track_deleted_layers_function",
			Up: []string{
				// updates function definition to start filling the new `event` column
				`CREATE OR REPLACE FUNCTION gc_track_deleted_layers ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					INSERT INTO gc_blob_review_queue (digest, review_after, event)
						VALUES (OLD.digest, gc_review_after ('layer_delete'), 'layer_delete')
					ON CONFLICT (digest)
						DO UPDATE SET
							review_after = gc_review_after ('layer_delete'), event = 'layer_delete';
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
			Down: []string{
				// restore previous function definition
				`CREATE OR REPLACE FUNCTION gc_track_deleted_layers ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					INSERT INTO gc_blob_review_queue (digest, review_after)
						VALUES (OLD.digest, gc_review_after ('layer_delete'))
					ON CONFLICT (digest)
						DO UPDATE SET
							review_after = gc_review_after ('layer_delete');
					RETURN NULL;
				END;
				$$
				LANGUAGE plpgsql`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
