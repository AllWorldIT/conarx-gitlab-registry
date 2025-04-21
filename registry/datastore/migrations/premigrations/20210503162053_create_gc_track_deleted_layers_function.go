package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503162053_create_gc_track_deleted_layers_function",
			Up: []string{
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
			Down: []string{
				"DROP FUNCTION IF EXISTS gc_track_deleted_layers CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
