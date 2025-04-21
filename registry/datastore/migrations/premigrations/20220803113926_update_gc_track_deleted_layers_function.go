package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220803113926_update_gc_track_deleted_layers_function",
			Up: []string{
				// updates function definition to support both row and statement-level trigger calls
				`CREATE OR REPLACE FUNCTION gc_track_deleted_layers ()
					RETURNS TRIGGER
					AS $$
				BEGIN
					IF (TG_LEVEL = 'STATEMENT') THEN
						INSERT INTO gc_blob_review_queue (digest, review_after, event)
						SELECT
							deleted_rows.digest,
							gc_review_after ('layer_delete'),
							'layer_delete'
						FROM
							old_table deleted_rows
						ORDER BY
							deleted_rows.digest ASC
						ON CONFLICT (digest)
							DO UPDATE SET
								review_after = gc_review_after ('layer_delete'),
								event = 'layer_delete';
					ELSIF (TG_LEVEL = 'ROW') THEN
						INSERT INTO gc_blob_review_queue (digest, review_after, event)
							VALUES (OLD.digest, gc_review_after ('layer_delete'), 'layer_delete')
						ON CONFLICT (digest)
							DO UPDATE SET
								review_after = gc_review_after ('layer_delete'), event = 'layer_delete';
					END IF;
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
		},
	}

	migrations.AppendPreMigration(m)
}
