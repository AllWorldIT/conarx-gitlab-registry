package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161719_create_gc_track_layer_blobs_trigger",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'gc_track_layer_blobs_trigger') THEN
						CREATE TRIGGER gc_track_layer_blobs_trigger
							AFTER INSERT ON layers
							FOR EACH ROW
							EXECUTE PROCEDURE gc_track_layer_blobs ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS gc_track_layer_blobs_trigger ON layers CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
