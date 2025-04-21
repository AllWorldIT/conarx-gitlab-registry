package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503162126_create_gc_track_deleted_layers_trigger",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'gc_track_deleted_layers_trigger') THEN
						CREATE TRIGGER gc_track_deleted_layers_trigger
							AFTER DELETE ON layers
							FOR EACH ROW
							EXECUTE PROCEDURE gc_track_deleted_layers ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS gc_track_deleted_layers_trigger ON layers CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
