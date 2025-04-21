package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503162502_create_gc_track_deleted_tags_trigger",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'gc_track_deleted_tags_trigger') THEN
						CREATE TRIGGER gc_track_deleted_tags_trigger
							AFTER DELETE ON tags
							FOR EACH ROW
							EXECUTE PROCEDURE gc_track_deleted_tags ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS gc_track_deleted_tags_trigger ON tags CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
