package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503162231_create_gc_track_deleted_manifest_lists_trigger",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'gc_track_deleted_manifest_lists_trigger') THEN
						CREATE TRIGGER gc_track_deleted_manifest_lists_trigger
							AFTER DELETE ON manifest_references
							FOR EACH ROW
							EXECUTE PROCEDURE gc_track_deleted_manifest_lists ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS gc_track_deleted_manifest_lists_trigger ON manifest_references CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
