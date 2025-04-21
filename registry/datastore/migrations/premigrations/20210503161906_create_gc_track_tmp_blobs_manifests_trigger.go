package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503161906_create_gc_track_tmp_blobs_manifests_trigger",
			Up: []string{
				`DO $$
				BEGIN
					IF NOT EXISTS (
						SELECT
							1
						FROM
							pg_trigger
						WHERE
							tgname = 'gc_track_tmp_blobs_manifests_trigger') THEN
						CREATE TRIGGER gc_track_tmp_blobs_manifests_trigger
							AFTER INSERT ON manifests
							FOR EACH ROW
							EXECUTE PROCEDURE gc_track_tmp_blobs_manifests ();
					END IF;
				END
				$$`,
			},
			Down: []string{
				"DROP TRIGGER IF EXISTS gc_track_tmp_blobs_manifests_trigger ON layers CASCADE",
			},
		},
	}

	migrations.AppendPreMigration(m)
}
