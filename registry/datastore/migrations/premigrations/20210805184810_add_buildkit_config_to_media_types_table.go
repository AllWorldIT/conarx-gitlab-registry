package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210805184810_add_buildkit_config_to_media_types_table",
			// We check if a given value already exists before attempting to insert to guarantee idempotence. This is not
			// done with an `ON CONFLICT DO NOTHING` statement to avoid bumping the media_types.id sequence, which is just
			// a smallint, so we would run out of integers if doing it repeatedly.
			Up: []string{
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.buildkit.cacheconfig.v0'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.buildkit.cacheconfig.v0'))`,
			},
			Down: []string{
				`DELETE FROM media_types WHERE media_type = 'application/vnd.buildkit.cacheconfig.v0'`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
