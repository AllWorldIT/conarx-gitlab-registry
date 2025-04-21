package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20240308075335_add_timoni_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/vnd.timoni.config.v1+json'),
						('application/vnd.timoni.content.v1.tar+gzip')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.timoni.config.v1+json',
						'application/vnd.timoni.content.v1.tar+gzip'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
