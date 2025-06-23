package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250623090017_add_application_spdx_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/spdx+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/spdx+json'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
