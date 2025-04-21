package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220603111337_add_more_misc_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/x-yaml'),
						('application/sap-cnudie+tar')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/x-yaml',
						'application/sap-cnudie+tar'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
