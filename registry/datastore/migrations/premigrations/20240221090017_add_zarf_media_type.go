package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20240221090017_add_zarf_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/vnd.zarf.layer.v1.blob'),
						('application/vnd.zarf.config.v1+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.zarf.layer.v1.blob',
						'application/vnd.zarf.config.v1+json'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
