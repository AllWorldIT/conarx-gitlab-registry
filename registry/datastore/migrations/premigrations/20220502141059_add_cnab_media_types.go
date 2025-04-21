package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220502141059_add_cnab_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.cnab.config.v1+json'), ('application/vnd.cnab.bundle.config.v1+json') 
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cnab.config.v1+json',
						'application/vnd.cnab.bundle.config.v1+json'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
