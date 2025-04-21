package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220502140611_add_artifacthub_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.cncf.artifacthub.config.v1+yaml'), ('application/vnd.cncf.artifacthub.repository-metadata.layer.v1.yaml')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.artifacthub.config.v1+yaml',
						'application/vnd.cncf.artifacthub.repository-metadata.layer.v1.yaml'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
