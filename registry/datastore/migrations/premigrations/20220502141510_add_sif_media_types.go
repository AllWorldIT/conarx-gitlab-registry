package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220502141510_add_sif_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.sylabs.sif.config.v1+json'), ('application/vnd.sylabs.sif.layer.v1.sif') 
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.sylabs.sif.config.v1+json',
						'application/vnd.sylabs.sif.layer.v1.sif'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
