package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220314173735_add_singularity_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.sylabs.sif.config.v1'), ('application/vnd.sylabs.sif.layer.v1.sif'), ('appliciation/vnd.sylabs.sif.layer.tar') 
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.sylabs.sif.config.v1',
						'application/vnd.sylabs.sif.layer.v1.sif',
						'appliciation/vnd.sylabs.sif.layer.tar'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
