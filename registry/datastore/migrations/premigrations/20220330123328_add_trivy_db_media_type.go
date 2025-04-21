package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220330123328_add_trivy_db_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.aquasec.trivy.db.layer.v1.tar+gzip'), ('application/vnd.aquasec.trivy.config.v1+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.aquasec.trivy.db.layer.v1.tar+gzip',
						'application/vnd.aquasec.trivy.config.v1+json'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
