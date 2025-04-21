package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220518140452_add_helm_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.cncf.helm.chart.content.layer.v1+tar') 
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.helm.chart.content.layer.v1+tar'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
