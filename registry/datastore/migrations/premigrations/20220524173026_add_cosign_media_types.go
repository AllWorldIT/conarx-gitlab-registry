package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220524173026_add_cosign_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.dsse.envelope.v1+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type = 'application/vnd.dsse.envelope.v1+json'`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
