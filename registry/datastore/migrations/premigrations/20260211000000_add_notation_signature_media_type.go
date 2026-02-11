package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20260211000000_add_notation_signature_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES
						('application/vnd.cncf.notary.signature')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.notary.signature'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
