package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220603122714_add_additional_misc_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('text/html; charset=utf-8')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type = 'text/html; charset=utf-8'`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
