package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250819132100_add_opentofu_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES
						('application/vnd.opentofu.modulepkg'),
						('application/vnd.opentofu.provider'),
						('application/vnd.opentofu.provider-target')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.opentofu.modulepkg',
						'application/vnd.opentofu.provider',
						'application/vnd.opentofu.provider-target'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
