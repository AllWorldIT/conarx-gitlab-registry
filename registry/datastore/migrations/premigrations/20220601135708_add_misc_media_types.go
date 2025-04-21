package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220601135708_add_misc_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/json'),
						('binary/octet-stream'),
						('text/spdx'),
						('/application/vnd.acme.rocket.config'),
						('application/vnd.acme.rocket.config'),
						('application/vnd.vivsoft.enbuild.config.v1+json'),
						('application/vnd.gitlab.packages.npm.config.v2+json'),
						('application/vnd.spack.package'),
						('text/plain; charset=utf-8')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/json',
						'binary/octet-stream',
						'text/spdx',
						'/application/vnd.acme.rocket.config',
						'application/vnd.acme.rocket.config',
						'application/vnd.vivsoft.enbuild.config.v1+json',
						'application/vnd.gitlab.packages.npm.config.v2+json',
						'application/vnd.spack.package',
						'text/plain; charset=utf-8'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
