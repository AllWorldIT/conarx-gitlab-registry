package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20230724115424_add_cosign_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/vnd.dev.cosign.artifact.sig.v1+json'),
						('application/vnd.dev.cosign.artifact.sbom.v1+json'),
						('application/vnd.cyclonedx'),
						('application/vnd.cyclonedx+xml'),
						('application/vnd.cyclonedx+json'),
						('text/spdx+xml'),
						('text/spdx+json'),
						('application/vnd.syft+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.dev.cosign.artifact.sig.v1+json',
						'application/vnd.dev.cosign.artifact.sbom.v1+json',
						'application/vnd.cyclonedx',
						'application/vnd.cyclonedx+xml',
						'application/vnd.cyclonedx+json',
						'text/spdx+xml',
						'text/spdx+json',
						'application/vnd.syft+json'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
