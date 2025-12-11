package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20250925151900_add_docker_compose_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES
						('application/vnd.docker.compose.project'),
						('application/vnd.docker.compose.file+yaml'),
						('application/vnd.docker.compose.config.empty.v1+json'),
						('application/vnd.docker.compose.envfile')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.docker.compose.project',
						'application/vnd.docker.compose.file+yaml',
						'application/vnd.docker.compose.config.empty.v1+json',
						'application/vnd.docker.compose.envfile'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
