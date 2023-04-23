package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230413163905_add_flux_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/vnd.cncf.flux.config.v1+json'),
						('application/vnd.cncf.flux.content.v1.tar+gzip')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.flux.config.v1+json',
						'application/vnd.cncf.flux.content.v1.tar+gzip'
					)`,
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
