package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20220503095033_add_cosign_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.dev.cosign.simplesigning.v1+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type = 'application/vnd.dev.cosign.simplesigning.v1+json'`,
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
