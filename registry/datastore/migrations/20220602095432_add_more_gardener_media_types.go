package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20220602095432_add_gardener_landscaper_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.gardener.landscaper.componentdefinition.v1+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type = 'application/vnd.gardener.landscaper.componentdefinition.v1+json'`,
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
