package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220620111144_add_ansible_collection_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES 
						('application/vnd.ansible.collection')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type = 'application/vnd.ansible.collection'`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
