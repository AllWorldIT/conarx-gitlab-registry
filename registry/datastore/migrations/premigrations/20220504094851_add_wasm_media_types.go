package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220504094851_add_wasm_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.wasm.config.v1+json'), ('application/vnd.wasm.content.layer.v1+wasm') 
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.wasm.config.v1+json',
						'application/vnd.wasm.content.layer.v1+wasm'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
