package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20220503095645_add_opa_media_types",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES ('application/vnd.cncf.openpolicyagent.config.v1+json'), ('application/vnd.cncf.openpolicyagent.manifest.layer.v1+json'), ('application/vnd.cncf.openpolicyagent.policy.layer.v1+rego'), ('application/vnd.cncf.openpolicyagent.data.layer.v1+json') 
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.openpolicyagent.config.v1+json',
						'application/vnd.cncf.openpolicyagent.manifest.layer.v1+json',
						'application/vnd.cncf.openpolicyagent.policy.layer.v1+rego',
						'application/vnd.cncf.openpolicyagent.data.layer.v1+json'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
