package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503160732_seed_gc_review_after_defaults_table",
			Up: []string{
				`INSERT INTO gc_review_after_defaults (event, value)
					VALUES ('blob_upload', interval '1 day'),
						   ('manifest_upload', interval '1 day'),
						   ('manifest_delete', interval '1 day'),
						   ('layer_delete', interval '1 day'),
						   ('manifest_list_delete', interval '1 day'),
						   ('tag_delete', interval '1 day'),
						   ('tag_switch', interval '1 day')
					ON CONFLICT (event)
						DO NOTHING`,
			},
			Down: []string{
				// Delete each record instead of truncating to guarantee idempotence.
				`DELETE FROM gc_review_after_defaults
					WHERE event IN (
						'blob_upload',
						'manifest_upload',
						'manifest_delete',
						'layer_delete',
						'manifest_list_delete',
						'tag_delete',
						'tag_switch'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
