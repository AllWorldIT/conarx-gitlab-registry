package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20211206152649_update_helm_charts_media_types",
			// We check if a given value already exists before attempting to insert to guarantee idempotence. This is not
			// done with an `ON CONFLICT DO NOTHING` statement to avoid bumping the media_types.id sequence, which is just
			// a smallint, so we would run out of integers if doing it repeatedly.
			Up: []string{
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.cncf.helm.chart.content.v1.tar+gzip'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.cncf.helm.chart.content.v1.tar+gzip'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.cncf.helm.chart.provenance.v1.prov'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.cncf.helm.chart.provenance.v1.prov'))`,
			},
			Down: []string{
				// We have to delete each record instead of truncating to guarantee idempotence.
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.helm.chart.provenance.v1.prov',
						'application/vnd.cncf.helm.chart.content.v1.tar+gzip'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
