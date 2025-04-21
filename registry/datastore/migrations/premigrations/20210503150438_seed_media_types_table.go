package premigrations

import (
	"github.com/docker/distribution/registry/datastore/migrations"
	migrate "github.com/rubenv/sql-migrate"
)

func init() {
	m := &migrations.Migration{
		Migration: &migrate.Migration{
			Id: "20210503150438_seed_media_types_table",
			// We check if a given value already exists before attempting to insert to guarantee idempotence. This is not
			// done with an `ON CONFLICT DO NOTHING` statement to avoid bumping the media_types.id sequence, which is just
			// a smallint, so we would run out of integers if doing it repeatedly.
			Up: []string{
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.distribution.manifest.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.distribution.manifest.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.distribution.manifest.v1+prettyjws'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.distribution.manifest.v1+prettyjws'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.distribution.manifest.v2+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.distribution.manifest.v2+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.distribution.manifest.list.v2+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.distribution.manifest.list.v2+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.image.rootfs.diff.tar'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.image.rootfs.diff.tar'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.image.rootfs.diff.tar.gzip'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.image.rootfs.diff.tar.gzip'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.image.rootfs.foreign.diff.tar.gzip'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.image.rootfs.foreign.diff.tar.gzip'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.container.image.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.container.image.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.container.image.rootfs.diff+x-gtar'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.container.image.rootfs.diff+x-gtar'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.docker.plugin.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.docker.plugin.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.layer.v1.tar'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.layer.v1.tar'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.layer.v1.tar+gzip'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.layer.v1.tar+gzip'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.layer.v1.tar+zstd'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.layer.v1.tar+zstd'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.layer.nondistributable.v1.tar'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.layer.nondistributable.v1.tar'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.layer.nondistributable.v1.tar+gzip'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.layer.nondistributable.v1.tar+gzip'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.config.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.config.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.manifest.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.manifest.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.oci.image.index.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.oci.image.index.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/vnd.cncf.helm.config.v1+json'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/vnd.cncf.helm.config.v1+json'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/tar+gzip'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/tar+gzip'))`,
				`INSERT INTO media_types (media_type)
					SELECT
						'application/octet-stream'
					WHERE
						NOT EXISTS (
							SELECT
								1
							FROM
								media_types
							WHERE (media_type = 'application/octet-stream'))`,
			},
			Down: []string{
				// We have to delete each record instead of truncating to guarantee idempotence.
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.docker.distribution.manifest.v1+json',
						'application/vnd.docker.distribution.manifest.v1+prettyjws',
						'application/vnd.docker.distribution.manifest.v2+json',
						'application/vnd.docker.distribution.manifest.list.v2+json',
						'application/vnd.docker.image.rootfs.diff.tar',
						'application/vnd.docker.image.rootfs.diff.tar.gzip',
						'application/vnd.docker.image.rootfs.foreign.diff.tar.gzip',
						'application/vnd.docker.container.image.v1+json',
						'application/vnd.docker.container.image.rootfs.diff+x-gtar',
						'application/vnd.docker.plugin.v1+json',
						'application/vnd.oci.image.layer.v1.tar',
						'application/vnd.oci.image.layer.v1.tar+gzip',
						'application/vnd.oci.image.layer.v1.tar+zstd',
						'application/vnd.oci.image.layer.nondistributable.v1.tar',
						'application/vnd.oci.image.layer.nondistributable.v1.tar+gzip',
						'application/vnd.oci.image.config.v1+json',
						'application/vnd.oci.image.manifest.v1+json',
						'application/vnd.oci.image.index.v1+json',
						'application/vnd.cncf.helm.config.v1+json',
						'application/tar+gzip',
						'application/octet-stream'
					)`,
			},
		},
	}

	migrations.AppendPreMigration(m)
}
