package migrations

import migrate "github.com/rubenv/sql-migrate"

// gathered mediaTypes information from here: https://github.com/opencontainers/image-spec/blob/v1.1.0-rc4/media-types.md
func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20240131005023_add_oci_spec_version_1_1_0_empty_media_type",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES
						('application/vnd.oci.empty.v1+json')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.oci.empty.v1+json'
					)`,
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
