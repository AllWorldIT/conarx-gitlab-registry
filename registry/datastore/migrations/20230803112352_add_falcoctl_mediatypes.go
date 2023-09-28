package migrations

import migrate "github.com/rubenv/sql-migrate"

// gathered mediaTypes information from here: https://github.com/falcosecurity/falcoctl/blob/3c017edef87b27f563c02028dbd0228c67d03bda/pkg/oci/constants.go#L17
func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20230803112352_add_falcoctl_mediatypes",
			Up: []string{
				`INSERT INTO media_types (media_type)
					VALUES
						('application/vnd.cncf.falco.rulesfile.config.v1+json'),
						('application/vnd.cncf.falco.rulesfile.layer.v1+tar.gz'),
						('application/vnd.cncf.falco.plugin.config.v1+json'),
						('application/vnd.cncf.falco.plugin.layer.v1+tar.gz')
				EXCEPT
				SELECT
					media_type
				FROM
					media_types`,
			},
			Down: []string{
				`DELETE FROM media_types
					WHERE media_type IN (
						'application/vnd.cncf.falco.rulesfile.config.v1+json',
						'application/vnd.cncf.falco.rulesfile.layer.v1+tar.gz',
						'application/vnd.cncf.falco.plugin.config.v1+json',
						'application/vnd.cncf.falco.plugin.layer.v1+tar.gz'
					)`,
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
