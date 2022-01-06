package migrations

import migrate "github.com/rubenv/sql-migrate"

func init() {
	m := &Migration{
		Migration: &migrate.Migration{
			Id: "20220106102726_add_manifests_non_distributable_layers_column",
			Up: []string{
				"ALTER TABLE manifests ADD COLUMN IF NOT EXISTS non_distributable_layers BOOLEAN DEFAULT FALSE",
			},
			Down: []string{
				"ALTER TABLE manifests DROP COLUMN IF EXISTS non_distributable_layers",
			},
		},
		PostDeployment: false,
	}

	allMigrations = append(allMigrations, m)
}
