//go:build integration

package migrationfixtures

import (
	"github.com/docker/distribution/registry/datastore/migrations"
)

var allMigrations []*migrations.Migration

func All() []*migrations.Migration {
	return allMigrations
}

func NonPostDeployment() []*migrations.Migration {
	migs := []*migrations.Migration{}
	for _, migration := range allMigrations {
		if !migration.PostDeployment {
			migs = append(migs, migration)
		}
	}

	return migs
}
