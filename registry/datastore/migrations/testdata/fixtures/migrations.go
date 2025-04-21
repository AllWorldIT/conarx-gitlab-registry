//go:build integration

package migrationfixtures

import (
	"github.com/docker/distribution/registry/datastore/migrations"
)

var allMigrations []*migrations.Migration

func All() []*migrations.Migration {
	return allMigrations
}
