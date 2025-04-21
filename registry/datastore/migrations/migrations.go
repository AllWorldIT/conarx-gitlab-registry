package migrations

import (
	migrate "github.com/rubenv/sql-migrate"
)

var allMigrations []*Migration

var (
	allPostMigrations []*Migration
	allPreMigrations  []*Migration
)

func AppendPostMigration(migrations ...*Migration) []*Migration {
	for _, migration := range migrations {
		if len(migration.RequiredPostDeploy) > 0 {
			panic("post-deployment migrations should not explicitly depend on other post-deployment migrations via the `RequiredPostDeploy` field, as their dependencies are already implicitly satisfied by the order of the timestamps at which they are created.")
		}
	}
	allPostMigrations = append(allPostMigrations, migrations...)
	return allPostMigrations
}

func AppendPreMigration(migrations ...*Migration) []*Migration {
	for _, migration := range migrations {
		if len(migration.RequiredPreDeploy) > 0 {
			panic("pre-deployment migrations should not explicitly depend on other pre-deployment migrations via the `RequiredPreDeploy` field, as their dependencies are already implicitly satisfied by the order of the timestamps at which they are created.")
		}
	}
	allPreMigrations = append(allPreMigrations, migrations...)
	return allPreMigrations
}

func ResetPostMigrations() {
	allPostMigrations = make([]*Migration, 0)
}

func ResetPreMigrations() {
	allPreMigrations = make([]*Migration, 0)
}

func AllPostMigrations() []*Migration {
	return allPostMigrations
}

func AllPreMigrations() []*Migration {
	return allPreMigrations
}

type Migration struct {
	*migrate.Migration
	RequiredBBMs       []string
	RequiredPreDeploy  []string
	RequiredPostDeploy []string
}
