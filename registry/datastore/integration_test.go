//go:build integration

package datastore_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/docker/distribution/registry/datastore/migrations/postmigrations"
	"github.com/docker/distribution/registry/datastore/migrations/premigrations"
	"github.com/docker/distribution/registry/datastore/testutil"
)

type testSuite struct {
	db           *datastore.DB
	basePath     string
	fixturesPath string
	goldenPath   string
	ctx          context.Context
}

var (
	suite *testSuite

	// flags for the `go test` tool, e.g. `go test -update ...`
	create = flag.Bool("create", false, "create missing .golden files")
	update = flag.Bool("update", false, "update .golden files")
)

func (s *testSuite) setup() error {
	db, err := testutil.NewDBFromEnv()
	if err != nil {
		return err
	}

	var m []migrations.PureMigrator
	m = append(m, premigrations.NewMigrator(db), postmigrations.NewMigrator(db))
	for _, mig := range m {
		if _, err := mig.Up(); err != nil {
			return err
		}
	}
	basePath, err := os.Getwd()
	if err != nil {
		return err
	}

	s.db = db
	s.basePath = basePath
	s.fixturesPath = path.Join(s.basePath, "testdata", "fixtures")
	s.goldenPath = path.Join(s.basePath, "testdata", "golden")
	s.ctx = context.Background()

	return nil
}

func (s *testSuite) teardown() error {
	if err := testutil.TruncateAllTables(s.db); err != nil {
		return err
	}
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("closing database: %w", err)
	}

	return nil
}

func TestMain(m *testing.M) {
	suite = &testSuite{}

	if err := suite.setup(); err != nil {
		panic(fmt.Errorf("setup: %w", err))
	}
	code := m.Run()
	if err := suite.teardown(); err != nil {
		panic(fmt.Errorf("teardown: %w", err))
	}

	os.Exit(code)
}
