//go:build integration && cli_test

package registry_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/lib/pq"
	migrate "github.com/rubenv/sql-migrate"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry"
	"github.com/docker/distribution/registry/datastore/migrations"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gopkg.in/yaml.v2"
)

type migrateCmdTestSuite struct {
	suite.Suite
	configFilePath string
	pgCredentials  pgCredentials
	db             *sql.DB
}

func TestMigrateCmdTestSuite(t *testing.T) {
	suite.Run(t, &migrateCmdTestSuite{})
}

func (s *migrateCmdTestSuite) SetupSuite() {
	s.T().Log("Setting up suite...")

	s.T().Log("Setting up postgres")

	pgCreds := pgCredentials{
		DB:       "registry_dev",
		User:     "registry",
		Password: "registry_password",
	}
	pgc, err := createPostgresContainer(s.T(), context.Background(), pgCreds)
	require.NoError(s.T(), err)
	s.pgCredentials = pgCreds

	db, err := newDB(pgc.Host, pgc.Port, pgCreds.User, pgCreds.Password, pgCreds.DB)
	require.NoError(s.T(), err)
	s.db = db

	port, err := strconv.Atoi(pgc.Port)
	require.NoError(s.T(), err)
	configFilePath, err := generateDBConfig(s.T(), pgc.Host, port, pgCreds.User, pgCreds.Password, pgCreds.DB)
	require.NoError(s.T(), err)

	s.configFilePath = configFilePath

	s.T().Log("Finished suite setup!")
}

func (*migrateCmdTestSuite) SetupTest() {
	resetGlobalVars()
}

func (s *migrateCmdTestSuite) TearDownTest() {
	resetDBSchema(s.T(), s.db)
	resetGlobalVars()
}

// Helper functions for creating migrations with standard patterns

func createPreDeployMigration(id string, up, down []string, requiredPostDeploy []string) *migrations.Migration {
	return &migrations.Migration{
		RequiredPostDeploy: requiredPostDeploy,
		Migration: &migrate.Migration{
			Id:   id,
			Up:   up,
			Down: down,
		},
	}
}

func createPostDeployMigration(id string, up, down []string, requiredPreDeploy []string) *migrations.Migration {
	return &migrations.Migration{
		RequiredPreDeploy: requiredPreDeploy,
		Migration: &migrate.Migration{
			Id:   id,
			Up:   up,
			Down: down,
		},
	}
}

// Standard table creation migrations
func createStandardPreDeployMigration(id string, table string) *migrations.Migration {
	return createPreDeployMigration(
		id,
		[]string{fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY)", table)},
		[]string{fmt.Sprintf("DROP TABLE IF EXISTS %s", table)},
		nil,
	)
}

func createStandardPostDeployMigration(id, table string) *migrations.Migration {
	return createPostDeployMigration(
		id,
		[]string{fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY)", table)},
		[]string{fmt.Sprintf("DROP TABLE IF EXISTS %s", table)},
		nil,
	)
}

// Create a set of standard migrations for testing
func createStandardMigrationSet(numPre, numPost int) (preMigrations, postMigrations []*migrations.Migration) {
	for i := 1; i <= numPre; i++ {
		id := fmt.Sprintf("%d_create_test_table", i)
		preMigrations = append(preMigrations, createStandardPreDeployMigration(id, fmt.Sprintf("migrations_test_%d", i)))
	}

	for i := 1; i <= numPost; i++ {
		id := fmt.Sprintf("%d_create_test_post_deploy_table", i)
		postMigrations = append(postMigrations, createStandardPostDeployMigration(id, fmt.Sprintf("post_deploy_migrations_test_%d", i)))
	}

	return preMigrations, postMigrations
}

// Helper to convert ID to a table name
func tableNameFromID(id string) string {
	if id[0:1] == "0" {
		return "migrations_test_" + id
	}
	return "migrations_test_" + id
}

// Helper to run up migrations and verify results
func (s *migrateCmdTestSuite) runMigrateUp(preMigrations, postMigrations []*migrations.Migration, expectedPreIDs, expectedPostIDs []string) {
	// Initialize migrations
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	// Start capturing stdout
	checkStdoutContains := captureStdOut(s.T())

	// Run the command
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	// Check output and migrations applied
	checkStdoutContains(fmt.Sprintf("OK: applied %d pre-deployment migration(s), %d post-deployment migration(s) and 0 background migration(s)",
		len(expectedPreIDs), len(expectedPostIDs)))
	assertMigrationApplied(s.T(), s.db, expectedPreIDs, expectedPostIDs)
}

// Helper to run down migrations
func (s *migrateCmdTestSuite) runMigrateDown(preMigrations, postMigrations []*migrations.Migration, expectedPreIDs, expectedPostIDs []string) {
	// Initialize migrations
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	// Start capturing stdout
	checkStdoutContains := captureStdOut(s.T())

	// Set force to bypass prompt
	registry.Force = true

	// Run the command
	err := registry.MigrateDownCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	// Check output and migrations applied
	if len(expectedPreIDs) > 0 {
		checkStdoutContains(fmt.Sprintf("OK: applied %d pre-deployment migration(s)", len(expectedPreIDs)))
	}
	if len(expectedPostIDs) > 0 {
		checkStdoutContains(fmt.Sprintf("OK: applied %d post-deployment migration(s)", len(expectedPostIDs)))
	}
	assertMigrationApplied(s.T(), s.db, expectedPreIDs, expectedPostIDs)
}

// Tests begin here

func (s *migrateCmdTestSuite) Test_Up_Migrate_No_PreDeploy_or_PostDeploy() {
	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 0 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_No_PreDeploy_or_PostDeploy_SkipPostDeploy() {
	checkStdoutContains := captureStdOut(s.T())
	registry.SkipPostDeployment = true
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 0 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Independent_PreDeploy_No_PostDeploy() {
	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")
	migrations.AppendPreMigration(preDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Independent_PreDeploy_No_PostDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")
	migrations.AppendPreMigration(preDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Independent_PostDeploy_No_PreDeploy() {
	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 0 pre-deployment migration(s), 1 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{}, []string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Independent_PostDeploy_No_PreDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 0 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Independent_PreDeploy_And_Independent_PostDeploy() {
	preMigrations, postMigrations := createStandardMigrationSet(1, 1)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table"},
		[]string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Independent_PreDeploy_And_Independent_PostDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preMigrations, postMigrations := createStandardMigrationSet(1, 1)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table"},
		[]string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Dependent_PreDeploy_on_PostDeploy() {
	preDeployMig := createPreDeployMigration(
		"1_create_test_table",
		[]string{"CREATE TABLE migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE migrations_test IF EXISTS"},
		[]string{"1_create_test_post_deploy_table"},
	)

	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 1 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Dependent_PreDeploy_on_PostDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preDeployMig := createPreDeployMigration(
		"1_create_test_table",
		[]string{"CREATE TABLE migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE migrations_test IF EXISTS"},
		[]string{"1_create_test_post_deploy_table"},
	)

	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "required post-deploy schema migration 1_create_test_post_deploy_table not yet applied and can not be skipped")

	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Dependent_PostDeploy_on_PreDeploy() {
	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")
	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table",
		[]string{"CREATE TABLE post_deploy_migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test IF EXISTS"},
		[]string{"1_create_test_table"},
	)

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 1 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Dependent_PostDeploy_on_PreDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")
	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table",
		[]string{"CREATE TABLE post_deploy_migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test IF EXISTS"},
		[]string{"1_create_test_table"},
	)

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_PostDeploy_Dependency_DoesNotExist() {
	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")
	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table",
		[]string{"CREATE TABLE post_deploy_migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test IF EXISTS"},
		[]string{"2_create_test_table"}, // Dependency that doesn't exist
	)

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "pre-deploy migration 2_create_test_table, does not exist in migration source")

	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_PostDeploy_Dependency_DoesNotExist_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")
	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table",
		[]string{"CREATE TABLE post_deploy_migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test IF EXISTS"},
		[]string{"2_create_test_table"}, // Dependency that doesn't exist
	)

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_PreDeploy_Dependency_DoesNotExist() {
	preDeployMig := createPreDeployMigration(
		"1_create_test_table",
		[]string{"CREATE TABLE migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE migrations_test IF EXISTS"},
		[]string{"2_create_test_post_deploy_table"}, // Dependency that doesn't exist
	)

	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "post-deploy migration 2_create_test_post_deploy_table does not exist in migration source")

	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_2_PreDeploy_1_Dependent_on_PostDeploy() {
	preDeployMig1 := createStandardPreDeployMigration("1_create_test_table", "migrations_test")

	preDeployMig2 := createPreDeployMigration(
		"2_add_record_to_post_deploy_test_table",
		[]string{"INSERT INTO post_deploy_migrations_test (id) VALUES (1);"},
		[]string{"DELETE FROM post_deploy_migrations_test WHERE id = 1;"},
		[]string{"1_create_test_post_deploy_table"},
	)

	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	migrations.AppendPreMigration(preDeployMig1, preDeployMig2)
	migrations.AppendPostMigration(postDeployMig)

	// start capturing the output to stdout
	checkStdoutContains := captureStdOut(s.T())
	// call the command
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	// verify the output is as expected
	checkStdoutContains("OK: applied 2 pre-deployment migration(s), 1 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table", "2_add_record_to_post_deploy_test_table"}, []string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_2_PreDeploy_1_Dependent_on_PostDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preDeployMig1 := createStandardPreDeployMigration("1_create_test_table", "migrations_test")

	preDeployMig2 := createPreDeployMigration(
		"2_add_record_to_post_deploy_test_table",
		[]string{"INSERT INTO post_deploy_migrations_test (id) VALUES (1);"},
		[]string{"DELETE FROM post_deploy_migrations_test WHERE id = 1;"},
		[]string{"1_create_test_post_deploy_table"},
	)

	postDeployMig := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	migrations.AppendPreMigration(preDeployMig1, preDeployMig2)
	migrations.AppendPostMigration(postDeployMig)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "required post-deploy schema migration 1_create_test_post_deploy_table not yet applied and can not be skipped")

	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_2_PostDeploy_1_Dependent_on_PreDeploy() {
	postDeployMig1 := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	postDeployMig2 := createPostDeployMigration(
		"2_add_record_to_test_table",
		[]string{"INSERT INTO migrations_test (id) VALUES (1);"},
		[]string{"DELETE FROM migrations_test WHERE id = 1;"},
		nil, // No pre-deploy dependencies
	)

	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig1, postDeployMig2)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 2 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{"1_create_test_post_deploy_table", "2_add_record_to_test_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_2_PostDeploy_1_Dependent_on_PreDeploy_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	postDeployMig1 := createStandardPostDeployMigration("1_create_test_post_deploy_table", "post_deploy_migrations_test")

	postDeployMig2 := createPostDeployMigration(
		"2_add_record_to_test_table",
		[]string{"INSERT INTO migrations_test (id) VALUES (1);"},
		[]string{"DELETE FROM migrations_test WHERE id = 1;"},
		[]string{"1_create_test_table"},
	)

	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig1, postDeployMig2)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_2_PreDeploy_1_Illogically_Depends_on_PostDeploy() {
	preDeployMig1 := createStandardPreDeployMigration("1_create_test_table", "migrations_table")

	preDeployMig2 := createPreDeployMigration(
		"2_add_record_to_post_deploy_test_table",
		[]string{"INSERT INTO post_deploy_migrations_test (id) VALUES (1);"},
		[]string{"DELETE FROM post_deploy_migrations_test WHERE id = 1;"},
		[]string{"1_create_test_post_deploy_table_2"},
	)

	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table_2",
		[]string{"CREATE TABLE post_deploy_migrations_test_2 (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test_2 IF EXISTS"},
		nil,
	)

	migrations.AppendPreMigration(preDeployMig1, preDeployMig2)
	migrations.AppendPostMigration(postDeployMig)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, `"post_deploy_migrations_test" does not exist`)

	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{"1_create_test_post_deploy_table_2"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_2_PostDeploy_1_Illogically_Depends_on_PreDeploy() {
	postDeployMig1 := createPostDeployMigration(
		"1_create_test_post_deploy_table_2",
		[]string{"CREATE TABLE post_deploy_migrations_test_2 (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test_2 IF EXISTS"},
		nil,
	)

	postDeployMig2 := createPostDeployMigration(
		"2_add_record_to_test_table",
		[]string{"INSERT INTO migrations_test_1 (id) VALUES (1);"},
		[]string{"DELETE FROM migrations_test_1 WHERE id = 1;"},
		[]string{"1_create_test_table"},
	)

	preDeployMig := createStandardPreDeployMigration("1_create_test_table", "migrations_test")

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig1, postDeployMig2)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, `"migrations_test_1" does not exist`)

	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{"1_create_test_post_deploy_table_2"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Ordered_Cyclical_Dependency() {
	preDeployMig := createPreDeployMigration(
		"1_create_test_table",
		[]string{"CREATE TABLE migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE migrations_test IF EXISTS"},
		[]string{"1_create_test_post_deploy_table"},
	)

	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table",
		[]string{"CREATE TABLE post_deploy_migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test IF EXISTS"},
		[]string{"1_create_test_table"},
	)

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 1 pre-deployment migration(s), 1 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{"1_create_test_table"}, []string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Up_Migrate_Ordered_Cyclical_Dependency_SkipPostDeploy() {
	registry.SkipPostDeployment = true
	preDeployMig := createPreDeployMigration(
		"1_create_test_table",
		[]string{"CREATE TABLE migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE migrations_test IF EXISTS"},
		[]string{"1_create_test_post_deploy_table"},
	)

	postDeployMig := createPostDeployMigration(
		"1_create_test_post_deploy_table",
		[]string{"CREATE TABLE post_deploy_migrations_test (id BIGINT PRIMARY KEY)"},
		[]string{"DROP TABLE post_deploy_migrations_test IF EXISTS"},
		[]string{"1_create_test_table"},
	)

	migrations.AppendPreMigration(preDeployMig)
	migrations.AppendPostMigration(postDeployMig)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "required post-deploy schema migration 1_create_test_post_deploy_table not yet applied and can not be skipped")
	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_UpN_Migrate_2Independent_PreDeploy_And_3Independent_PostDeploy() {
	var (
		maxPre  = 2
		maxPost = 3
	)
	registry.MaxNumPostMigrations = &maxPost
	registry.MaxNumPreMigrations = &maxPre

	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 2 pre-deployment migration(s), 3 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db,
		[]string{"1_create_test_table", "2_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_UpN_Migrate_AllIndependent_PreDeploy_And_AllIndependent_PostDeploy() {
	var (
		maxPre  = 3
		maxPost = 3
	)
	registry.MaxNumPostMigrations = &maxPost
	registry.MaxNumPreMigrations = &maxPre

	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 3 pre-deployment migration(s), 3 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_UpN_Migrate_0() {
	var (
		maxPre  = 0
		maxPost = 1
	)
	registry.MaxNumPostMigrations = &maxPost
	registry.MaxNumPreMigrations = &maxPre

	preMigrations, postMigrations := createStandardMigrationSet(1, 1)
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.Error(s.T(), err)
	require.ErrorContains(s.T(), err, "both pre and post migration limits must be greater than or equal to 1")
}

func (s *migrateCmdTestSuite) Test_UpN_Migrate_2PostDeploy_Avoid_PreDeploy() {
	maxPost := 2
	registry.MaxNumPostMigrations = &maxPost

	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 0 pre-deployment migration(s), 2 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_UpN_Migrate_2PreDeploy_Avoid_PostDeploy() {
	maxPre := 2
	registry.MaxNumPreMigrations = &maxPre

	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateUpCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 2 pre-deployment migration(s), 0 post-deployment migration(s) and 0 background migration(s)")
	assertMigrationApplied(s.T(), s.db,
		[]string{"1_create_test_table", "2_create_test_table"}, []string{})
}

func (s *migrateCmdTestSuite) Test_Down_Migrate_NoExisitingMigration() {
	preMigrations, postMigrations := createStandardMigrationSet(1, 1)
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	err := registry.MigrateDownCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)
}

func (s *migrateCmdTestSuite) Test_Down_Migrate_PreDeploy_And_PostDeploy() {
	// Setup - apply migrations first
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	// Reset and prepare for down migration
	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)
	registry.Force = true // bypass interactive prompt

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateDownCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 3 post-deployment migration(s)", "OK: applied 3 pre-deployment migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{}, []string{})
}

func (s *migrateCmdTestSuite) Test_Down_Migrate_PreDeploy_And_PostDeploy_SkipPostDeploy() {
	// Setup - apply migrations first
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	// Reset and prepare for down migration
	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)
	registry.Force = true // bypass interactive prompt
	registry.SkipPostDeployment = true

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateDownCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 3 pre-deployment migration(s)")
	assertMigrationApplied(s.T(), s.db, []string{},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Down_Migrate_PreDeploy2_And_PostDeploy1() {
	// Setup - apply migrations first
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	// Reset and prepare for down migration
	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)
	registry.Force = true // bypass interactive prompt

	var (
		maxPre  = 1
		maxPost = 2
	)
	registry.MaxNumPostMigrations = &maxPost
	registry.MaxNumPreMigrations = &maxPre

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateDownCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("OK: applied 1 pre-deployment migration(s)", "OK: applied 2 post-deployment migration(s)")
	assertMigrationApplied(s.T(), s.db,
		[]string{"1_create_test_table", "2_create_test_table"},
		[]string{"1_create_test_post_deploy_table"})
}

func (s *migrateCmdTestSuite) Test_Status_Migrate_PreDeploy_And_PostDeploy() {
	// Setup - apply migrations first
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	// Reset and check status
	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateStatusCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("pre-deployment", "1_create_test_table", "2_create_test_table", "3_create_test_table",
		"post-deployment", "1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table")
}

func (s *migrateCmdTestSuite) Test_Status_Migrate_PreDeploy_And_PostDeploy_SkipPostDeploy() {
	// Setup - apply migrations first
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	// Reset and check status with SkipPostDeployment
	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)
	registry.SkipPostDeployment = true

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateStatusCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("pre-deployment", "1_create_test_table", "2_create_test_table", "3_create_test_table")
}

func (s *migrateCmdTestSuite) Test_Version_Migrate_PreDeploy_And_PostDeploy() {
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateVersionCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("pre-deployment:3_create_test_table", "post-deployment:3_create_test_post_deploy_table")
}

func (s *migrateCmdTestSuite) Test_Version_Migrate_PreDeploy_And_PostDeploy_SkipPostDeploy() {
	preMigrations, postMigrations := createStandardMigrationSet(3, 3)
	s.runMigrateUp(preMigrations, postMigrations,
		[]string{"1_create_test_table", "2_create_test_table", "3_create_test_table"},
		[]string{"1_create_test_post_deploy_table", "2_create_test_post_deploy_table", "3_create_test_post_deploy_table"})

	resetGlobalVars()
	migrations.AppendPreMigration(preMigrations...)
	migrations.AppendPostMigration(postMigrations...)
	registry.SkipPostDeployment = true

	checkStdoutContains := captureStdOut(s.T())
	err := registry.MigrateVersionCmd.RunE(nil, []string{s.configFilePath})
	require.NoError(s.T(), err)

	checkStdoutContains("pre-deployment:3_create_test_table")
}

func captureStdOut(t *testing.T) func(...string) {
	// Create a pipe to capture the output
	originalStdout := os.Stdout // Save original stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	return func(contains ...string) {
		err := w.Close()
		require.NoError(t, err)
		os.Stdout = originalStdout

		var buf bytes.Buffer
		_, err = buf.ReadFrom(r)
		require.NoError(t, err)

		s := buf.String()
		log.Print(s)
		for _, contain := range contains {
			require.Contains(t, s, contain)
		}
	}
}

func createPostgresContainer(_ *testing.T, ctx context.Context, cfg pgCredentials) (*postgresContainer, error) {
	pgCurrVersion := os.Getenv("PG_CURR_VERSION")
	if pgCurrVersion == "" {
		pgCurrVersion = "16"
	}
	pgContainer, err := postgres.Run(ctx, "postgres:"+pgCurrVersion+"-alpine",
		postgres.WithDatabase(cfg.DB),
		postgres.WithUsername(cfg.User),
		postgres.WithPassword(cfg.Password),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(15*time.Second)),
	)
	if err != nil {
		return nil, err
	}
	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, err
	}

	containerIP, err := pgContainer.ContainerIP(ctx)
	if err != nil {
		return nil, err
	}

	ip, err := pgContainer.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := pgContainer.MappedPort(ctx, "5432")
	if err != nil {
		return nil, err
	}

	return &postgresContainer{
		PostgresContainer: pgContainer,
		ConnectionString:  connStr,
		Host:              ip,
		Port:              port.Port(),
		ContainerIP:       containerIP,
	}, nil
}

type postgresContainer struct {
	*postgres.PostgresContainer
	ConnectionString string
	Host             string
	Port             string
	ContainerIP      string
}

type pgCredentials struct {
	DB       string
	User     string
	Password string
}

// generateDBConfig generates the YAML file with provided database parameters
func generateDBConfig(t *testing.T, dbHost string, dbPort int, dbUser, dbPassword, dbName string) (string, error) {
	// Define the configuration with default values and input database parameters
	config := configuration.Configuration{
		Version: "0.1",
		Database: configuration.Database{
			Enabled:  true,
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			DBName:   dbName,
			SSLMode:  "disable",
		},
	}

	// Create a YAML file in the temp directory
	yamlFilePath := t.TempDir() + "/config.yaml"
	yamlData, err := yaml.Marshal(&config)
	if err != nil {
		return "", err
	}

	err = os.WriteFile(yamlFilePath, yamlData, 0o644)
	if err != nil {
		return "", err
	}

	// Return the path to the temp directory containing the YAML file
	return yamlFilePath, nil
}

func resetGlobalVars() {
	migrations.ResetPostMigrations()
	migrations.ResetPreMigrations()
	migrations.StatusCache = nil
	registry.MaxNumPreMigrations = nil
	registry.MaxNumPostMigrations = nil
	registry.SkipPostDeployment = false
	registry.Force = false
}

func resetDBSchema(t *testing.T, db *sql.DB) {
	tx, err := db.Begin()
	require.NoError(t, err, "Failed to begin transaction")

	sqlCommands := []string{
		"DROP SCHEMA public CASCADE;",
		"CREATE SCHEMA public;",
		"GRANT ALL ON SCHEMA public TO registry;",
		"GRANT ALL ON SCHEMA public TO public;",
		"COMMENT ON SCHEMA public IS 'standard public schema';",
	}

	for _, cmd := range sqlCommands {
		_, err := tx.Exec(cmd)
		require.NoError(t, err, "Failed to execute: %s", cmd)
	}

	err = tx.Commit()
	require.NoError(t, err, "Failed to commit transaction")

	t.Log("Database schema restored successfully!")
}

func assertMigrationApplied(t *testing.T, db *sql.DB, preMigrations, postMigrations []string) {
	migrationsApplied := map[string][]string{
		migrations.PreDeployMigrationTableName:  {},
		migrations.PostDeployMigrationTableName: {},
	}

	for tableName := range migrationsApplied {
		query := fmt.Sprintf("SELECT id FROM %s ORDER BY id ASC", tableName)
		rows, err := db.Query(query)
		if err != nil {
			// Check if the error is specifically due to a missing table
			// 42P01 is the error code for "undefined_table"
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "42P01" {
				continue
			}
			require.NoError(t, err)
		}
		defer rows.Close()

		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				require.NoError(t, err)
				continue
			}
			migrationsApplied[tableName] = append(migrationsApplied[tableName], id)
		}
	}

	require.Equal(t, preMigrations, migrationsApplied[migrations.PreDeployMigrationTableName], "Pre deployment migration(s) do not match expected")
	require.Equal(t, postMigrations, migrationsApplied[migrations.PostDeployMigrationTableName], "Post deployment migration(s) do not match expected")
}

func newDB(dbHost, dbPort, dbUser, dbPassword, dbName string) (*sql.DB, error) {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		dbHost, dbPort, dbUser, dbPassword, dbName)

	sqldb, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	err = sqldb.Ping()
	if err != nil {
		return nil, err
	}
	return sqldb, nil
}
