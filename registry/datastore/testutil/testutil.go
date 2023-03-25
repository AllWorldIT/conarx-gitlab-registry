package testutil

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/registry/datastore"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// table represents a table in the test database.
type table string

// trigger represents a trigger in the test database.
type trigger struct {
	name  string
	table table
}

const (
	NamespacesTable            table = "top_level_namespaces"
	RepositoriesTable          table = "repositories"
	MediaTypesTable            table = "media_types"
	ManifestsTable             table = "manifests"
	ManifestReferencesTable    table = "manifest_references"
	BlobsTable                 table = "blobs"
	RepositoryBlobsTable       table = "repository_blobs"
	LayersTable                table = "layers"
	TagsTable                  table = "tags"
	GCBlobReviewQueueTable     table = "gc_blob_review_queue"
	GCBlobsConfigurationsTable table = "gc_blobs_configurations"
	GCBlobsLayersTable         table = "gc_blobs_layers"
	GCManifestReviewQueueTable table = "gc_manifest_review_queue"
	GCTmpBlobsManifestsTable   table = "gc_tmp_blobs_manifests"
	GCReviewAfterDefaultsTable table = "gc_review_after_defaults"
)

// AllTables represents all tables in the test database.
var (
	AllTables = []table{
		NamespacesTable,
		RepositoriesTable,
		ManifestsTable,
		ManifestReferencesTable,
		BlobsTable,
		RepositoryBlobsTable,
		LayersTable,
		TagsTable,
		GCBlobReviewQueueTable,
		GCBlobsConfigurationsTable,
		GCBlobsLayersTable,
		GCManifestReviewQueueTable,
		GCTmpBlobsManifestsTable,
	}

	GCTrackBlobUploadsTrigger = trigger{
		name:  "gc_track_blob_uploads_trigger",
		table: BlobsTable,
	}
	GCTrackConfigurationBlobsTrigger = trigger{
		name:  "gc_track_configuration_blobs_trigger",
		table: ManifestsTable,
	}
	GCTrackLayerBlobsTrigger = trigger{
		name:  "gc_track_layer_blobs_trigger",
		table: LayersTable,
	}
	GCTrackManifestUploadsTrigger = trigger{
		name:  "gc_track_manifest_uploads_trigger",
		table: ManifestsTable,
	}
	GCTrackDeletedManifestsTrigger = trigger{
		name:  "gc_track_deleted_manifests_trigger",
		table: ManifestsTable,
	}
	GCTrackDeletedLayersTrigger = trigger{
		name:  "gc_track_deleted_layers_trigger",
		table: LayersTable,
	}
	GCTrackDeletedManifestListsTrigger = trigger{
		name:  "gc_track_deleted_manifest_lists_trigger",
		table: ManifestReferencesTable,
	}
	GCTrackSwitchedTagsTrigger = trigger{
		name:  "gc_track_switched_tags_trigger",
		table: TagsTable,
	}
	GCTrackDeletedTagsTrigger = trigger{
		name:  "gc_track_deleted_tags_trigger",
		table: TagsTable,
	}
)

// truncate truncates t in the test database.
func (t table) truncate(db *datastore.DB) error {
	if _, err := db.Exec(fmt.Sprintf("TRUNCATE %s RESTART IDENTITY CASCADE", t)); err != nil {
		return fmt.Errorf("truncating table %q: %w", t, err)
	}
	return nil
}

// seedFileName generates the expected seed filename based on the convention `<table name>.sql`.
func (t table) seedFileName() string {
	return fmt.Sprintf("%s.sql", t)
}

// DumpAsJSON dumps the table contents in JSON format using the PostgresSQL `json_agg` function. `bytea` columns are
// automatically decoded for easy visualization/comparison. The output from each table is sorted for consistency. When
// sorting, we use the `(top_level_namespace_id, id)` columns, the `digest` column or the `id` column, in this order
// of preference for deterministic reasons.
func (t table) DumpAsJSON(ctx context.Context, db datastore.Queryer) ([]byte, error) {
	var tmpl string
	switch t {
	case ManifestsTable:
		tmpl = `SELECT
				json_agg(t)
			FROM (
				SELECT
					id,
					top_level_namespace_id,
					repository_id,
					created_at,
					total_size,
					schema_version,
					encode(digest, 'hex') AS digest,
					convert_from(payload, 'UTF8')::json AS payload,
					media_type_id,
					configuration_media_type_id,
					convert_from(configuration_payload, 'UTF8')::json AS configuration_payload,
					encode(configuration_blob_digest, 'hex') AS configuration_blob_digest,
					non_conformant,
					non_distributable_layers
				FROM
					%s
				ORDER BY
					(top_level_namespace_id, id)) t`
	case NamespacesTable:
		tmpl = "SELECT json_agg(t) FROM (SELECT * FROM %s ORDER BY id) t"
	case RepositoriesTable, ManifestReferencesTable, RepositoryBlobsTable, LayersTable, TagsTable,
		GCBlobsConfigurationsTable:
		tmpl = "SELECT json_agg(t) FROM (SELECT * FROM %s ORDER BY (top_level_namespace_id, id)) t"
	case GCManifestReviewQueueTable:
		tmpl = "SELECT json_agg(t) FROM (SELECT * FROM %s ORDER BY (top_level_namespace_id, repository_id)) t"
	case GCBlobsLayersTable:
		tmpl = "SELECT json_agg(t) FROM (SELECT * FROM %s ORDER BY id) t"
	default:
		tmpl = "SELECT json_agg(t) FROM (SELECT * FROM %s ORDER BY digest) t"
	}

	var dump []byte
	row := db.QueryRowContext(ctx, fmt.Sprintf(tmpl, t))
	if err := row.Scan(&dump); err != nil {
		return nil, err
	}

	return dump, nil
}

// Disable disables a trigger in the test database. Returns a function that can be deferred to re-enable the trigger.
func (t trigger) Disable(db *datastore.DB) (func() error, error) {
	_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", t.table, t.name))
	return func() error {
		_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ENABLE TRIGGER %s", t.table, t.name))
		return err
	}, err
}

// NewDSNFromEnv generates a new DSN for the test database based on environment variable configurations.
func NewDSNFromEnv() (*datastore.DSN, error) {
	port, err := strconv.Atoi(os.Getenv("REGISTRY_DATABASE_PORT"))
	if err != nil {
		return nil, fmt.Errorf("parsing DSN port: %w", err)
	}
	dsn := &datastore.DSN{
		Host:        os.Getenv("REGISTRY_DATABASE_HOST"),
		Port:        port,
		User:        os.Getenv("REGISTRY_DATABASE_USER"),
		Password:    os.Getenv("REGISTRY_DATABASE_PASSWORD"),
		DBName:      "registry_test",
		SSLMode:     os.Getenv("REGISTRY_DATABASE_SSLMODE"),
		SSLCert:     os.Getenv("REGISTRY_DATABASE_SSLCERT"),
		SSLKey:      os.Getenv("REGISTRY_DATABASE_SSLKEY"),
		SSLRootCert: os.Getenv("REGISTRY_DATABASE_SSLROOTCERT"),
	}

	return dsn, nil
}

// NewDSNFromConfig generates a new DSN for the test database based on configuration options.
func NewDSNFromConfig(config configuration.Database) (*datastore.DSN, error) {
	dsn := &datastore.DSN{
		Host:        config.Host,
		Port:        config.Port,
		User:        config.User,
		Password:    config.Password,
		DBName:      "registry_test",
		SSLMode:     config.SSLMode,
		SSLCert:     config.SSLCert,
		SSLKey:      config.SSLKey,
		SSLRootCert: config.SSLRootCert,
	}

	return dsn, nil
}

func newDB(dsn *datastore.DSN, logLevel logrus.Level, logOut io.Writer, poolConfig datastore.PoolConfig) (*datastore.DB, error) {
	log := logrus.New()
	log.SetLevel(logLevel)
	log.SetOutput(logOut)

	db, err := datastore.Open(dsn, datastore.WithLogger(logrus.NewEntry(log)))
	if err != nil {
		return nil, fmt.Errorf("opening database connection: %w", err)
	}
	db.SetMaxOpenConns(poolConfig.MaxOpen)

	return db, nil
}

// NewDBFromEnv generates a new datastore.DB and opens the underlying connection based on environment variable settings.
func NewDBFromEnv() (*datastore.DB, error) {
	dsn, err := NewDSNFromEnv()
	if err != nil {
		return nil, err
	}

	logLevel, err := logrus.ParseLevel(os.Getenv("REGISTRY_LOG_LEVEL"))
	if err != nil {
		logLevel = logrus.InfoLevel
	}

	var logOut io.Writer
	switch os.Getenv("REGISTRY_LOG_OUTPUT") {
	case "stdout":
		logOut = os.Stdout
	case "stderr":
		logOut = os.Stderr
	case "discard":
		logOut = io.Discard
	default:
		logOut = os.Stdout
	}

	poolConfig := datastore.PoolConfig{}
	tmp := os.Getenv("REGISTRY_DATABASE_POOL_MAXOPEN")
	if tmp != "" {
		poolMaxOpen, err := strconv.Atoi(tmp)
		if err != nil {
			return nil, fmt.Errorf("invalid REGISTRY_DATABASE_POOL_MAXOPEN: %w", err)
		}
		poolConfig.MaxOpen = poolMaxOpen
	}

	return newDB(dsn, logLevel, logOut, poolConfig)
}

// NewDBFromConfig generates a new datastore.DB and opens the underlying connection based on configuration settings.
func NewDBFromConfig(config *configuration.Configuration) (*datastore.DB, error) {
	dsn, err := NewDSNFromConfig(config.Database)
	if err != nil {
		return nil, err
	}

	logLevel, err := logrus.ParseLevel(config.Log.Level.String())
	if err != nil {
		logLevel = logrus.InfoLevel
	}

	var logOut io.Writer
	switch config.Log.Output {
	case configuration.LogOutputStdout:
		logOut = configuration.LogOutputStdout.Descriptor()
	case configuration.LogOutputStderr:
		logOut = configuration.LogOutputStderr.Descriptor()
	case configuration.LogOutputDiscard:
	default:
		logOut = configuration.LogOutputStdout.Descriptor()
	}

	poolConfig := datastore.PoolConfig{MaxOpen: config.Database.Pool.MaxOpen}

	return newDB(dsn, logLevel, logOut, poolConfig)
}

// TruncateTables truncates a set of tables in the test database.
func TruncateTables(db *datastore.DB, tables ...table) error {
	for _, table := range tables {
		if err := table.truncate(db); err != nil {
			return fmt.Errorf("truncating tables: %w", err)
		}
	}
	return nil
}

// TruncateAllTables truncates all tables in the test database.
func TruncateAllTables(db *datastore.DB) error {
	return TruncateTables(db, AllTables...)
}

// ReloadFixtures truncates all a given set of tables and then injects related fixtures.
func ReloadFixtures(tb testing.TB, db *datastore.DB, basePath string, tables ...table) {
	tb.Helper()

	require.NoError(tb, TruncateTables(db, tables...))

	for _, table := range tables {
		path := filepath.Join(basePath, "testdata", "fixtures", table.seedFileName())

		query, err := os.ReadFile(path)
		require.NoErrorf(tb, err, "error reading fixture")

		_, err = db.Exec(string(query))
		require.NoErrorf(tb, err, "error loading fixture")
	}
}

// ParseTimestamp parses a timestamp into a time.Time, matching a given location.
func ParseTimestamp(tb testing.TB, timestamp string, location *time.Location) time.Time {
	tb.Helper()

	t, err := time.Parse("2006-01-02 15:04:05.000000", timestamp)
	require.NoError(tb, err)

	return t.In(location)
}

func createGoldenFile(tb testing.TB, path string) {
	tb.Helper()

	if _, err := os.Stat(path); os.IsNotExist(err) {
		tb.Log("creating .golden file")

		f, err := os.Create(path)
		require.NoError(tb, err, "error creating .golden file")
		require.NoError(tb, f.Close())
	}
}

func updateGoldenFile(tb testing.TB, path string, content []byte) {
	tb.Helper()

	tb.Log("updating .golden file")
	err := os.WriteFile(path, content, 0644)
	require.NoError(tb, err, "error updating .golden file")
}

func readGoldenFile(tb testing.TB, path string) []byte {
	tb.Helper()

	content, err := os.ReadFile(path)
	require.NoError(tb, err, "error reading .golden file")

	return content
}

// CompareWithGoldenFile compares an actual value with the content of a .golden file. If requested, a missing golden
// file is automatically created and an outdated golden file automatically updated to match the actual content.
func CompareWithGoldenFile(tb testing.TB, path string, actual []byte, create, update bool) {
	tb.Helper()

	if create {
		createGoldenFile(tb, path)
	}
	if update {
		updateGoldenFile(tb, path, actual)
	}

	expected := readGoldenFile(tb, path)
	require.Equal(tb, string(expected), string(actual), "does not match .golden file")
}
