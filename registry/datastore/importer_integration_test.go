//go:build integration

package datastore_test

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/docker/distribution/registry/storage"
	storageDriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/filesystem"
	"github.com/docker/libtrust"
	"github.com/stretchr/testify/require"
)

func newFilesystemStorageDriver(tb testing.TB) *filesystem.Driver {
	tb.Helper()

	return newFilesystemStorageDriverWithRoot(tb, "happy-path")
}

func newFilesystemStorageDriverWithRoot(tb testing.TB, root string) *filesystem.Driver {
	tb.Helper()

	driver, err := filesystem.FromParameters(map[string]interface{}{
		"rootdirectory": path.Join(suite.fixturesPath, "importer", root),
	})
	require.NoError(tb, err, "error creating storage driver")

	return driver
}

func newRegistry(tb testing.TB, driver storageDriver.StorageDriver) distribution.Namespace {
	tb.Helper()

	// load custom key to be used for manifest signing, ensuring that we have reproducible signatures
	pemKey, err := os.ReadFile(path.Join(suite.fixturesPath, "keys", "manifest_sign"))
	require.NoError(tb, err)
	block, _ := pem.Decode([]byte(pemKey))
	require.NotNil(tb, block)
	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	require.NoError(tb, err)
	k, err := libtrust.FromCryptoPrivateKey(privateKey)
	require.NoErrorf(tb, err, "error loading signature key")

	registry, err := storage.NewRegistry(suite.ctx, driver, storage.Schema1SigningKey(k), storage.EnableSchema1)
	require.NoError(tb, err, "error creating registry")

	return registry
}

// overrideDynamicData is required to override all attributes that change with every test run. This is needed to ensure
// that we can consistently compare the output of a database dump with the stored reference snapshots (.golden files).
// For example, all entities have a `created_at` attribute that changes with every test run, therefore, when we dump
// the database we have to override this attribute value so that it matches the one in the .golden files.
func overrideDynamicData(tb testing.TB, actual []byte) []byte {
	tb.Helper()

	// the created_at timestamps for all entities change with every test run
	re := regexp.MustCompile(`"created_at":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d*\+\d{2}:\d{2}"`)
	actual = re.ReplaceAllLiteral(actual, []byte(`"created_at":"2020-04-15T12:04:28.95584"`))

	// the review_after timestamps for the GC review queue entities change with every test run
	re = regexp.MustCompile(`"review_after":"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d*\+\d{2}:\d{2}"`)
	actual = re.ReplaceAllLiteral(actual, []byte(`"review_after":"2020-04-16T12:04:28.95584"`))

	// schema 1 manifests have `signature` and `protected` attributes that changes with every test run
	re = regexp.MustCompile(`"signature": ".*"`)
	actual = re.ReplaceAllLiteral(actual, []byte(`"signature": "lBzn6_e7f0mdqQXKhkRMdI"`))
	re = regexp.MustCompile(`"protected": ".*"`)
	actual = re.ReplaceAllLiteral(actual, []byte(`"protected": "eyJmb3JtYXRMZW5ndGgiOj"`))

	return actual
}

func newImporter(t *testing.T, db *datastore.DB, opts ...datastore.ImporterOption) *datastore.Importer {
	t.Helper()

	return newImporterWithRoot(t, db, "happy-path", opts...)
}

func newImporterWithRoot(t *testing.T, db *datastore.DB, root string, opts ...datastore.ImporterOption) *datastore.Importer {
	t.Helper()

	driver := newFilesystemStorageDriverWithRoot(t, root)
	registry := newRegistry(t, driver)

	imp := datastore.NewImporter(db, registry, opts...)

	// we need to ensure the lockfiles are returned to their original state
	t.Cleanup(restoreLockfiles(t, imp))

	return imp
}

func newTempDirDriver(t *testing.T) *filesystem.Driver {
	rootDir := t.TempDir()

	d, err := filesystem.FromParameters(map[string]interface{}{
		"rootdirectory": rootDir,
	})
	require.NoError(t, err)

	return d
}

// Dump each table as JSON and compare the output against reference snapshots (.golden files)
func validateImport(t *testing.T, db *datastore.DB) {
	t.Helper()

	for _, tt := range testutil.AllTables {
		t.Run(string(tt), func(t *testing.T) {
			actual, err := tt.DumpAsJSON(suite.ctx, db)
			require.NoError(t, err, "error dumping table")

			// see testdata/golden/<test name>/<table>.golden
			p := filepath.Join(suite.goldenPath, t.Name()+".golden")
			actual = overrideDynamicData(t, actual)
			testutil.CompareWithGoldenFile(t, p, actual, *create, *update)
		})
	}
}

func TestImporter_ImportAll_DryRun(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db, datastore.WithDryRun)
	require.NoError(t, imp.ImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportAll_DryRunDanglingBlobs(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db, datastore.WithDryRun, datastore.WithImportDanglingBlobs)
	require.NoError(t, imp.ImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportAll_ContinuesAfterRepositoryNotFound(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-tags")
	require.NoError(t, imp.ImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportAll_DanglingManifests_ContinuesAfterRepositoryNotFound(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-revisions", datastore.WithImportDanglingManifests)
	require.NoError(t, imp.ImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportAll_DanglingManifests_StopsOnMissingConfigBlob(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unlinked-config", datastore.WithImportDanglingManifests)
	require.Error(t, imp.ImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_PreImport_UnlinkedConfigBlob_SkipManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unlinked-config")
	require.NoError(t, imp.PreImport(suite.ctx, "c-unlinked-config-blob"))
	validateImport(t, suite.db)
}

func TestImporter_Import_UnlinkedConfigBlob_SkipManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unlinked-config")
	require.NoError(t, imp.Import(suite.ctx, "c-unlinked-config-blob"))
	validateImport(t, suite.db)
}

func TestImporter_PreImport_UnsupportedDigest_SkipManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unsupported-digest")
	require.NoError(t, imp.PreImport(suite.ctx, "unsupported-digest"))
	validateImport(t, suite.db)
}

func TestImporter_Import_UnsupportedDigest_SkipManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unsupported-digest")
	require.NoError(t, imp.Import(suite.ctx, "unsupported-digest"))
	validateImport(t, suite.db)
}

func TestImporter_ImportAll_DanglingBlobs_StopsOnError(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "invalid-blob", datastore.WithImportDanglingBlobs)
	require.Error(t, imp.ImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportAll_BadManifestFormat(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-format")
	err := imp.ImportAll(suite.ctx)
	require.EqualError(t, err, `importing tags: retrieving manifest "sha256:a2490cec4484ee6c1068ba3a05f89934010c85242f736280b35343483b2264b6" from filesystem: failed to unmarshal manifest payload: invalid character 's' looking for beginning of value`)
}

func TestImporter_ImportAll_DanglingManifests_BadManifestFormat(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-format", datastore.WithImportDanglingManifests)
	err := imp.ImportAll(suite.ctx)
	require.EqualError(t, err, `importing manifests: retrieving manifest "sha256:a2490cec4484ee6c1068ba3a05f89934010c85242f736280b35343483b2264b6" from filesystem: failed to unmarshal manifest payload: invalid character 's' looking for beginning of value`)
}

func TestImporter_ImportAll_BadTagLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-tag-link")
	err := imp.ImportAll(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db, datastore.WithImportDanglingManifests)
	require.NoError(t, imp.Import(suite.ctx, "b-nested/older"))
	validateImport(t, suite.db)
}

func TestImporter_Import_TaggedOnly(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.Import(suite.ctx, "f-dangling-manifests"))
	validateImport(t, suite.db)
}

func TestImporter_Import_AllowIdempotent(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// Import a single repository twice, should succeed.
	imp := newImporter(t, suite.db, datastore.WithImportDanglingManifests)
	require.NoError(t, imp.Import(suite.ctx, "b-nested/older"))
	require.NoError(t, imp.Import(suite.ctx, "b-nested/older"))
	validateImport(t, suite.db)
}

func TestImporter_Import_DryRun(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db, datastore.WithDryRun)
	require.NoError(t, imp.Import(suite.ctx, "a-simple"))
	validateImport(t, suite.db)
}

func TestImporter_Import_UnlinkedLayers(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unlinked-layers")
	require.NoError(t, imp.Import(suite.ctx, "a-unlinked-layers"))
	validateImport(t, suite.db)
}

func TestImporter_Import_BadManifestFormat(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-format")
	err := imp.Import(suite.ctx, "a-yaml-manifest")
	require.EqualError(t, err, `importing tags: retrieving manifest "sha256:a2490cec4484ee6c1068ba3a05f89934010c85242f736280b35343483b2264b6" from filesystem: failed to unmarshal manifest payload: invalid character 's' looking for beginning of value`)
}

func TestImporter_Import_DanglingManifests_BadManifestFormat(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-format", datastore.WithImportDanglingManifests)
	err := imp.Import(suite.ctx, "a-yaml-manifest")
	require.EqualError(t, err, `importing manifests: retrieving manifest "sha256:a2490cec4484ee6c1068ba3a05f89934010c85242f736280b35343483b2264b6" from filesystem: failed to unmarshal manifest payload: invalid character 's' looking for beginning of value`)
}

func TestImporter_Import_BadTagLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-tag-link")
	err := imp.Import(suite.ctx, "alpine")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_BadTagLink_WithConcurrency(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-tag-link", datastore.WithTagConcurrency(5))
	err := imp.Import(suite.ctx, "alpine")
	// Just check that there was no error, we can't validate against golden files as with concurrency the order of rows
	// may change.
	require.NoError(t, err)
}

func TestImporter_PreImport_MissingTagLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-tag-link")
	err := imp.PreImport(suite.ctx, "alpine")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_MissingTagLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-tag-link")
	err := imp.Import(suite.ctx, "alpine")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_MissingTagLink_WithConcurrency(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-tag-link", datastore.WithTagConcurrency(2))
	err := imp.Import(suite.ctx, "alpine")
	// Just check that there was no error, we can't validate against golden files as with concurrency the order of rows
	// may change.
	require.NoError(t, err)
}

// lastTagErrorDriver mocks an unknown storage driver error when reading the
// tag in the last-tag-error repository only.
type lastTagErrorDriver struct {
	*filesystem.Driver
}

func (d *lastTagErrorDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	tagLinkFile := "/docker/registry/v2/repositories/last-tag-error/_manifests/tags/2.1.1/current/link"
	if path == tagLinkFile {
		return []byte{}, errors.New("test tag details read error")
	}

	return d.Driver.GetContent(ctx, path)
}

func TestImporter_Import_LastTagError(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	driver := &lastTagErrorDriver{newFilesystemStorageDriverWithRoot(t, "last-tag-error")}
	registry := newRegistry(t, driver)

	imp := datastore.NewImporter(suite.db, registry)
	err := imp.Import(suite.ctx, "last-tag-error")

	require.EqualError(t, err, "importing tags: reading tag details: test tag details read error")
}

func TestImporter_PreImport(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.PreImport(suite.ctx, "f-dangling-manifests"))
	validateImport(t, suite.db)
}

func TestImporter_PreImport_Import(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.PreImport(suite.ctx, "f-dangling-manifests"))
	require.NoError(t, imp.Import(suite.ctx, "f-dangling-manifests"))
	validateImport(t, suite.db)
}

func TestImporter_PreImport_DryRun(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db, datastore.WithDryRun)
	require.NoError(t, imp.PreImport(suite.ctx, "a-simple"))
	validateImport(t, suite.db)
}

func TestImporter_PreImport_BadManifestFormat(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-format")
	err := imp.PreImport(suite.ctx, "a-yaml-manifest")
	require.EqualError(t, err, `pre importing tagged manifests: pre importing manifest: retrieving manifest "sha256:a2490cec4484ee6c1068ba3a05f89934010c85242f736280b35343483b2264b6" from filesystem: failed to unmarshal manifest payload: invalid character 's' looking for beginning of value`)
}

func TestImporter_PreImport_EmptyManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "empty-manifest")
	err := imp.PreImport(suite.ctx, "empty-manifest")
	require.NoError(t, err)
}

func TestImporter_Import_EmptyManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "empty-manifest")
	err := imp.Import(suite.ctx, "empty-manifest")
	require.NoError(t, err)
}

func TestImporter_PreImport_BadTagLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-tag-link")
	err := imp.PreImport(suite.ctx, "alpine")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_BadManifestLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-link")
	err := imp.PreImport(suite.ctx, "alpine")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_BadManifestLink(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-link")
	err := imp.Import(suite.ctx, "alpine")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_BadManifestListRef(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-list-ref")
	err := imp.PreImport(suite.ctx, "multi-arch")
	require.NoError(t, err)
}

func TestImporter_Import_BadManifestListRef(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "bad-manifest-list-ref")
	err := imp.Import(suite.ctx, "multi-arch")
	require.NoError(t, err)
}

func TestImporter_PreImportAll_UnknownLayerMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-layer-mediatype")
	err := imp.PreImportAll(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImportAll_UnknownLayerMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))
	t.Cleanup(func() {
		require.NoError(t, testutil.TruncateAllTables(suite.db))
		deleteMediaType(t, "application/foo.bar.layer.v1.tar+gzip")
	})

	imp := newImporterWithRoot(t, suite.db, "unknown-layer-mediatype")
	err := imp.PreImportAll(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_FullImport_UnknownLayerMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-layer-mediatype")
	err := imp.FullImport(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_FullImport_UnknownLayerMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))
	t.Cleanup(func() {
		require.NoError(t, testutil.TruncateAllTables(suite.db))
		deleteMediaType(t, "application/foo.bar.layer.v1.tar+gzip")
	})

	imp := newImporterWithRoot(t, suite.db, "unknown-layer-mediatype")
	err := imp.FullImport(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_FullImport_ErrTagsTableNotEmpty(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	// First, import a single repository, only tagged manifests and referenced blobs.
	imp1 := newImporter(t, suite.db)
	require.NoError(t, imp1.Import(suite.ctx, "f-dangling-manifests"))

	// Now try to import the entire contents of the registry including what was previously imported.
	// Expect importer to fail because the tags table is not empty.
	imp2 := newImporter(t, suite.db)
	require.EqualError(t, imp2.FullImport(suite.ctx), "importing all repositories: tags table is not empty")
}

func TestImporter_ImportAllRepositories_UnknownLayerMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-layer-mediatype")
	err := imp.ImportAllRepositories(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_ImportAllRepositories_UnknownLayerMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))
	t.Cleanup(func() {
		require.NoError(t, testutil.TruncateAllTables(suite.db))
		deleteMediaType(t, "application/foo.bar.layer.v1.tar+gzip")
	})

	imp := newImporterWithRoot(t, suite.db, "unknown-layer-mediatype")
	err := imp.ImportAllRepositories(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImportAll_UnknownManifestMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifest-mediatype")
	err := imp.PreImportAll(suite.ctx)
	require.EqualError(t, err, "pre importing all repositories: pre importing tagged manifests: pre importing manifest: retrieving manifest \"sha256:3742a2977c3f5663dd12ddc406d45ed7cda2760842c9da514c35f9069581e7a2\" from filesystem: errors verifying manifest: unrecognized manifest content type application/foo.bar.manfiest.v1.tar+gzip")
}

func TestImporter_PreImportAll_UnknownManifestMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifest-mediatype")
	err := imp.PreImportAll(suite.ctx)
	require.EqualError(t, err, "pre importing all repositories: pre importing tagged manifests: pre importing manifest: retrieving manifest \"sha256:3742a2977c3f5663dd12ddc406d45ed7cda2760842c9da514c35f9069581e7a2\" from filesystem: errors verifying manifest: unrecognized manifest content type application/foo.bar.manfiest.v1.tar+gzip")
}

func TestImporter_ImportAllRepositories_UnknownManifestMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifest-mediatype")
	err := imp.ImportAllRepositories(suite.ctx)
	require.EqualError(t, err, "importing all repositories: importing tags: retrieving manifest \"sha256:3742a2977c3f5663dd12ddc406d45ed7cda2760842c9da514c35f9069581e7a2\" from filesystem: errors verifying manifest: unrecognized manifest content type application/foo.bar.manfiest.v1.tar+gzip")
}

func TestImporter_ImportAllRepositories_UnknownManifestMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifest-mediatype")
	err := imp.ImportAllRepositories(suite.ctx)
	require.EqualError(t, err, "importing all repositories: importing tags: retrieving manifest \"sha256:3742a2977c3f5663dd12ddc406d45ed7cda2760842c9da514c35f9069581e7a2\" from filesystem: errors verifying manifest: unrecognized manifest content type application/foo.bar.manfiest.v1.tar+gzip")
}

func TestImporter_FullImport_UnknownManifestMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifest-mediatype")
	err := imp.FullImport(suite.ctx)
	require.EqualError(t, err, "pre importing all repositories: pre importing tagged manifests: pre importing manifest: retrieving manifest \"sha256:3742a2977c3f5663dd12ddc406d45ed7cda2760842c9da514c35f9069581e7a2\" from filesystem: errors verifying manifest: unrecognized manifest content type application/foo.bar.manfiest.v1.tar+gzip")
}

func TestImporter_FullImport_UnknownManifestMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifest-mediatype")
	err := imp.FullImport(suite.ctx)
	require.EqualError(t, err, "pre importing all repositories: pre importing tagged manifests: pre importing manifest: retrieving manifest \"sha256:3742a2977c3f5663dd12ddc406d45ed7cda2760842c9da514c35f9069581e7a2\" from filesystem: errors verifying manifest: unrecognized manifest content type application/foo.bar.manfiest.v1.tar+gzip")
}

func TestImporter_PreImportAll_UnknownManifestConfigMediaType(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "unknown-manifestconfig-mediatype")
	err := imp.PreImportAll(suite.ctx)
	require.EqualError(t, err, "pre importing all repositories: pre importing tagged manifests: pre importing manifest: creating manifest: mapping config media type: unknown media type: application/foo.bar.container.image.v1+json")
}

func TestImporter_PreImportAll_UnknownManifestConfigMediaTypeWithDynamicMediaTypes(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, strconv.FormatBool(true))
	t.Cleanup(func() {
		require.NoError(t, testutil.TruncateAllTables(suite.db))
		deleteMediaType(t, "application/foo.bar.container.image.v1+json")
	})

	imp := newImporterWithRoot(t, suite.db, "unknown-manifestconfig-mediatype")
	err := imp.PreImportAll(suite.ctx)
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_NoTagsPrefix(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-tags")
	err := imp.PreImport(suite.ctx, "b-missing-tags")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_NoTagsPrefix(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-tags")
	err := imp.Import(suite.ctx, "b-missing-tags")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_MissingRevision(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-revisions")
	err := imp.PreImport(suite.ctx, "a-missing-revisions")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_MissingRevision(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "missing-revisions")
	err := imp.Import(suite.ctx, "a-missing-revisions")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_SchemaV1(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "happy-path")
	err := imp.PreImport(suite.ctx, "d-schema1")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_SchemaV1(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "happy-path")
	err := imp.Import(suite.ctx, "d-schema1")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_EmptyLayerLinks(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "empty-layer-links")
	err := imp.PreImport(suite.ctx, "broken-layer-links")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_Import_EmptyLayerLinks(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "empty-layer-links")
	err := imp.Import(suite.ctx, "broken-layer-links")
	require.NoError(t, err)
	validateImport(t, suite.db)
}

func TestImporter_PreImport_BuildkitIndexAsManifestList(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "buildkit-index-as-manifest-list")
	err := imp.PreImport(suite.ctx, "buildx")
	require.NoError(t, err)

	// validate DB state
	validateImport(t, suite.db)
}

func TestImporter_PreImport_BuildkitIndexAsManifestListWithoutLayers(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "buildkit-index-as-manifest-list-without-layers")
	err := imp.PreImport(suite.ctx, "buildx")
	require.NoError(t, err)

	// validate DB state
	validateImport(t, suite.db)
}

func TestImporter_PreImport_DoesNotFailWhenProcessingPreviouslySkippedManifest(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporterWithRoot(t, suite.db, "empty-layer-links")
	err := imp.PreImport(suite.ctx, "broken-layer-links")
	require.NoError(t, err)
}

func TestImporter_ImportBlobs(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.ImportBlobs(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportBlobs_DryRun(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db, datastore.WithDryRun)
	require.NoError(t, imp.ImportBlobs(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_PreImportAll(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.PreImportAll(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_FullImport(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.FullImport(suite.ctx))
	validateImport(t, suite.db)
}

func TestImporter_ImportAllRepositories(t *testing.T) {
	require.NoError(t, testutil.TruncateAllTables(suite.db))

	imp := newImporter(t, suite.db)
	require.NoError(t, imp.ImportAllRepositories(suite.ctx))
	validateImport(t, suite.db)
}

func restoreLockfiles(t *testing.T, imp *datastore.Importer) func() {
	t.Helper()

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, imp.RestoreLockfiles(ctx))
	}
}
