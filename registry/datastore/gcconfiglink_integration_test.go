//go:build integration

package datastore_test

import (
	"testing"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
)

func reloadGCConfigLinkFixtures(tb testing.TB) {
	// We want to disable the trigger before loading fixtures, otherwise gc_blobs_configurations will be filled
	// by the trigger once the manifest fixtures are loaded. This will result in an error when trying to load the
	// gc_blobs_configurations fixtures, as the records already exist.
	enable, err := testutil.GCTrackConfigurationBlobsTrigger.Disable(suite.db)
	require.NoError(tb, err)
	defer enable()

	testutil.ReloadFixtures(tb, suite.db, suite.basePath,
		testutil.NamespacesTable, testutil.RepositoriesTable, testutil.BlobsTable, testutil.ManifestsTable,
		testutil.GCBlobsConfigurationsTable)
}

func unloadGCConfigLinkFixtures(tb testing.TB) {
	enable, err := testutil.GCTrackConfigurationBlobsTrigger.Disable(suite.db)
	require.NoError(tb, err)
	defer enable()

	require.NoError(tb, testutil.TruncateTables(suite.db,
		testutil.NamespacesTable, testutil.RepositoriesTable, testutil.BlobsTable, testutil.ManifestsTable,
		testutil.GCBlobsConfigurationsTable))
}

func TestGCConfigLinkStore_FindAll(t *testing.T) {
	reloadGCConfigLinkFixtures(t)

	s := datastore.NewGCConfigLinkStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/gc_blobs_configurations.sql
	expected := []*models.GCConfigLink{
		{
			ID:           9,
			NamespaceID:  3,
			RepositoryID: 10,
			ManifestID:   14,
			Digest:       "sha256:829ae805ecbcdd4165484a69f5f65c477da69c9f181887f7953022cba209525e",
		},
		{
			ID:           2,
			NamespaceID:  1,
			RepositoryID: 3,
			ManifestID:   2,
			Digest:       "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
		},
		{
			ID:           5,
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   9,
			Digest:       "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
		},
		{
			ID:           7,
			NamespaceID:  2,
			RepositoryID: 7,
			ManifestID:   11,
			Digest:       "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
		},
		{
			ID:           8,
			NamespaceID:  3,
			RepositoryID: 10,
			ManifestID:   13,
			Digest:       "sha256:b051081eac10ae5607e7846677924d7ac3824954248d0247e0d24dd5063fb4c0",
		},
		{
			ID:           1,
			NamespaceID:  1,
			RepositoryID: 3,
			ManifestID:   1,
			Digest:       "sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9",
		},
		{
			ID:           4,
			NamespaceID:  2,
			RepositoryID: 6,
			ManifestID:   5,
			Digest:       "sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9",
		},
		{
			ID:           6,
			NamespaceID:  2,
			RepositoryID: 7,
			ManifestID:   10,
			Digest:       "sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9",
		},
		{
			ID:           10,
			NamespaceID:  3,
			RepositoryID: 10,
			ManifestID:   15,
			Digest:       "sha256:0a450fb93c7bd4ee53d05ba63842d6c2cf73089198cbaccc115d470e6ae2ffc9",
		},
		{
			ID:           11,
			NamespaceID:  3,
			RepositoryID: 10,
			ManifestID:   17,
			Digest:       "sha256:3ded4e17612c66f216041fe6f15002d9406543192095d689f14e8063b1a503df",
		},
		{
			ID:           13,
			NamespaceID:  2,
			RepositoryID: 7,
			ManifestID:   29,
			Digest:       "sha256:eb110b254f038cd0d464b20de5070099a6f675b9d8eb7e5035d929540a041ab1",
		},
		{
			ID:           3,
			NamespaceID:  1,
			RepositoryID: 4,
			ManifestID:   3,
			Digest:       "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
		},
		{
			ID:           12,
			NamespaceID:  2,
			RepositoryID: 7,
			ManifestID:   28,
			Digest:       "sha256:af47096251092caf59498806ab8d58e8173ecf5a182f024ce9d635b5b4a55d66",
		},
	}

	require.Equal(t, expected, rr)
}

func TestGCConfigLinkStore_FindAll_NotFound(t *testing.T) {
	unloadGCConfigLinkFixtures(t)

	s := datastore.NewGCConfigLinkStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestGcConfigLinkStore_Count(t *testing.T) {
	reloadGCConfigLinkFixtures(t)

	s := datastore.NewGCConfigLinkStore(suite.db)
	count, err := s.Count(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/gc_blobs_configurations.sql
	require.Equal(t, 13, count)
}
