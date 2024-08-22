//go:build integration

package datastore_test

import (
	"testing"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
)

func reloadNamespaceFixtures(tb testing.TB) {
	testutil.ReloadFixtures(tb, suite.db, suite.basePath, testutil.NamespacesTable)
}

func unloadNamespaceFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(suite.db, testutil.NamespacesTable))
}

func TestNamespaceStore_ImplementsReaderAndWriter(t *testing.T) {
	require.Implements(t, (*datastore.NamespaceStore)(nil), datastore.NewNamespaceStore(suite.db))
}

func TestNamespaceStore_FindByName(t *testing.T) {
	reloadNamespaceFixtures(t)

	s := datastore.NewNamespaceStore(suite.db)
	n, err := s.FindByName(suite.ctx, "gitlab-org")
	require.NoError(t, err)

	// see testdata/fixtures/top_level_namespaces.sql
	require.Equal(t, &models.Namespace{
		ID:        1,
		Name:      "gitlab-org",
		CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:47:39.849864", n.CreatedAt.Location()),
	}, n)
}

func TestNamespaceStore_FindByName_NotFound(t *testing.T) {
	unloadNamespaceFixtures(t)

	s := datastore.NewNamespaceStore(suite.db)
	n, err := s.FindByName(suite.ctx, "foo")
	require.Nil(t, n)
	require.NoError(t, err)
}

func TestNamespaceStore_SafeFindOrCreate(t *testing.T) {
	unloadNamespaceFixtures(t)

	s := datastore.NewNamespaceStore(suite.db)

	// create non-existing `foo`
	n := &models.Namespace{
		Name: "foo",
	}
	err := s.SafeFindOrCreate(suite.ctx, n)
	require.NoError(t, err)
	require.NotEmpty(t, n.ID)
	require.Equal(t, "foo", n.Name)
	require.Empty(t, n.UpdatedAt)
	require.NotEmpty(t, n.CreatedAt)

	// attempt to create existing `foo`
	n2 := &models.Namespace{
		Name: "foo",
	}
	err = s.SafeFindOrCreate(suite.ctx, n2)
	require.NoError(t, err)
	require.Equal(t, n, n2)
}
