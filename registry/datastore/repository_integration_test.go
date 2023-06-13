//go:build integration

package datastore_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/docker/distribution/registry/internal/migration"
	itestutil "github.com/docker/distribution/registry/internal/testutil"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func reloadRepositoryFixtures(tb testing.TB) {
	testutil.ReloadFixtures(tb, suite.db, suite.basePath, testutil.NamespacesTable, testutil.RepositoriesTable)
}

func unloadRepositoryFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(suite.db, testutil.NamespacesTable, testutil.RepositoriesTable))
}

func TestRepositoryStore_ImplementsReaderAndWriter(t *testing.T) {
	require.Implements(t, (*datastore.RepositoryStore)(nil), datastore.NewRepositoryStore(suite.db))
}

func TestRepositoryStore_FindByID(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org")
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	expected := &models.Repository{
		ID:              1,
		NamespaceID:     1,
		Name:            "gitlab-org",
		Path:            "gitlab-org",
		MigrationStatus: migration.RepositoryStatusNative,
		CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:39.849864", r.CreatedAt.Location()),
	}
	require.Equal(t, expected, r)
}

func TestRepositoryStore_FindByID_NotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "a/b/c")
	require.Nil(t, r)
	require.NoError(t, err)
}

func TestRepositoryStore_FindByPath(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	expected := &models.Repository{
		ID:              2,
		NamespaceID:     1,
		Name:            "gitlab-test",
		Path:            "gitlab-org/gitlab-test",
		ParentID:        sql.NullInt64{Int64: 1, Valid: true},
		MigrationStatus: migration.RepositoryStatusNative,
		CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:40.866312", r.CreatedAt.Location()),
	}
	require.Equal(t, expected, r)
}

func TestRepositoryStore_FindByPath_NotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/bar")
	require.Nil(t, r)
	require.NoError(t, err)
}

func TestRepositoryStore_FindByPath_NamespaceNotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "foo/gitlab-org/gitlab-test")
	require.Nil(t, r)
	require.NoError(t, err)
}

func TestRepositoryStore_FindByPath_SingleRepositoryCache(t *testing.T) {
	reloadRepositoryFixtures(t)

	path := "a-test-group/foo"
	c := datastore.NewSingleRepositoryCache()

	ctx := context.Background()
	require.Nil(t, c.Get(ctx, path))

	s := datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(c))
	r, err := s.FindByPath(suite.ctx, path)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	expected := &models.Repository{
		ID:              6,
		NamespaceID:     2,
		Name:            "foo",
		Path:            path,
		ParentID:        sql.NullInt64{Int64: 5, Valid: true},
		MigrationStatus: migration.RepositoryStatusNative,
		CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", r.CreatedAt.Location()),
	}

	require.NotEqual(t, expected, c.Get(ctx, "fake/path"))
	require.Equal(t, expected, c.Get(ctx, path))
}

func TestRepositoryStore_FindByPath_WithCentralRepositoryCache(t *testing.T) {
	reloadRepositoryFixtures(t)

	path := "a-test-group/foo"

	// first grab sample repository without a cache to capture expected Repository object
	s := datastore.NewRepositoryStore(suite.db)
	expected, err := s.FindByPath(suite.ctx, path)
	require.NoError(t, err)
	require.NotNil(t, expected)

	// repeat with cache
	cache := datastore.NewCentralRepositoryCache(itestutil.RedisCache(t, 0))

	ctx := context.Background()
	require.Nil(t, cache.Get(ctx, path))

	s = datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(cache))
	got, err := s.FindByPath(suite.ctx, path)
	require.NoError(t, err)
	require.Equal(t, expected, got)

	require.Equal(t, expected, cache.Get(ctx, path))
}

func TestRepositoryStore_FindAll(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	require.Len(t, rr, 16)
	local := rr[0].CreatedAt.Location()
	expected := models.Repositories{
		{
			ID:              1,
			NamespaceID:     1,
			Name:            "gitlab-org",
			Path:            "gitlab-org",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:39.849864", local),
		},
		{
			ID:              2,
			NamespaceID:     1,
			Name:            "gitlab-test",
			Path:            "gitlab-org/gitlab-test",
			ParentID:        sql.NullInt64{Int64: 1, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:40.866312", local),
		},
		{
			ID:              3,
			NamespaceID:     1,
			Name:            "backend",
			Path:            "gitlab-org/gitlab-test/backend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:42:12.566212", local),
		},
		{
			ID:              4,
			NamespaceID:     1,
			Name:            "frontend",
			Path:            "gitlab-org/gitlab-test/frontend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:43:39.476421", local),
		},
		{
			ID:              5,
			NamespaceID:     2,
			Name:            "a-test-group",
			Path:            "a-test-group",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", local),
		},
		{
			ID:              6,
			NamespaceID:     2,
			Name:            "foo",
			Path:            "a-test-group/foo",
			ParentID:        sql.NullInt64{Int64: 5, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", local),
		},
		{
			ID:              7,
			NamespaceID:     2,
			Name:            "bar",
			Path:            "a-test-group/bar",
			ParentID:        sql.NullInt64{Int64: 5, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", local),
		},
		{
			ID:              8,
			NamespaceID:     3,
			Name:            "usage-group",
			Path:            "usage-group",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			ID:              9,
			NamespaceID:     3,
			Name:            "sub-group-1",
			Path:            "usage-group/sub-group-1",
			ParentID:        sql.NullInt64{Int64: 8, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			ID:              10,
			NamespaceID:     3,
			Name:            "repository-1",
			Path:            "usage-group/sub-group-1/repository-1",
			ParentID:        sql.NullInt64{Int64: 9, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			ID:              11,
			NamespaceID:     3,
			Name:            "repository-2",
			Path:            "usage-group/sub-group-1/repository-2",
			ParentID:        sql.NullInt64{Int64: 9, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:12:43.561123", local),
		},
		{
			ID:              12,
			NamespaceID:     3,
			Name:            "sub-group-2",
			Path:            "usage-group/sub-group-2",
			ParentID:        sql.NullInt64{Int64: 8, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:33:12.312211", local),
		},
		{
			ID:              13,
			NamespaceID:     3,
			Name:            "repository-1",
			Path:            "usage-group/sub-group-2/repository-1",
			ParentID:        sql.NullInt64{Int64: 12, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:33:12.434732", local),
		},
		{
			ID:              14,
			NamespaceID:     3,
			Name:            "sub-repository-1",
			Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
			ParentID:        sql.NullInt64{Int64: 13, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:33:12.434732", local),
		},
		{
			ID:              15,
			NamespaceID:     4,
			Name:            "usage-group-2",
			Path:            "usage-group-2",
			ParentID:        sql.NullInt64{},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 15:36:04.692846", local),
		},
		{
			ID:              16,
			NamespaceID:     4,
			Name:            "project-1",
			Path:            "usage-group-2/sub-group-1/project-1",
			ParentID:        sql.NullInt64{},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 15:36:04.692846", local),
		},
	}

	require.Equal(t, expected, rr)
}

func TestRepositoryStore_FindAll_NotFound(t *testing.T) {
	unloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestRepositoryStore_FindAllPaginated(t *testing.T) {
	reloadManifestFixtures(t)

	tt := []struct {
		name     string
		limit    int
		lastPath string

		// see testdata/fixtures/[repositories|repository_manifests].sql:
		//
		// 		gitlab-org 						(0 manifests, 0 manifest lists)
		// 		gitlab-org/gitlab-test 			(0 manifests, 0 manifest lists)
		// 		gitlab-org/gitlab-test/backend 	(2 manifests, 1 manifest list)
		// 		gitlab-org/gitlab-test/frontend (2 manifests, 1 manifest list)
		// 		a-test-group 					(0 manifests, 0 manifest lists)
		// 		a-test-group/foo  				(1 manifests, 0 manifest lists)
		// 		a-test-group/bar 				(0 manifests, 1 manifest list)
		expectedRepos models.Repositories
	}{
		{
			name:     "no limit and no last path",
			limit:    100, // there are only 16 repositories in the DB, so this is equivalent to no limit
			lastPath: "",  // this is the equivalent to no last path, as all repository paths are non-empty
			expectedRepos: models.Repositories{
				{
					ID:              7,
					NamespaceID:     2,
					Name:            "bar",
					Path:            "a-test-group/bar",
					ParentID:        sql.NullInt64{Int64: 5, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              6,
					NamespaceID:     2,
					Name:            "foo",
					Path:            "a-test-group/foo",
					ParentID:        sql.NullInt64{Int64: 5, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              3,
					NamespaceID:     1,
					Name:            "backend",
					Path:            "gitlab-org/gitlab-test/backend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              4,
					NamespaceID:     1,
					Name:            "frontend",
					Path:            "gitlab-org/gitlab-test/frontend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              9,
					NamespaceID:     3,
					Name:            "sub-group-1",
					Path:            "usage-group/sub-group-1",
					ParentID:        sql.NullInt64{Int64: 8, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              10,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-1/repository-1",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              11,
					NamespaceID:     3,
					Name:            "repository-2",
					Path:            "usage-group/sub-group-1/repository-2",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              13,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-2/repository-1",
					ParentID:        sql.NullInt64{Int64: 12, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              14,
					NamespaceID:     3,
					Name:            "sub-repository-1",
					Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
					ParentID:        sql.NullInt64{Int64: 13, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:     "1st part",
			limit:    2,
			lastPath: "",
			expectedRepos: models.Repositories{
				{
					ID:              7,
					NamespaceID:     2,
					Name:            "bar",
					Path:            "a-test-group/bar",
					ParentID:        sql.NullInt64{Int64: 5, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              6,
					NamespaceID:     2,
					Name:            "foo",
					Path:            "a-test-group/foo",
					ParentID:        sql.NullInt64{Int64: 5, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:     "nth part",
			limit:    1,
			lastPath: "a-test-group/foo",
			expectedRepos: models.Repositories{
				{
					ID:              3,
					NamespaceID:     1,
					Name:            "backend",
					Path:            "gitlab-org/gitlab-test/backend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:     "last part",
			limit:    100,
			lastPath: "gitlab-org/gitlab-test/backend",
			expectedRepos: models.Repositories{
				{
					ID:              4,
					NamespaceID:     1,
					Name:            "frontend",
					Path:            "gitlab-org/gitlab-test/frontend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              9,
					NamespaceID:     3,
					Name:            "sub-group-1",
					Path:            "usage-group/sub-group-1",
					ParentID:        sql.NullInt64{Int64: 8, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              10,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-1/repository-1",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              11,
					NamespaceID:     3,
					Name:            "repository-2",
					Path:            "usage-group/sub-group-1/repository-2",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              13,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-2/repository-1",
					ParentID:        sql.NullInt64{Int64: 12, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              14,
					NamespaceID:     3,
					Name:            "sub-repository-1",
					Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
					ParentID:        sql.NullInt64{Int64: 13, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:     "non existent last path",
			limit:    100,
			lastPath: "does-not-exist",
			expectedRepos: models.Repositories{
				{
					ID:              3,
					NamespaceID:     1,
					Name:            "backend",
					Path:            "gitlab-org/gitlab-test/backend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              4,
					NamespaceID:     1,
					Name:            "frontend",
					Path:            "gitlab-org/gitlab-test/frontend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              9,
					NamespaceID:     3,
					Name:            "sub-group-1",
					Path:            "usage-group/sub-group-1",
					ParentID:        sql.NullInt64{Int64: 8, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              10,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-1/repository-1",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              11,
					NamespaceID:     3,
					Name:            "repository-2",
					Path:            "usage-group/sub-group-1/repository-2",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              13,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-2/repository-1",
					ParentID:        sql.NullInt64{Int64: 12, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              14,
					NamespaceID:     3,
					Name:            "sub-repository-1",
					Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
					ParentID:        sql.NullInt64{Int64: 13, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			filters := datastore.FilterParams{
				MaxEntries: test.limit,
				LastEntry:  test.lastPath,
			}

			rr, err := s.FindAllPaginated(suite.ctx, filters)

			// reset created_at attributes for reproducible comparisons
			for _, r := range rr {
				require.NotEmpty(t, r.CreatedAt)
				r.CreatedAt = time.Time{}
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedRepos, rr)
		})
	}
}

func TestRepositoryStore_FindAllPaginated_NoRepositories(t *testing.T) {
	unloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	rr, err := s.FindAllPaginated(suite.ctx, datastore.FilterParams{MaxEntries: 100})
	require.NoError(t, err)
	require.Empty(t, rr)
}

func TestRepositoryStore_DescendantsOf(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindDescendantsOf(suite.ctx, 1)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	local := rr[0].CreatedAt.Location()
	expected := models.Repositories{
		{
			ID:              2,
			NamespaceID:     1,
			Name:            "gitlab-test",
			Path:            "gitlab-org/gitlab-test",
			ParentID:        sql.NullInt64{Int64: 1, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:40.866312", local),
		},
		{
			ID:              3,
			NamespaceID:     1,
			Name:            "backend",
			Path:            "gitlab-org/gitlab-test/backend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:42:12.566212", local),
		},
		{
			ID:              4,
			NamespaceID:     1,
			Name:            "frontend",
			Path:            "gitlab-org/gitlab-test/frontend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:43:39.476421", local),
		},
	}

	require.Equal(t, expected, rr)
}

func TestRepositoryStore_DescendantsOf_Leaf(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindDescendantsOf(suite.ctx, 3)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestRepositoryStore_DescendantsOf_NotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindDescendantsOf(suite.ctx, 0)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestRepositoryStore_AncestorsOf(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindAncestorsOf(suite.ctx, 3)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	local := rr[0].CreatedAt.Location()
	expected := models.Repositories{
		{
			ID:              2,
			NamespaceID:     1,
			Name:            "gitlab-test",
			Path:            "gitlab-org/gitlab-test",
			ParentID:        sql.NullInt64{Int64: 1, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:40.866312", local),
		},
		{
			ID:              1,
			NamespaceID:     1,
			Name:            "gitlab-org",
			Path:            "gitlab-org",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:39.849864", local),
		},
	}

	require.Equal(t, expected, rr)
}

func TestRepositoryStore_AncestorsOf_Root(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindAncestorsOf(suite.ctx, 1)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestRepositoryStore_AncestorsOf_NotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindAncestorsOf(suite.ctx, 0)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestRepositoryStore_SiblingsOf(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindSiblingsOf(suite.ctx, 3)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	local := rr[0].CreatedAt.Location()
	expected := models.Repositories{
		{
			ID:              4,
			NamespaceID:     1,
			Name:            "frontend",
			Path:            "gitlab-org/gitlab-test/frontend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:43:39.476421", local),
		},
	}

	require.Equal(t, expected, rr)
}

func TestRepositoryStore_SiblingsOf_OnlyChild(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindSiblingsOf(suite.ctx, 2)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	require.Len(t, rr, 0)
}

func TestRepositoryStore_SiblingsOf_NotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)
	rr, err := s.FindSiblingsOf(suite.ctx, 0)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestRepositoryStore_Manifests(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	mm, err := s.Manifests(suite.ctx, &models.Repository{NamespaceID: 1, ID: 3})
	require.NoError(t, err)

	// see testdata/fixtures/repository_manifests.sql
	local := mm[0].CreatedAt.Location()
	expected := models.Manifests{
		{
			ID:            1,
			NamespaceID:   1,
			RepositoryID:  3,
			TotalSize:     2480932,
			SchemaVersion: 2,
			MediaType:     "application/vnd.docker.distribution.manifest.v2+json",
			Digest:        "sha256:bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155",
			Payload:       models.Payload(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1640,"digest":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9"},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":2802957,"digest":"sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":108,"digest":"sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21"}]}`),
			Configuration: &models.Configuration{
				MediaType: "application/vnd.docker.container.image.v1+json",
				Digest:    "sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9",
				Payload:   models.Payload(`{"architecture":"amd64","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh"],"ArgsEscaped":true,"Image":"sha256:e7d92cdc71feacf90708cb59182d0df1b911f8ae022d29e8e95d75ca6a99776a","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"container":"7980908783eb05384926afb5ffad45856f65bc30029722a4be9f1eb3661e9c5e","container_config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh","-c","echo \"1\" \u003e /data"],"Image":"sha256:e7d92cdc71feacf90708cb59182d0df1b911f8ae022d29e8e95d75ca6a99776a","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"created":"2020-03-02T12:21:53.8027967Z","docker_version":"19.03.5","history":[{"created":"2020-01-18T01:19:37.02673981Z","created_by":"/bin/sh -c #(nop) ADD file:e69d441d729412d24675dcd33e04580885df99981cec43de8c9b24015313ff8e in / "},{"created":"2020-01-18T01:19:37.187497623Z","created_by":"/bin/sh -c #(nop)  CMD [\"/bin/sh\"]","empty_layer":true},{"created":"2020-03-02T12:21:53.8027967Z","created_by":"/bin/sh -c echo \"1\" \u003e /data"}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:5216338b40a7b96416b8b9858974bbe4acc3096ee60acbc4dfb1ee02aecceb10","sha256:99cb4c5d9f96432a00201f4b14c058c6235e563917ba7af8ed6c4775afa5780f"]}}`),
			},
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:50:26.461745", local),
		},
		{
			ID:            2,
			NamespaceID:   1,
			RepositoryID:  3,
			TotalSize:     82384923,
			SchemaVersion: 2,
			MediaType:     "application/vnd.docker.distribution.manifest.v2+json",
			Digest:        "sha256:56b4b2228127fd594c5ab2925409713bd015ae9aa27eef2e0ddd90bcb2b1533f",
			Payload:       models.Payload(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1819,"digest":"sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073"},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":2802957,"digest":"sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":108,"digest":"sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":109,"digest":"sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1"}]}`),
			Configuration: &models.Configuration{
				MediaType: "application/vnd.docker.container.image.v1+json",
				Digest:    "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
				Payload:   models.Payload(`{"architecture":"amd64","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh"],"ArgsEscaped":true,"Image":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"container":"cb78c8a8058712726096a7a8f80e6a868ffb514a07f4fef37639f42d99d997e4","container_config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh","-c","echo \"2\" \u003e\u003e /data"],"Image":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"created":"2020-03-02T12:24:16.7039823Z","docker_version":"19.03.5","history":[{"created":"2020-01-18T01:19:37.02673981Z","created_by":"/bin/sh -c #(nop) ADD file:e69d441d729412d24675dcd33e04580885df99981cec43de8c9b24015313ff8e in / "},{"created":"2020-01-18T01:19:37.187497623Z","created_by":"/bin/sh -c #(nop)  CMD [\"/bin/sh\"]","empty_layer":true},{"created":"2020-03-02T12:21:53.8027967Z","created_by":"/bin/sh -c echo \"1\" \u003e /data"},{"created":"2020-03-02T12:24:16.7039823Z","created_by":"/bin/sh -c echo \"2\" \u003e\u003e /data"}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:5216338b40a7b96416b8b9858974bbe4acc3096ee60acbc4dfb1ee02aecceb10","sha256:99cb4c5d9f96432a00201f4b14c058c6235e563917ba7af8ed6c4775afa5780f","sha256:6322c07f5c6ad456f64647993dfc44526f4548685ee0f3d8f03534272b3a06d8"]}}`),
			},
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:50:26.461745", local),
		},
		{
			ID:            6,
			NamespaceID:   1,
			RepositoryID:  3,
			TotalSize:     0,
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
			Digest:        "sha256:dc27c897a7e24710a2821878456d56f3965df7cc27398460aa6f21f8b385d2d0",
			Payload:       models.Payload(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","size":23321,"digest":"sha256:bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155","platform":{"architecture":"amd64","os":"linux"}},{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","size":24123,"digest":"sha256:56b4b2228127fd594c5ab2925409713bd015ae9aa27eef2e0ddd90bcb2b1533f","platform":{"architecture":"amd64","os":"windows","os.version":"10.0.14393.2189"}}]}`),
			CreatedAt:     testutil.ParseTimestamp(t, "2020-04-02 18:45:03.470711", local),
		},
	}
	require.Equal(t, expected, mm)
}

func TestRepositoryStore_Tags(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	tt, err := s.Tags(suite.ctx, &models.Repository{NamespaceID: 1, ID: 4})
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	local := tt[0].CreatedAt.Location()
	expected := models.Tags{
		{
			ID:           4,
			NamespaceID:  1,
			Name:         "1.0.0",
			RepositoryID: 4,
			ManifestID:   3,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:46.283783", local),
		},
		{
			ID:           5,
			NamespaceID:  1,
			Name:         "stable-9ede8db0",
			RepositoryID: 4,
			ManifestID:   3,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:47.283783", local),
		},
		{
			ID:           6,
			NamespaceID:  1,
			Name:         "stable-91ac07a9",
			RepositoryID: 4,
			ManifestID:   4,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-04-15 09:47:26.461413", local),
		},
		{
			ID:           8,
			NamespaceID:  1,
			Name:         "rc2",
			RepositoryID: 4,
			ManifestID:   7,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-04-15 09:47:26.461413", local),
		},
	}
	require.Equal(t, expected, tt)
}

func TestRepositoryStore_TagsPaginated(t *testing.T) {
	reloadTagFixtures(t)

	// see testdata/fixtures/tags.sql (sorted):
	// 1.0.0
	// rc2
	// stable-91ac07a9
	// stable-9ede8db0
	r := &models.Repository{NamespaceID: 1, ID: 4}

	tt := []struct {
		name         string
		limit        int
		lastName     string
		expectedTags models.Tags
	}{
		{
			name:     "no limit and no last name",
			limit:    100, // there are only 4 tags in the DB for repository 4, so this is equivalent to no limit
			lastName: "",  // this is the equivalent to no last name, as all tag names are non-empty
			expectedTags: models.Tags{
				{
					ID:           4,
					NamespaceID:  1,
					Name:         "1.0.0",
					RepositoryID: 4,
					ManifestID:   3,
				},
				{
					ID:           8,
					NamespaceID:  1,
					Name:         "rc2",
					RepositoryID: 4,
					ManifestID:   7,
				},
				{
					ID:           6,
					NamespaceID:  1,
					Name:         "stable-91ac07a9",
					RepositoryID: 4,
					ManifestID:   4,
				},
				{
					ID:           5,
					NamespaceID:  1,
					Name:         "stable-9ede8db0",
					RepositoryID: 4,
					ManifestID:   3,
				},
			},
		},
		{
			name:     "1st part",
			limit:    2,
			lastName: "",
			expectedTags: models.Tags{
				{
					ID:           4,
					NamespaceID:  1,
					Name:         "1.0.0",
					RepositoryID: 4,
					ManifestID:   3,
				},
				{
					ID:           8,
					NamespaceID:  1,
					Name:         "rc2",
					RepositoryID: 4,
					ManifestID:   7,
				},
			},
		},
		{
			name:     "nth part",
			limit:    1,
			lastName: "rc2",
			expectedTags: models.Tags{
				{
					ID:           6,
					NamespaceID:  1,
					Name:         "stable-91ac07a9",
					RepositoryID: 4,
					ManifestID:   4,
				},
			},
		},
		{
			name:     "last part",
			limit:    100,
			lastName: "stable-91ac07a9",
			expectedTags: models.Tags{
				{
					ID:           5,
					NamespaceID:  1,
					Name:         "stable-9ede8db0",
					RepositoryID: 4,
					ManifestID:   3,
				},
			},
		},
		{
			name:     "non existent last name",
			limit:    100,
			lastName: "does-not-exist",
			expectedTags: models.Tags{
				{
					ID:           8,
					NamespaceID:  1,
					Name:         "rc2",
					RepositoryID: 4,
					ManifestID:   7,
				},
				{
					ID:           6,
					NamespaceID:  1,
					Name:         "stable-91ac07a9",
					RepositoryID: 4,
					ManifestID:   4,
				},
				{
					ID:           5,
					NamespaceID:  1,
					Name:         "stable-9ede8db0",
					RepositoryID: 4,
					ManifestID:   3,
				},
			},
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			filters := datastore.FilterParams{
				MaxEntries: test.limit,
				LastEntry:  test.lastName,
			}

			rr, err := s.TagsPaginated(suite.ctx, r, filters)
			// reset created_at and updated_at attributes for reproducible comparisons
			for _, r := range rr {
				r.CreatedAt = time.Time{}
				r.UpdatedAt = sql.NullTime{}
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedTags, rr)
		})
	}
}

func TestRepositoryStore_TagsCountAfterName(t *testing.T) {
	reloadTagFixtures(t)

	// see testdata/fixtures/tags.sql (sorted):
	// 1.0.0
	// rc2
	// stable-91ac07a9
	// stable-9ede8db0
	r := &models.Repository{NamespaceID: 1, ID: 4}

	tt := []struct {
		name          string
		lastName      string
		expectedCount int
	}{
		{
			name:          "all",
			lastName:      "",
			expectedCount: 4,
		},
		{
			name:          "first",
			lastName:      "1.0.0",
			expectedCount: 3,
		},
		{
			name:          "last",
			lastName:      "stable-9ede8db0",
			expectedCount: 0,
		},
		{
			name:          "non existent",
			lastName:      "does-not-exist",
			expectedCount: 3,
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			filters := datastore.FilterParams{
				LastEntry: test.lastName,
			}

			c, err := s.TagsCountAfterName(suite.ctx, r, filters)
			require.NoError(t, err)
			require.Equal(t, test.expectedCount, c)
		})
	}
}

func TestRepositoryStore_ManifestTags(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	tt, err := s.ManifestTags(suite.ctx, &models.Repository{NamespaceID: 1, ID: 3}, &models.Manifest{NamespaceID: 1, ID: 1})
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	local := tt[0].CreatedAt.Location()
	expected := models.Tags{
		{
			ID:           1,
			NamespaceID:  1,
			Name:         "1.0.0",
			RepositoryID: 3,
			ManifestID:   1,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:43.283783", local),
		},
	}
	require.Equal(t, expected, tt)
}

func TestRepositoryStore_Count(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	count, err := s.Count(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	require.Equal(t, 16, count)
}

func TestRepositoryStore_CountAfterPath(t *testing.T) {
	reloadManifestFixtures(t)

	tt := []struct {
		name string
		path string

		// see testdata/fixtures/[repositories|repository_manifests].sql:
		//
		// 		gitlab-org 								(0 manifests, 0 manifest lists)
		// 		gitlab-org/gitlab-test 					(0 manifests, 0 manifest lists)
		// 		gitlab-org/gitlab-test/backend 			(2 manifests, 1 manifest list)
		// 		gitlab-org/gitlab-test/frontend 		(2 manifests, 1 manifest list)
		// 		a-test-group 							(0 manifests, 0 manifest lists)
		// 		a-test-group/foo  						(1 manifests, 0 manifest lists)
		// 		a-test-group/bar 						(0 manifests, 1 manifest list)
		//		usage-group								(0 manifests, 0 manifest lists)
		//		usage-group/sub-group-1					(0 manifests, 0 manifest lists)
		//		usage-group/sub-group-1/repository-1	(5 manifests, 2 manifest lists)
		expectedNumRepos int
	}{
		{
			name: "all",
			path: "",
			// all non-empty repositories (9) are lexicographically after ""
			expectedNumRepos: 9,
		},
		{
			name: "first",
			path: "a-test-group/bar",
			// there are 8 non-empty repositories lexicographically after "a-test-group/bar"
			expectedNumRepos: 8,
		},
		{
			name: "last",
			path: "gitlab-org/gitlab-test/frontend",
			// there are 5 repositories lexicographically after "gitlab-org/gitlab-test/frontend"
			expectedNumRepos: 5,
		},
		{
			name: "non existent",
			path: "does-not-exist",
			// there are 7 non-empty repositories lexicographically after "does-not-exist"
			expectedNumRepos: 7,
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			c, err := s.CountAfterPath(suite.ctx, test.path)
			require.NoError(t, err)
			require.Equal(t, test.expectedNumRepos, c)
		})
	}
}

func TestRepositoryStore_CountAfterPath_NoRepositories(t *testing.T) {
	unloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	c, err := s.CountAfterPath(suite.ctx, "")
	require.NoError(t, err)
	require.Equal(t, 0, c)
}

func TestRepositoryStore_CountPathSubRepositories(t *testing.T) {
	reloadManifestFixtures(t)

	tt := []struct {
		name string
		path string
		// see testdata/fixtures/[repositories|repository_manifests].sql:
		//
		// 		gitlab-org 								(0 manifests, 0 manifest lists)
		// 		gitlab-org/gitlab-test 					(0 manifests, 0 manifest lists)
		// 		gitlab-org/gitlab-test/backend 			(2 manifests, 1 manifest list)
		// 		gitlab-org/gitlab-test/frontend 		(2 manifests, 1 manifest list)
		// 		a-test-group 							(0 manifests, 0 manifest lists)
		// 		a-test-group/foo  						(1 manifests, 0 manifest lists)
		// 		a-test-group/bar 						(0 manifests, 1 manifest list)
		namespaceID      int64
		expectedNumRepos int
	}{
		{
			name:             "non existent path",
			path:             "non-existent",
			namespaceID:      1,
			expectedNumRepos: 0,
		},
		{
			name:             "path with only one repository",
			path:             "a-test-group/bar",
			namespaceID:      2,
			expectedNumRepos: 1,
		},
		{
			name:             "path with more than one repository",
			path:             "gitlab-org",
			namespaceID:      1,
			expectedNumRepos: 4,
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			c, err := s.CountPathSubRepositories(suite.ctx, test.namespaceID, test.path)
			require.NoError(t, err)
			require.Equal(t, test.expectedNumRepos, c)
		})
	}
}

func TestRepositoryStore_FindManifestByDigest(t *testing.T) {
	reloadManifestFixtures(t)

	d := digest.Digest("sha256:56b4b2228127fd594c5ab2925409713bd015ae9aa27eef2e0ddd90bcb2b1533f")
	s := datastore.NewRepositoryStore(suite.db)

	m, err := s.FindManifestByDigest(suite.ctx, &models.Repository{NamespaceID: 1, ID: 3}, d)
	require.NoError(t, err)
	require.NotNil(t, m)
	// see testdata/fixtures/repository_manifests.sql
	expected := &models.Manifest{
		ID:            2,
		NamespaceID:   1,
		RepositoryID:  3,
		TotalSize:     82384923,
		SchemaVersion: 2,
		MediaType:     "application/vnd.docker.distribution.manifest.v2+json",
		Digest:        d,
		Payload:       models.Payload(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1819,"digest":"sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073"},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":2802957,"digest":"sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":108,"digest":"sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":109,"digest":"sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1"}]}`),
		Configuration: &models.Configuration{
			MediaType: "application/vnd.docker.container.image.v1+json",
			Digest:    "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
			Payload:   models.Payload(`{"architecture":"amd64","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh"],"ArgsEscaped":true,"Image":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"container":"cb78c8a8058712726096a7a8f80e6a868ffb514a07f4fef37639f42d99d997e4","container_config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh","-c","echo \"2\" \u003e\u003e /data"],"Image":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"created":"2020-03-02T12:24:16.7039823Z","docker_version":"19.03.5","history":[{"created":"2020-01-18T01:19:37.02673981Z","created_by":"/bin/sh -c #(nop) ADD file:e69d441d729412d24675dcd33e04580885df99981cec43de8c9b24015313ff8e in / "},{"created":"2020-01-18T01:19:37.187497623Z","created_by":"/bin/sh -c #(nop)  CMD [\"/bin/sh\"]","empty_layer":true},{"created":"2020-03-02T12:21:53.8027967Z","created_by":"/bin/sh -c echo \"1\" \u003e /data"},{"created":"2020-03-02T12:24:16.7039823Z","created_by":"/bin/sh -c echo \"2\" \u003e\u003e /data"}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:5216338b40a7b96416b8b9858974bbe4acc3096ee60acbc4dfb1ee02aecceb10","sha256:99cb4c5d9f96432a00201f4b14c058c6235e563917ba7af8ed6c4775afa5780f","sha256:6322c07f5c6ad456f64647993dfc44526f4548685ee0f3d8f03534272b3a06d8"]}}`),
		},
		CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:50:26.461745", m.CreatedAt.Location()),
	}
	require.Equal(t, expected, m)
}

func TestRepositoryStore_FindManifestByTagName(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	m, err := s.FindManifestByTagName(suite.ctx, &models.Repository{NamespaceID: 1, ID: 3}, "latest")
	require.NoError(t, err)
	require.NotNil(t, m)

	// see testdata/fixtures/repository_manifests.sql
	expected := &models.Manifest{
		ID:            2,
		NamespaceID:   1,
		RepositoryID:  3,
		TotalSize:     82384923,
		SchemaVersion: 2,
		MediaType:     "application/vnd.docker.distribution.manifest.v2+json",
		Digest:        "sha256:56b4b2228127fd594c5ab2925409713bd015ae9aa27eef2e0ddd90bcb2b1533f",
		Payload:       models.Payload(`{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1819,"digest":"sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073"},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":2802957,"digest":"sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":108,"digest":"sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":109,"digest":"sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1"}]}`),
		Configuration: &models.Configuration{
			MediaType: "application/vnd.docker.container.image.v1+json",
			Digest:    "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
			Payload:   models.Payload(`{"architecture":"amd64","config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh"],"ArgsEscaped":true,"Image":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"container":"cb78c8a8058712726096a7a8f80e6a868ffb514a07f4fef37639f42d99d997e4","container_config":{"Hostname":"","Domainname":"","User":"","AttachStdin":false,"AttachStdout":false,"AttachStderr":false,"Tty":false,"OpenStdin":false,"StdinOnce":false,"Env":["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"],"Cmd":["/bin/sh","-c","echo \"2\" \u003e\u003e /data"],"Image":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9","Volumes":null,"WorkingDir":"","Entrypoint":null,"OnBuild":null,"Labels":null},"created":"2020-03-02T12:24:16.7039823Z","docker_version":"19.03.5","history":[{"created":"2020-01-18T01:19:37.02673981Z","created_by":"/bin/sh -c #(nop) ADD file:e69d441d729412d24675dcd33e04580885df99981cec43de8c9b24015313ff8e in / "},{"created":"2020-01-18T01:19:37.187497623Z","created_by":"/bin/sh -c #(nop)  CMD [\"/bin/sh\"]","empty_layer":true},{"created":"2020-03-02T12:21:53.8027967Z","created_by":"/bin/sh -c echo \"1\" \u003e /data"},{"created":"2020-03-02T12:24:16.7039823Z","created_by":"/bin/sh -c echo \"2\" \u003e\u003e /data"}],"os":"linux","rootfs":{"type":"layers","diff_ids":["sha256:5216338b40a7b96416b8b9858974bbe4acc3096ee60acbc4dfb1ee02aecceb10","sha256:99cb4c5d9f96432a00201f4b14c058c6235e563917ba7af8ed6c4775afa5780f","sha256:6322c07f5c6ad456f64647993dfc44526f4548685ee0f3d8f03534272b3a06d8"]}}`),
		},
		CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:50:26.461745", m.CreatedAt.Location()),
	}
	require.Equal(t, expected, m)
}

func TestRepositoryStore_FindManifestByTagName_NotFound(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	m, err := s.FindManifestByTagName(suite.ctx, &models.Repository{NamespaceID: 1, ID: 3}, "foo")
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestRepositoryManifestService_ManifestExists(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// See testdata/fixtures/{manifests,repositories}.sql
	rms := &datastore.RepositoryManifestService{
		RepositoryReader: s,
		RepositoryPath:   "gitlab-org/gitlab-test/backend",
	}

	ok, err := rms.Exists(suite.ctx, "sha256:56b4b2228127fd594c5ab2925409713bd015ae9aa27eef2e0ddd90bcb2b1533f")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRepositoryManifestService_ManifestExists_NotFound(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// See testdata/fixtures/{manifests,repositories}.sql
	rms := &datastore.RepositoryManifestService{
		RepositoryReader: s,
		RepositoryPath:   "gitlab-org/gitlab-test/backend",
	}

	ok, err := rms.Exists(suite.ctx, "sha256:4f4f2828206afd685c3ab9925409777bd015ae9cc27ddf2e0ddb90bcb2b1624c")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRepositoryStore_FindTagByName(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	tag, err := s.FindTagByName(suite.ctx, &models.Repository{NamespaceID: 1, ID: 4}, "1.0.0")
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	expected := &models.Tag{
		ID:           4,
		NamespaceID:  1,
		Name:         "1.0.0",
		RepositoryID: 4,
		ManifestID:   3,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:46.283783", tag.CreatedAt.Location()),
	}
	require.Equal(t, expected, tag)
}

func TestRepositoryStore_Blobs(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test/backend")
	require.NoError(t, err)
	require.NotNil(t, r)

	bb, err := s.Blobs(suite.ctx, r)
	require.NoError(t, err)
	require.NotEmpty(t, bb)

	// see testdata/fixtures/repository_blobs.sql
	local := bb[0].CreatedAt.Location()
	expected := models.Blobs{
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
			Size:      2802957,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:05:35.338639", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21",
			Size:      108,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:05:35.338639", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1",
			Size:      109,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:06:32.856423", local),
		},
	}
	require.Equal(t, expected, bb)
}

func TestRepositoryStore_BlobsNone(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org")
	require.NoError(t, err)
	require.NotNil(t, r)

	// see testdata/fixtures/repository_blobs.sql
	bb, err := s.Blobs(suite.ctx, r)
	require.NoError(t, err)
	require.Empty(t, bb)
}

func TestRepositoryStore_FindBlobByDigest(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test/backend")
	require.NoError(t, err)
	require.NotNil(t, r)

	b, err := s.FindBlob(suite.ctx, r, "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.NoError(t, err)
	require.NotNil(t, b)

	// see testdata/fixtures/repository_blobs.sql
	local := b.CreatedAt.Location()
	expected := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
		Size:      2802957,
		CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:05:35.338639", local),
	}
	require.Equal(t, expected, b)
}

func TestRepositoryStore_FindBlobByDigest_NotFound(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org")
	require.NoError(t, err)
	require.NotNil(t, r)

	// see testdata/fixtures/repository_blobs.sql
	b, err := s.FindBlob(suite.ctx, r, "sha256:d9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.NoError(t, err)
	require.Nil(t, b)
}

func TestRepositoryStore_ExistsBlobByDigest(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test/backend")
	require.NoError(t, err)
	require.NotNil(t, r)

	// see testdata/fixtures/repository_blobs.sql
	exists, err := s.ExistsBlob(suite.ctx, r, "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.NoError(t, err)
	require.True(t, exists)
}

func TestRepositoryStore_ExistsBlobByDigest_NotFound(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org")
	require.NoError(t, err)
	require.NotNil(t, r)

	// see testdata/fixtures/repository_blobs.sql
	exists, err := s.ExistsBlob(suite.ctx, r, "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.NoError(t, err)
	require.False(t, exists)
}

// Here we use the test fixtures for the `usage-group/sub-group-1/repository-1` repository (see testdata/fixtures/*.sql).
// This repository was set up in the following way:
//
// **Layers:**
//
// | Identifier | Digest                                                                  | Size   |
// | ---------- | ----------------------------------------------------------------------- | ------ |
// | La         | sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31 | 468294 |
// | Lb         | sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568 | 428360 |
// | Lc         | sha256:cf15cd200b0d2358579e1b561ec750ba8230f86e34e45cff89547c1217959752 | 253193 |
// | Ld         | sha256:8cb22990f6b627016f2f2000d2f29da7c2bc87b80d21efb4f89ed148e00df6ee | 361786 |
// | Le         | sha256:ad4309f23d757351fba1698406f09c79667ecde8863dba39407cb915ebbe549d | 255232 |
// | Lf         | sha256:0159a862a1d3a25886b9f029af200f15a27bd0a5552b5861f34b1cb02cc14fb2 | 107728 |
// | Lg         | sha256:cdb2596a54a1c291f041b1c824e87f4c6ed282a69b42f18c60dc801818e8a144 | 146656 |
//
// **Manifests:**
//
// | Identifier | Digest                                                                  | References |
// | ---------- | ----------------------------------------------------------------------- | ---------- |
// | Ma         | sha256:85fe223d9762cb7c409635e4072bf52aa11d08fc55d0e7a61ac339fd2e41570f | La, Lb     |
// | Mb         | sha256:af468acedecdad7e7a40ecc7b497ca972ada9778911e340e51791a4a606dbc85 | La, Lb, Lc |
// | Mc         | sha256:557489fa71a8276bdfbbfb042e97eb3d5a72dcd7a6a4840824756e437775393d | Lb, Ld     |
// | Md         | sha256:0c3cf8ca7d3a3e72d804a5508484af4bcce14c184a344af7d72458ec91fb5708 | Le         |
// | Me         | sha256:59afc836e997438c844162d0216a3f3ae222560628df3d3608cb1c536ed9637b | Lf, Lg     |
//
// **Manifest Lists:**
//
// | Identifier | Digest                                                                  | References |
// | ---------- | ----------------------------------------------------------------------- | ---------- |
// | La         | sha256:47be6fe0d7fe76bd73bf8ab0b2a8a08c76814ca44cde20cea0f0073a5f3788e6 | Ma, Md     |
// | Lb         | sha256:624a638727aaa9b1fd5d7ebfcde3eb3771fb83ecf143ec1aa5965401d1573f2a | Me, La     |
//
// **Tags:**
//
// | Identifier | Target |
// | ---------- | ------ |
// | Ta         | Ma     |
// | Tb         | Mb     |
// | Tc         | La     |
// | Td         | Lb     |
//
// Based on the above, we know:
//
// - `Ma` is tagged, so we need to account for the size of `La` and `Lb`. The repository size so far is `1*La + 1*Lb`;
//
//   - `Mb` is tagged, so we need to account for the size of `Lc`. `La` and `Lb` were already accounted for once.
//     Therefore, the repository size so far is `1*La + 1*Lb + 1*Lc`;
//
// - `Mc` is not tagged, so `Ld` should not be accounted for, and `Lb` was already. The size formula remains unchanged;
//
//   - `La` is tagged and references `Ma` and `Md`. The `Ma` layers were already accounted, so we should only sum the size
//     of `Le` referenced by `Md`. The repository size is now `1*La + 1*Lb + 1*Lc + 1*Le`;
//
// - `Lb` is tagged and references `Me` and `La`. `La` was already accounted for, so we ignore it. `Me` is "new", and
// references `Lf` and `Lg`, which we haven't seen anywhere else. The final deduplicated repository size is threfore
// `1*La + 1*Lb + 1*Lc + 1*Le+ 1*Lf+ 1*Lg`, which equals to 1659463.
func TestRepositoryStore_Size(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.Size(suite.ctx, &models.Repository{NamespaceID: 3, ID: 10})
	require.NoError(t, err)
	require.Equal(t, int64(1659463), size)
}

func TestRepositoryStore_Size_Empty(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.Size(suite.ctx, &models.Repository{NamespaceID: 3, ID: 8})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_Size_NotFound(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.Size(suite.ctx, &models.Repository{NamespaceID: 3, ID: 100})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_Size_SingleRepositoryCache(t *testing.T) {
	reloadRepositoryFixtures(t)

	path := "a-test-group/foo"
	c := datastore.NewSingleRepositoryCache()

	ctx := context.Background()
	require.Nil(t, c.Get(ctx, path))

	s := datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(c))
	_, err := s.FindByPath(suite.ctx, path)
	require.NoError(t, err)
	require.NotNil(t, c.Get(ctx, path)) // see testdata/fixtures/repositories.sql

	// the size of an existing repo is initially nil in the cache
	// if no preceeding calls to `Size` have been made to populate
	// the calculated size attribute from the db into the cache.
	require.Nil(t, c.Get(ctx, path).Size)

	expectedSize, err := s.Size(suite.ctx,
		&models.Repository{Path: path, NamespaceID: 2, ID: 6}) // see testdata/fixtures/repositories.sql
	require.NotNil(t, c.Get(ctx, path))
	// the size attribute of an existing repo is calculated from the db
	// on the very first call to `Size`,  once calculated the size
	// attribute is pegged to the cache as well for future calls to utilize
	require.Equal(t, expectedSize, *c.Get(ctx, path).Size)
}

func TestRepositoryStore_Size_WithCentralRepositoryCache(t *testing.T) {
	reloadRepositoryFixtures(t)

	// First grab a valid repo present in a store (that only utilizes db and no cache)
	// see testdata/fixtures/repositories.sql for definitions of valid repos set up in the test's db.
	path := "a-test-group/foo"
	s := datastore.NewRepositoryStore(suite.db)
	expectedRepoFromDB, err := s.FindByPath(suite.ctx, path)
	require.NoError(t, err)
	require.NotNil(t, expectedRepoFromDB)
	// The size attribute of an existing repo is always set to  nil in the returned repo object
	// whenever the repo object is extracted directly from the db. Validation:
	require.Nil(t, expectedRepoFromDB.Size)

	// The size of an existing repo can be found by calling the `Size` function.
	// For a store (that only utilizes db and no cache). The calculation of the size is done directly on the db.
	expectedRepoSizeFromDB, err := s.Size(suite.ctx, expectedRepoFromDB)
	require.NoError(t, err)

	// Add a cache to the store and try fetching the repo again
	cache := datastore.NewCentralRepositoryCache(itestutil.RedisCache(t, 0))
	s = datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(cache))
	expectedRepoFromDB, err = s.FindByPath(suite.ctx, path)
	require.NoError(t, err)
	// Verify the repo object in the cache is identical to the one in the db:
	require.Equal(t, expectedRepoFromDB, cache.Get(suite.ctx, path))

	// The size of an existing repo is initially nil in the cache if no preceeding calls to `Size`
	// have been made to populate the calculated size attribute from the db into the cache
	// (after the cache was attatched to the store). Verify that the size attribute was not cached:
	require.Equal(t, expectedRepoFromDB.Size, cache.Get(suite.ctx, path).Size)

	// The size of an existing repo can be found by calling the `Size` function.
	// For a store (that utilizes both db and cache), the size attribute of an existing repo is calculated from the
	// db on the very first call to `Size` (after a cache was attatched). Once calculated from the db, the size attribute is
	// pegged to the repo object in the cache, which can respond to subsequent `Size` calls without accessing the db.
	_, err = s.Size(suite.ctx, expectedRepoFromDB)
	require.NoError(t, err)
	require.Equal(t, expectedRepoSizeFromDB, *cache.Get(suite.ctx, path).Size)
}

// This comment describes the repository size calculation in detail, explaining the results of the
// following calls to RepositoryStore.SizeWithDescendants.
//
// Here we use the test fixtures for the `usage-group` top-level repository (see testdata/fixtures/*.sql).
// This repository was set up in the following way:
//
// **Repositories:**
//
// | Identifier | Path                                                  |
// |------------|-------------------------------------------------------|
// | Ra         | usage-group                                           |
// | Rb         | usage-group/sub-group-1                               |
// | Rc         | usage-group/sub-group-1/repository-1                  |
// | Rd         | usage-group/sub-group-1/repository-2                  |
// | Re         | usage-group/sub-group-2                               |
// | Rg         | usage-group/sub-group-2/repository-1                  |
// | Rh         | usage-group/sub-group-2/repository-1/sub-repository-1 |
// | Ri         | usage-group-2                                         |
// | Rj         | usage-group-2/sub-group-1/project-1                   |
//
// **Layers:**
//
// | Identifier | Digest                                                                  | Size    |
// |------------|-------------------------------------------------------------------------|---------|
// | La         | sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31 | 468294  |
// | Lb         | sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568 | 428360  |
// | Lc         | sha256:cf15cd200b0d2358579e1b561ec750ba8230f86e34e45cff89547c1217959752 | 253193  |
// | Ld         | sha256:8cb22990f6b627016f2f2000d2f29da7c2bc87b80d21efb4f89ed148e00df6ee | 361786  |
// | Le         | sha256:ad4309f23d757351fba1698406f09c79667ecde8863dba39407cb915ebbe549d | 255232  |
// | Lf         | sha256:0159a862a1d3a25886b9f029af200f15a27bd0a5552b5861f34b1cb02cc14fb2 | 107728  |
// | Lg         | sha256:cdb2596a54a1c291f041b1c824e87f4c6ed282a69b42f18c60dc801818e8a144 | 146656  |
// | Lh         | sha256:52f7f1bb6469c3c075e08bf1d2f15ce51c9db79ee715d6649ce9b0d67c84b5ef | 563125  |
// | Li         | sha256:476a8fceb48f8f8db4dbad6c79d1087fb456950f31143a93577507f11cce789f | 421341  |
// | Lj         | sha256:eb5683307d3554d282fb9101ad7220cdfc81078b2da6dcb4a683698c972136c5 | 5462210 |
//
// **Manifests:**
//
// | Identifier | Repositories | Digest                                                                  | References |
// |------------|--------------|-------------------------------------------------------------------------|------------|
// | Ma         | Rb, Rc       | sha256:85fe223d9762cb7c409635e4072bf52aa11d08fc55d0e7a61ac339fd2e41570f | La, Lb     |
// | Mb         | Rc, Rd       | sha256:af468acedecdad7e7a40ecc7b497ca972ada9778911e340e51791a4a606dbc85 | La, Lb, Lc |
// | Mc         | Rc           | sha256:557489fa71a8276bdfbbfb042e97eb3d5a72dcd7a6a4840824756e437775393d | Lb, Ld     |
// | Md         | Rc, Rg       | sha256:0c3cf8ca7d3a3e72d804a5508484af4bcce14c184a344af7d72458ec91fb5708 | Le         |
// | Me         | Rc           | sha256:59afc836e997438c844162d0216a3f3ae222560628df3d3608cb1c536ed9637b | Lf, Lg     |
// | Mf         | Rc, Rg       | sha256:e05aa8bc6bd8f5298442bb036fdd7b57896ea4ae30213cd01a1a928cc5a3e98e | Lh         |
// | Mg         | Rh           | sha256:9199190e776bbfa0f9fbfb031bcba73546e063462cefc2aa0d425b65643c28ea | Li, Lj     |
//
// **Manifest Lists:**
//
// | Identifier | Repositories | Digest                                                                  | References |
// |------------|--------------|-------------------------------------------------------------------------|------------|
// | MLa        | Rc           | sha256:47be6fe0d7fe76bd73bf8ab0b2a8a08c76814ca44cde20cea0f0073a5f3788e6 | Ma, Md     |
// | MLb        | Rc           | sha256:624a638727aaa9b1fd5d7ebfcde3eb3771fb83ecf143ec1aa5965401d1573f2a | Me, MLa    |
//
// **Tags:**
//
// | Identifier | Repository | Target |
// |------------|------------|--------|
// | Ta         | Rc         | Ma     |
// | Tb         | Rc         | Mb     |
// | Tc         | Rc         | MLa    |
// | Td         | Rc         | MLb    |
// | Te         | Rd         | Mb     |
// | Tf         | Rg         | Md     |
// | Tg         | Rg         | Mf     |
// | Th         | Rh         | Mg     |
// | Ti         | Rb         | Ma     |
//
// Based on the above, we know:
//
// - `Ma` is tagged, so we need to account for the size of `La` and `Lb`. The repository size so far is `1*La + 1*Lb`;
//
//   - `Mb` is tagged, so we need to account for the size of `Lc`. `La` and `Lb` were already accounted for once.
//     Therefore, the repository size so far is `1*La + 1*Lb + 1*Lc`;
//
// - `Mc` is not tagged, so `Ld` should not be accounted for, and `Lb` was already. The size formula remains unchanged;
//
//   - `MLa` is tagged and references `Ma` and `Md`. The `Ma` layers were already accounted, so we should only sum the size
//     of `Le` referenced by `Md`. The repository size is now `1*La + 1*Lb + 1*Lc + 1*Le`;
//
// - `MLb` is tagged and references `Me` and `La`. `La` was already accounted for, so we ignore it. `Me` is "new", and
// references `Lf` and `Lg`, which we haven't seen anywhere else. The repository size is now
// `1*La + 1*Lb + 1*Lc + 1*Le+ 1*Lf+ 1*Lg`;
//
// - `Mg` is tagged and references `Li` and `Lj`, which did not appear before. The final repository size is therefore
// `1*La + 1*Lb + 1*Lc + 1*Le+ 1*Lf+ 1*Lg + 1*Li + 1*Lj`, which equals to 7543014;
func TestRepositoryStore_SizeWithDescendants_TopLevel(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.SizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 3, ID: 8, Path: "usage-group"})
	require.NoError(t, err)
	require.Equal(t, int64(7543014), size)
}

// Here we use the test fixtures for the `usage-group/sub-group-1` repository (see testdata/fixtures/*.sql). See the
// inline documentation for TestRepositoryStore_SizeWithDescendants_TopLevel for a breakdown of the test repositories,
// their contents, and the rationale behind the expected size.
//
// Based on that, we know that `Ma`, `Mb`, `Mc`, `Md` and `Me` are all tagged (directly or indirectly, once or multiple
// times). Therefore, the repository size is `1*La + 1*Lb + 1*Lc + 1*Le + 1*Lf + 1*Lg`, which is 1659463.
func TestRepositoryStore_SizeWithDescendants_NonTopLevel(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.SizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 3, ID: 9, Path: "usage-group/sub-group-1"})
	require.NoError(t, err)
	require.Equal(t, int64(1659463), size)
}

func TestRepositoryStore_SizeWithDescendants_TopLevelEmpty(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.SizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 4, ID: 15, Path: "usage-group-2"})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_SizeWithDescendants_NonTopLevelEmpty(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.SizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 4, ID: 16, Path: "usage-group-2/sub-group-1/project-1"})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_SizeWithDescendants_TopLevelNotFound(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.SizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 100, ID: 1000, Path: "foo"})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_SizeWithDescendants_NonTopLevelNotFound(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.SizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 100, ID: 1001, Path: "foo/bar"})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_SizeWithDescendants_TopLevel_ChecksCacheForPreviousTimeout(t *testing.T) {
	reloadManifestFixtures(t)

	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	cache := datastore.NewCentralRepositoryCache(redisCache)

	s := datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(cache))

	repo := &models.Repository{NamespaceID: 3, ID: 8, Path: "usage-group"}
	redisKey := fmt.Sprintf("registry:db:{repository:%s:%s}:swd-timeout", repo.Path, digest.FromString(repo.Path).Hex())

	// Checks Redis to see if the latest invocation has failed. Proceeds with query execution if not.
	redisMock.ExpectGet(redisKey).RedisNil()

	size, err := s.SizeWithDescendants(suite.ctx, repo)
	require.NoError(t, err)
	require.Equal(t, int64(7543014), size)

	// Checks Redis to see if the latest invocation has failed. Halts if so.
	redisMock.ExpectGet(redisKey).SetVal("value does not matter")

	size, err = s.SizeWithDescendants(suite.ctx, repo)
	require.ErrorIs(t, err, datastore.ErrSizeHasTimedOut)
	require.Zero(t, size)
}

func TestRepositoryStore_SizeWithDescendants_TopLevel_SetsCacheOnTimeout(t *testing.T) {
	reloadManifestFixtures(t)

	redisCache, redisMock := itestutil.RedisCacheMock(t, 0)
	cache := datastore.NewCentralRepositoryCache(redisCache)

	// use transaction with a statement timeout of 1ms, so that all queries within time out
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = tx.ExecContext(suite.ctx, "SET statement_timeout TO 1")
	require.NoError(t, err)
	// wait a bit so that PG has time to flush the update
	time.Sleep(250 * time.Millisecond)

	s := datastore.NewRepositoryStore(tx, datastore.WithRepositoryCache(cache))

	repo := &models.Repository{NamespaceID: 3, ID: 8, Path: "usage-group"}
	redisKey := fmt.Sprintf("registry:db:{repository:%s:%s}:swd-timeout", repo.Path, digest.FromString(repo.Path).Hex())

	redisMock.ExpectGet(redisKey).RedisNil()
	redisMock.ExpectSet(redisKey, "true", 24*time.Hour).SetVal("true")

	size, err := s.SizeWithDescendants(suite.ctx, repo)
	require.NotNil(t, err)

	// make sure the error is not masked
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, pgErr.Code, pgerrcode.QueryCanceled)
	require.Zero(t, size)
}

func TestRepositoryStore_SizeWithDescendants_NonTopLevel_DoesNotTouchCacheTimeout(t *testing.T) {
	reloadManifestFixtures(t)

	redisCache, _ := itestutil.RedisCacheMock(t, 0)
	cache := datastore.NewCentralRepositoryCache(redisCache)

	repo := &models.Repository{NamespaceID: 3, ID: 9, Path: "usage-group/sub-group-1"}

	// Test that the cache is not read before a successful query. There are no expectations set on the redis mock, so
	// this would fail if it got called.
	s := datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(cache))
	size, err := s.SizeWithDescendants(suite.ctx, repo)
	require.NoError(t, err)
	require.Equal(t, int64(1659463), size)

	// Test that the cache is not set after a failed query. Use transaction with a statement timeout of 1ms, so that all
	// queries within time out.
	tx, err := suite.db.BeginTx(suite.ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = tx.ExecContext(suite.ctx, "SET statement_timeout TO 1")
	require.NoError(t, err)
	// wait a bit so that PG has time to flush the update
	time.Sleep(250 * time.Millisecond)

	s = datastore.NewRepositoryStore(tx, datastore.WithRepositoryCache(cache))
	size, err = s.SizeWithDescendants(suite.ctx, repo)
	// make sure the error is not masked
	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	require.Equal(t, pgErr.Code, pgerrcode.QueryCanceled)
	require.Zero(t, size)
}

// TestRepositoryStore_EstimatedSizeWithDescendants_TopLevel is similar to
// TestRepositoryStore_SizeWithDescendants_TopLevel (see its description for details), but here we expect the returned
// size to be the sum of all layers, including the unreferenced ones. The expected repository size is therefore
// `1*La + 1*Lb + 1*Lc + 1*Ld + 1*Le+ 1*Lf+ 1*Lg + 1*Lh + 1*Li + 1*Lj`, which equals to 8467925.
func TestRepositoryStore_EstimatedSizeWithDescendants_TopLevel(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.EstimatedSizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 3, ID: 8, Path: "usage-group"})
	require.NoError(t, err)
	require.Equal(t, int64(8467925), size)
}

func TestRepositoryStore_EstimatedSizeWithDescendants_TopLevelNotFound(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.EstimatedSizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 100, ID: 1000, Path: "foo"})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_EstimatedSizeWithDescendants_TopLevelEmpty(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.EstimatedSizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 4, ID: 15, Path: "usage-group-2"})
	require.NoError(t, err)
	require.Zero(t, size)
}

func TestRepositoryStore_EstimatedSizeWithDescendants_NonTopLevel(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	size, err := s.EstimatedSizeWithDescendants(suite.ctx, &models.Repository{NamespaceID: 3, ID: 9, Path: "usage-group/sub-group-1"})
	require.ErrorIs(t, err, datastore.ErrOnlyRootEstimates)
	require.Zero(t, size)
}

func TestRepositoryBlobService_Stat(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// See testdata/fixtures/{repository_blobs,repositories}.sql
	rbs := &datastore.RepositoryBlobService{
		RepositoryReader: s,
		RepositoryPath:   "a-test-group/bar",
	}

	dgst := digest.Digest("sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21")

	desc, err := rbs.Stat(suite.ctx, dgst)
	require.NoError(t, err)
	require.EqualValues(t, distribution.Descriptor{Digest: dgst, Size: int64(108), MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip"}, desc)
}

func TestRepositoryBlobService_Stat_NotFound(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// See testdata/fixtures/{repository_blobs,repositories}.sql
	rbs := &datastore.RepositoryBlobService{
		RepositoryReader: s,
		RepositoryPath:   "a-test-group/bar",
	}

	desc, err := rbs.Stat(suite.ctx, "sha256:fe0982e263ce911b75630b823fab12836fe51bda5f7db834020edc1390b19a45")
	require.EqualError(t, err, distribution.ErrBlobUnknown.Error())
	require.Equal(t, distribution.Descriptor{}, desc)
}

func TestRepositoryStore_Create(t *testing.T) {
	unloadRepositoryFixtures(t)
	reloadNamespaceFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r := &models.Repository{
		NamespaceID: 1,
		Name:        "bar",
		Path:        "gitlab-org/bar",
	}
	err := s.Create(suite.ctx, r)

	require.NoError(t, err)
	require.NotEmpty(t, r.ID)
	require.NotEmpty(t, r.CreatedAt)
	require.Equal(t, migration.RepositoryStatusNative, r.MigrationStatus)
}

func TestRepositoryStore_Create_NonUniquePathFails(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	r := &models.Repository{
		NamespaceID: 1,
		Name:        "gitlab-test",
		Path:        "gitlab-org/gitlab-test",
		ParentID:    sql.NullInt64{Int64: 1, Valid: true},
	}
	err := s.Create(suite.ctx, r)
	require.Error(t, err)
}

func TestRepositoryStore_CreateOrFindByPath(t *testing.T) {
	unloadRepositoryFixtures(t)
	s := datastore.NewRepositoryStore(suite.db)

	// validate return
	r, err := s.CreateOrFindByPath(suite.ctx, "a")
	require.NoError(t, err)
	require.NotNil(t, r)
	require.NotEmpty(t, r.ID)
	require.NotEmpty(t, r.NamespaceID)
	require.Equal(t, "a", r.Name)
	require.Equal(t, "a", r.Path)
	require.NotEmpty(t, r.CreatedAt)

	// validate database state
	actual, err := s.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Len(t, actual, 1)
	require.Equal(t, r, actual[0])
}

func TestRepositoryStore_CreateOrFindByPath_ExistingDoesNotFail(t *testing.T) {
	unloadRepositoryFixtures(t)
	s := datastore.NewRepositoryStore(suite.db)

	r, err := s.CreateByPath(suite.ctx, "a")
	require.NoError(t, err)

	// validate return
	r2, err := s.CreateOrFindByPath(suite.ctx, "a")
	require.NoError(t, err)
	require.NotNil(t, r2)
	require.NotEmpty(t, r2.ID)
	require.NotEmpty(t, r2.NamespaceID)
	require.Equal(t, "a", r2.Name)
	require.Equal(t, "a", r2.Path)
	require.NotEmpty(t, r2.CreatedAt)

	// validate database state
	actual, err := s.FindAll(suite.ctx)
	require.NoError(t, err)
	require.Len(t, actual, 1)
	require.Equal(t, r, actual[0])
}

func TestRepositoryStore_CreateOrFindByPath_SingleRepositoryCache(t *testing.T) {
	unloadRepositoryFixtures(t)
	c := datastore.NewSingleRepositoryCache()
	s := datastore.NewRepositoryStore(suite.db, datastore.WithRepositoryCache(c))

	// Create a new repository, filling the cache.
	r1, err := s.CreateOrFindByPath(suite.ctx, "pineapple/banana")
	require.NoError(t, err)
	ctx := context.Background()
	require.Equal(t, r1, c.Get(ctx, r1.Path))

	// Create another new repository, replacing the old cache value.
	r2, err := s.CreateOrFindByPath(suite.ctx, "kiwi/mango")
	require.NoError(t, err)
	require.NotEqual(t, r1, c.Get(ctx, r1.Path))
	require.NotEqual(t, r2, c.Get(ctx, r1.Path))
	require.Equal(t, r2, c.Get(ctx, r2.Path))
}

func TestRepositoryStore_CreateOrFind(t *testing.T) {
	unloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// create non existing `foo/bar`
	r := &models.Repository{
		Name: "bar",
		Path: "foo/bar",
	}
	err := s.CreateOrFind(suite.ctx, r)
	require.NoError(t, err)
	require.NotEmpty(t, r.ID)
	require.NotEmpty(t, r.NamespaceID)
	require.Equal(t, "bar", r.Name)
	require.Equal(t, "foo/bar", r.Path)
	require.NotEmpty(t, r.CreatedAt)

	// attempt to create existing `foo/bar`
	r2 := &models.Repository{
		Name: "bar",
		Path: "foo/bar",
	}
	err = s.CreateOrFind(suite.ctx, r2)
	require.NoError(t, err)
	require.Equal(t, r, r2)
}

func TestRepositoryStore_Update(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)
	update := &models.Repository{
		NamespaceID:     1,
		ID:              4,
		Name:            "bar",
		Path:            "bar",
		ParentID:        sql.NullInt64{Int64: 0, Valid: false},
		MigrationStatus: migration.RepositoryStatusPreImportInProgress,
		MigrationError: sql.NullString{
			String: "something went wrong", Valid: true,
		},
	}
	err := s.Update(suite.ctx, update)
	require.NoError(t, err)

	r, err := s.FindByPath(suite.ctx, "bar")
	require.NoError(t, err)

	update.CreatedAt = r.CreatedAt
	require.Equal(t, update, r)
}

func TestRepositoryStore_Update_MigrationError_LengthConstraint(t *testing.T) {
	reloadRepositoryFixtures(t)
	// The check_repositories_migration_error_length constraint allows strings of up to 255 characters, this string is 256 chars long so it will trigger the error
	longString := "ad165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155ad165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155ad165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155ad165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155"
	s := datastore.NewRepositoryStore(suite.db)
	update := &models.Repository{
		NamespaceID:     1,
		ID:              4,
		Name:            "bar",
		Path:            "bar",
		ParentID:        sql.NullInt64{Int64: 0, Valid: false},
		MigrationStatus: migration.RepositoryStatusPreImportInProgress,
		MigrationError: sql.NullString{
			String: longString, Valid: true,
		},
	}

	err := s.Update(suite.ctx, update)
	// we truncate the value of error using `left($1, 255)` so no error is expected
	require.NoError(t, err)
}

func TestRepositoryStore_Update_NotFound(t *testing.T) {
	s := datastore.NewRepositoryStore(suite.db)

	update := &models.Repository{
		ID:   100,
		Name: "bar",
	}
	err := s.Update(suite.ctx, update)
	require.EqualError(t, err, "repository not found")
}

func isBlobLinked(t *testing.T, r *models.Repository, d digest.Digest) bool {
	t.Helper()

	s := datastore.NewRepositoryStore(suite.db)
	linked, err := s.ExistsBlob(suite.ctx, r, d)
	require.NoError(t, err)

	return linked
}

func TestRepositoryStore_LinkLayer(t *testing.T) {
	reloadBlobFixtures(t)
	require.NoError(t, testutil.TruncateTables(suite.db, testutil.RepositoryBlobsTable))

	s := datastore.NewRepositoryStore(suite.db)

	r := &models.Repository{NamespaceID: 1, ID: 3}
	d := digest.Digest("sha256:68ced04f60ab5c7a5f1d0b0b4e7572c5a4c8cce44866513d30d9df1a15277d6b")

	err := s.LinkBlob(suite.ctx, r, d)
	require.NoError(t, err)

	require.True(t, isBlobLinked(t, r, d))
}

func TestRepositoryStore_LinkBlob_AlreadyLinkedDoesNotFail(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/repository_blobs.sql
	r := &models.Repository{NamespaceID: 1, ID: 3}
	d := digest.Digest("sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1")
	require.True(t, isBlobLinked(t, r, d))

	err := s.LinkBlob(suite.ctx, r, d)
	require.NoError(t, err)
}

func TestRepositoryStore_UnlinkBlob(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/repository_blobs.sql
	r := &models.Repository{NamespaceID: 1, ID: 3}
	d := digest.Digest("sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1")
	require.True(t, isBlobLinked(t, r, d))

	found, err := s.UnlinkBlob(suite.ctx, r, d)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, isBlobLinked(t, r, d))
}

func TestRepositoryStore_UnlinkBlob_NotLinkedDoesNotFail(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/repository_blobs.sql
	r := &models.Repository{NamespaceID: 1, ID: 3}
	d := digest.Digest("sha256:68ced04f60ab5c7a5f1d0b0b4e7572c5a4c8cce44866513d30d9df1a15277d6b")

	found, err := s.UnlinkBlob(suite.ctx, r, d)
	require.NoError(t, err)
	require.False(t, found)
	require.False(t, isBlobLinked(t, r, d))
}

func TestRepositoryStore_DeleteTagByName(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/tags.sql
	r := &models.Repository{NamespaceID: 1, ID: 3}
	name := "1.0.0"

	found, err := s.DeleteTagByName(suite.ctx, r, name)
	require.NoError(t, err)
	require.True(t, found)

	tag, err := s.FindTagByName(suite.ctx, r, name)
	require.NoError(t, err)
	require.Nil(t, tag)
}

func TestRepositoryStore_DeleteTagByName_NotFoundDoesNotFail(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/repository_blobs.sql
	r := &models.Repository{NamespaceID: 1, ID: 3}
	name := "10.0.0"

	found, err := s.DeleteTagByName(suite.ctx, r, name)
	require.NoError(t, err)
	require.False(t, found)
}

func TestRepositoryStore_DeleteManifest(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/manifests.sql
	r := &models.Repository{NamespaceID: 1, ID: 4}
	d := digest.Digest("sha256:ea1650093606d9e76dfc78b986d57daea6108af2d5a9114a98d7198548bfdfc7")

	found, err := s.DeleteManifest(suite.ctx, r, d)
	require.NoError(t, err)
	require.True(t, found)

	m, err := s.FindManifestByDigest(suite.ctx, r, d)
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestRepositoryStore_DeleteManifest_FailsIfReferencedInList(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	// see testdata/fixtures/manifests.sql
	r := &models.Repository{NamespaceID: 1, ID: 3}
	d := digest.Digest("sha256:bd165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155")

	ok, err := s.DeleteManifest(suite.ctx, r, d)
	require.EqualError(t, err, fmt.Errorf("deleting manifest: %w", datastore.ErrManifestReferencedInList).Error())
	require.False(t, ok)

	// make sure the manifest was not deleted
	m, err := s.FindManifestByDigest(suite.ctx, r, d)
	require.NoError(t, err)
	require.NotNil(t, m)
}

func TestRepositoryStore_DeleteManifest_NotFoundDoesNotFail(t *testing.T) {
	reloadRepositoryFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	r := &models.Repository{NamespaceID: 1, ID: 3}
	d := digest.Digest("sha256:ad165db4bd480656a539e8e00db265377d162d6b98eebbfe5805d0fbd5144155")

	found, err := s.DeleteManifest(suite.ctx, r, d)
	require.NoError(t, err)
	require.False(t, found)
}

func softDeleteRepository(ctx context.Context, db *datastore.DB, r *models.Repository) error {
	q := `UPDATE
			repositories
		SET
			deleted_at = now()
		WHERE
			top_level_namespace_id = $1
			AND id = $2
		RETURNING
			deleted_at` // Return deleted_at here for validation purposes

	row := db.QueryRowContext(ctx, q, r.NamespaceID, r.ID)
	if err := row.Scan(&r.DeletedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("repository not found")
		}
		return fmt.Errorf("soft deleting repository: %w", err)
	}

	return nil
}

func TestSoftDeleteRepository(t *testing.T) {
	reloadRepositoryFixtures(t)

	// grab a random repository and soft delete it
	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)
	require.NotNil(t, r)

	err = softDeleteRepository(suite.ctx, suite.db, r)
	require.NoError(t, err)
	require.True(t, r.DeletedAt.Valid)
	require.NotZero(t, r.DeletedAt.Time)
}

func TestRepositoryStore_FindByPath_SoftDeleted(t *testing.T) {
	reloadRepositoryFixtures(t)

	// grab a random repository and soft delete it
	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)
	require.NotNil(t, r)

	err = softDeleteRepository(suite.ctx, suite.db, r)
	require.NoError(t, err)

	// confirm that attempting to read the soft-deleted repositories yields nothing
	r, err = s.FindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestRepositoryStore_CreateOrFindByPath_SoftDeleted(t *testing.T) {
	reloadRepositoryFixtures(t)

	// grab a random repository and soft delete it
	s := datastore.NewRepositoryStore(suite.db)
	r, err := s.FindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)
	require.NotNil(t, r)

	err = softDeleteRepository(suite.ctx, suite.db, r)
	require.NoError(t, err)

	// attempt to create repository, there should be no error and the soft delete must be reverted
	r, err = s.CreateOrFindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)
	require.NotNil(t, r)
	require.False(t, r.DeletedAt.Valid)
	require.Zero(t, r.DeletedAt.Time)

	// ensure that the `migration_status` is also updated to a new value (if applicable)
	require.Equal(t, migration.RepositoryStatusNative, r.MigrationStatus)
	err = softDeleteRepository(suite.ctx, suite.db, r)
	require.NoError(t, err)

	r, err = s.CreateOrFindByPath(
		suite.ctx,
		"gitlab-org/gitlab-test",
		datastore.WithMigrationStatus(migration.RepositoryStatusPreImportInProgress),
	)

	r, err = s.FindByPath(suite.ctx, "gitlab-org/gitlab-test")
	require.NoError(t, err)
	require.Equal(t, migration.RepositoryStatusPreImportInProgress, r.MigrationStatus)
}

func TestRepositoryStore_TagsDetailPaginated(t *testing.T) {
	reloadTagFixtures(t)

	// see testdata/fixtures/tags.sql (sorted):
	// 1.0.0
	// rc2
	// stable-91ac07a9
	// stable-9ede8db0
	r := &models.Repository{NamespaceID: 1, ID: 4}

	tt := []struct {
		name         string
		limit        int
		lastName     string
		expectedTags []*models.TagDetail
	}{
		{
			name:     "no limit and no last name",
			limit:    100, // there are only 4 tags in the DB for repository 4, so this is equivalent to no limit
			lastName: "",  // this is the equivalent to no last name, as all tag names are non-empty
			expectedTags: []*models.TagDetail{
				{
					Name:   "1.0.0",
					Digest: digest.Digest("sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6"),
					ConfigDigest: models.NullDigest{
						Digest: "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
						Valid:  true,
					},
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Size:      489234,
				},
				{
					Name:      "rc2",
					Digest:    digest.Digest("sha256:45e85a20d32f249c323ed4085026b6b0ee264788276aa7c06cf4b5da1669067a"),
					MediaType: "application/vnd.docker.distribution.manifest.list.v2+json",
					Size:      0,
				},
				{
					Name:      "stable-91ac07a9",
					Digest:    digest.Digest("sha256:ea1650093606d9e76dfc78b986d57daea6108af2d5a9114a98d7198548bfdfc7"),
					MediaType: "application/vnd.docker.distribution.manifest.v1+json",
					Size:      23847,
				},
				{
					Name:   "stable-9ede8db0",
					Digest: digest.Digest("sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6"),
					ConfigDigest: models.NullDigest{
						Digest: "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
						Valid:  true,
					},
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Size:      489234,
				},
			},
		},
		{
			name:     "1st part",
			limit:    2,
			lastName: "",
			expectedTags: []*models.TagDetail{
				{
					Name:   "1.0.0",
					Digest: digest.Digest("sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6"),
					ConfigDigest: models.NullDigest{
						Digest: "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
						Valid:  true,
					},
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Size:      489234,
				},
				{
					Name:      "rc2",
					Digest:    digest.Digest("sha256:45e85a20d32f249c323ed4085026b6b0ee264788276aa7c06cf4b5da1669067a"),
					MediaType: "application/vnd.docker.distribution.manifest.list.v2+json",
					Size:      0,
				},
			},
		},
		{
			name:     "nth part",
			limit:    1,
			lastName: "rc2",
			expectedTags: []*models.TagDetail{
				{
					Name:      "stable-91ac07a9",
					Digest:    digest.Digest("sha256:ea1650093606d9e76dfc78b986d57daea6108af2d5a9114a98d7198548bfdfc7"),
					MediaType: "application/vnd.docker.distribution.manifest.v1+json",
					Size:      23847,
				},
			},
		},
		{
			name:     "last part",
			limit:    100,
			lastName: "stable-91ac07a9",
			expectedTags: []*models.TagDetail{
				{
					Name:   "stable-9ede8db0",
					Digest: digest.Digest("sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6"),
					ConfigDigest: models.NullDigest{
						Digest: "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
						Valid:  true,
					},
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Size:      489234,
				},
			},
		},
		{
			name:     "non existent last name",
			limit:    100,
			lastName: "does-not-exist",
			expectedTags: []*models.TagDetail{
				{
					Name:      "rc2",
					Digest:    digest.Digest("sha256:45e85a20d32f249c323ed4085026b6b0ee264788276aa7c06cf4b5da1669067a"),
					MediaType: "application/vnd.docker.distribution.manifest.list.v2+json",
					Size:      0,
				},
				{
					Name:      "stable-91ac07a9",
					Digest:    digest.Digest("sha256:ea1650093606d9e76dfc78b986d57daea6108af2d5a9114a98d7198548bfdfc7"),
					MediaType: "application/vnd.docker.distribution.manifest.v1+json",
					Size:      23847,
				},
				{
					Name:   "stable-9ede8db0",
					Digest: digest.Digest("sha256:bca3c0bf2ca0cde987ad9cab2dac986047a0ccff282f1b23df282ef05e3a10a6"),
					ConfigDigest: models.NullDigest{
						Digest: "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
						Valid:  true,
					},
					MediaType: "application/vnd.docker.distribution.manifest.v2+json",
					Size:      489234,
				},
			},
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			filters := datastore.FilterParams{
				LastEntry:  test.lastName,
				MaxEntries: test.limit,
			}

			rr, err := s.TagsDetailPaginated(suite.ctx, r, filters)
			// reset created_at and updated_at attributes for reproducible comparisons
			for _, r := range rr {
				r.CreatedAt = time.Time{}
				r.UpdatedAt = sql.NullTime{}
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedTags, rr)
		})
	}
}

func TestRepositoryStore_TagsDetailPaginated_None(t *testing.T) {
	reloadTagFixtures(t)

	r := &models.Repository{NamespaceID: 1, ID: 1}

	s := datastore.NewRepositoryStore(suite.db)
	tt, err := s.TagsDetailPaginated(suite.ctx, r, datastore.FilterParams{MaxEntries: 100})
	require.NoError(t, err)
	require.Empty(t, tt)
}

func TestRepositoryStore_FindPagingatedRepositoriesForPath(t *testing.T) {
	reloadTagFixtures(t)

	tt := []struct {
		name     string
		limit    int
		baseRepo *models.Repository
		lastPath string

		// see testdata/fixtures/[repositories|tags].sql:
		//
		// 		gitlab-org 												(0 tag(s))
		// 		gitlab-org/gitlab-test 									(0 tag(s))
		// 		gitlab-org/gitlab-test/backend 							(4 tag(s))
		// 		gitlab-org/gitlab-test/frontend 						(4 tag(s))
		// 		a-test-group 											(0 tag(s))
		// 		a-test-group/foo  										(0 tag(s))
		// 		a-test-group/bar 										(0 tag(s))
		// 		usage-group 											(0 tag(s))
		// 		usage-group/sub-group-1 								(1 tag(s))
		// 		usage-group/sub-group-1/repository-1					(4 tag(s))
		// 		usage-group/sub-group-1/repository-2 					(1 tag(s))
		// 		usage-group/sub-group-2 								(0 tag(s))
		// 		usage-group/sub-group-2/repository-1 					(1 tag(s))
		// 		usage-group/sub-group-2/repository-1/sub-repository-1 	(1 tag(s))
		// 		usage-group-2 											(0 tag(s))
		// 		usage-group-2/sub-group-1/project-1 					(0 tag(s))
		expectedRepos models.Repositories
	}{
		{
			name:     "no limit and no last path",
			limit:    100, // there are only 16 repositories in the DB, so this is equivalent to no limit
			lastPath: "",  // this is the equivalent to no last path, as all repository paths are non-empty
			baseRepo: &models.Repository{
				NamespaceID: 3,
				Path:        "usage-group",
			},
			expectedRepos: models.Repositories{
				{
					ID:              9,
					NamespaceID:     3,
					Name:            "sub-group-1",
					Path:            "usage-group/sub-group-1",
					ParentID:        sql.NullInt64{Int64: 8, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              10,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-1/repository-1",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              11,
					NamespaceID:     3,
					Name:            "repository-2",
					Path:            "usage-group/sub-group-1/repository-2",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              13,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-2/repository-1",
					ParentID:        sql.NullInt64{Int64: 12, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              14,
					NamespaceID:     3,
					Name:            "sub-repository-1",
					Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
					ParentID:        sql.NullInt64{Int64: 13, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:     "1st part",
			limit:    2,
			lastPath: "",
			baseRepo: &models.Repository{
				NamespaceID: 3,
				Path:        "usage-group",
			},
			expectedRepos: models.Repositories{
				{
					ID:              9,
					NamespaceID:     3,
					Name:            "sub-group-1",
					Path:            "usage-group/sub-group-1",
					ParentID:        sql.NullInt64{Int64: 8, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              10,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-1/repository-1",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:  "last part",
			limit: 100,
			baseRepo: &models.Repository{
				NamespaceID: 3,
				Path:        "usage-group",
			},
			lastPath: "usage-group/sub-group-1/repository-1",
			expectedRepos: models.Repositories{
				{
					ID:              11,
					NamespaceID:     3,
					Name:            "repository-2",
					Path:            "usage-group/sub-group-1/repository-2",
					ParentID:        sql.NullInt64{Int64: 9, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              13,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-2/repository-1",
					ParentID:        sql.NullInt64{Int64: 12, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              14,
					NamespaceID:     3,
					Name:            "sub-repository-1",
					Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
					ParentID:        sql.NullInt64{Int64: 13, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:  "nth part",
			limit: 1,
			baseRepo: &models.Repository{
				NamespaceID: 3,
				Path:        "usage-group",
			},
			lastPath: "usage-group/sub-group-1/repository-2",
			expectedRepos: models.Repositories{
				{
					ID:              13,
					NamespaceID:     3,
					Name:            "repository-1",
					Path:            "usage-group/sub-group-2/repository-1",
					ParentID:        sql.NullInt64{Int64: 12, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:  "non existent last path starting with d",
			limit: 100,
			baseRepo: &models.Repository{
				NamespaceID: 1,
				Path:        "gitlab-org",
			},
			lastPath: "does-not-exist",
			expectedRepos: models.Repositories{
				{
					ID:              3,
					NamespaceID:     1,
					Name:            "backend",
					Path:            "gitlab-org/gitlab-test/backend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
				{
					ID:              4,
					NamespaceID:     1,
					Name:            "frontend",
					Path:            "gitlab-org/gitlab-test/frontend",
					ParentID:        sql.NullInt64{Int64: 2, Valid: true},
					MigrationStatus: migration.RepositoryStatusNative,
				},
			},
		},
		{
			name:  "non existent last path starting with z",
			limit: 100,
			baseRepo: &models.Repository{
				NamespaceID: 1,
				Path:        "gitlab-org",
			},
			lastPath:      "z-does-not-exist",
			expectedRepos: models.Repositories{},
		},
	}

	s := datastore.NewRepositoryStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			filters := datastore.FilterParams{
				MaxEntries: test.limit,
				LastEntry:  test.lastPath,
			}

			rr, err := s.FindPagingatedRepositoriesForPath(suite.ctx, test.baseRepo, filters)

			// reset created_at attributes for reproducible comparisons
			for _, r := range rr {
				require.NotEmpty(t, r.CreatedAt)
				r.CreatedAt = time.Time{}
			}

			require.NoError(t, err)
			require.Equal(t, test.expectedRepos, rr)
		})
	}
}

func TestRepositoryStore_FindPagingatedRepositoriesForPath_None(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewRepositoryStore(suite.db)

	rr, err := s.FindPagingatedRepositoriesForPath(suite.ctx, &models.Repository{
		NamespaceID: 2,
		Path:        "a-test-group",
	}, datastore.FilterParams{MaxEntries: 100})
	require.NoError(t, err)
	require.Empty(t, rr)
}

func TestRepositoryStore_RenamePathForSubRepositories(t *testing.T) {
	reloadRepositoryFixtures(t)
	test := struct {
		name                 string
		baseRepo             *models.Repository
		topLevelNamespaceID  int64
		newPath              string
		expectedUpdatedRepos map[string]*models.Repository
		// see testdata/fixtures/repositories.sql:
		//
		// 		gitlab-org 												(0 tag(s))
		// 		gitlab-org/gitlab-test 									(0 tag(s))
		// 		gitlab-org/gitlab-test/backend 							(4 tag(s))
		// 		gitlab-org/gitlab-test/frontend 						(4 tag(s))
	}{

		name: "update all sub-repository paths starting with path `gitlab-org`",
		baseRepo: &models.Repository{
			ID:              1,
			NamespaceID:     1,
			Name:            "gitlab-org",
			Path:            "gitlab-org",
			ParentID:        sql.NullInt64{Valid: false},
			MigrationStatus: migration.RepositoryStatusNative,
		},
		topLevelNamespaceID: 1,
		newPath:             "not-gitlab-org",
		expectedUpdatedRepos: map[string]*models.Repository{
			"gitlab-org/gitlab-test": {
				ID:              2,
				NamespaceID:     1,
				Name:            "gitlab-test",
				Path:            "not-gitlab-org/gitlab-test",
				ParentID:        sql.NullInt64{Int64: 1, Valid: true},
				MigrationStatus: migration.RepositoryStatusNative,
			},
			"gitlab-org/gitlab-test/backend": {
				ID:              3,
				NamespaceID:     1,
				Name:            "backend",
				Path:            "not-gitlab-org/gitlab-test/backend",
				ParentID:        sql.NullInt64{Int64: 2, Valid: true},
				MigrationStatus: migration.RepositoryStatusNative,
			},
			"gitlab-org/gitlab-test/frontend": {
				ID:              4,
				NamespaceID:     1,
				Name:            "frontend",
				Path:            "not-gitlab-org/gitlab-test/frontend",
				ParentID:        sql.NullInt64{Int64: 2, Valid: true},
				MigrationStatus: migration.RepositoryStatusNative,
			},
		},
	}

	s := datastore.NewRepositoryStore(suite.db)
	t.Run(test.name, func(t *testing.T) {
		err := s.RenamePathForSubRepositories(suite.ctx, test.topLevelNamespaceID, test.baseRepo.Path, test.newPath)
		require.NoError(t, err)
		// verify base repository remains unchanged
		actualOldrepo, err := s.FindByPath(suite.ctx, test.baseRepo.Path)
		require.NoError(t, err)
		// reset created_at attributes for reproducible comparisons
		require.NotEmpty(t, actualOldrepo.CreatedAt)
		actualOldrepo.CreatedAt = time.Time{}
		require.Equal(t, test.baseRepo, actualOldrepo)
		// verify only paths were updated for sub-repositories
		for oldPath, expectedNewRepo := range test.expectedUpdatedRepos {
			oldrepo, err := s.FindByPath(suite.ctx, oldPath)
			require.NoError(t, err)
			require.Empty(t, oldrepo)
			newRepo, err := s.FindByPath(suite.ctx, expectedNewRepo.Path)
			require.NoError(t, err)
			// reset created_at attributes for reproducible comparisons
			require.NotEmpty(t, newRepo.CreatedAt)
			newRepo.CreatedAt = time.Time{}
			require.Equal(t, expectedNewRepo, newRepo)
		}
	})
}

func TestRepositoryStore_RenamePathForSubRepositories_None(t *testing.T) {
	reloadRepositoryFixtures(t)
	s := datastore.NewRepositoryStore(suite.db)
	repo, err := s.FindByPath(suite.ctx, "a-non-existent-repository")
	require.NoError(t, err)
	require.Empty(t, repo)
	err = s.RenamePathForSubRepositories(suite.ctx, 2, "a-non-existent-repository", "a-new-repository-name")
	require.NoError(t, err)
}

func TestRepositoryStore_RenamePathForSubRepositories_OnlyNecessaryChanged(t *testing.T) {
	reloadRepositoryFixtures(t)
	s := datastore.NewRepositoryStore(suite.db)

	err := s.RenamePathForSubRepositories(suite.ctx, 3, "usage-group/sub-group-1", "usage-group/sub-group-foo")
	require.NoError(t, err)
	rr, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/repositories.sql
	require.Len(t, rr, 16)
	local := rr[0].CreatedAt.Location()
	// we only expect changes to repository with ID:10 and ID: 11
	expected := models.Repositories{
		{
			ID:              1,
			NamespaceID:     1,
			Name:            "gitlab-org",
			Path:            "gitlab-org",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:39.849864", local),
		},
		{
			ID:              2,
			NamespaceID:     1,
			Name:            "gitlab-test",
			Path:            "gitlab-org/gitlab-test",
			ParentID:        sql.NullInt64{Int64: 1, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:47:40.866312", local),
		},
		{
			ID:              3,
			NamespaceID:     1,
			Name:            "backend",
			Path:            "gitlab-org/gitlab-test/backend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:42:12.566212", local),
		},
		{
			ID:              4,
			NamespaceID:     1,
			Name:            "frontend",
			Path:            "gitlab-org/gitlab-test/frontend",
			ParentID:        sql.NullInt64{Int64: 2, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:43:39.476421", local),
		},
		{
			ID:              5,
			NamespaceID:     2,
			Name:            "a-test-group",
			Path:            "a-test-group",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", local),
		},
		{
			ID:              6,
			NamespaceID:     2,
			Name:            "foo",
			Path:            "a-test-group/foo",
			ParentID:        sql.NullInt64{Int64: 5, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", local),
		},
		{
			ID:              7,
			NamespaceID:     2,
			Name:            "bar",
			Path:            "a-test-group/bar",
			ParentID:        sql.NullInt64{Int64: 5, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2020-06-08 16:01:39.476421", local),
		},
		{
			ID:              8,
			NamespaceID:     3,
			Name:            "usage-group",
			Path:            "usage-group",
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			ID:              9,
			NamespaceID:     3,
			Name:            "sub-group-1",
			Path:            "usage-group/sub-group-1",
			ParentID:        sql.NullInt64{Int64: 8, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			ID:              10,
			NamespaceID:     3,
			Name:            "repository-1",
			Path:            "usage-group/sub-group-foo/repository-1",
			ParentID:        sql.NullInt64{Int64: 9, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			ID:              11,
			NamespaceID:     3,
			Name:            "repository-2",
			Path:            "usage-group/sub-group-foo/repository-2",
			ParentID:        sql.NullInt64{Int64: 9, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:12:43.561123", local),
		},
		{
			ID:              12,
			NamespaceID:     3,
			Name:            "sub-group-2",
			Path:            "usage-group/sub-group-2",
			ParentID:        sql.NullInt64{Int64: 8, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:33:12.312211", local),
		},
		{
			ID:              13,
			NamespaceID:     3,
			Name:            "repository-1",
			Path:            "usage-group/sub-group-2/repository-1",
			ParentID:        sql.NullInt64{Int64: 12, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:33:12.434732", local),
		},
		{
			ID:              14,
			NamespaceID:     3,
			Name:            "sub-repository-1",
			Path:            "usage-group/sub-group-2/repository-1/sub-repository-1",
			ParentID:        sql.NullInt64{Int64: 13, Valid: true},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 11:33:12.434732", local),
		},
		{
			ID:              15,
			NamespaceID:     4,
			Name:            "usage-group-2",
			Path:            "usage-group-2",
			ParentID:        sql.NullInt64{},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 15:36:04.692846", local),
		},
		{
			ID:              16,
			NamespaceID:     4,
			Name:            "project-1",
			Path:            "usage-group-2/sub-group-1/project-1",
			ParentID:        sql.NullInt64{},
			MigrationStatus: migration.RepositoryStatusNative,
			CreatedAt:       testutil.ParseTimestamp(t, "2022-02-22 15:36:04.692846", local),
		},
	}

	require.ElementsMatch(t, expected, rr)
}

func TestRepositoryStore_RenameRepository(t *testing.T) {
	reloadRepositoryFixtures(t)
	tests := []struct {
		name                string
		oldPath             string
		namespaceID         int64
		newName             string
		newPath             string
		expectedUpdatedRepo *models.Repository
		// see testdata/fixtures/repositories.sql:
		//
		// 		gitlab-org 												(0 tag(s))
		// 		gitlab-org/gitlab-test 									(0 tag(s))
	}{
		{

			name:        "update repository name and path for path `gitlab-org`",
			oldPath:     "gitlab-org",
			namespaceID: 1,
			newName:     "not-gitlab-org",
			newPath:     "not-gitlab-org",
			expectedUpdatedRepo: &models.Repository{
				ID:              1,
				NamespaceID:     1,
				Name:            "not-gitlab-org",
				Path:            "not-gitlab-org",
				ParentID:        sql.NullInt64{Valid: false},
				MigrationStatus: migration.RepositoryStatusNative,
			},
		},
		{

			name:        "update repository name and path for nested repo `gitlab-org/gitlab-test`",
			oldPath:     "gitlab-org/gitlab-test",
			namespaceID: 1,
			newName:     "not-gitlab-test",
			newPath:     "gitlab-org/not-gitlab-test",
			expectedUpdatedRepo: &models.Repository{
				ID:              2,
				NamespaceID:     1,
				Name:            "not-gitlab-test",
				Path:            "gitlab-org/not-gitlab-test",
				ParentID:        sql.NullInt64{Int64: 1, Valid: true},
				MigrationStatus: migration.RepositoryStatusNative,
			},
		},
	}

	s := datastore.NewRepositoryStore(suite.db)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.Rename(suite.ctx, &models.Repository{Path: test.oldPath, NamespaceID: test.namespaceID}, test.newPath, test.newName)
			require.NoError(t, err)
			repo, err := s.FindByPath(suite.ctx, test.newPath)
			require.NoError(t, err)
			// reset created_at attributes for reproducible comparisons
			require.NotEmpty(t, repo.CreatedAt)
			repo.CreatedAt = time.Time{}
			require.Equal(t, test.expectedUpdatedRepo, repo)

		})
	}
}

func TestRepositoryStore_RenameRepository_None(t *testing.T) {
	reloadRepositoryFixtures(t)
	s := datastore.NewRepositoryStore(suite.db)
	err := s.Rename(suite.ctx, &models.Repository{Path: "a-non-existent-repository", NamespaceID: 1},
		"a-new-repository-path", "a-new-repository-name")
	require.EqualError(t, err, "repository not found")
}
