//go:build integration

package datastore_test

import (
	"database/sql"
	"testing"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/stretchr/testify/require"
)

func reloadTagFixtures(tb testing.TB) {
	testutil.ReloadFixtures(
		tb, suite.db, suite.basePath,
		// A Tag has a foreign key for a Manifest, which in turn references a Repository (insert order matters)
		testutil.NamespacesTable, testutil.RepositoriesTable, testutil.BlobsTable, testutil.ManifestsTable, testutil.TagsTable,
	)
}

func unloadTagFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(
		suite.db,
		// A Tag has a foreign key for a Manifest, which in turn references a Repository (insert order matters)
		testutil.NamespacesTable, testutil.RepositoriesTable, testutil.BlobsTable, testutil.ManifestsTable, testutil.TagsTable,
	))
}

func TestTagStore_ImplementsReaderAndWriter(t *testing.T) {
	require.Implements(t, (*datastore.TagStore)(nil), datastore.NewTagStore(suite.db))
}

func TestTagStore_FindByID(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewTagStore(suite.db)

	tag, err := s.FindByID(suite.ctx, 1)
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	expected := &models.Tag{
		ID:           1,
		NamespaceID:  1,
		Name:         "1.0.0",
		RepositoryID: 3,
		ManifestID:   1,
		CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:43.283783", tag.CreatedAt.Location()),
	}
	require.Equal(t, expected, tag)
}

func TestTagStore_FindByID_NotFound(t *testing.T) {
	s := datastore.NewTagStore(suite.db)

	r, err := s.FindByID(suite.ctx, 0)
	require.Nil(t, r)
	require.NoError(t, err)
}

func TestTagStore_FindAll(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewTagStore(suite.db)

	tt, err := s.FindAll(suite.ctx)
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
		{
			ID:           2,
			NamespaceID:  1,
			Name:         "2.0.0",
			RepositoryID: 3,
			ManifestID:   2,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:44.283783", local),
		},
		{
			ID:           3,
			NamespaceID:  1,
			Name:         "latest",
			RepositoryID: 3,
			ManifestID:   2,
			CreatedAt:    testutil.ParseTimestamp(t, "2020-03-02 17:57:45.283783", local),
			UpdatedAt: sql.NullTime{
				Time:  testutil.ParseTimestamp(t, "2020-03-02 17:57:53.029514", local),
				Valid: true,
			},
		},
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
			ID:           7,
			NamespaceID:  1,
			Name:         "0.2.0",
			RepositoryID: 3,
			ManifestID:   6,
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
		{
			ID:           9,
			NamespaceID:  3,
			Name:         "a",
			RepositoryID: 10,
			ManifestID:   12,
			CreatedAt:    testutil.ParseTimestamp(t, "2021-11-24 11:36:05.210908", local),
		},
		{
			ID:           10,
			NamespaceID:  3,
			Name:         "b",
			RepositoryID: 10,
			ManifestID:   13,
			CreatedAt:    testutil.ParseTimestamp(t, "2021-11-24 11:38:57.313230", local),
		},
		{
			ID:           11,
			NamespaceID:  3,
			Name:         "c",
			RepositoryID: 10,
			ManifestID:   16,
			CreatedAt:    testutil.ParseTimestamp(t, "2021-11-24 11:49:45.008435", local),
		},
		{
			ID:           12,
			NamespaceID:  3,
			Name:         "d",
			RepositoryID: 10,
			ManifestID:   18,
			CreatedAt:    testutil.ParseTimestamp(t, "2021-11-24 11:59:13.421427", local),
		},
		{
			ID:           13,
			NamespaceID:  3,
			Name:         "e",
			RepositoryID: 11,
			ManifestID:   19,
			CreatedAt:    testutil.ParseTimestamp(t, "2022-02-22 11:59:13.421427", local),
		},
		{
			ID:           14,
			NamespaceID:  3,
			Name:         "f",
			RepositoryID: 13,
			ManifestID:   20,
			CreatedAt:    testutil.ParseTimestamp(t, "2022-02-22 11:59:13.421427", local),
		},
		{
			ID:           15,
			NamespaceID:  3,
			Name:         "h",
			RepositoryID: 14,
			ManifestID:   23,
			CreatedAt:    testutil.ParseTimestamp(t, "2022-02-22 11:59:13.421427", local),
		},
		{
			ID:           16,
			NamespaceID:  3,
			Name:         "i",
			RepositoryID: 9,
			ManifestID:   24,
			CreatedAt:    testutil.ParseTimestamp(t, "2022-02-22 12:01:05.114178", local),
		},
	}
	require.Equal(t, expected, tt)
}

func TestTagStore_FindAll_NotFound(t *testing.T) {
	unloadTagFixtures(t)

	s := datastore.NewTagStore(suite.db)

	tt, err := s.FindAll(suite.ctx)
	require.Empty(t, tt)
	require.NoError(t, err)
}

func TestTagStore_Count(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewTagStore(suite.db)
	count, err := s.Count(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	require.Equal(t, 16, count)
}

func TestTagStore_Repository(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewTagStore(suite.db)

	r, err := s.Repository(suite.ctx, &models.Tag{ID: 2, NamespaceID: 1, RepositoryID: 3})
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	expected := &models.Repository{
		NamespaceID:     1,
		ID:              3,
		Name:            "backend",
		Path:            "gitlab-org/gitlab-test/backend",
		ParentID:        sql.NullInt64{Int64: 2, Valid: true},
		MigrationStatus: migration.RepositoryStatusNative,
		CreatedAt:       testutil.ParseTimestamp(t, "2020-03-02 17:42:12.566212", r.CreatedAt.Location()),
	}
	require.Equal(t, expected, r)
}

func TestTagStore_Manifest(t *testing.T) {
	reloadTagFixtures(t)

	s := datastore.NewTagStore(suite.db)

	m, err := s.Manifest(suite.ctx, &models.Tag{
		ID:           2,
		NamespaceID:  1,
		RepositoryID: 3,
		ManifestID:   2,
	})
	require.NoError(t, err)

	// see testdata/fixtures/tags.sql
	expected := &models.Manifest{
		ID:            2,
		NamespaceID:   1,
		RepositoryID:  3,
		SchemaVersion: 2,
		TotalSize:     82384923,
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

func TestTagStore_CreateOrUpdate(t *testing.T) {
	reloadRepositoryFixtures(t)
	reloadManifestFixtures(t)
	require.NoError(t, testutil.TruncateTables(suite.db, testutil.TagsTable))

	s := datastore.NewTagStore(suite.db)
	tag := &models.Tag{
		NamespaceID:  1,
		Name:         "3.0.0",
		RepositoryID: 3,
		ManifestID:   1,
	}
	err := s.CreateOrUpdate(suite.ctx, tag)

	require.NoError(t, err)
	require.NotEmpty(t, tag.ID)
	require.NotEmpty(t, tag.CreatedAt)
	require.Empty(t, tag.UpdatedAt)
}

func TestTagStore_CreateOrUpdate_ManifestSwitch(t *testing.T) {
	reloadRepositoryFixtures(t)
	reloadManifestFixtures(t)
	require.NoError(t, testutil.TruncateTables(suite.db, testutil.TagsTable))

	s := datastore.NewTagStore(suite.db)

	// create tag
	tag := &models.Tag{
		NamespaceID:  1,
		Name:         "1.0.0",
		RepositoryID: 3,
		ManifestID:   1,
	}
	err := s.CreateOrUpdate(suite.ctx, tag)
	require.NoError(t, err)
	require.Empty(t, tag.UpdatedAt)

	// switch tag to another manifest
	tag.ManifestID = 2
	err = s.CreateOrUpdate(suite.ctx, tag)
	require.NoError(t, err)
	require.NotEmpty(t, tag.UpdatedAt)
}

func TestTagStore_CreateOrUpdate_Idempotent(t *testing.T) {
	reloadRepositoryFixtures(t)
	reloadManifestFixtures(t)
	require.NoError(t, testutil.TruncateTables(suite.db, testutil.TagsTable))

	s := datastore.NewTagStore(suite.db)

	// create tag
	tag := &models.Tag{
		NamespaceID:  1,
		Name:         "1.0.0",
		RepositoryID: 3,
		ManifestID:   1,
	}
	err := s.CreateOrUpdate(suite.ctx, tag)
	require.NoError(t, err)
	require.Empty(t, tag.UpdatedAt)

	// retry
	err = s.CreateOrUpdate(suite.ctx, tag)
	require.NoError(t, err)
	require.Empty(t, tag.UpdatedAt)
}

func TestTagStore_CreateOrUpdate_ManifestNotFound(t *testing.T) {
	reloadRepositoryFixtures(t)
	reloadManifestFixtures(t)
	require.NoError(t, testutil.TruncateTables(suite.db, testutil.TagsTable))

	s := datastore.NewTagStore(suite.db)
	tag := &models.Tag{
		NamespaceID:  1,
		Name:         "3.0.0",
		RepositoryID: 3,
		ManifestID:   100,
	}
	err := s.CreateOrUpdate(suite.ctx, tag)

	require.EqualError(t, err, datastore.ErrManifestNotFound.Error())
}
