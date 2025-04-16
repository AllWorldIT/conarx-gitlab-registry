//go:build integration

package datastore_test

import (
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	"github.com/stretchr/testify/require"
)

func reloadBlobFixtures(tb testing.TB) {
	testutil.ReloadFixtures(tb, suite.db, suite.basePath,
		testutil.NamespacesTable, testutil.RepositoriesTable, testutil.BlobsTable, testutil.RepositoryBlobsTable)
}

func unloadBlobFixtures(tb testing.TB) {
	require.NoError(tb, testutil.TruncateTables(suite.db,
		testutil.NamespacesTable, testutil.RepositoriesTable, testutil.BlobsTable, testutil.RepositoryBlobsTable))
}

func TestBlobStore_ImplementsReaderAndWriter(t *testing.T) {
	require.Implements(t, (*datastore.BlobStore)(nil), datastore.NewBlobStore(suite.db))
}

func TestBlobStore_FindByDigest(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	b, err := s.FindByDigest(suite.ctx, "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21")
	require.NoError(t, err)

	// see testdata/fixtures/blobs.sql
	expected := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21",
		Size:      108,
		CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:05:35.338639", b.CreatedAt.Location()),
	}
	require.Equal(t, expected, b)
}

func TestBlobStore_FindByDigest_NotFound(t *testing.T) {
	s := datastore.NewBlobStore(suite.db)
	b, err := s.FindByDigest(suite.ctx, "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e22")
	require.Nil(t, b)
	require.NoError(t, err)
}

func TestBlobStore_All(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	bb, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/blobs.sql
	local := bb[0].CreatedAt.Location()
	expected := models.Blobs{
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:68ced04f60ab5c7a5f1d0b0b4e7572c5a4c8cce44866513d30d9df1a15277d6b",
			Size:      27091819,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:08:00.405042", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:a0696058fc76fe6f456289f5611efe5c3411814e686f59f28b2e2069ed9e7d28",
			Size:      107,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:08:00.405042", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:65f60633aab53c6abe938ac80b761342c1f7880a95e7f233168b0575dd2dad17",
			Size:      633,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:36:05.042839", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:829ae805ecbcdd4165484a69f5f65c477da69c9f181887f7953022cba209525e",
			Size:      633,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:41:24.514618", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:476a8fceb48f8f8db4dbad6c79d1087fb456950f31143a93577507f11cce789f",
			Size:      421341,
			CreatedAt: testutil.ParseTimestamp(t, "2022-02-22 11:52:35.336065", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:9c0abc9c5bd3a7854141800ba1f4a227baa88b11b49d8207eadc483c3f2496de",
			Size:      2155907,
			CreatedAt: testutil.ParseTimestamp(t, "2025-02-14 09:52:35.336066", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
			Size:      2802957,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:05:35.338639", local),
		},
		{
			MediaType: "application/vnd.docker.container.image.v1+json",
			Digest:    "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073",
			Size:      321,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:57:23.405516", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:cf15cd200b0d2358579e1b561ec750ba8230f86e34e45cff89547c1217959752",
			Size:      253193,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:38:56.958663", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:b051081eac10ae5607e7846677924d7ac3824954248d0247e0d24dd5063fb4c0",
			Size:      825,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:38:57.164793", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:8cb22990f6b627016f2f2000d2f29da7c2bc87b80d21efb4f89ed148e00df6ee",
			Size:      361786,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:41:24.297000", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:b6755e9ecd752632aa972b4d51ec0775fe00c1c329dfebcfd17b73550795b1b7",
			Size:      121,
			CreatedAt: testutil.ParseTimestamp(t, "2022-02-22 11:52:35.336065", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:52f7f1bb6469c3c075e08bf1d2f15ce51c9db79ee715d6649ce9b0d67c84b5ef",
			Size:      563125,
			CreatedAt: testutil.ParseTimestamp(t, "2022-02-22 11:52:35.336065", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:5ebf9ebafcaed068c33e5bb6fcdf92a5154d56f952def7942dbce815f45e4276",
			Size:      259,
			CreatedAt: testutil.ParseTimestamp(t, "2025-02-14 09:52:36.412032", local),
		},
		{
			MediaType: "application/vnd.docker.container.image.v1+json",
			Digest:    "sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9",
			Size:      123,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:56:26.573726", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31",
			Size:      468294,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:36:04.748415", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:ad4309f23d757351fba1698406f09c79667ecde8863dba39407cb915ebbe549d",
			Size:      255232,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:45:22.595596", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:0a450fb93c7bd4ee53d05ba63842d6c2cf73089198cbaccc115d470e6ae2ffc9",
			Size:      441,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:45:22.805228", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:cdb2596a54a1c291f041b1c824e87f4c6ed282a69b42f18c60dc801818e8a144",
			Size:      146656,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:52:34.991527", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:0159a862a1d3a25886b9f029af200f15a27bd0a5552b5861f34b1cb02cc14fb2",
			Size:      107728,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:52:34.992595", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:3ded4e17612c66f216041fe6f15002d9406543192095d689f14e8063b1a503df",
			Size:      633,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:52:35.336065", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:9f6fc12aeb556c70245041829b3aed906d5c439d6cf4e7eb6d90216ffe182a3d",
			Size:      86,
			CreatedAt: testutil.ParseTimestamp(t, "2022-02-22 11:52:35.336065", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:eb5683307d3554d282fb9101ad7220cdfc81078b2da6dcb4a683698c972136c5",
			Size:      5462210,
			CreatedAt: testutil.ParseTimestamp(t, "2022-02-22 11:52:35.336065", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:eb110b254f038cd0d464b20de5070099a6f675b9d8eb7e5035d929540a041ab1",
			Size:      354,
			CreatedAt: testutil.ParseTimestamp(t, "2025-02-14 09:52:36.412031", local),
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
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:c4039fd85dccc8e267c98447f8f1b27a402dbb4259d86586f4097acb5e6634af",
			Size:      23882259,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:08:00.405042", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:c16ce02d3d6132f7059bf7e9ff6205cbf43e86c538ef981c37598afd27d01efa",
			Size:      203,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-04 20:08:00.405042", local),
		},
		{
			MediaType: "application/vnd.docker.container.image.v1+json",
			Digest:    "sha256:33f3ef3322b28ecfc368872e621ab715a04865471c47ca7426f3e93846157780",
			Size:      252,
			CreatedAt: testutil.ParseTimestamp(t, "2020-03-02 17:57:23.405516", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568",
			Size:      428360,
			CreatedAt: testutil.ParseTimestamp(t, "2021-11-24 11:36:04.692846", local),
		},
		{
			MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
			Digest:    "sha256:af47096251092caf59498806ab8d58e8173ecf5a182f024ce9d635b5b4a55d66",
			Size:      372,
			CreatedAt: testutil.ParseTimestamp(t, "2025-02-14 09:52:35.336065", local),
		},
	}

	require.Equal(t, expected, bb)
}

func TestBlobStore_All_NotFound(t *testing.T) {
	unloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	bb, err := s.FindAll(suite.ctx)
	require.Empty(t, bb)
	require.NoError(t, err)
}

func TestBlobStore_Count(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	count, err := s.Count(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/blobs.sql
	require.Equal(t, 31, count)
}

func TestBlobStore_Create(t *testing.T) {
	unloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	b := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:1d9136cd62c9b60083de7763cfac547b1e571d10648393ade10325055a810556",
		Size:      203,
	}
	err := s.Create(suite.ctx, b)

	require.NoError(t, err)
	require.NotEmpty(t, b.CreatedAt)
}

func TestBlobStore_CreateOrFind(t *testing.T) {
	unloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	tmp := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:1d9136cd62c9b60083de7763cfac547b1e571d10648393ade10325055a810556",
		Size:      203,
	}

	// create non existing blob
	b := &models.Blob{
		MediaType: tmp.MediaType,
		Digest:    tmp.Digest,
		Size:      tmp.Size,
	}
	err := s.CreateOrFind(suite.ctx, b)
	require.NoError(t, err)
	require.Equal(t, tmp.MediaType, b.MediaType)
	require.Equal(t, tmp.Digest, b.Digest)
	require.Equal(t, tmp.Size, b.Size)
	require.NotEmpty(t, b.CreatedAt)

	// attempt to create existing blob
	l2 := &models.Blob{
		MediaType: tmp.MediaType,
		Digest:    tmp.Digest,
		Size:      tmp.Size,
	}
	err = s.CreateOrFind(suite.ctx, l2)
	require.NoError(t, err)
	require.Equal(t, b, l2)
}

func TestBlobStore_Create_NonUniqueDigestFails(t *testing.T) {
	reloadBlobFixtures(t)

	s := datastore.NewBlobStore(suite.db)
	b := &models.Blob{
		MediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip",
		Digest:    "sha256:a0696058fc76fe6f456289f5611efe5c3411814e686f59f28b2e2069ed9e7d28",
		Size:      108,
	}
	err := s.Create(suite.ctx, b)
	require.Error(t, err)
}

func TestBlobStore_Delete(t *testing.T) {
	reloadBlobFixtures(t)

	dgst := digest.Digest("sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	s := datastore.NewBlobStore(suite.db)
	err := s.Delete(suite.ctx, dgst)
	require.NoError(t, err)

	b, err := s.FindByDigest(suite.ctx, dgst)
	require.NoError(t, err)
	require.Nil(t, b)
}

func TestBlobStore_Delete_NotFound(t *testing.T) {
	s := datastore.NewBlobStore(suite.db)
	err := s.Delete(suite.ctx, "sha256:b9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9")
	require.EqualError(t, err, datastore.ErrNotFound.Error())
}
