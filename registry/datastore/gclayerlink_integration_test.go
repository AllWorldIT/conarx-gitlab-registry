//go:build integration

package datastore_test

import (
	"testing"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/stretchr/testify/require"
)

func TestGCLayerLinkStore_FindAll(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewGCLayerLinkStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	// The table is auto populated by the `gc_track_layer_blobs_trigger` trigger as soon as `reloadManifestFixtures`
	// loads the `layers` fixtures. See testdata/fixtures/layers.sql
	expected := []*models.GCLayerLink{
		{
			ID:           8,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      8,
			Digest:       "sha256:68ced04f60ab5c7a5f1d0b0b4e7572c5a4c8cce44866513d30d9df1a15277d6b",
		},
		{
			ID:           11,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      11,
			Digest:       "sha256:a0696058fc76fe6f456289f5611efe5c3411814e686f59f28b2e2069ed9e7d28",
		},
		{
			ID:           12,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      12,
			Digest:       "sha256:68ced04f60ab5c7a5f1d0b0b4e7572c5a4c8cce44866513d30d9df1a15277d6b",
		},
		{
			ID:           31,
			NamespaceID:  3,
			RepositoryID: 14,
			LayerID:      31,
			Digest:       "sha256:476a8fceb48f8f8db4dbad6c79d1087fb456950f31143a93577507f11cce789f",
		},
		{
			ID:           1,
			NamespaceID:  1,
			RepositoryID: 3,
			LayerID:      1,
			Digest:       "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
		},
		{
			ID:           3,
			NamespaceID:  2,
			RepositoryID: 6,
			LayerID:      3,
			Digest:       "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
		},
		{
			ID:           5,
			NamespaceID:  1,
			RepositoryID: 3,
			LayerID:      5,
			Digest:       "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9",
		},
		{
			ID:           19,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      19,
			Digest:       "sha256:cf15cd200b0d2358579e1b561ec750ba8230f86e34e45cff89547c1217959752",
		},
		{
			ID:           21,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      21,
			Digest:       "sha256:8cb22990f6b627016f2f2000d2f29da7c2bc87b80d21efb4f89ed148e00df6ee",
		},
		{
			ID:           27,
			NamespaceID:  3,
			RepositoryID: 11,
			LayerID:      27,
			Digest:       "sha256:cf15cd200b0d2358579e1b561ec750ba8230f86e34e45cff89547c1217959752",
		},
		{
			ID:           29,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      29,
			Digest:       "sha256:52f7f1bb6469c3c075e08bf1d2f15ce51c9db79ee715d6649ce9b0d67c84b5ef",
		},
		{
			ID:           30,
			NamespaceID:  3,
			RepositoryID: 13,
			LayerID:      30,
			Digest:       "sha256:52f7f1bb6469c3c075e08bf1d2f15ce51c9db79ee715d6649ce9b0d67c84b5ef",
		},
		{
			ID:           15,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      15,
			Digest:       "sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31",
		},
		{
			ID:           17,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      17,
			Digest:       "sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31",
		},
		{
			ID:           22,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      22,
			Digest:       "sha256:ad4309f23d757351fba1698406f09c79667ecde8863dba39407cb915ebbe549d",
		},
		{
			ID:           23,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      23,
			Digest:       "sha256:0159a862a1d3a25886b9f029af200f15a27bd0a5552b5861f34b1cb02cc14fb2",
		},
		{
			ID:           24,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      24,
			Digest:       "sha256:cdb2596a54a1c291f041b1c824e87f4c6ed282a69b42f18c60dc801818e8a144",
		},
		{
			ID:           25,
			NamespaceID:  3,
			RepositoryID: 11,
			LayerID:      25,
			Digest:       "sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31",
		},
		{
			ID:           28,
			NamespaceID:  3,
			RepositoryID: 13,
			LayerID:      28,
			Digest:       "sha256:ad4309f23d757351fba1698406f09c79667ecde8863dba39407cb915ebbe549d",
		},
		{
			ID:           32,
			NamespaceID:  3,
			RepositoryID: 14,
			LayerID:      32,
			Digest:       "sha256:eb5683307d3554d282fb9101ad7220cdfc81078b2da6dcb4a683698c972136c5",
		},
		{
			ID:           33,
			NamespaceID:  3,
			RepositoryID: 9,
			LayerID:      33,
			Digest:       "sha256:683f96d2165726d760aa085adfc03a62cb3ce070687a4248b6451c6a84766a31",
		},
		{
			ID:           2,
			NamespaceID:  1,
			RepositoryID: 3,
			LayerID:      2,
			Digest:       "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21",
		},
		{
			ID:           4,
			NamespaceID:  2,
			RepositoryID: 6,
			LayerID:      4,
			Digest:       "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21",
		},
		{
			ID:           6,
			NamespaceID:  1,
			RepositoryID: 3,
			LayerID:      6,
			Digest:       "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21",
		},
		{
			ID:           7,
			NamespaceID:  1,
			RepositoryID: 3,
			LayerID:      7,
			Digest:       "sha256:f01256086224ded321e042e74135d72d5f108089a1cda03ab4820dfc442807c1",
		},
		{
			ID:           9,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      9,
			Digest:       "sha256:c4039fd85dccc8e267c98447f8f1b27a402dbb4259d86586f4097acb5e6634af",
		},
		{
			ID:           10,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      10,
			Digest:       "sha256:c16ce02d3d6132f7059bf7e9ff6205cbf43e86c538ef981c37598afd27d01efa",
		},
		{
			ID:           13,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      13,
			Digest:       "sha256:c4039fd85dccc8e267c98447f8f1b27a402dbb4259d86586f4097acb5e6634af",
		},
		{
			ID:           14,
			NamespaceID:  1,
			RepositoryID: 4,
			LayerID:      14,
			Digest:       "sha256:c16ce02d3d6132f7059bf7e9ff6205cbf43e86c538ef981c37598afd27d01efa",
		},
		{
			ID:           16,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      16,
			Digest:       "sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568",
		},
		{
			ID:           18,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      18,
			Digest:       "sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568",
		},
		{
			ID:           20,
			NamespaceID:  3,
			RepositoryID: 10,
			LayerID:      20,
			Digest:       "sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568",
		},
		{
			ID:           26,
			NamespaceID:  3,
			RepositoryID: 11,
			LayerID:      26,
			Digest:       "sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568",
		},
		{
			ID:           34,
			NamespaceID:  3,
			RepositoryID: 9,
			LayerID:      34,
			Digest:       "sha256:a9a96131ae93ca1ea6936aabddac48626c5749cb6f0c00f5e274d4078c5f4568",
		},
	}

	require.Equal(t, expected, rr)
}

func TestGCLayerLinkStore_FindAll_NotFound(t *testing.T) {
	unloadManifestFixtures(t)

	s := datastore.NewGCLayerLinkStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.Empty(t, rr)
	require.NoError(t, err)
}

func TestGcLayerLinkStore_Count(t *testing.T) {
	reloadManifestFixtures(t)

	s := datastore.NewGCLayerLinkStore(suite.db)
	count, err := s.Count(suite.ctx)
	require.NoError(t, err)

	// see testdata/fixtures/gc_blobs_layers.sql
	require.Equal(t, 34, count)
}
