//go:build integration && handlers_test

package handlers

import (
	"net/http"
	"testing"

	"github.com/docker/distribution/manifest/schema2"
	g_digest "github.com/opencontainers/go-digest"
	"go.uber.org/mock/gomock"

	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/stretchr/testify/require"
)

func TestGetManifest(t *testing.T) {
	env := newEnv(t)
	defer env.shutdown(t)

	repoPath := "ilikecats"

	ctrl := gomock.NewController(t)
	repositoryMockCache := mocks.NewMockRepositoryCache(ctrl)
	matchFn := func(x any) bool {
		repoArg := x.(*models.Repository)
		return repoArg.Path == repoPath
	}
	gomock.InOrder(
		repositoryMockCache.EXPECT().Get(env.ctx, repoPath).Return(nil).Times(1),
		repositoryMockCache.EXPECT().Set(env.ctx, gomock.Cond(matchFn)).Times(1),
	)

	// build test repository
	rStore := datastore.NewRepositoryStore(env.db)
	r, err := rStore.CreateByPath(env.ctx, repoPath)
	require.NoError(t, err)
	require.NotNil(t, r)

	// add a manifest
	manifestJson := `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.v2+json","config":{"mediaType":"application/vnd.docker.container.image.v1+json","size":1640,"digest":"sha256:ea8a54fd13889d3649d0a4e45735116474b8a650815a2cda4940f652158579b9"},"layers":[{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":2802957,"digest":"sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9"},{"mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip","size":108,"digest":"sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21"}]}`
	mStore := datastore.NewManifestStore(env.db)
	m := &models.Manifest{
		NamespaceID:   r.NamespaceID,
		RepositoryID:  r.ID,
		SchemaVersion: 2,
		MediaType:     schema2.MediaTypeManifest,
		Digest:        g_digest.FromString(manifestJson),
		Payload:       models.Payload(manifestJson),
	}
	err = mStore.Create(env.ctx, m)
	require.NoError(t, err)

	// tag manifest
	tagName := "mycatiscalledoksana"
	tStore := datastore.NewTagStore(env.db)
	tag := &models.Tag{
		Name:         tagName,
		NamespaceID:  r.NamespaceID,
		RepositoryID: r.ID,
		ManifestID:   m.ID,
	}
	err = tStore.CreateOrUpdate(env.ctx, tag)
	require.NoError(t, err)

	// Test
	manifestsGetter, err := newDBManifestGetter(env.db, repositoryMockCache, r.Path, &http.Request{})
	require.NoError(t, err)
	manifest, digest, err := manifestsGetter.GetByTag(env.ctx, tagName)
	require.NoError(t, err)
	require.EqualValues(t, g_digest.FromString(manifestJson), digest)
	mediaType, payload, err := manifest.Payload()
	require.NoError(t, err)
	require.EqualValues(t, "application/vnd.docker.distribution.manifest.v2+json", mediaType)
	require.JSONEq(t, manifestJson, string(payload))
}
