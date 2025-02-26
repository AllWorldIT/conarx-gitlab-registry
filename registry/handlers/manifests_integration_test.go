//go:build integration && handlers_test

package handlers

import (
	"net/http"
	"testing"

	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/testutil"
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
	defer ctrl.Finish()

	repoCacheMock := mocks.NewMockRepositoryCache(ctrl)
	matchFn := func(x any) bool {
		repoArg := x.(*models.Repository)
		return repoArg.Path == repoPath
	}
	gomock.InOrder(
		repoCacheMock.EXPECT().Get(env.ctx, repoPath).Return(nil).Times(1),
		repoCacheMock.EXPECT().Set(env.ctx, gomock.Cond(matchFn)).Times(1),
	)

	// build test repository
	rStore := datastore.NewRepositoryStore(env.db)
	r, err := rStore.CreateByPath(env.ctx, repoPath)
	require.NoError(t, err)
	require.NotNil(t, r)

	// add a manifest
	mStore := datastore.NewManifestStore(env.db)
	m := &models.Manifest{
		NamespaceID:   r.NamespaceID,
		RepositoryID:  r.ID,
		SchemaVersion: 2,
		MediaType:     schema2.MediaTypeManifest,
		Digest:        g_digest.FromString(testutil.SampleManifestJSON),
		Payload:       models.Payload(testutil.SampleManifestJSON),
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
	manifestsGetter := newDBManifestGetter(env.db, repoCacheMock, r.Path, &http.Request{})
	require.NotNil(t, manifestsGetter)
	manifest, digest, err := manifestsGetter.GetByTag(env.ctx, tagName)
	require.NoError(t, err)
	require.EqualValues(t, g_digest.FromString(testutil.SampleManifestJSON), digest)
	mediaType, payload, err := manifest.Payload()
	require.NoError(t, err)
	require.EqualValues(t, schema2.MediaTypeManifest, mediaType)
	require.JSONEq(t, testutil.SampleManifestJSON, string(payload))
}
