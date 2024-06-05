//go:build integration

package datastore_test

import (
	"testing"

	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/registry/datastore"
	"github.com/stretchr/testify/require"
)

func TestMediaTypeStore_Exists(t *testing.T) {
	tt := []struct {
		name       string
		mediaType  string
		wantExists bool
	}{
		{
			name:       "application/json should exist",
			mediaType:  "application/json",
			wantExists: true,
		},
		{
			name:      "application/foobar should not exist",
			mediaType: "application/foobar",
		},
		{
			name:      "empty string should not exist",
			mediaType: "",
		},
		{
			name:      "unicode produces no error",
			mediaType: "不要/将其粘贴到谷歌翻译中",
		},
	}

	s := datastore.NewMediaTypeStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			exists, err := s.Exists(suite.ctx, test.mediaType)
			require.NoError(t, err)
			require.Equal(t, test.wantExists, exists)
		})
	}
}

func TestMediaTypeStore_FindIDMediaTypeExists(t *testing.T) {
	tt := []struct {
		name      string
		mediaType string
		wantID    int
	}{
		{
			name:      "application/json should be found",
			mediaType: "application/json",
			wantID:    54,
		},
		{
			name:      "application/vnd.docker.distribution.manifest.v2+json should be found",
			mediaType: "application/vnd.docker.distribution.manifest.v2+json",
			wantID:    3,
		},
	}

	s := datastore.NewMediaTypeStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			id, err := s.FindID(suite.ctx, test.mediaType)
			require.NoError(t, err)
			require.Equal(t, test.wantID, id)
		})
	}
}

func TestMediaTypeStore_FindIDMediaTypeDoesNotExist(t *testing.T) {
	tt := []struct {
		name      string
		mediaType string
		wantError error
	}{
		{
			name:      "application/foobar should not be found",
			mediaType: "application/foobar",
			wantError: datastore.ErrUnknownMediaType{"application/foobar"},
		},
		{
			name:      "empty string should not be found",
			mediaType: "",
			wantError: datastore.ErrUnknownMediaType{},
		},
		{
			name:      "unicode produces expected error",
			mediaType: "不要/将其粘贴到谷歌翻译中",
			wantError: datastore.ErrUnknownMediaType{"不要/将其粘贴到谷歌翻译中"},
		},
	}

	s := datastore.NewMediaTypeStore(suite.db)

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			id, err := s.FindID(suite.ctx, test.mediaType)
			require.EqualError(t, err, test.wantError.Error())
			require.Equal(t, 0, id)
		})
	}
}

func TestMediaTypeStore_SafeFindOrCreate(t *testing.T) {
	s := datastore.NewMediaTypeStore(suite.db)

	// Find an existing media type without the feature flag enabled.
	existingMT := "application/vnd.docker.distribution.manifest.v2+json"

	id, err := s.SafeFindOrCreateID(suite.ctx, existingMT)
	require.NoError(t, err)
	require.Equal(t, id, 3)

	// Create a new media type without the feature flag enabled.
	newMT := "application/bogus.test.media.type.not.real.midi"

	exists, err := s.Exists(suite.ctx, newMT)
	require.NoError(t, err)
	require.Equal(t, false, exists)

	id, err = s.SafeFindOrCreateID(suite.ctx, newMT)
	t.Cleanup(func() { deleteMediaType(t, newMT) })
	wantErr := datastore.ErrUnknownMediaType{newMT}
	require.EqualError(t, err, wantErr.Error())

	exists, err = s.Exists(suite.ctx, newMT)
	require.NoError(t, err)
	require.Equal(t, false, exists)

	t.Setenv(feature.DynamicMediaTypes.EnvVariable, "true")

	// Find an existing media type with the feature flag enabled.
	existingMT = "application/vnd.docker.distribution.manifest.v2+json"

	id, err = s.SafeFindOrCreateID(suite.ctx, existingMT)
	require.NoError(t, err)
	require.Equal(t, id, 3)

	// Create a new media type with the feature flag enabled.
	newMT = "application/fake.media.type.for.testing.rar"

	exists, err = s.Exists(suite.ctx, newMT)
	require.NoError(t, err)
	require.Equal(t, false, exists)

	id, err = s.SafeFindOrCreateID(suite.ctx, newMT)
	t.Cleanup(func() { deleteMediaType(t, newMT) })
	require.NoError(t, err)
	// We can't anticipate what the ID will be, but we can check that it's not
	// the default value.
	require.NotZero(t, id)

	exists, err = s.Exists(suite.ctx, newMT)
	require.NoError(t, err)
	require.Equal(t, true, exists)
}

// Unlike other tables used during testing, the media types table comes pre-seeded
// this means that resetting the table as we do with test fixtures for other
// tables would put the media_types table into a empty state that we would never
// expect the media_types table to be in. Therefore, we must remove the test
// media types introduced during testing individually.
func deleteMediaType(t *testing.T, mediaType string) {
	t.Helper()

	q := "DELETE from media_types WHERE media_type = $1"

	_, err := suite.db.ExecContext(suite.ctx, q, mediaType)
	require.NoError(t, err)
}
