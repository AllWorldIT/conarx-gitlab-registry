//go:build integration

package datastore_test

import (
	"testing"

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
