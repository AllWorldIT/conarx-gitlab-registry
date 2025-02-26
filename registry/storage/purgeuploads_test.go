package storage

import (
	"context"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testUploadFS(t *testing.T, numUploads int, repoName string, startedAt time.Time) (driver.StorageDriver, context.Context) {
	d := inmemory.New()
	ctx := context.Background()
	for i := 0; i < numUploads; i++ {
		addUploads(ctx, t, d, uuid.Generate().String(), repoName, startedAt)
	}
	return d, ctx
}

func addUploads(ctx context.Context, t *testing.T, d driver.StorageDriver, uploadID, repo string, startedAt time.Time) {
	dataPath, err := pathFor(uploadDataPathSpec{name: repo, id: uploadID})
	require.NoError(t, err, "unable to resolve path")
	require.NoError(t, d.PutContent(ctx, dataPath, []byte("")), "unable to write data file")

	startedAtPath, err := pathFor(uploadStartedAtPathSpec{name: repo, id: uploadID})
	require.NoError(t, err, "unable to resolve path")

	require.NoError(t, d.PutContent(ctx, startedAtPath, []byte(startedAt.Format(time.RFC3339))), "unable to write startedAt file")
}

func TestPurgeGather(t *testing.T) {
	uploadCount := 5
	fs, ctx := testUploadFS(t, uploadCount, "test-repo", time.Now())
	uploadData, errs := getOutstandingUploads(ctx, fs)
	assert.Empty(t, errs, "unexpected errors: %q", errs)
	assert.Len(t, uploadData, uploadCount, "unexpected upload file count")
}

func TestPurgeNone(t *testing.T) {
	fs, ctx := testUploadFS(t, 10, "test-repo", time.Now())
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	deleted, errs := PurgeUploads(ctx, fs, oneHourAgo, true)
	assert.Empty(t, errs, "unexpected errors")
	assert.Empty(t, deleted, "unexpectedly deleted files for time: %s", oneHourAgo)
}

func TestPurgeAll(t *testing.T) {
	uploadCount := 10
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	fs, ctx := testUploadFS(t, uploadCount, "test-repo", oneHourAgo)

	// Ensure > 1 repos are purged
	addUploads(ctx, t, fs, uuid.Generate().String(), "test-repo2", oneHourAgo)
	uploadCount++

	deleted, errs := PurgeUploads(ctx, fs, time.Now(), true)
	assert.Empty(t, errs, "unexpected errors")
	fileCount := uploadCount
	assert.Len(t, deleted, fileCount, "unexpectedly deleted file count")
}

func TestPurgeSome(t *testing.T) {
	oldUploadCount := 5
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	fs, ctx := testUploadFS(t, oldUploadCount, "library/test-repo", oneHourAgo)

	newUploadCount := 4

	for i := 0; i < newUploadCount; i++ {
		addUploads(ctx, t, fs, uuid.Generate().String(), "test-repo", time.Now().Add(1*time.Hour))
	}

	deleted, errs := PurgeUploads(ctx, fs, time.Now(), true)
	assert.Empty(t, errs, "unexpected errors")
	assert.Len(t, deleted, oldUploadCount, "unexpectedly deleted file count")
}

func TestPurgeOnlyUploads(t *testing.T) {
	oldUploadCount := 5
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	fs, ctx := testUploadFS(t, oldUploadCount, "test-repo", oneHourAgo)

	// Create a directory tree outside _uploads and ensure
	// these files aren't deleted.
	dataPath, err := pathFor(uploadDataPathSpec{name: "test-repo", id: uuid.Generate().String()})
	require.NoError(t, err)
	nonUploadPath := strings.ReplaceAll(dataPath, "_upload", "_important")
	require.NotContains(t, nonUploadPath, "_upload", "non-upload path not created correctly")

	nonUploadFile := path.Join(nonUploadPath, "file")
	require.NoError(t, fs.PutContent(ctx, nonUploadFile, []byte("")), "unable to write data file")

	deleted, errs := PurgeUploads(ctx, fs, time.Now(), true)
	assert.Empty(t, errs, "unexpected errors")
	for _, file := range deleted {
		assert.Contains(t, file, "_upload", "non-upload file deleted")
	}
}

func TestPurgeMissingStartedAt(t *testing.T) {
	oneHourAgo := time.Now().Add(-1 * time.Hour)
	fs, ctx := testUploadFS(t, 1, "test-repo", oneHourAgo)

	err := fs.WalkParallel(ctx, "/", func(fileInfo driver.FileInfo) error {
		filePath := fileInfo.Path()
		_, file := path.Split(filePath)

		if file == "startedat" {
			require.NoError(t, fs.Delete(ctx, filePath), "unable to delete startedat file: %s", filePath)
		}
		return nil
	})
	require.NoError(t, err, "unexpected error during Walk")
	deleted, errs := PurgeUploads(ctx, fs, time.Now(), true)
	assert.Empty(t, errs, "unexpected errors")
	assert.Empty(t, deleted, "files unexpectedly deleted: %s", deleted)
}
