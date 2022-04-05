//go:build integration
// +build integration

package handlers_test

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/stretchr/testify/require"
)

var waitForever = time.Duration(math.MaxInt64)

func preImportRepository(t *testing.T, env *testEnv, mockNotificationSrv *mockImportNotification, repoPath string) string {
	t.Helper()

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	if mockNotificationSrv != nil {
		mockNotificationSrv.waitForImportNotification(
			t,
			repoPath,
			string(migration.RepositoryStatusPreImportComplete),
			"pre import completed successfully",
			5*time.Second,
		)
	}

	return importURL
}

func TestGitlabAPI_RepositoryImport_Get(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	// Before starting the import.
	req, err := http.NewRequest(http.MethodGet, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should return 404.
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Start Repository Import.
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusImportComplete), "final import completed successfully", 2*time.Second,
	)

	req, err = http.NewRequest(http.MethodGet, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import completed successfully.
	require.Equal(t, http.StatusOK, resp.StatusCode)

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var s handlers.RepositoryImportStatus
	err = json.Unmarshal(b, &s)
	require.NoError(t, err)

	expectedStatus := handlers.RepositoryImportStatus{
		Name:   repositoryName(repoPath),
		Path:   repoPath,
		Status: migration.RepositoryStatusImportComplete,
		Detail: "final import completed successfully",
	}

	require.Equal(t, expectedStatus, s)

	// response content type must be application/json
	require.Equal(t, resp.Header.Get("Content-Type"), "application/json")
}

func TestGitlabAPI_RepositoryImport_Put(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Start Repository Import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusImportComplete), "final import completed successfully", 2*time.Second,
	)

	// Subsequent calls to the same repository should not start another import.
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGitlabAPI_RepositoryPreImport_Put_PreImportTimeout(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withMigrationPreImportTimeout(time.Millisecond),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Start Repository Import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// pre import timed out but notification is sent anyway.
	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusPreImportFailed),
		"timeout:", 2*time.Second,
	)
}

func TestGitlabAPI_RepositoryImport_Put_ImportTimeout(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withMigrationImportTimeout(time.Millisecond),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Start Repository Import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// final import timed out but notification is sent anyway.
	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusImportFailed),
		"timeout:", 2*time.Second,
	)
}

func TestGitlabAPI_RepositoryImport_Put_ConcurrentTags(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagTmpl := "import-tag-%d"
	tags := make([]string, 10)

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withMigrationTagConcurrency(5),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	// Push up a series of images to the old side of the registry, so we can
	// test the importer works as expectd when launching multiple goroutines.
	for n := range tags {
		tagName := fmt.Sprintf(tagTmpl, n)
		tags[n] = tagName

		seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)
	}

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Start Repository Import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Spin up a non-migartion mode env to test that the repository imported correctly.
	env3 := newTestEnv(t, withFSDriver(migrationDir))
	defer env3.Shutdown()

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusImportComplete), "final import completed successfully", 2*time.Second,
	)

	// Subsequent calls to the same repository should not start another import.
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGitlabAPI_RepositoryImport_Put_PreImport(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)
	// Start repository pre import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Spin up a non-migartion mode env to test that the repository pre imported correctly.
	env3 := newTestEnv(t, withFSDriver(migrationDir))
	defer env3.Shutdown()

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 2*time.Second,
	)

	// The tag should not have been imported.
	tagURL := buildManifestTagURL(t, env3, repoPath, tagName)
	resp, err = http.Get(tagURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Subsequent calls to the same repository should start another pre import.
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 2*time.Second,
	)

	// Final import after pre import should succeed.
	importURL, err = env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusImportComplete), "final import completed successfully", 2*time.Second,
	)
}

func TestGitlabAPI_RepositoryImport_PreImportInProgress(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Simulate a long running import.
		withMigrationTestSlowImport(waitForever),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Start repository pre import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Additonal pre import attemps should fail
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusTooEarly, resp.StatusCode)

	// Additonal import attemps should fail as well
	importURL, err = env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusTooEarly, resp.StatusCode)

	// Import GET should return appropriate status
	req, err = http.NewRequest(http.MethodGet, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var s handlers.RepositoryImportStatus
	err = json.Unmarshal(b, &s)
	require.NoError(t, err)

	expectedStatus := handlers.RepositoryImportStatus{
		Name:   repositoryName(repoPath),
		Path:   repoPath,
		Status: migration.RepositoryStatusPreImportInProgress,
		Detail: string(migration.RepositoryStatusPreImportInProgress),
	}

	require.Equal(t, expectedStatus, s)
}

func TestGitlabAPI_RepositoryImport_ImportInProgress(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	env2 := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Simulate a long running import.
		withMigrationTestSlowImport(waitForever),
	)
	defer env2.Shutdown()

	// Start repository import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env2.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Additonal import attemps should fail
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// Pre import attemps should fail as well
	importURL, err = env2.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)

	// Import GET should return appropriate status
	req, err = http.NewRequest(http.MethodGet, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var s handlers.RepositoryImportStatus
	err = json.Unmarshal(b, &s)
	require.NoError(t, err)

	expectedStatus := handlers.RepositoryImportStatus{
		Name:   repositoryName(repoPath),
		Path:   repoPath,
		Status: migration.RepositoryStatusImportInProgress,
		Detail: string(migration.RepositoryStatusImportInProgress),
	}

	require.Equal(t, expectedStatus, s)
}

func TestGitlabAPI_RepositoryImport_Put_PreImportFailed(t *testing.T) {
	// FIXME
	t.Skip("Skipped, see https://gitlab.com/gitlab-org/container-registry/-/issues/633")

	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "notags/repo"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// Push up a image to the old side of the registry, but do not push any tags,
	// the pre import will start without error, but the actual pre import will fail.
	seedRandomSchema2Manifest(t, env, repoPath, putByDigest, writeToFilesystemOnly)

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	preImportURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, preImportURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusPreImportFailed),
		"pre importing tagged manifests: reading tags: unknown repository name=notags/repo", 2*time.Second,
	)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)
	// Subsequent import attempts fail.
	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusFailedDependency, resp.StatusCode)

	// Starting a pre import after a failed pre import attempt should succeed.
	req, err = http.NewRequest(http.MethodPut, preImportURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// It's good to wait here as well, otherwise the test env will close the
	// connection to the database before the import goroutine is finished, logging
	// an error that could be misleading.
	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusPreImportFailed),
		"pre importing tagged manifests: reading tags: unknown repository name=notags/repo", 2*time.Second,
	)

	req, err = http.NewRequest(http.MethodGet, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var s handlers.RepositoryImportStatus
	err = json.Unmarshal(b, &s)
	require.NoError(t, err)

	expectedStatus := handlers.RepositoryImportStatus{
		Name:   repositoryName(repoPath),
		Path:   repoPath,
		Status: migration.RepositoryStatusPreImportFailed,
	}

	require.Equal(t, expectedStatus, s)
}

func TestGitlabAPI_RepositoryImport_Put_RepositoryNotPresentOnOldSide(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	env := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	repoPath := "old/repo"

	// Start Repository Import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// We should get a repository not found error
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "repository not found", resp, v2.ErrorCodeNameUnknown)
}

func TestGitlabAPI_RepositoryImport_Put_Import_PreImportCanceled(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// make the pre-import take a long time so we can cancel it
		withMigrationTestSlowImport(waitForever),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Begin a pre-import
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	preImportURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, preImportURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// DELETE the same URL
	req, err = http.NewRequest(http.MethodDelete, preImportURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// DELETE pre import should be accepted
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusPreImportCanceled),
		"pre import was canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportCanceled, "pre import was canceled manually")

	// attempting a final import should not be allowed
	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err = http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start
	require.Equal(t, http.StatusFailedDependency, resp.StatusCode)
	checkBodyHasErrorCodes(t, "a previous pre import was canceled", resp, v1.ErrorCodePreImportCanceled)
}

func TestGitlabAPI_RepositoryImport_Migration_PreImportInProgress(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Simulate a long running pre import.
		withMigrationTestSlowImport(waitForever),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	repoPath := "old/repo"
	tagName := "import-tag"

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Start repository pre import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Pull the Manifest and ensure that the migration path header indicates
	// that the old code path is still taken during import for reads.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	require.NoError(t, err)

	resp, err = http.Get(tagURL)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, migration.OldCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))

	// Push up a image to the same repository and ensure that the migration path header
	// indicates that the old code path is still taken during pre import for writes.
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)
	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp = putManifest(t, "putting manifest no error", manifestDigestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, migration.OldCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))
}

func TestGitlabAPI_RepositoryImport_Migration_ImportInProgress(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old-repo"
	repoTag := "schema2-old-repo"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a random image to create the repository on the filesystem
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(repoTag), writeToFilesystemOnly)
	tagURL := buildManifestTagURL(t, env, repoPath, repoTag)

	// Prepare to push up a second image to the same repository. It's easiest to
	// prepare the layers successfully and later fail the manifest put.
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Bring up a new environment in migration mode.
	env2 := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Simulate a long running import.
		withMigrationTestSlowImport(waitForever),
	)
	t.Cleanup(env2.Shutdown)

	env2.requireDB(t)

	// Start repository full import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env2.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Full import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Push up the second image from above to the same repository while the full
	// import is underway. The write should be rejected with a service unavailable error.
	resp = putManifest(t, "putting manifest error", tagURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// Ensure that read requests are allowed as normal and that the old code path
	// is still taken during pre import for reads.
	resp, err = http.Get(tagURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, migration.OldCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))

	// Ensure that DELETE requests are also rejected.
	resp, err = httpDelete(tagURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	// Ensure that write requests for repositories not currently being imported succeed.
	seedRandomSchema2Manifest(t, env2, "different/repo", putByTag("latest"))
}

func TestGitlabAPI_RepositoryImport_Migration_PreImportComplete(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Start repository pre import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should complete without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 2*time.Second,
	)

	// Pull the Manifest and ensure that the migration path header indicates
	// that the old code path is still taken after pre import for reads.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	require.NoError(t, err)

	resp, err = http.Get(tagURL)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, migration.OldCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))

	// Push up a image to the same repository and ensure that the migration path header
	// indicates that the old code path is still taken after pre import for writes.
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)
	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp = putManifest(t, "putting manifest no error", manifestDigestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, migration.OldCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))
}

func TestGitlabAPI_RepositoryImport_Migration_ImportComplete(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Start repository import.
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should complete without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath, string(migration.RepositoryStatusImportComplete), "final import completed successfully", 2*time.Second,
	)

	// Pull the Manifest and ensure that the migration path header indicates
	// that the new code path is taken after import for reads.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	require.NoError(t, err)

	resp, err = http.Get(tagURL)
	require.NoError(t, err)

	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, migration.NewCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))

	// Push up a image to the same repository and ensure that the migration path header
	// indicates that the new code path is taken after import for writes.
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)
	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp = putManifest(t, "putting manifest no error", manifestDigestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, migration.NewCodePath.String(), resp.Header.Get("Gitlab-Migration-Path"))
}

// iso8601MsFormat is a regular expression to validate ISO8601 timestamps with millisecond precision.
var iso8601MsFormat = regexp.MustCompile(`^(?:[0-9]{4}-[0-9]{2}-[0-9]{2})?(?:[ T][0-9]{2}:[0-9]{2}:[0-9]{2})?(?:[.][0-9]{3})`)

func TestGitlabAPI_Repository_Get(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	repoName := "bar"
	repoPath := fmt.Sprintf("foo/%s", repoName)
	tagName := "latest"
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	// try to get details of non-existing repository
	u, err := env.builder.BuildGitlabV1RepositoryURL(repoRef)
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	checkBodyHasErrorCodes(t, "wrong response body error code", resp, v2.ErrorCodeNameUnknown)

	// try getting the details of an "empty" (no tagged artifacts) repository
	seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

	u, err = env.builder.BuildGitlabV1RepositoryURL(repoRef, url.Values{
		"size": []string{"self"},
	})
	require.NoError(t, err)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var r handlers.RepositoryAPIResponse
	p, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, r.Name, repoName)
	require.Equal(t, r.Path, repoPath)
	require.Zero(t, *r.Size)
	require.NotEmpty(t, r.CreatedAt)
	require.Regexp(t, iso8601MsFormat, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)

	// repeat, but before that push another image, this time tagged
	dm := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))
	var expectedSize int64
	for _, d := range dm.Layers() {
		expectedSize += d.Size
	}

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	r = handlers.RepositoryAPIResponse{}
	p, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, r.Name, repoName)
	require.Equal(t, r.Path, repoPath)
	require.Equal(t, *r.Size, expectedSize)
	require.NotEmpty(t, r.CreatedAt)
	require.Regexp(t, iso8601MsFormat, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)

	// Now create a new sub repository and push a new tagged image. When called with size=self_with_descendants, the
	// returned size should have been incremented when compared with size=self.
	subRepoPath := fmt.Sprintf("%s/car", repoPath)
	m2 := seedRandomSchema2Manifest(t, env, subRepoPath, putByTag(tagName))
	for _, d := range m2.Layers() {
		expectedSize += d.Size
	}

	u, err = env.builder.BuildGitlabV1RepositoryURL(repoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	r = handlers.RepositoryAPIResponse{}
	p, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, r.Name, repoName)
	require.Equal(t, r.Path, repoPath)
	require.Equal(t, *r.Size, expectedSize)
	require.NotEmpty(t, r.CreatedAt)
	require.Regexp(t, iso8601MsFormat, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)

	// use invalid `size` query param value
	u, err = env.builder.BuildGitlabV1RepositoryURL(repoRef, url.Values{
		"size": []string{"selfff"},
	})
	require.NoError(t, err)

	resp, err = http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "wrong response body error code", resp, v1.ErrorCodeInvalidQueryParamValue)
}

func TestGitlabAPI_RepositoryImport_MaxConcurrentImports(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	repoPathTemplate := "old/repo-%d"
	tagName := "import-tag"
	repoCount := 5

	allRepoPaths := generateOldRepoPaths(t, repoPathTemplate, repoCount)

	mockedImportNotifSrv := newMockImportNotification(t, allRepoPaths...)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
		// only allow a maximum of 3 imports at a time
		withMigrationMaxConcurrentImports(3))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	seedMultipleFSManifestsWithTag(t, env, tagName, allRepoPaths)

	attemptImportFn := func(count int, expectedStatus int, waitForNotif bool) {
		repoPath := fmt.Sprintf(repoPathTemplate, count)
		// Start Repository Import.
		repoRef, err := reference.WithName(repoPath)
		require.NoError(t, err)

		importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodPut, importURL, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		require.Equalf(t, expectedStatus, resp.StatusCode, "repo path: %q", repoPath)

		if waitForNotif {
			mockedImportNotifSrv.waitForImportNotification(
				t, repoPath, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 5*time.Second,
			)
		}
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < repoCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if i < 3 {
				// expect first 3 imports to succeed
				attemptImportFn(i, http.StatusAccepted, true)
			} else {
				// let the first 3 request go first
				time.Sleep(10 * time.Millisecond)
				attemptImportFn(i, http.StatusTooManyRequests, false)
			}
		}(i)
	}

	wg.Wait()

	wg2 := &sync.WaitGroup{}

	// attempt to pre import again should succeed
	for i := 3; i < 5; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()

			attemptImportFn(i, http.StatusAccepted, true)
		}(i)
	}

	wg2.Wait()
}

func TestGitlabAPI_RepositoryImport_MaxConcurrentImports_IsZero(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Explicitly set to 0 so no imports would be allowed.
		withMigrationMaxConcurrentImports(0),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	repoPath := "old/repo"
	tagName := "import-tag"

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	attemptImportFn := func(preImport bool) {
		urlValues := url.Values{}
		if preImport {
			urlValues.Set("import_type", "pre")
		}

		importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, urlValues)
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodPut, importURL, nil)
		require.NoError(t, err)

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// should be rate limited
		require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	}

	// attempt pre import
	attemptImportFn(true)

	// attempt import
	attemptImportFn(false)
}

func TestGitlabAPI_RepositoryImport_MaxConcurrentImports_OneByOne(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	repoPathTemplate := "old/repo-%d"
	tagName := "import-tag"

	allRepoPaths := generateOldRepoPaths(t, repoPathTemplate, 2)

	mockedImportNotifSrv := newMockImportNotification(t, allRepoPaths...)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
		// only allow 1 import at a time
		withMigrationMaxConcurrentImports(1))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	seedMultipleFSManifestsWithTag(t, env, tagName, allRepoPaths)

	repoPath1 := fmt.Sprintf(repoPathTemplate, 0)
	repoRef1, err := reference.WithName(repoPath1)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef1, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// repoPath1 should be accepted
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	repoPath2 := fmt.Sprintf(repoPathTemplate, 1)

	repoRef2, err := reference.WithName(repoPath2)
	require.NoError(t, err)

	importURL2, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef2, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req2, err := http.NewRequest(http.MethodPut, importURL2, nil)
	require.NoError(t, err)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	// repoPath2 should be rate limited
	require.Equal(t, http.StatusTooManyRequests, resp2.StatusCode)

	// wait for repoPath1 import notification
	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath1, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 2*time.Second,
	)

	// attempt second import for repoPath2 should succeed
	req3, err := http.NewRequest(http.MethodPut, importURL2, nil)
	require.NoError(t, err)

	resp3, err := http.DefaultClient.Do(req3)
	require.NoError(t, err)
	defer resp3.Body.Close()

	// should be accepted
	require.Equal(t, http.StatusAccepted, resp3.StatusCode)

	// second import should succeed
	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath2, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 2*time.Second,
	)
}

func TestGitlabAPI_RepositoryImport_MaxConcurrentImports_ErrorShouldNotBlockLimits(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	repoPathTemplate := "old/repo-%d"
	tagName := "import-tag"

	allRepoPaths := generateOldRepoPaths(t, repoPathTemplate, 2)

	mockedImportNotifSrv := newMockImportNotification(t, allRepoPaths...)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
		// only allow 1 import at a time
		withMigrationMaxConcurrentImports(1))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	seedMultipleFSManifestsWithTag(t, env, tagName, allRepoPaths)

	repoPath1 := fmt.Sprintf(repoPathTemplate, 0)
	repoRef1, err := reference.WithName(repoPath1)
	require.NoError(t, err)

	// using an invalid value for `import_type` should raise an error and not allow the import to proceed
	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef1, url.Values{"import_type": []string{"invalid"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// repoPath1 fail
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "wrong response body error code", resp, v1.ErrorCodeInvalidQueryParamValue)

	repoPath2 := fmt.Sprintf(repoPathTemplate, 1)

	repoRef2, err := reference.WithName(repoPath2)
	require.NoError(t, err)

	importURL2, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef2, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req2, err := http.NewRequest(http.MethodPut, importURL2, nil)
	require.NoError(t, err)

	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()

	// repoPath2 should be accepted because the first request failed
	require.Equal(t, http.StatusAccepted, resp2.StatusCode)

	// second import should succeed
	mockedImportNotifSrv.waitForImportNotification(
		t, repoPath2, string(migration.RepositoryStatusPreImportComplete), "pre import completed successfully", 2*time.Second,
	)
}

func TestGitlabAPI_RepositoryImport_MaxConcurrentImports_NoopShouldNotBlockLimits(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	tagName := "tag"
	repoPath := "foo/bar"

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// only allow 1 import at a time
		withMigrationMaxConcurrentImports(1))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// create a repository on the database side
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// try to import it
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)
	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// should be a noop
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// repeat request, should not be rate limited
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestGitlabAPI_RepositoryImport_NoImportTypeParam(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir))
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	repoPath := "foo/bar"
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)
	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "invalid query param", resp, v1.ErrorCodeInvalidQueryParamValue)
}

func TestGitlabAPI_RepositoryImport_PreImportRequired(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	repoPath := "old/repo"
	tagName := "import-tag"

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Try repository final import (without a preceding pre import)
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Final import should not be allowed
	require.Equal(t, http.StatusFailedDependency, resp.StatusCode)
	checkBodyHasErrorCodes(t, "failed dependency", resp, v1.ErrorCodePreImportRequired)
}

func TestGitlabAPI_RepositoryImport_Delete_PreImportInProgress(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// make the pre-import take a long time so we can cancel it
		withMigrationTestSlowImport(waitForever),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Begin a pre-import
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	preImportURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"pre"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, preImportURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// DELETE the same URL
	req, err = http.NewRequest(http.MethodDelete, preImportURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// DELETE pre import should be accepted
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusPreImportCanceled),
		"pre import was canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportCanceled, "pre import was canceled manually")
}

func TestGitlabAPI_RepositoryImport_Delete_ImportInProgress(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	preImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, preImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// pre import repository and wait for it to complete
	preImportRepository(t, env, preImportNotifSrv, repoPath)

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	// Create a new environment with a long wait so we can cancel the import in progress
	env2 := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// make the pre-import take a long time so we can cancel it
		withMigrationTestSlowImport(waitForever),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	// Begin a final import
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env2.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Final import should start
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// DELETE the import using the same URL
	req, err = http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// DELETE final import should be accepted
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusImportCanceled),
		"final import was canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusImportCanceled, "final import was canceled manually")
}

func TestGitlabAPI_RepositoryImport_Delete_PreImportComplete_BadRequest(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	importURL := preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// DELETE the same URL
	req, err := http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// DELETE pre import should not be accepted
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "failed to cancel (pre)import", resp, v1.ErrorCodeImportCannotBeCanceled)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusPreImportComplete, "pre import completed successfully")
}

func TestGitlabAPI_RepositoryImport_Delete_ImportComplete_BadRequest(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// pre import repository and wait for it to complete
	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Create a new environment with a long wait so we can cancel the import in progress
	env2 := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)

	// Begin a final import
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env2.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Final import should start
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusImportComplete),
		"final import completed successfully",
		5*time.Second,
	)

	// DELETE the same URL
	req, err = http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// DELETE final import should not be accepted
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "failed to cancel (pre)import", resp, v1.ErrorCodeImportCannotBeCanceled)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusImportComplete, "final import completed successfully")
}

func TestGitlabAPI_RepositoryImport_Delete_NotFound(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Attempt to delete an import for a repository that is not found on the DB yet
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
