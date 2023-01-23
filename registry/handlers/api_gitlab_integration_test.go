//go:build integration && api_gitlab_test

package handlers_test

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/handlers"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/stretchr/testify/require"
)

var waitForever = time.Duration(math.MaxInt64)

func importRepository(t *testing.T, env *testEnv, mockNotificationSrv *mockImportNotification, repoPath, importType string) string {
	t.Helper()

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{importType}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	if mockNotificationSrv != nil {
		status := string(migration.RepositoryStatusImportComplete)
		detail := "import completed successfully"
		if importType == "pre" {
			status = string(migration.RepositoryStatusPreImportComplete)
			detail = "pre import completed successfully"
		}

		mockNotificationSrv.waitForImportNotification(
			t,
			repoPath,
			status,
			detail,
			5*time.Second,
		)
	}

	return importURL
}

// preImportRepository expects the pre import for repoPath to succeed, it waits for the import notification
// if mockNotificationSrv is not nil
func preImportRepository(t *testing.T, env *testEnv, mockNotificationSrv *mockImportNotification, repoPath string) string {
	return importRepository(t, env, mockNotificationSrv, repoPath, "pre")
}

// finalImportRepository expects the final import for repoPath to succeed, it waits for the import notification
// if mockNotificationSrv is not nil
func finalImportRepository(t *testing.T, env *testEnv, mockNotificationSrv *mockImportNotification, repoPath string) string {
	return importRepository(t, env, mockNotificationSrv, repoPath, "final")
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
	finalImportRepository(t, env, mockedImportNotifSrv, repoPath)

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

	importURL := finalImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Subsequent calls to the same repository should not start another import.
	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
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
		"timeout", 2*time.Second,
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
		"timeout", 2*time.Second,
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

func TestGitlabAPI_RepositoryImport_PreImportShutdown(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Simulate a long running import and prevent context from being canceled.
		withMigrationTestSlowImport(waitForever),
		withMigrationPreImportTimeout(time.Second*20),
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

	// Pre import should start without error.
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Pause to ensure the import goroutine has a chance to start, or we won't
	// have any ongoing imports queued up to cancel and the test will flake.
	time.Sleep(time.Millisecond * 200)

	// We need to cancel the imports in a second goroutine to avoid blocking
	// the notification server from getting the import notification.
	done := make(chan bool)
	go func() {
		env.app.CancelAllImportsAndWait()
		done <- true
	}()

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusPreImportCanceled),
		"forced cancelation",
		3*time.Second,
	)

	require.Eventually(t, func() bool { return <-done }, time.Second*2, time.Millisecond*500)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusPreImportCanceled, "forced cancelation")
}

func TestGitlabAPI_RepositoryImport_ImportShutdown(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled, withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// Push up a image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	env2 := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// Simulate a long running import and prevent context from being canceled.
		withMigrationTestSlowImport(waitForever),
		withMigrationImportTimeout(time.Second*20),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)
	env.app.Config.Migration.ImportNotification.Timeout = 20 * time.Second

	t.Cleanup(env.Shutdown)

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

	// Pause to ensure the import goroutine has a chance to start, or we won't
	// have any ongoing imports queued up to cancel and the test will flake.
	time.Sleep(time.Millisecond * 200)

	// We need to cancel the imports in a second goroutine to avoid blocking
	// the notification server from getting the import notification.
	done := make(chan bool)
	go func() {
		env2.app.CancelAllImportsAndWait()
		done <- true
	}()

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusImportCanceled),
		"forced cancelation",
		3*time.Second,
	)

	require.Eventually(t, func() bool { return <-done }, time.Second*2, time.Millisecond*500)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusImportCanceled, "forced cancelation")
}

func TestGitlabAPI_RepositoryImport_NoImportShutdown(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	// Ensure a registry instance not running any imports is able to shutdown
	// gracefully as normal.
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
	)

	// Set check invernal to the usual default as the check interval is used as
	// a timeout for canceling ongoing imports
	originalInterval := handlers.OngoingImportCheckIntervalSeconds
	handlers.OngoingImportCheckIntervalSeconds = time.Second * 5
	t.Cleanup(func() {
		handlers.OngoingImportCheckIntervalSeconds = originalInterval
		env.Shutdown()
	})

	env.requireDB(t)

	start := time.Now()
	require.NotPanics(t, env.app.CancelAllImportsAndWait)
	require.WithinDuration(t, time.Now(), start, time.Second*1)
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
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)

	env := newTestEnv(t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationTestSlowImport(-1),
		withMigrationRootDirectory(migrationDir),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)))
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

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
		"negative testing delay", 2*time.Second,
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
		"negative testing delay", 2*time.Second,
	)

	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusPreImportFailed, "negative testing delay")
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
		"pre import canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportCanceled, "pre import canceled")

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

func TestGitlabAPI_RepositoryImport_Put_PreImport_CanceledNearEndOfImport(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// make the pre import take a second longer, so we have time to cancel it,
		// but not enough time for the context to be canceled.
		withMigrationTestSlowImport(time.Second*2),
		withMigrationPreImportTimeout(time.Second*20),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)

	// Ensure that the import context is never canceled, and reset the check
	// internal back at the end of the test.
	originalInterval := handlers.OngoingImportCheckIntervalSeconds
	handlers.OngoingImportCheckIntervalSeconds = time.Second * 12
	t.Cleanup(func() {
		handlers.OngoingImportCheckIntervalSeconds = originalInterval
		env.Shutdown()
	})

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Begin an import
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
		"pre import canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportCanceled, "pre import canceled")
}

func TestGitlabAPI_RepositoryImport_Put_Import_CanceledNearEndOfImport(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		// make the pre import take a second longer, so we have time to cancel it,
		// but not enough time for the context to be canceled.
		withMigrationTestSlowImport(time.Second*2),
		withMigrationImportTimeout(time.Second*20),
		withImportNotification(mockImportNotificationServer(t, mockedImportNotifSrv)),
	)

	// Ensure that the import context is never canceled, and reset the check
	// internal back at the end of the test.
	originalInterval := handlers.OngoingImportCheckIntervalSeconds
	handlers.OngoingImportCheckIntervalSeconds = time.Second * 12
	t.Cleanup(func() {
		handlers.OngoingImportCheckIntervalSeconds = originalInterval
		env.Shutdown()
	})

	env.requireDB(t)

	// Push up an image to the old side of the registry, so we can migrate it below.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Pre import before import.
	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Begin an import
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"import_type": []string{"final"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Import should start
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// DELETE the same URL
	req, err = http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// DELETE import should be accepted
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusImportCanceled),
		"import canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusImportCanceled, "final import canceled")
}

func TestGitlabAPI_RepositoryImport_Put_Import_PreImport_Retry(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "import-tag"

	mockedImportNotifSrv := newMockImportNotification(t, repoPath)
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		withMigrationTestSlowImport(-1),
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

	mockedImportNotifSrv.waitForImportNotification(
		t,
		repoPath,
		string(migration.RepositoryStatusPreImportFailed),
		"negative testing delay",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportFailed, "negative testing delay")

	// reset delay
	env.app.Config.Migration.TestSlowImport = waitForever

	// retry pre import
	req, err = http.NewRequest(http.MethodPut, preImportURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// Pre import should start
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportInProgress, string(migration.RepositoryStatusPreImportInProgress))
}

func TestGitlabAPI_RepositoryImport_Put_PreImportTooSoon(t *testing.T) {
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

	// Second pre import attempts should not be able to occur immediately.
	req, err = http.NewRequest(http.MethodPut, preImportURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	checkBodyHasErrorCodes(t, "failed to begin (pre)import", resp, v1.ErrorCodeImportRepositoryNotReady)
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

func testGitlabApiRepositoryGet(t *testing.T, opts ...configOpt) {
	t.Helper()

	env := newTestEnv(t, opts...)
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

func TestGitlabAPI_Repository_Get(t *testing.T) {
	testGitlabApiRepositoryGet(t, disableMirrorFS)
}

func TestGitlabAPI_Repository_Get_WithCentralRepositoryCache(t *testing.T) {
	srv := testutil.RedisServer(t)
	testGitlabApiRepositoryGet(t, disableMirrorFS, withRedisCache(srv.Addr()))
}

func TestGitlabAPI_Repository_Get_SizeWithDescendants_NonExistingBase(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// creating sub repository by pushing an image to it
	targetRepoPath := "foo/bar/car"
	dm := seedRandomSchema2Manifest(t, env, targetRepoPath, putByTag("latest"))
	var expectedSize int64
	for _, d := range dm.Layers() {
		expectedSize += d.Size
	}

	// get size with descendants of base (non-existing) repository
	baseRepoPath := "foo/bar"
	baseRepoRef, err := reference.WithName(baseRepoPath)
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	r := handlers.RepositoryAPIResponse{}
	p, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	err = json.Unmarshal(p, &r)
	require.NoError(t, err)

	require.Equal(t, "bar", r.Name)
	require.Equal(t, baseRepoPath, r.Path)
	require.Equal(t, *r.Size, expectedSize)
	require.Empty(t, r.CreatedAt)
	require.Empty(t, r.UpdatedAt)
}

func TestGitlabAPI_Repository_Get_SizeWithDescendants_NonExistingTopLevel(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	baseRepoPath := "foo/bar"
	baseRepoRef, err := reference.WithName(baseRepoPath)
	u, err := env.builder.BuildGitlabV1RepositoryURL(baseRepoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
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

	// attempt second import for repoPath2 after waiting a bit should succeed
	time.Sleep(50 * time.Millisecond)

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
		"pre import canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, preImportURL, repoPath, migration.RepositoryStatusPreImportCanceled, "pre import canceled")
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
		"final import canceled",
		5*time.Second,
	)

	// Get the import status
	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusImportCanceled, "final import canceled")
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
	t.Cleanup(env2.Shutdown)

	importURL := finalImportRepository(t, env2, mockedImportNotifSrv, repoPath)

	// DELETE the same URL
	req, err := http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
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

func TestGitlabAPI_RepositoryImport_Delete_Force_Native(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "tag-name"
	env := newTestEnv(
		t, withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
	)
	t.Cleanup(env.Shutdown)

	env.requireDB(t)

	// Push up a native image.
	seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Attempt to force delete an import for a native repository
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"force": []string{"true"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	checkBodyHasErrorCodes(t, "a native repository cannot be forcefully canceled", resp, v1.ErrorCodeImportCannotBeCanceled)
}

func TestGitlabAPI_RepositoryImport_Delete_PreImportComplete_Force(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "tag-name"

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

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Force a pre import that has completed
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"force": []string{"true"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusPreImportCanceled, "forced cancelation")
}

func TestGitlabAPI_RepositoryImport_Delete_ImportComplete_Force(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")
	repoPath := "old/repo"
	tagName := "tag-name"

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

	preImportRepository(t, env, mockedImportNotifSrv, repoPath)
	finalImportRepository(t, env, mockedImportNotifSrv, repoPath)

	// Force a pre import that has completed
	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	importURL, err := env.builder.BuildGitlabV1RepositoryImportURL(repoRef, url.Values{"force": []string{"true"}})
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodDelete, importURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	assertImportStatus(t, importURL, repoPath, migration.RepositoryStatusImportCanceled, "forced cancelation")
}

func TestGitlabAPI_RepositoryTagsList(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	sortedTags := []string{
		"2j2ar",
		"asj9e",
		"dcsl6",
		"hpgkt",
		"jyi7b",
		"jyi7b-fxt1v",
		"jyi7b-sgv2q",
		"kb0j5",
		"n343n",
		"sb71y",
	}

	// shuffle tags before creation to make sure results are consistent regardless of creation order
	shuffledTags := shuffledCopy(sortedTags)

	// To simplify and speed up things we don't create N new images but rather N tags for the same new image. As result,
	// the `digest` and `size` for all returned tag details will be the same and only `name` varies. This allows us to
	// simplify the test setup and assertions.
	dgst, mediaType, size := createRepositoryWithMultipleIdenticalTags(t, env, imageName.Name(), shuffledTags)

	tt := []struct {
		name                string
		queryParams         url.Values
		expectedOrderedTags []string
		expectedLinkHeader  string
		expectedStatus      int
		expectedError       *errcode.ErrorCode
	}{
		{
			name:                "no query parameters",
			expectedStatus:      http.StatusOK,
			expectedOrderedTags: sortedTags,
		},
		{
			name:           "empty last query parameter",
			queryParams:    url.Values{"last": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "empty n query parameter",
			queryParams:    url.Values{"n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "empty last and n query parameters",
			queryParams:    url.Values{"last": []string{""}, "n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "non integer n query parameter",
			queryParams:    url.Values{"n": []string{"foo"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "1st page",
			queryParams:    url.Values{"n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"2j2ar",
				"asj9e",
				"dcsl6",
				"hpgkt",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=hpgkt&n=4>; rel="next"`,
		},
		{
			name:           "nth page",
			queryParams:    url.Values{"last": []string{"hpgkt"}, "n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"jyi7b",
				"jyi7b-fxt1v",
				"jyi7b-sgv2q",
				"kb0j5",
			},
			expectedLinkHeader: `</gitlab/v1/repositories/foo/bar/tags/list/?last=kb0j5&n=4>; rel="next"`,
		},
		{
			name:           "last page",
			queryParams:    url.Values{"last": []string{"kb0j5"}, "n": []string{"4"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"n343n",
				"sb71y",
			},
		},
		{
			name:           "zero page size",
			queryParams:    url.Values{"n": []string{"0"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "negative page size",
			queryParams:    url.Values{"n": []string{"-1"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:                "page size bigger than full list",
			queryParams:         url.Values{"n": []string{"1000"}},
			expectedStatus:      http.StatusOK,
			expectedOrderedTags: sortedTags,
		},
		{
			name:           "after marker",
			queryParams:    url.Values{"last": []string{"kb0j5/pic0i"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"n343n",
				"sb71y",
			},
		},
		{
			name:           "non existent marker",
			queryParams:    url.Values{"last": []string{"does-not-exist"}},
			expectedStatus: http.StatusOK,
			expectedOrderedTags: []string{
				"hpgkt",
				"jyi7b",
				"jyi7b-fxt1v",
				"jyi7b-sgv2q",
				"kb0j5",
				"n343n",
				"sb71y",
			},
		},
		{
			name:           "invalid marker",
			queryParams:    url.Values{"last": []string{"-"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName, test.queryParams)
			require.NoError(t, err)
			resp, err := http.Get(u)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, test.expectedStatus, resp.StatusCode)

			if test.expectedError != nil {
				checkBodyHasErrorCodes(t, "", resp, *test.expectedError)
				return
			}

			var body []*handlers.RepositoryTagResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)

			var expectedBody []*handlers.RepositoryTagResponse
			for _, name := range test.expectedOrderedTags {
				expectedBody = append(expectedBody, &handlers.RepositoryTagResponse{
					// this is what changes
					Name: name,
					// the rest is the same for all objects as we have a single image that all tags point to
					Digest:    dgst.String(),
					MediaType: mediaType,
					Size:      size,
				})
			}

			// Check that created_at is not empty but updated_at is. We then need to erase the created_at attribute from
			// the response payload before comparing. This is the best we can do as we have no control/insight into the
			// timestamps at which records are inserted on the DB.
			for _, d := range body {
				require.Empty(t, d.UpdatedAt)
				require.NotEmpty(t, d.CreatedAt)
				d.CreatedAt = ""
			}

			require.Equal(t, expectedBody, body)
			require.Equal(t, test.expectedLinkHeader, resp.Header.Get("Link"))
		})
	}
}

// TestGitlabAPI_RepositoryTagsList_DefaultPageSize asserts that the API enforces a default page size of 100. We do it
// here instead of TestGitlabAPI_RepositoryTagsList because we have to create more than 100 tags to test this. Doing it
// in the former test would mean more complicated table test definitions, instead of the current small set of tags that
// make it easy to follow/understand the expected results.
func TestGitlabAPI_RepositoryTagsList_DefaultPageSize(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// generate 100+1 random tag names
	tags := make([]string, 0, 101)
	for i := 0; i <= 100; i++ {
		b := make([]byte, 10)
		rand.Read(b)
		tags = append(tags, fmt.Sprintf("%x", b)[:10])
	}

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)
	createRepositoryWithMultipleIdenticalTags(t, env, imageName.Name(), tags)

	u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// simply assert the number of tag detail objects in the body
	var body []*handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)

	require.Len(t, body, 100)

	// make sure the next page link starts at tag 100th
	sort.Strings(tags)
	expectedLink := fmt.Sprintf(`</gitlab/v1/repositories/%s/tags/list/?last=%s&n=100>; rel="next"`, imageName.Name(), tags[99])
	require.Equal(t, expectedLink, resp.Header.Get("Link"))
}

func TestGitlabAPI_RepositoryTagsList_RepositoryNotFound(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	u, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName)
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Link"))
	checkBodyHasErrorCodes(t, "repository not found", resp, v2.ErrorCodeNameUnknown)
}

func TestGitlabAPI_RepositoryTagsList_EmptyRepository(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	imageName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	// create repository and then delete its only tag
	tag := "latest"
	createRepository(t, env, imageName.Name(), tag)

	ref, err := reference.WithTag(imageName, tag)
	require.NoError(t, err)

	tagURL, err := env.builder.BuildTagURL(ref)
	require.NoError(t, err)

	res, err := httpDelete(tagURL)
	require.NoError(t, err)
	defer res.Body.Close()

	require.Equal(t, http.StatusAccepted, res.StatusCode)

	// assert response
	tagsURL, err := env.builder.BuildGitlabV1RepositoryTagsURL(imageName)
	require.NoError(t, err)

	resp, err := http.Get(tagsURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	var list []handlers.RepositoryTagResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&list)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Link"))
	require.Empty(t, list)
}

func TestGitlabAPI_SubRepositoryList(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	sortedReposWithTag := []string{
		"foo/bar",
		"foo/bar/a",
		"foo/bar/b",
		"foo/bar/b/c",
	}

	baseRepoName, err := reference.WithName("foo/bar")

	repoWithoutTag := "foo/bar/b2"

	require.NoError(t, err)
	tagName := "latest"
	// seed repos with the same base path foo/bar with tags
	seedMultipleRepositoriesWithTaggedManifest(t, env, tagName, sortedReposWithTag)
	// seed a repo under the same base path foo/bar but without tags
	seedRandomSchema2Manifest(t, env, repoWithoutTag, putByDigest)

	tt := []struct {
		name               string
		queryParams        url.Values
		expectedRepoPaths  []string
		expectedLinkHeader string
		expectedStatus     int
		expectedError      *errcode.ErrorCode
	}{
		{
			name:              "no query parameters",
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: sortedReposWithTag,
		},
		{
			name:           "empty last query parameter",
			queryParams:    url.Values{"last": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "empty n query parameter",
			queryParams:    url.Values{"n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "empty last and n query parameters",
			queryParams:    url.Values{"last": []string{""}, "n": []string{""}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:           "non integer n query parameter",
			queryParams:    url.Values{"n": []string{"foo"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamType,
		},
		{
			name:               "1st page",
			queryParams:        url.Values{"n": []string{"3"}},
			expectedStatus:     http.StatusOK,
			expectedRepoPaths:  sortedReposWithTag[:3],
			expectedLinkHeader: fmt.Sprintf(`</gitlab/v1/repository-paths/%s/repositories/list/?last=%s&n=3>; rel="next"`, baseRepoName.Name(), url.QueryEscape(sortedReposWithTag[2])),
		},
		{
			name:               "nth page",
			queryParams:        url.Values{"last": []string{"foo/bar"}, "n": []string{"2"}},
			expectedStatus:     http.StatusOK,
			expectedRepoPaths:  sortedReposWithTag[1:3],
			expectedLinkHeader: fmt.Sprintf(`</gitlab/v1/repository-paths/%s/repositories/list/?last=%s&n=2>; rel="next"`, baseRepoName.Name(), url.QueryEscape(sortedReposWithTag[2])),
		},
		{
			name:              "last page",
			queryParams:       url.Values{"last": []string{"foo/bar/b/c"}, "n": []string{"4"}},
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: []string{},
		},
		{
			name:           "zero page size",
			queryParams:    url.Values{"n": []string{"0"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:           "negative page size",
			queryParams:    url.Values{"n": []string{"-1"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
		{
			name:              "page size bigger than full list",
			queryParams:       url.Values{"n": []string{"1000"}},
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: sortedReposWithTag,
		},
		{
			name:              "non existent marker sort",
			queryParams:       url.Values{"last": []string{"foo/bar/0"}},
			expectedStatus:    http.StatusOK,
			expectedRepoPaths: sortedReposWithTag[1:],
		},
		{
			name:           "invalid marker format",
			queryParams:    url.Values{"last": []string{":"}},
			expectedStatus: http.StatusBadRequest,
			expectedError:  &v1.ErrorCodeInvalidQueryParamValue,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName, test.queryParams)
			require.NoError(t, err)
			resp, err := http.Get(u)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, test.expectedStatus, resp.StatusCode)

			if test.expectedError != nil {
				checkBodyHasErrorCodes(t, "", resp, *test.expectedError)
				return
			}

			var body []*handlers.RepositoryAPIResponse
			dec := json.NewDecoder(resp.Body)
			err = dec.Decode(&body)
			require.NoError(t, err)
			expectedBody := make([]*handlers.RepositoryAPIResponse, 0, len(test.expectedRepoPaths))
			for _, path := range test.expectedRepoPaths {
				splitPath := strings.Split(path, "/")
				expectedBody = append(expectedBody, &handlers.RepositoryAPIResponse{
					Name:          splitPath[len(splitPath)-1],
					Path:          path,
					Size:          nil,
					SizePrecision: "",
				})
			}
			// Check that created_at is not empty but updated_at is. We then need to erase the created_at attribute from
			// the response payload before comparing. This is the best we can do as we have no control/insight into the
			// timestamps at which records are inserted on the DB.
			for _, d := range body {
				require.Empty(t, d.UpdatedAt)
				require.NotEmpty(t, d.CreatedAt)
				d.CreatedAt = ""
			}

			require.Equal(t, expectedBody, body)
			require.Equal(t, test.expectedLinkHeader, resp.Header.Get("Link"))
		})
	}
}

// TestGitlabAPI_SubRepositoryList_DefaultPageSize asserts that the API enforces a default page size of 100. We do it
// here instead of TestGitlabAPI_SubRepositoryList because we have to create more than 100 repositories
// w/tags to test this. Doing it in the former test would mean more complicated table test definitions,
// instead of the current small set of repositories w/tags that make it easy to follow/understand the expected results.
func TestGitlabAPI_SubRepositoryList_DefaultPageSize(t *testing.T) {

	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	// generate 100+1 repos with tagged images
	reposWithTag := make([]string, 0, 101)
	for i := 0; i <= 100; i++ {
		reposWithTag = append(reposWithTag, fmt.Sprintf("foo/bar/%d", i))
	}

	baseRepoName, err := reference.WithName("foo/bar")
	tagName := "latest"
	require.NoError(t, err)

	// seed repos of the same base path foo/bar but with a tagged manifest
	seedMultipleRepositoriesWithTaggedManifest(t, env, tagName, reposWithTag)

	u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// simply assert the number of repositories in the body
	var body []*handlers.RepositoryAPIResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)

	require.Len(t, body, 100)

	// make sure the next page link starts at repo 100th
	sort.Strings(reposWithTag)
	expectedLink := fmt.Sprintf(`</gitlab/v1/repository-paths/%s/repositories/list/?last=%s&n=100>; rel="next"`, baseRepoName.Name(), url.QueryEscape(reposWithTag[99]))
	require.Equal(t, expectedLink, resp.Header.Get("Link"))
}

func TestGitlabAPI_SubRepositoryList_EmptyRepository(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	t.Cleanup(env.Shutdown)
	env.requireDB(t)

	baseRepoName, err := reference.WithName("foo/bar")
	require.NoError(t, err)

	u, err := env.builder.BuildGitlabV1SubRepositoriesURL(baseRepoName)
	require.NoError(t, err)
	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body []*handlers.RepositoryAPIResponse
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&body)
	require.NoError(t, err)
	require.NotNil(t, body)
	require.ElementsMatch(t, body, []*handlers.RepositoryAPIResponse{})
}
