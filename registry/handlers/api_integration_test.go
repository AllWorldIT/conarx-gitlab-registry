//go:build integration && handlers_test

package handlers_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/testutil"
	"github.com/docker/distribution/version"

	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

// TestBlobAPI conducts a full test of the of the blob api.
func TestBlobAPI(t *testing.T) {
	env1 := newTestEnv(t)
	args := makeBlobArgs(t)
	testBlobAPI(t, env1, args)
	env1.Shutdown()

	env2 := newTestEnv(t, withDelete)
	args = makeBlobArgs(t)
	testBlobAPI(t, env2, args)
	env2.Shutdown()
}

func TestBlobAPI_Get_BlobNotInDatabase(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// Disable the database so writes only go to the filesytem.
	env.config.Database.Enabled = false

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// Enable the database again so that reads first check the database.
	env.config.Database.Enabled = true

	// fetch layer
	res, err := http.Get(blobURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func TestBlobAPI_GetBlobFromFilesystemAfterDatabaseWrites(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// Disable the database to check that the filesystem mirroring worked correctly.
	env.config.Database.Enabled = false

	// fetch layer
	res, err := http.Get(blobURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	// verify response headers
	_, err = args.layerFile.Seek(0, io.SeekStart)
	require.NoError(t, err)
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(args.layerFile)
	require.NoError(t, err)

	require.Equal(t, res.Header.Get("Content-Length"), strconv.Itoa(buf.Len()))
	require.Equal(t, res.Header.Get("Content-Type"), "application/octet-stream")
	require.Equal(t, res.Header.Get("Docker-Content-Digest"), args.layerDigest.String())
	require.Equal(t, res.Header.Get("ETag"), fmt.Sprintf(`"%s"`, args.layerDigest))
	require.Equal(t, res.Header.Get("Cache-Control"), "max-age=31536000")

	// verify response body
	v := args.layerDigest.Verifier()
	_, err = io.Copy(v, res.Body)
	require.NoError(t, err)
	require.True(t, v.Verified())
}

func TestBlobAPI_GetBlobFromFilesystemAfterDatabaseWrites_DisableMirrorFS(t *testing.T) {
	env := newTestEnv(t, disableMirrorFS)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// create repository with a layer
	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	// Disable the database to check that the filesystem mirroring was disabled.
	env.config.Database.Enabled = false

	// fetch layer
	res, err := http.Get(blobURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func TestBlobDelete(t *testing.T) {
	env := newTestEnv(t, withDelete)
	defer env.Shutdown()

	args := makeBlobArgs(t)
	env = testBlobAPI(t, env, args)
	testBlobDelete(t, env, args)
}

func TestRelativeURL(t *testing.T) {
	config := newConfig()
	config.HTTP.RelativeURLs = false
	env := newTestEnvWithConfig(t, &config)
	defer env.Shutdown()
	ref, _ := reference.WithName("foo/bar")
	uploadURLBaseAbs, _ := startPushLayer(t, env, ref)

	u, err := url.Parse(uploadURLBaseAbs)
	if err != nil {
		t.Fatal(err)
	}
	if !u.IsAbs() {
		t.Fatal("Relative URL returned from blob upload chunk with non-relative configuration")
	}

	args := makeBlobArgs(t)
	resp, err := doPushLayer(t, env.builder, ref, args.layerDigest, uploadURLBaseAbs, args.layerFile)
	if err != nil {
		t.Fatalf("unexpected error doing layer push relative url: %v", err)
	}
	defer resp.Body.Close()
	checkResponse(t, "relativeurl blob upload", resp, http.StatusCreated)
	u, err = url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if !u.IsAbs() {
		t.Fatal("Relative URL returned from blob upload with non-relative configuration")
	}

	config.HTTP.RelativeURLs = true
	args = makeBlobArgs(t)
	uploadURLBaseRelative, _ := startPushLayer(t, env, ref)
	u, err = url.Parse(uploadURLBaseRelative)
	if err != nil {
		t.Fatal(err)
	}
	if u.IsAbs() {
		t.Fatal("Absolute URL returned from blob upload chunk with relative configuration")
	}

	// Start a new upload in absolute mode to get a valid base URL
	config.HTTP.RelativeURLs = false
	uploadURLBaseAbs, _ = startPushLayer(t, env, ref)
	u, err = url.Parse(uploadURLBaseAbs)
	if err != nil {
		t.Fatal(err)
	}
	if !u.IsAbs() {
		t.Fatal("Relative URL returned from blob upload chunk with non-relative configuration")
	}

	// Complete upload with relative URLs enabled to ensure the final location is relative
	config.HTTP.RelativeURLs = true
	resp, err = doPushLayer(t, env.builder, ref, args.layerDigest, uploadURLBaseAbs, args.layerFile)
	if err != nil {
		t.Fatalf("unexpected error doing layer push relative url: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "relativeurl blob upload", resp, http.StatusCreated)
	u, err = url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if u.IsAbs() {
		t.Fatal("Relative URL returned from blob upload with non-relative configuration")
	}
}

func TestBlobDeleteDisabled(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()
	args := makeBlobArgs(t)

	imageName := args.imageName
	layerDigest := args.layerDigest
	ref, _ := reference.WithDigest(imageName, layerDigest)
	layerURL, err := env.builder.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf("error building url: %v", err)
	}

	resp, err := httpDelete(layerURL)
	if err != nil {
		t.Fatalf("unexpected error deleting when disabled: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "status of disabled delete", resp, http.StatusMethodNotAllowed)
}

func testBlobAPI(t *testing.T, env *testEnv, args blobArgs) *testEnv {
	// TODO(stevvooe): This test code is complete junk but it should cover the
	// complete flow. This must be broken down and checked against the
	// specification *before* we submit the final to docker core.
	imageName := args.imageName
	layerFile := args.layerFile
	layerDigest := args.layerDigest

	ref, _ := reference.WithDigest(imageName, layerDigest)
	layerURL, err := env.builder.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf("error building url: %v", err)
	}

	// ------------------------------------------
	// Start an upload, check the status then cancel
	uploadURLBase, uploadUUID := startPushLayer(t, env, imageName)

	// A status check should work
	resp, err := http.Get(uploadURLBase)
	if err != nil {
		t.Fatalf("unexpected error getting upload status: %v", err)
	}
	defer resp.Body.Close()
	checkResponse(t, "status of deleted upload", resp, http.StatusNoContent)
	checkHeaders(t, resp, http.Header{
		"Location":           []string{"*"},
		"Range":              []string{"0-0"},
		"Docker-Upload-UUID": []string{uploadUUID},
	})

	req, err := http.NewRequest("DELETE", uploadURLBase, nil)
	if err != nil {
		t.Fatalf("unexpected error creating delete request: %v", err)
	}

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error sending delete request: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "deleting upload", resp, http.StatusNoContent)

	// A status check should result in 404
	resp, err = http.Get(uploadURLBase)
	if err != nil {
		t.Fatalf("unexpected error getting upload status: %v", err)
	}
	defer resp.Body.Close()
	checkResponse(t, "status of deleted upload", resp, http.StatusNotFound)

	// -----------------------------------------
	// Do layer push with an empty body and different digest
	uploadURLBase, _ = startPushLayer(t, env, imageName)
	resp, err = doPushLayer(t, env.builder, imageName, layerDigest, uploadURLBase, bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error doing bad layer push: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "bad layer push", resp, http.StatusBadRequest)
	checkBodyHasErrorCodes(t, "bad layer push", resp, v2.ErrorCodeDigestInvalid)

	// -----------------------------------------
	// Do layer push with an empty body and correct digest
	zeroDigest, err := digest.FromReader(bytes.NewReader([]byte{}))
	if err != nil {
		t.Fatalf("unexpected error digesting empty buffer: %v", err)
	}

	uploadURLBase, _ = startPushLayer(t, env, imageName)
	pushLayer(t, env.builder, imageName, zeroDigest, uploadURLBase, bytes.NewReader([]byte{}))

	// -----------------------------------------
	// Do layer push with an empty body and correct digest

	// This is a valid but empty tarfile!
	emptyTar := bytes.Repeat([]byte("\x00"), 1024)
	emptyDigest, err := digest.FromReader(bytes.NewReader(emptyTar))
	if err != nil {
		t.Fatalf("unexpected error digesting empty tar: %v", err)
	}

	uploadURLBase, _ = startPushLayer(t, env, imageName)
	pushLayer(t, env.builder, imageName, emptyDigest, uploadURLBase, bytes.NewReader(emptyTar))

	// ------------------------------------------
	// Now, actually do successful upload.
	layerLength, _ := layerFile.Seek(0, io.SeekEnd)
	layerFile.Seek(0, io.SeekStart)

	uploadURLBase, _ = startPushLayer(t, env, imageName)
	pushLayer(t, env.builder, imageName, layerDigest, uploadURLBase, layerFile)

	// ------------------------------------------
	// Now, push just a chunk
	layerFile.Seek(0, 0)

	canonicalDigester := digest.Canonical.Digester()
	if _, err := io.Copy(canonicalDigester.Hash(), layerFile); err != nil {
		t.Fatalf("error copying to digest: %v", err)
	}
	canonicalDigest := canonicalDigester.Digest()

	layerFile.Seek(0, 0)
	uploadURLBase, _ = startPushLayer(t, env, imageName)
	uploadURLBase, dgst := pushChunk(t, env.builder, imageName, uploadURLBase, layerFile, layerLength)
	finishUpload(t, env.builder, imageName, uploadURLBase, dgst)

	// ------------------------
	// Use a head request to see if the layer exists.
	resp, err = http.Head(layerURL)
	if err != nil {
		t.Fatalf("unexpected error checking head on existing layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "checking head on existing layer", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Content-Length":        []string{fmt.Sprint(layerLength)},
		"Docker-Content-Digest": []string{canonicalDigest.String()},
	})

	// ----------------
	// Fetch the layer!
	resp, err = http.Get(layerURL)
	if err != nil {
		t.Fatalf("unexpected error fetching layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching layer", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Content-Length":        []string{fmt.Sprint(layerLength)},
		"Docker-Content-Digest": []string{canonicalDigest.String()},
	})

	// Verify the body
	verifier := layerDigest.Verifier()
	io.Copy(verifier, resp.Body)

	if !verifier.Verified() {
		t.Fatalf("response body did not pass verification")
	}

	// ----------------
	// Fetch the layer with an invalid digest
	badURL := strings.Replace(layerURL, "sha256", "sha257", 1)
	resp, err = http.Get(badURL)
	if err != nil {
		t.Fatalf("unexpected error fetching layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching layer bad digest", resp, http.StatusBadRequest)

	// Cache headers
	resp, err = http.Get(layerURL)
	if err != nil {
		t.Fatalf("unexpected error fetching layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching layer", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Content-Length":        []string{fmt.Sprint(layerLength)},
		"Docker-Content-Digest": []string{canonicalDigest.String()},
		"ETag":                  []string{fmt.Sprintf(`"%s"`, canonicalDigest)},
		"Cache-Control":         []string{"max-age=31536000"},
	})

	// Matching etag, gives 304
	etag := resp.Header.Get("Etag")
	req, err = http.NewRequest("GET", layerURL, nil)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	req.Header.Set("If-None-Match", etag)

	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching layer with etag", resp, http.StatusNotModified)

	// Non-matching etag, gives 200
	req, err = http.NewRequest("GET", layerURL, nil)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	req.Header.Set("If-None-Match", "")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	checkResponse(t, "fetching layer with invalid etag", resp, http.StatusOK)

	// Missing tests:
	//	- Upload the same tar file under and different repository and
	//       ensure the content remains uncorrupted.
	return env
}

func testBlobDelete(t *testing.T, env *testEnv, args blobArgs) {
	// Upload a layer
	imageName := args.imageName
	layerFile := args.layerFile
	layerDigest := args.layerDigest

	ref, _ := reference.WithDigest(imageName, layerDigest)
	layerURL, err := env.builder.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf(err.Error())
	}
	// ---------------
	// Delete a layer
	resp, err := httpDelete(layerURL)
	if err != nil {
		t.Fatalf("unexpected error deleting layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "deleting layer", resp, http.StatusAccepted)
	checkHeaders(t, resp, http.Header{
		"Content-Length": []string{"0"},
	})

	// ---------------
	// Try and get it back
	// Use a head request to see if the layer exists.
	resp, err = http.Head(layerURL)
	if err != nil {
		t.Fatalf("unexpected error checking head on existing layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "checking existence of deleted layer", resp, http.StatusNotFound)

	// Delete already deleted layer
	resp, err = httpDelete(layerURL)
	if err != nil {
		t.Fatalf("unexpected error deleting layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "deleting layer", resp, http.StatusNotFound)

	// ----------------
	// Attempt to delete a layer with an invalid digest
	badURL := strings.Replace(layerURL, "sha256", "sha257", 1)
	resp, err = httpDelete(badURL)
	if err != nil {
		t.Fatalf("unexpected error fetching layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "deleting layer bad digest", resp, http.StatusBadRequest)

	// ----------------
	// Reupload previously deleted blob
	layerFile.Seek(0, io.SeekStart)

	uploadURLBase, _ := startPushLayer(t, env, imageName)
	pushLayer(t, env.builder, imageName, layerDigest, uploadURLBase, layerFile)

	layerFile.Seek(0, io.SeekStart)
	canonicalDigester := digest.Canonical.Digester()
	if _, err := io.Copy(canonicalDigester.Hash(), layerFile); err != nil {
		t.Fatalf("error copying to digest: %v", err)
	}
	canonicalDigest := canonicalDigester.Digest()

	// ------------------------
	// Use a head request to see if it exists
	resp, err = http.Head(layerURL)
	if err != nil {
		t.Fatalf("unexpected error checking head on existing layer: %v", err)
	}
	defer resp.Body.Close()

	layerLength, _ := layerFile.Seek(0, io.SeekEnd)
	checkResponse(t, "checking head on reuploaded layer", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Content-Length":        []string{fmt.Sprint(layerLength)},
		"Docker-Content-Digest": []string{canonicalDigest.String()},
	})
}

func TestDeleteDisabled(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	imageName, _ := reference.WithName("foo/bar")
	// "build" our layer file
	layerFile, layerDigest, err := testutil.CreateRandomTarFile()
	if err != nil {
		t.Fatalf("error creating random layer file: %v", err)
	}

	ref, _ := reference.WithDigest(imageName, layerDigest)
	layerURL, err := env.builder.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf("Error building blob URL")
	}
	uploadURLBase, _ := startPushLayer(t, env, imageName)
	pushLayer(t, env.builder, imageName, layerDigest, uploadURLBase, layerFile)

	resp, err := httpDelete(layerURL)
	if err != nil {
		t.Fatalf("unexpected error deleting layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "deleting layer with delete disabled", resp, http.StatusMethodNotAllowed)
}

func TestBlobMount_Migration_FromOldToNewRepoWithMigrationRoot(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	// Create a repository on the old code path and seed it with a layer.
	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	env1.config.Database.Enabled = false

	args, _ := createRepoWithBlob(t, env1)

	// Create a repository on the new code path with migration enabled and a
	// migration root directory. The filesystem should not find the source repo
	// since it's under the old root and we will not attempt a blob mount.
	toRepo := "new-target"
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	defer env2.Shutdown()

	assertBlobPostMountResponse(t, env2, args.imageName.String(), toRepo, args.layerDigest, http.StatusAccepted)
}

func TestBlobMount_Migration_FromNewToOldRepoWithMigrationRoot(t *testing.T) {
	rootDir := t.TempDir()

	migrationDir := filepath.Join(rootDir, "/new")

	// Create a repository on the old code path and seed it with a layer.
	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	env1.config.Database.Enabled = false

	oldRepoArgs, _ := createNamedRepoWithBlob(t, env1, "old/repo")

	// Create a repository on the new code path with migration enabled and a
	// migration root directory. The filesystem should not find the source repo
	// since it's under the new root and we will not attempt a blob mount.
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	defer env2.Shutdown()

	newRepoArgs, _ := createNamedRepoWithBlob(t, env2, "new/repo")

	assertBlobPostMountResponse(t, env1, newRepoArgs.imageName.String(), oldRepoArgs.imageName.String(), newRepoArgs.layerDigest, http.StatusAccepted)
}

func TestBlobMount_Migration_FromOldToOldRepoWithMigrationRoot(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	env1.config.Database.Enabled = false

	// Create two repositores on the old code path and seed it with layers.
	oldRepo1 := "old/repo-1"
	oldRepo2 := "old/repo-2"

	seedRandomSchema2Manifest(t, env1, oldRepo1, putByTag("test"))
	seedRandomSchema2Manifest(t, env1, oldRepo2, putByTag("test"))
	args1, _ := createNamedRepoWithBlob(t, env1, "old/repo-1")
	args2, _ := createNamedRepoWithBlob(t, env1, "old/repo-2")

	// Create a repository on the new code path to ensure that its presence does
	// not effect the behavior of the old repositories.
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	defer env2.Shutdown()

	seedRandomSchema2Manifest(t, env2, "new/repo", putByTag("test"))

	assertBlobPostMountResponse(t, env2, args1.imageName.String(), args2.imageName.String(), args1.layerDigest, http.StatusCreated)
}

func TestBlobMount_Migration_FromNewToNewRepoWithMigrationRoot(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	// Create a repository on the old code path and seed it with a layer to ensure
	// that its presence does not effect the behavior of the new repositories.
	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	env1.config.Database.Enabled = false

	createNamedRepoWithBlob(t, env1, "old/repo")

	// Create a repository on the new code path and seed it with a layer.
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	defer env2.Shutdown()

	args, _ := createNamedRepoWithBlob(t, env2, "new/repo")

	// Create another repository on the new code path. The database should find
	// the source repo and mount the blob.
	assertBlobPostMountResponse(t, env2, args.imageName.String(), "bar/repo", args.layerDigest, http.StatusCreated)

	// Try to delete the mounted blob on the target's filesystem. Should succeed
	// since we mirrored it to the filesystem.
	env3 := newTestEnv(t, withFSDriver(migrationDir), withDelete)
	defer env3.Shutdown()

	env3.config.Database.Enabled = false

	ref, err := reference.WithDigest(args.imageName, args.layerDigest)
	require.NoError(t, err)

	layerURL, err := env3.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	resp, err := httpDelete(layerURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusAccepted, resp.StatusCode)
}

func TestBlobMount_Migration_FromNewToNewRepoWithMigrationRootFSMirroringDisabled(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := filepath.Join(rootDir, "/new")

	// Create a repository on the old code path and seed it with a layer to ensure
	// that its presence does not effect the behavior of the new repositories.
	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	env1.config.Database.Enabled = false

	createNamedRepoWithBlob(t, env1, "old/repo")

	// Create a repository on the new code path and seed it with a layer, without
	// filesystem metadata.
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir), disableMirrorFS)
	defer env2.Shutdown()

	args, _ := createNamedRepoWithBlob(t, env2, "new/repo")

	// Create another repository on the new code path. The database should find
	// the source repo and mount the blob.
	assertBlobPostMountResponse(t, env2, args.imageName.String(), "bar/repo", args.layerDigest, http.StatusCreated)

	// Try to delete the mounted blob on the target's filesystem. Should fail
	// since we are not mirroring it.
	env3 := newTestEnv(t, withFSDriver(migrationDir), withDelete)
	defer env3.Shutdown()

	env3.config.Database.Enabled = false

	ref, err := reference.WithDigest(args.imageName, args.layerDigest)
	require.NoError(t, err)

	layerURL, err := env3.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	resp, err := httpDelete(layerURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDeleteReadOnly(t *testing.T) {
	rootDir := t.TempDir()

	setupEnv := newTestEnv(t, withFSDriver(rootDir))
	defer setupEnv.Shutdown()

	imageName, _ := reference.WithName("foo/bar")
	// "build" our layer file
	layerFile, layerDigest, err := testutil.CreateRandomTarFile()
	if err != nil {
		t.Fatalf("error creating random layer file: %v", err)
	}

	ref, _ := reference.WithDigest(imageName, layerDigest)
	uploadURLBase, _ := startPushLayer(t, setupEnv, imageName)
	pushLayer(t, setupEnv.builder, imageName, layerDigest, uploadURLBase, layerFile)

	// Reconfigure environment with withReadOnly enabled.
	setupEnv.Shutdown()
	env := newTestEnv(t, withFSDriver(rootDir), withReadOnly)
	defer env.Shutdown()

	layerURL, err := env.builder.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf("Error building blob URL")
	}

	resp, err := httpDelete(layerURL)
	if err != nil {
		t.Fatalf("unexpected error deleting layer: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "deleting layer in read-only mode", resp, http.StatusMethodNotAllowed)
}

func TestStartPushReadOnly(t *testing.T) {
	env := newTestEnv(t, withDelete, withReadOnly)
	defer env.Shutdown()

	imageName, _ := reference.WithName("foo/bar")

	layerUploadURL, err := env.builder.BuildBlobUploadURL(imageName)
	if err != nil {
		t.Fatalf("unexpected error building layer upload url: %v", err)
	}

	resp, err := http.Post(layerUploadURL, "", nil)
	if err != nil {
		t.Fatalf("unexpected error starting layer push: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "starting push in read-only mode", resp, http.StatusMethodNotAllowed)
}

type manifestArgs struct {
	imageName reference.Named
	mediaType string
	manifest  distribution.Manifest
	dgst      digest.Digest
}

// storageManifestErrDriverFactory implements the factory.StorageDriverFactory interface.
type storageManifestErrDriverFactory struct{}

const (
	repositoryWithManifestNotFound    = "manifesttagnotfound"
	repositoryWithManifestInvalidPath = "manifestinvalidpath"
	repositoryWithManifestBadLink     = "manifestbadlink"
	repositoryWithGenericStorageError = "genericstorageerr"
)

func (factory *storageManifestErrDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	// Initialize the mock driver
	var errGenericStorage = errors.New("generic storage error")
	return &mockErrorDriver{
		returnErrs: []mockErrorMapping{
			{
				pathMatch: fmt.Sprintf("%s/_manifests/tags", repositoryWithManifestNotFound),
				content:   nil,
				err:       storagedriver.PathNotFoundError{},
			},
			{
				pathMatch: fmt.Sprintf("%s/_manifests/tags", repositoryWithManifestInvalidPath),
				content:   nil,
				err:       storagedriver.InvalidPathError{},
			},
			{
				pathMatch: fmt.Sprintf("%s/_manifests/tags", repositoryWithManifestBadLink),
				content:   []byte("this is a bad sha"),
				err:       nil,
			},
			{
				pathMatch: fmt.Sprintf("%s/_manifests/tags", repositoryWithGenericStorageError),
				content:   nil,
				err:       errGenericStorage,
			},
		},
	}, nil
}

type mockErrorMapping struct {
	pathMatch string
	content   []byte
	err       error
}

// mockErrorDriver implements StorageDriver to force storage error on manifest request
type mockErrorDriver struct {
	storagedriver.StorageDriver
	returnErrs []mockErrorMapping
}

func (dr *mockErrorDriver) GetContent(ctx context.Context, path string) ([]byte, error) {
	for _, returns := range dr.returnErrs {
		if strings.Contains(path, returns.pathMatch) {
			return returns.content, returns.err
		}
	}
	return nil, errors.New("Unknown storage error")
}

func TestGetManifestWithStorageError(t *testing.T) {
	factory.Register("storagemanifesterror", &storageManifestErrDriverFactory{})
	config := configuration.Configuration{
		Storage: configuration.Storage{
			"storagemanifesterror": configuration.Parameters{},
			"maintenance": configuration.Parameters{"uploadpurging": map[interface{}]interface{}{
				"enabled": false,
			}},
		},
	}
	config.HTTP.Headers = headerConfig
	env1 := newTestEnvWithConfig(t, &config)
	defer env1.Shutdown()

	repo, _ := reference.WithName(repositoryWithManifestNotFound)
	testManifestWithStorageError(t, env1, repo, http.StatusNotFound, v2.ErrorCodeManifestUnknown)

	repo, _ = reference.WithName(repositoryWithGenericStorageError)
	testManifestWithStorageError(t, env1, repo, http.StatusInternalServerError, errcode.ErrorCodeUnknown)

	repo, _ = reference.WithName(repositoryWithManifestInvalidPath)
	testManifestWithStorageError(t, env1, repo, http.StatusInternalServerError, errcode.ErrorCodeUnknown)

	repo, _ = reference.WithName(repositoryWithManifestBadLink)
	testManifestWithStorageError(t, env1, repo, http.StatusNotFound, v2.ErrorCodeManifestUnknown)
}

func testManifestWithStorageError(t *testing.T, env *testEnv, imageName reference.Named, expectedStatusCode int, expectedErrorCode errcode.ErrorCode) {
	tag := "latest"
	tagRef, _ := reference.WithTag(imageName, tag)
	manifestURL, err := env.builder.BuildManifestURL(tagRef)
	if err != nil {
		t.Fatalf("unexpected error getting manifest url: %v", err)
	}

	// -----------------------------
	// Attempt to fetch the manifest
	resp, err := http.Get(manifestURL)
	if err != nil {
		t.Fatalf("unexpected error getting manifest: %v", err)
	}
	defer resp.Body.Close()
	checkResponse(t, "getting non-existent manifest", resp, expectedStatusCode)
	checkBodyHasErrorCodes(t, "getting non-existent manifest", resp, expectedErrorCode)
	return
}

func TestManifestAPI_Get_Schema2LayersAndConfigNotInDatabase(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	tagName := "schema2fallbacktag"
	repoPath := "schema2/fallback"

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", schema2.MediaTypeManifest)
			if test.etag != "" {
				req.Header.Set("If-None-Match", test.etag)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusNotFound, resp.StatusCode)
		})
	}
}

func TestManifestAPI_Put_Schema2LayersNotAssociatedWithRepositoryButArePresentInDatabase(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	tagName := "schema2missinglayerstag"
	repoPath := "schema2/missinglayers"

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	manifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	manifest.Config = cfgDesc

	// Create and push up 2 random layers to an unrelated repo so that they are
	// present within the database, but not associated with the manifest's repository.
	// Then push them to the normal repository with the database disabled.
	manifest.Layers = make([]distribution.Descriptor, 2)

	fakeRepoRef, err := reference.WithName("fakerepo")
	require.NoError(t, err)

	for i := range manifest.Layers {
		rs, dgst, size := createRandomSmallLayer()

		// Save the layer content as pushLayer exhausts the io.ReadSeeker
		layerBytes, err := io.ReadAll(rs)
		require.NoError(t, err)

		uploadURLBase, _ := startPushLayer(t, env, fakeRepoRef)
		pushLayer(t, env.builder, fakeRepoRef, dgst, uploadURLBase, bytes.NewReader(layerBytes))

		// Disable the database so writes only go to the filesytem.
		env.config.Database.Enabled = false

		uploadURLBase, _ = startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, bytes.NewReader(layerBytes))

		// Enable the database again so that reads first check the database.
		env.config.Database.Enabled = true

		manifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	deserializedManifest, err := schema2.FromStruct(*manifest)
	require.NoError(t, err)

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	resp := putManifest(t, "putting manifest, layers not associated with repository", tagURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestManifestAPI_BuildkitIndex tests that the API will accept pushes and pulls of Buildkit cache image index.
// Related to https://gitlab.com/gitlab-org/container-registry/-/issues/407.
func TestManifestAPI_BuildkitIndex(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	tagName := "latest"
	repoPath := "cache"

	// Create and push config
	cfgPayload := `{"layers":[{"blob":"sha256:136482bf81d1fa351b424ebb8c7e34d15f2c5ed3fc0b66b544b8312bda3d52d9","parent":-1},{"blob":"sha256:cc28e5fb26aec14963e8cf2987c137b84755a031068ea9284631a308dc087b35"}],"records":[{"digest":"sha256:16a28dbbe0151c1ab102d9414f78aa338627df3ce3c450905cd36d41b3e3d08e"},{"digest":"sha256:ef9770ef24f7942c1ccbbcac2235d9c0fbafc80d3af78ca0b483886adeac8960"}]}`
	cfgDesc := distribution.Descriptor{
		MediaType: mlcompat.MediaTypeBuildxCacheConfig,
		Digest:    digest.FromString(cfgPayload),
		Size:      int64(len(cfgPayload)),
	}
	assertBlobPutResponse(t, env, repoPath, cfgDesc.Digest, strings.NewReader(cfgPayload), 201)

	// Create and push 2 random layers
	layers := make([]distribution.Descriptor, 2)
	for i := range layers {
		rs, dgst, size := createRandomSmallLayer()
		assertBlobPutResponse(t, env, repoPath, dgst, rs, 201)

		layers[i] = distribution.Descriptor{
			MediaType: v1.MediaTypeImageLayerGzip,
			Digest:    dgst,
			Size:      size,
			Annotations: map[string]string{
				"buildkit/createdat":         time.Now().String(),
				"containerd.io/uncompressed": digest.FromString(strconv.Itoa(i)).String(),
			},
		}
	}

	idx := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     v1.MediaTypeImageIndex,
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{Descriptor: layers[0]},
			{Descriptor: layers[1]},
			{Descriptor: cfgDesc},
		},
	}

	didx, err := manifestlist.FromDescriptorsWithMediaType(idx.Manifests, v1.MediaTypeImageIndex)
	require.NoError(t, err)
	_, payload, err := didx.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	// Push index
	assertManifestPutByTagResponse(t, env, repoPath, didx, v1.MediaTypeImageIndex, tagName, 201)

	// Get index
	u := buildManifestTagURL(t, env, repoPath, tagName)
	req, err := http.NewRequest("GET", u, nil)
	require.NoError(t, err)

	req.Header.Set("Accept", v1.MediaTypeImageIndex)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

	var respIdx *manifestlist.DeserializedManifestList
	dec := json.NewDecoder(resp.Body)
	err = dec.Decode(&respIdx)
	require.NoError(t, err)

	require.EqualValues(t, didx, respIdx)

	// Stat each one of its references
	for _, d := range didx.References() {
		assertBlobHeadResponse(t, env, repoPath, d.Digest, 200)
	}
}

func TestManifestAPI_OCIIndexNoMediaType(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoRef, err := reference.WithName("foo")
	require.NoError(t, err)

	tag := "latest"
	tagRef, err := reference.WithTag(repoRef, tag)
	require.NoError(t, err)

	sentIndex := seedRandomOCIImageIndex(t, env, repoRef.Name(), putByTag(tag), withoutMediaType)

	manifestURL, err := env.builder.BuildManifestURL(tagRef)
	require.NoError(t, err)

	req, err := http.NewRequest("GET", manifestURL, nil)
	require.NoError(t, err)

	// v1.MediaTypeImageIndex would be enough, but this replicates the behavior of the Docker client and others
	req.Header.Set("Accept", schema2.MediaTypeManifest)
	req.Header.Add("Accept", v1.MediaTypeImageManifest)
	req.Header.Add("Accept", manifestlist.MediaTypeManifestList)
	req.Header.Add("Accept", v1.MediaTypeImageIndex)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	checkResponse(t, "", resp, http.StatusOK)

	// ensure content-type header is properly set and the digest matches the one we know
	_, payload, err := sentIndex.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	checkHeaders(t, resp, http.Header{
		"Content-Type":          []string{v1.MediaTypeImageIndex},
		"Docker-Content-Digest": []string{dgst.String()},
	})

	// ensure payload matches the one sent and double-check that the mediaType field is not filled
	var fetchedIndex *manifestlist.DeserializedManifestList
	err = json.NewDecoder(resp.Body).Decode(&fetchedIndex)
	require.NoError(t, err)

	require.EqualValues(t, sentIndex, fetchedIndex)
	require.Empty(t, fetchedIndex.MediaType)
}

// TestManifestAPI_ManifestListWithLayerReferences tests that the API will not
// accept pushes and pulls of non Buildkit cache image manifest lists which
// reference blobs.
// Related to https://gitlab.com/gitlab-org/container-registry/-/issues/407.
func TestManifestAPI_ManifestListWithLayerReferences(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	tagName := "latest"
	repoPath := "malformed-manifestlist"

	// Create and push 2 random layers
	layers := make([]distribution.Descriptor, 2)
	for i := range layers {
		rs, dgst, size := createRandomSmallLayer()
		assertBlobPutResponse(t, env, repoPath, dgst, rs, 201)

		layers[i] = distribution.Descriptor{
			MediaType: v1.MediaTypeImageLayerGzip,
			Digest:    dgst,
			Size:      size,
		}
	}

	idx := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{Descriptor: layers[0]},
			{Descriptor: layers[1]},
		},
	}

	didx, err := manifestlist.FromDescriptorsWithMediaType(idx.Manifests, manifestlist.MediaTypeManifestList)
	require.NoError(t, err)

	// Push index, since there is no buildx config layer, we should reject the push as invalid.
	assertManifestPutByTagResponse(t, env, repoPath, didx, manifestlist.MediaTypeManifestList, tagName, 400)
	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, didx)

	resp := putManifest(t, "putting manifest list bad request", manifestDigestURL, manifestlist.MediaTypeManifestList, didx)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	_, p, counts := checkBodyHasErrorCodes(t, "manifest list with layer blobs", resp, v2.ErrorCodeManifestBlobUnknown)
	expectedCounts := map[errcode.ErrorCode]int{v2.ErrorCodeManifestBlobUnknown: 2}
	require.EqualValuesf(t, expectedCounts, counts, "response body: %s", p)
}

func TestManifestAPI_Migration_Schema2(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := t.TempDir()

	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	oldRepoPath := "old-repo"

	// Push up a random image to create the repository on the filesystem
	seedRandomSchema2Manifest(t, env1, oldRepoPath, putByTag("test"), writeToFilesystemOnly)

	// Bring up a new environment in migration mode.
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	defer env2.Shutdown()

	// Push up a new manifest to the old repo.
	oldRepoTag := "schema2-old-repo"

	seedRandomSchema2Manifest(t, env2, oldRepoPath, putByTag(oldRepoTag))
	oldTagURL := buildManifestTagURL(t, env2, oldRepoPath, oldRepoTag)

	// Push a new manifest to a new repo.
	newRepoPath := "new-repo"
	newRepoTag := "schema2-new-repo"

	seedRandomSchema2Manifest(t, env2, newRepoPath, putByTag(newRepoTag))
	newTagURL := buildManifestTagURL(t, env2, newRepoPath, newRepoTag)

	// Ensure both repos are accessible in migration mode.
	req, err := http.NewRequest("GET", oldTagURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest("GET", newTagURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Bring up an environment in migration mode with fs mirroring turned off,
	// should only effect repositories on the new side, old repos must still
	// write fs metadata.
	env3 := newTestEnv(
		t,
		withFSDriver(rootDir),
		withMigrationEnabled,
		withMigrationRootDirectory(migrationDir),
		disableMirrorFS,
	)
	defer env3.Shutdown()

	// Push a new manifest to a new repo.
	newRepoNoMirroringPath := "new-repo-no-mirroring"
	newRepoNoMirroringTag := "schema2-new-no-mirroring"

	seedRandomSchema2Manifest(t, env3, newRepoNoMirroringPath, putByTag(newRepoNoMirroringTag))
	newRepoNoMirroringTagURL := buildManifestTagURL(t, env3, newRepoNoMirroringPath, newRepoNoMirroringTag)

	// Ensure that old repos can still be pushed to.
	oldRepoNoMirroringTag := "old-repo-no-mirroring"
	seedRandomSchema2Manifest(t, env3, oldRepoPath, putByTag(oldRepoNoMirroringTag))
	oldRepoNoMirroringTagURL := buildManifestTagURL(t, env3, oldRepoPath, oldRepoNoMirroringTag)

	// Ensure both repos are accessible in migration mode with mirroring disabled.
	req, err = http.NewRequest("GET", oldRepoNoMirroringTagURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	req, err = http.NewRequest("GET", newRepoNoMirroringTagURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Bring up an environment that uses database only metadata and the migration root.
	env4 := newTestEnv(t, withFSDriver(migrationDir))
	defer env4.Shutdown()

	// Rebuild URLS for new env.
	oldTagURL = buildManifestTagURL(t, env4, oldRepoPath, oldRepoTag)
	newTagURL = buildManifestTagURL(t, env4, newRepoPath, newRepoTag)
	oldRepoNoMirroringTagURL = buildManifestTagURL(t, env4, oldRepoPath, oldRepoNoMirroringTag)
	newRepoNoMirroringTagURL = buildManifestTagURL(t, env4, newRepoNoMirroringPath, newRepoNoMirroringTag)

	var tests = []struct {
		name            string
		databaseEnabled bool
		url             string
		expectedStatus  int
	}{
		{
			name:            "get old manifest from before migration database enabled",
			databaseEnabled: true,
			url:             oldTagURL,
			expectedStatus:  http.StatusNotFound,
		},
		{
			name:            "get old manifest from before migration database disabled",
			databaseEnabled: false,
			url:             oldTagURL,
			expectedStatus:  http.StatusNotFound,
		},
		{
			name:            "get old manifest from during migration database enabled",
			databaseEnabled: true,
			url:             oldRepoNoMirroringTagURL,
			expectedStatus:  http.StatusNotFound,
		},
		{
			name:            "get old manifest from during migration database disabled",
			databaseEnabled: false,
			url:             oldRepoNoMirroringTagURL,
			expectedStatus:  http.StatusNotFound,
		},
		{
			name:            "get new manifest database enabled",
			databaseEnabled: true,
			url:             newTagURL,
			expectedStatus:  http.StatusOK,
		},
		{
			name:            "get new manifest database disabled",
			databaseEnabled: false,
			url:             newTagURL,
			expectedStatus:  http.StatusOK,
		},
		{
			name:            "get new manifest no mirroring database enabled",
			databaseEnabled: true,
			url:             newRepoNoMirroringTagURL,
			expectedStatus:  http.StatusOK,
		},
		{
			name:            "get new manifest no mirroring database disabled",
			databaseEnabled: false,
			url:             newRepoNoMirroringTagURL,
			expectedStatus:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env4.config.Database.Enabled = tt.databaseEnabled

			req, err = http.NewRequest("GET", tt.url, nil)
			require.NoError(t, err)

			resp, err = http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, tt.expectedStatus, resp.StatusCode)
		})
	}
}

// The `Gitlab-Migration-Path` response header is set at the dispatcher level, and therefore transversal to all routes,
// so testing it for the simplest write (starting a blob upload) and read (unknown manifest get) operations is enough.
// The validation of the routing logic lies elsewhere.
func TestAPI_MigrationPathResponseHeader(t *testing.T) {
	rootDir := t.TempDir()
	migrationDir := t.TempDir()

	env1 := newTestEnv(t, withFSDriver(rootDir))
	defer env1.Shutdown()

	if !env1.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// Disable the database or the repository will be written out of migration
	// mode as a native repository.
	env1.config.Database.Enabled = false

	oldRepoRef, err := reference.WithName("old-repo")
	require.NoError(t, err)
	newRepoRef, err := reference.WithName("new-repo")
	require.NoError(t, err)

	// Write and read against the old repo. With migration disabled, the header should not be added to the response
	testMigrationPathRespHeader(t, env1, oldRepoRef, "")

	// Bring up a new environment in migration mode
	env2 := newTestEnv(t, withFSDriver(rootDir), withMigrationEnabled, withMigrationRootDirectory(migrationDir))
	defer env2.Shutdown()

	// Run the same tests again. Now the header should mention that the requests followed the old code path
	testMigrationPathRespHeader(t, env2, oldRepoRef, "old")

	// Write and read against a new repo. The header should mention that the requests followed the new code path
	testMigrationPathRespHeader(t, env2, newRepoRef, "new")
}

func testMigrationPathRespHeader(t *testing.T, env *testEnv, repoRef reference.Named, expectedValue string) {
	t.Helper()

	// test write operation, with a manifest upload
	manifest := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
		Layers: make([]distribution.Descriptor, 1),
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := schema2Config()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	manifest.Config = cfgDesc

	// Create and push up a random layer.
	rs, dgst, size := createRandomSmallLayer()

	uploadURLBase, _ = startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

	manifest.Layers[0] = distribution.Descriptor{
		Digest:    dgst,
		MediaType: schema2.MediaTypeLayer,
		Size:      size,
	}

	deserializedManifest, err := schema2.FromStruct(*manifest)
	require.NoError(t, err)

	manifestURL := buildManifestTagURL(t, env, repoRef.Name(), "test")

	resp := putManifest(t, "putting manifest no error", manifestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, expectedValue, resp.Header.Get("Gitlab-Migration-Path"))

	// test read operation, with a get for an unknown manifest
	manifestURL = buildManifestTagURL(t, env, repoRef.Name(), "foo")

	resp, err = http.Get(manifestURL)
	require.NoError(t, err)

	defer resp.Body.Close()

	checkResponse(t, "", resp, http.StatusNotFound)
	require.Equal(t, expectedValue, resp.Header.Get("Gitlab-Migration-Path"))
}

func TestManifestAPI_Get_Schema1(t *testing.T) {
	env := newTestEnv(t, withSchema1PreseededInMemoryDriver)
	defer env.Shutdown()

	// Seed manifest in database directly since schema1 manifests are unpushable.
	if env.config.Database.Enabled {
		repositoryStore := datastore.NewRepositoryStore(env.db)
		dbRepo, err := repositoryStore.CreateByPath(env.ctx, preseededSchema1RepoPath)

		mStore := datastore.NewManifestStore(env.db)

		dbManifest := &models.Manifest{
			NamespaceID:   dbRepo.NamespaceID,
			RepositoryID:  dbRepo.ID,
			SchemaVersion: 1,
			MediaType:     schema1.MediaTypeManifest,
			Digest:        preseededSchema1Digest,
			Payload:       models.Payload{},
		}

		err = mStore.Create(env.ctx, dbManifest)
		require.NoError(t, err)

		tagStore := datastore.NewTagStore(env.db)

		dbTag := &models.Tag{
			Name:         preseededSchema1TagName,
			NamespaceID:  dbRepo.NamespaceID,
			RepositoryID: dbRepo.ID,
			ManifestID:   dbManifest.ID,
		}

		err = tagStore.CreateOrUpdate(env.ctx, dbTag)
		require.NoError(t, err)
	}

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, preseededSchema1RepoPath, preseededSchema1TagName)

	repoRef, err := reference.WithName(preseededSchema1RepoPath)
	require.NoError(t, err)

	digestRef, err := reference.WithDigest(repoRef, preseededSchema1Digest)
	require.NoError(t, err)

	digestURL, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	tt := []struct {
		name        string
		manifestURL string
		etag        string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", test.manifestURL, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
			checkBodyHasErrorCodes(t, "invalid manifest", resp, v2.ErrorCodeManifestInvalid)
		})
	}
}

func TestManifestAPI_Get_Schema2FromFilesystemAfterDatabaseWrites(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	tagName := "schema2consistentfstag"
	repoPath := "schema2/consistentfs"

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	// Disable the database to check that the filesystem mirroring worked correctly.
	env.config.Database.Enabled = false

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", schema2.MediaTypeManifest)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
			require.Equal(t, fmt.Sprintf(`"%s"`, dgst), resp.Header.Get("ETag"))

			var fetchedManifest *schema2.DeserializedManifest
			dec := json.NewDecoder(resp.Body)

			err = dec.Decode(&fetchedManifest)
			require.NoError(t, err)

			require.EqualValues(t, deserializedManifest, fetchedManifest)
		})
	}
}

func TestManifestAPI_Delete_Schema2ManifestNotInDatabase(t *testing.T) {
	env := newTestEnv(t, withDelete)
	defer env.Shutdown()

	tagName := "schema2deletetag"
	repoPath := "schema2/delete"

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	// Push a random schema 2 manifest to the repository so that it is present in
	// the database, so only the manifest is not present in the database.
	seedRandomSchema2Manifest(t, env, repoPath, putByDigest)

	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tagName), writeToFilesystemOnly)

	manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp, err := httpDelete(manifestDigestURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestManifestAPI_Delete_ManifestReferencedByList(t *testing.T) {
	env := newTestEnv(t, withDelete)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	repoPath := "test"
	ml := seedRandomOCIImageIndex(t, env, repoPath, putByDigest)
	m := ml.References()[0]

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)
	digestRef, err := reference.WithDigest(repoRef, m.Digest)
	require.NoError(t, err)
	u, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	resp, err := httpDelete(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	checkBodyHasErrorCodes(t, "", resp, v2.ErrorCodeManifestReferencedInList)
}

func TestManifestAPI_Put_OCIFilesystemFallbackLayersNotInDatabase(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	tagName := "ocifallbacktag"
	repoPath := "oci/fallback"

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	deserializedManifest := seedRandomOCIManifest(t, env, repoPath, writeToFilesystemOnly)

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	resp := putManifest(t, "putting manifest no error", tagURL, v1.MediaTypeImageManifest, deserializedManifest.Manifest)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	require.Equal(t, digestURL, resp.Header.Get("Location"))

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)
	require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
}

func TestManifestAPI_Put_DatabaseEnabled_InvalidConfigMediaType(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	tagName := "latest"
	repoPath := "cache"
	unknownMediaType := "application/vnd.foo.container.image.v1+json"

	// Create and push config
	cfgPayload := `{"foo":"bar"}`
	cfgDesc := distribution.Descriptor{
		MediaType: unknownMediaType,
		Digest:    digest.FromString(cfgPayload),
		Size:      int64(len(cfgPayload)),
	}
	assertBlobPutResponse(t, env, repoPath, cfgDesc.Digest, strings.NewReader(cfgPayload), 201)

	// Create and push 1 random layer
	rs, dgst, size := createRandomSmallLayer()
	assertBlobPutResponse(t, env, repoPath, dgst, rs, 201)
	layerDesc := distribution.Descriptor{
		MediaType: v1.MediaTypeImageLayerGzip,
		Digest:    dgst,
		Size:      size,
	}

	m := ocischema.Manifest{
		Versioned: ocischema.SchemaVersion,
		Config:    cfgDesc,
		Layers:    []distribution.Descriptor{layerDesc},
	}

	dm, err := ocischema.FromStruct(m)
	require.NoError(t, err)

	// Push index
	u := buildManifestTagURL(t, env, repoPath, tagName)
	resp := putManifest(t, "", u, v1.MediaTypeImageManifest, dm.Manifest)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	errs, _, _ := checkBodyHasErrorCodes(t, "", resp, v2.ErrorCodeManifestInvalid)
	require.Len(t, errs, 1)
	errc, ok := errs[0].(errcode.Error)
	require.True(t, ok)
	require.Equal(t, datastore.ErrUnknownMediaType{MediaType: unknownMediaType}.Error(), errc.Detail)
}

func TestManifestAPI_Put_OCIImageIndexByTagManifestsNotPresentInDatabase(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	tagName := "ociindexmissingmanifeststag"
	repoPath := "ociindex/missingmanifests"

	// putRandomOCIImageIndex with putByTag tests that the manifest put happened without issue.
	deserializedManifest := seedRandomOCIImageIndex(t, env, repoPath, writeToFilesystemOnly)

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)

	resp := putManifest(t, "putting OCI image index missing manifests", tagURL, v1.MediaTypeImageIndex, deserializedManifest.ManifestList)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestManifestAPI_Get_OCIIndexFromFilesystemAfterDatabaseWrites(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	if !env.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	tagName := "ociindexconsistentfstag"
	repoPath := "ociindex/consistenfs"

	deserializedManifest := seedRandomOCIImageIndex(t, env, repoPath, putByTag(tagName))

	// Build URLs.
	tagURL := buildManifestTagURL(t, env, repoPath, tagName)
	digestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	tt := []struct {
		name        string
		manifestURL string
	}{
		{
			name:        "by tag",
			manifestURL: tagURL,
		},
		{
			name:        "by digest",
			manifestURL: digestURL,
		},
	}

	// Disable the database to check that the filesystem mirroring worked correctly.
	env.config.Database.Enabled = false

	for _, test := range tt {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest("GET", test.manifestURL, nil)
			require.NoError(t, err)

			req.Header.Set("Accept", v1.MediaTypeImageIndex)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
			require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
			require.Equal(t, fmt.Sprintf(`"%s"`, dgst), resp.Header.Get("ETag"))

			var fetchedManifest *manifestlist.DeserializedManifestList
			dec := json.NewDecoder(resp.Body)

			err = dec.Decode(&fetchedManifest)
			require.NoError(t, err)

			require.EqualValues(t, deserializedManifest, fetchedManifest)
		})
	}
}

func TestManifestAPI_Put_ManifestWithAllPossibleMediaTypeAndContentTypeCombinations(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	unknownMediaType := "application/vnd.foo.manifest.v1+json"

	tt := []struct {
		Name              string
		PayloadMediaType  string
		ContentTypeHeader string
		ExpectedStatus    int
		ExpectedErrCode   *errcode.ErrorCode
		ExpectedErrDetail string
	}{
		{
			Name:              "schema 2 in payload and content type",
			PayloadMediaType:  schema2.MediaTypeManifest,
			ContentTypeHeader: schema2.MediaTypeManifest,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:             "schema 2 in payload and no content type",
			PayloadMediaType: schema2.MediaTypeManifest,
			ExpectedStatus:   http.StatusCreated,
		},
		{
			Name:              "none in payload and schema 2 in content type",
			ContentTypeHeader: schema2.MediaTypeManifest,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: "no mediaType in manifest",
		},
		{
			Name:              "oci in payload and content type",
			PayloadMediaType:  v1.MediaTypeImageManifest,
			ContentTypeHeader: v1.MediaTypeImageManifest,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:              "oci in payload and no content type",
			PayloadMediaType:  v1.MediaTypeImageManifest,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, v1.MediaTypeImageManifest),
		},
		{
			Name:              "none in payload and oci in content type",
			ContentTypeHeader: v1.MediaTypeImageManifest,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:              "none in payload and content type",
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: "no mediaType in manifest",
		},
		{
			Name:              "schema 2 in payload and oci in content type",
			PayloadMediaType:  schema2.MediaTypeManifest,
			ContentTypeHeader: v1.MediaTypeImageManifest,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("if present, mediaType in manifest should be '%s' not '%s'", v1.MediaTypeImageManifest, schema2.MediaTypeManifest),
		},
		{
			Name:              "oci in payload and schema 2 in content type",
			PayloadMediaType:  v1.MediaTypeImageManifest,
			ContentTypeHeader: schema2.MediaTypeManifest,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, v1.MediaTypeImageManifest),
		},
		{
			Name:              "unknown in payload and schema 2 in content type",
			PayloadMediaType:  unknownMediaType,
			ContentTypeHeader: schema2.MediaTypeManifest,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, unknownMediaType),
		},
		{
			Name:              "unknown in payload and oci in content type",
			PayloadMediaType:  unknownMediaType,
			ContentTypeHeader: v1.MediaTypeImageManifest,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("if present, mediaType in manifest should be '%s' not '%s'", v1.MediaTypeImageManifest, unknownMediaType),
		},
		{
			Name:              "unknown in payload and content type",
			PayloadMediaType:  unknownMediaType,
			ContentTypeHeader: unknownMediaType,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, unknownMediaType),
		},
		{
			Name:              "unknown in payload and no content type",
			PayloadMediaType:  unknownMediaType,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, unknownMediaType),
		},
	}

	repoRef, err := reference.WithName("foo")
	require.NoError(t, err)

	// push random config blob
	cfgPayload, cfgDesc := schema2Config()
	u, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, u, bytes.NewReader(cfgPayload))

	// push random layer blob
	rs, layerDgst, size := createRandomSmallLayer()
	u, _ = startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, layerDgst, u, rs)

	for _, test := range tt {
		t.Run(test.Name, func(t *testing.T) {
			// build and push manifest
			m := &schema2.Manifest{
				Versioned: manifest.Versioned{
					SchemaVersion: 2,
					MediaType:     test.PayloadMediaType,
				},
				Config: distribution.Descriptor{
					MediaType: schema2.MediaTypeImageConfig,
					Digest:    cfgDesc.Digest,
				},
				Layers: []distribution.Descriptor{
					{
						Digest:    layerDgst,
						MediaType: schema2.MediaTypeLayer,
						Size:      size,
					},
				},
			}
			dm, err := schema2.FromStruct(*m)
			require.NoError(t, err)

			u = buildManifestDigestURL(t, env, repoRef.Name(), dm)
			resp := putManifest(t, "", u, test.ContentTypeHeader, dm.Manifest)
			defer resp.Body.Close()

			require.Equal(t, test.ExpectedStatus, resp.StatusCode)

			if test.ExpectedErrCode != nil {
				errs, _, _ := checkBodyHasErrorCodes(t, "", resp, v2.ErrorCodeManifestInvalid)
				require.Len(t, errs, 1)
				errc, ok := errs[0].(errcode.Error)
				require.True(t, ok)
				require.Equal(t, test.ExpectedErrDetail, errc.Detail)
			}
		})
	}
}

func TestManifestAPI_Put_ManifestListWithAllPossibleMediaTypeAndContentTypeCombinations(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	unknownMediaType := "application/vnd.foo.manifest.list.v1+json"

	tt := []struct {
		Name              string
		PayloadMediaType  string
		ContentTypeHeader string
		ExpectedStatus    int
		ExpectedErrCode   *errcode.ErrorCode
		ExpectedErrDetail string
	}{
		{
			Name:              "schema 2 in payload and content type",
			PayloadMediaType:  manifestlist.MediaTypeManifestList,
			ContentTypeHeader: manifestlist.MediaTypeManifestList,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:              "schema 2 in payload and no content type",
			PayloadMediaType:  manifestlist.MediaTypeManifestList,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "none in payload and schema 2 in content type",
			ContentTypeHeader: manifestlist.MediaTypeManifestList,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:              "oci in payload and content type",
			PayloadMediaType:  v1.MediaTypeImageIndex,
			ContentTypeHeader: v1.MediaTypeImageIndex,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("if present, mediaType in image index should be '%s' not '%s'", v1.MediaTypeImageIndex, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "oci in payload and no content type",
			PayloadMediaType:  v1.MediaTypeImageIndex,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "none in payload and oci in content type",
			ContentTypeHeader: v1.MediaTypeImageIndex,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("if present, mediaType in image index should be '%s' not '%s'", v1.MediaTypeImageIndex, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "none in payload and content type",
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "schema 2 in payload and oci in content type",
			PayloadMediaType:  manifestlist.MediaTypeManifestList,
			ContentTypeHeader: v1.MediaTypeImageIndex,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("if present, mediaType in image index should be '%s' not '%s'", v1.MediaTypeImageIndex, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "oci in payload and schema 2 in content type",
			PayloadMediaType:  v1.MediaTypeImageIndex,
			ContentTypeHeader: manifestlist.MediaTypeManifestList,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:              "unknown in payload and schema 2 in content type",
			PayloadMediaType:  unknownMediaType,
			ContentTypeHeader: manifestlist.MediaTypeManifestList,
			ExpectedStatus:    http.StatusCreated,
		},
		{
			Name:              "unknown in payload and oci in content type",
			PayloadMediaType:  unknownMediaType,
			ContentTypeHeader: v1.MediaTypeImageIndex,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("if present, mediaType in image index should be '%s' not '%s'", v1.MediaTypeImageIndex, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "unknown in payload and content type",
			PayloadMediaType:  unknownMediaType,
			ContentTypeHeader: unknownMediaType,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, manifestlist.MediaTypeManifestList),
		},
		{
			Name:              "unknown in payload and no content type",
			PayloadMediaType:  unknownMediaType,
			ExpectedStatus:    http.StatusBadRequest,
			ExpectedErrCode:   &v2.ErrorCodeManifestInvalid,
			ExpectedErrDetail: fmt.Sprintf("mediaType in manifest should be '%s' not '%s'", schema2.MediaTypeManifest, manifestlist.MediaTypeManifestList),
		},
	}

	repoRef, err := reference.WithName("foo")
	require.NoError(t, err)

	// push random manifest
	dm := seedRandomSchema2Manifest(t, env, repoRef.Name(), putByDigest)

	_, payload, err := dm.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	for _, test := range tt {
		t.Run(test.Name, func(t *testing.T) {
			// build and push manifest list
			ml := &manifestlist.ManifestList{
				Versioned: manifest.Versioned{
					SchemaVersion: 2,
					MediaType:     test.PayloadMediaType,
				},
				Manifests: []manifestlist.ManifestDescriptor{
					{
						Descriptor: distribution.Descriptor{
							Digest:    dgst,
							MediaType: dm.MediaType,
						},
						Platform: manifestlist.PlatformSpec{
							Architecture: "amd64",
							OS:           "linux",
						},
					},
				},
			}

			dml, err := manifestlist.FromDescriptors(ml.Manifests)
			require.NoError(t, err)

			manifestDigestURL := buildManifestDigestURL(t, env, repoRef.Name(), dml)
			resp := putManifest(t, "", manifestDigestURL, test.ContentTypeHeader, dml)
			defer resp.Body.Close()

			require.Equal(t, test.ExpectedStatus, resp.StatusCode)

			if test.ExpectedErrCode != nil {
				errs, _, _ := checkBodyHasErrorCodes(t, "", resp, v2.ErrorCodeManifestInvalid)
				require.Len(t, errs, 1)
				errc, ok := errs[0].(errcode.Error)
				require.True(t, ok)
				require.Equal(t, test.ExpectedErrDetail, errc.Detail)
			}
		})
	}
}

// TODO: Misc testing that's not currently covered by TestManifestAPI
// https://gitlab.com/gitlab-org/container-registry/-/issues/143
func TestManifestAPI_Get_UnknownSchema(t *testing.T) {}
func TestManifestAPI_Put_UnknownSchema(t *testing.T) {}

func TestManifestAPI_Get_UnknownMediaType(t *testing.T) {}
func TestManifestAPI_Put_UnknownMediaType(t *testing.T) {}

func TestManifestAPI_Put_ReuseTagManifestToManifestList(t *testing.T)     {}
func TestManifestAPI_Put_ReuseTagManifestListToManifest(t *testing.T)     {}
func TestManifestAPI_Put_ReuseTagManifestListToManifestList(t *testing.T) {}

func TestManifestAPI_Put_DigestReadOnly(t *testing.T) {}
func TestManifestAPI_Put_TagReadOnly(t *testing.T)    {}

func testManifestAPIManifestList(t *testing.T, env *testEnv, args manifestArgs) {
	imageName := args.imageName
	tag := "manifestlisttag"

	tagRef, _ := reference.WithTag(imageName, tag)
	manifestURL, err := env.builder.BuildManifestURL(tagRef)
	if err != nil {
		t.Fatalf("unexpected error getting manifest url: %v", err)
	}

	// --------------------------------
	// Attempt to push manifest list that refers to an unknown manifest
	manifestList := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     manifestlist.MediaTypeManifestList,
		},
		Manifests: []manifestlist.ManifestDescriptor{
			{
				Descriptor: distribution.Descriptor{
					Digest:    "sha256:1a9ec845ee94c202b2d5da74a24f0ed2058318bfa9879fa541efaecba272e86b",
					Size:      3253,
					MediaType: schema2.MediaTypeManifest,
				},
				Platform: manifestlist.PlatformSpec{
					Architecture: "amd64",
					OS:           "linux",
				},
			},
		},
	}

	resp := putManifest(t, "putting missing manifest manifestlist", manifestURL, manifestlist.MediaTypeManifestList, manifestList)
	defer resp.Body.Close()
	checkResponse(t, "putting missing manifest manifestlist", resp, http.StatusBadRequest)
	_, p, counts := checkBodyHasErrorCodes(t, "putting missing manifest manifestlist", resp, v2.ErrorCodeManifestBlobUnknown)

	expectedCounts := map[errcode.ErrorCode]int{
		v2.ErrorCodeManifestBlobUnknown: 1,
	}

	if !reflect.DeepEqual(counts, expectedCounts) {
		t.Fatalf("unexpected number of error codes encountered: %v\n!=\n%v\n---\n%s", counts, expectedCounts, string(p))
	}

	// -------------------
	// Push a manifest list that references an actual manifest
	manifestList.Manifests[0].Digest = args.dgst
	deserializedManifestList, err := manifestlist.FromDescriptors(manifestList.Manifests)
	if err != nil {
		t.Fatalf("could not create DeserializedManifestList: %v", err)
	}
	_, canonical, err := deserializedManifestList.Payload()
	if err != nil {
		t.Fatalf("could not get manifest list payload: %v", err)
	}
	dgst := digest.FromBytes(canonical)

	digestRef, _ := reference.WithDigest(imageName, dgst)
	manifestDigestURL, err := env.builder.BuildManifestURL(digestRef)
	checkErr(t, err, "building manifest url")

	resp = putManifest(t, "putting manifest list no error", manifestURL, manifestlist.MediaTypeManifestList, deserializedManifestList)
	defer resp.Body.Close()
	checkResponse(t, "putting manifest list no error", resp, http.StatusCreated)
	checkHeaders(t, resp, http.Header{
		"Location":              []string{manifestDigestURL},
		"Docker-Content-Digest": []string{dgst.String()},
	})

	// --------------------
	// Push by digest -- should get same result
	resp = putManifest(t, "putting manifest list by digest", manifestDigestURL, manifestlist.MediaTypeManifestList, deserializedManifestList)
	defer resp.Body.Close()
	checkResponse(t, "putting manifest list by digest", resp, http.StatusCreated)
	checkHeaders(t, resp, http.Header{
		"Location":              []string{manifestDigestURL},
		"Docker-Content-Digest": []string{dgst.String()},
	})

	// ------------------
	// Fetch by tag name
	req, err := http.NewRequest("GET", manifestURL, nil)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	// multiple headers in mixed list format to ensure we parse correctly server-side
	req.Header.Set("Accept", fmt.Sprintf(` %s ; q=0.8 , %s ; q=0.5 `, manifestlist.MediaTypeManifestList, v1.MediaTypeImageManifest))
	req.Header.Add("Accept", schema2.MediaTypeManifest)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error fetching manifest list: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching uploaded manifest list", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Docker-Content-Digest": []string{dgst.String()},
		"ETag":                  []string{fmt.Sprintf(`"%s"`, dgst)},
	})

	var fetchedManifestList manifestlist.DeserializedManifestList
	dec := json.NewDecoder(resp.Body)

	if err := dec.Decode(&fetchedManifestList); err != nil {
		t.Fatalf("error decoding fetched manifest list: %v", err)
	}

	_, fetchedCanonical, err := fetchedManifestList.Payload()
	if err != nil {
		t.Fatalf("error getting manifest list payload: %v", err)
	}

	if !bytes.Equal(fetchedCanonical, canonical) {
		t.Fatalf("manifest lists do not match")
	}

	// ---------------
	// Fetch by digest
	req, err = http.NewRequest("GET", manifestDigestURL, nil)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	req.Header.Set("Accept", manifestlist.MediaTypeManifestList)
	resp, err = http.DefaultClient.Do(req)
	checkErr(t, err, "fetching manifest list by digest")
	defer resp.Body.Close()

	checkResponse(t, "fetching uploaded manifest list", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Docker-Content-Digest": []string{dgst.String()},
		"ETag":                  []string{fmt.Sprintf(`"%s"`, dgst)},
	})

	var fetchedManifestListByDigest manifestlist.DeserializedManifestList
	dec = json.NewDecoder(resp.Body)
	if err := dec.Decode(&fetchedManifestListByDigest); err != nil {
		t.Fatalf("error decoding fetched manifest: %v", err)
	}

	_, fetchedCanonical, err = fetchedManifestListByDigest.Payload()
	if err != nil {
		t.Fatalf("error getting manifest list payload: %v", err)
	}

	if !bytes.Equal(fetchedCanonical, canonical) {
		t.Fatalf("manifests do not match")
	}

	// Get by name with etag, gives 304
	etag := resp.Header.Get("Etag")
	req, err = http.NewRequest("GET", manifestURL, nil)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	req.Header.Set("If-None-Match", etag)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching manifest by name with etag", resp, http.StatusNotModified)

	// Get by digest with etag, gives 304
	req, err = http.NewRequest("GET", manifestDigestURL, nil)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	req.Header.Set("If-None-Match", etag)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Error constructing request: %s", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "fetching manifest by dgst with etag", resp, http.StatusNotModified)
}

func testManifestDelete(t *testing.T, env *testEnv, args manifestArgs) {
	imageName := args.imageName
	dgst := args.dgst
	manifest := args.manifest

	ref, _ := reference.WithDigest(imageName, dgst)
	manifestDigestURL, _ := env.builder.BuildManifestURL(ref)
	// ---------------
	// Delete by digest
	resp, err := httpDelete(manifestDigestURL)
	checkErr(t, err, "deleting manifest by digest")
	defer resp.Body.Close()

	checkResponse(t, "deleting manifest", resp, http.StatusAccepted)
	checkHeaders(t, resp, http.Header{
		"Content-Length": []string{"0"},
	})

	// ---------------
	// Attempt to fetch deleted manifest
	resp, err = http.Get(manifestDigestURL)
	checkErr(t, err, "fetching deleted manifest by digest")
	defer resp.Body.Close()

	checkResponse(t, "fetching deleted manifest", resp, http.StatusNotFound)

	// ---------------
	// Delete already deleted manifest by digest
	resp, err = httpDelete(manifestDigestURL)
	checkErr(t, err, "re-deleting manifest by digest")
	defer resp.Body.Close()

	checkResponse(t, "re-deleting manifest", resp, http.StatusNotFound)

	// --------------------
	// Re-upload manifest by digest
	resp = putManifest(t, "putting manifest", manifestDigestURL, args.mediaType, manifest)
	defer resp.Body.Close()
	checkResponse(t, "putting manifest", resp, http.StatusCreated)
	checkHeaders(t, resp, http.Header{
		"Location":              []string{manifestDigestURL},
		"Docker-Content-Digest": []string{dgst.String()},
	})

	// ---------------
	// Attempt to fetch re-uploaded deleted digest
	resp, err = http.Get(manifestDigestURL)
	checkErr(t, err, "fetching re-uploaded manifest by digest")
	defer resp.Body.Close()

	checkResponse(t, "fetching re-uploaded manifest", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Docker-Content-Digest": []string{dgst.String()},
	})

	// ---------------
	// Attempt to delete an unknown manifest
	unknownDigest := digest.Digest("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	unknownRef, _ := reference.WithDigest(imageName, unknownDigest)
	unknownManifestDigestURL, err := env.builder.BuildManifestURL(unknownRef)
	checkErr(t, err, "building unknown manifest url")

	resp, err = httpDelete(unknownManifestDigestURL)
	checkErr(t, err, "delting unknown manifest by digest")
	defer resp.Body.Close()
	checkResponse(t, "fetching deleted manifest", resp, http.StatusNotFound)

	// --------------------
	// Upload manifest by tag
	tag := "atag"
	tagRef, _ := reference.WithTag(imageName, tag)
	manifestTagURL, _ := env.builder.BuildManifestURL(tagRef)
	resp = putManifest(t, "putting manifest by tag", manifestTagURL, args.mediaType, manifest)
	defer resp.Body.Close()
	checkResponse(t, "putting manifest by tag", resp, http.StatusCreated)
	checkHeaders(t, resp, http.Header{
		"Location":              []string{manifestDigestURL},
		"Docker-Content-Digest": []string{dgst.String()},
	})

	tagsURL, err := env.builder.BuildTagsURL(imageName)
	if err != nil {
		t.Fatalf("unexpected error building tags url: %v", err)
	}

	// Ensure that the tag is listed.
	resp, err = http.Get(tagsURL)
	if err != nil {
		t.Fatalf("unexpected error getting unknown tags: %v", err)
	}
	defer resp.Body.Close()

	dec := json.NewDecoder(resp.Body)
	var tagsResponse tagsAPIResponse
	if err := dec.Decode(&tagsResponse); err != nil {
		t.Fatalf("unexpected error decoding error response: %v", err)
	}

	if tagsResponse.Name != imageName.Name() {
		t.Fatalf("tags name should match image name: %v != %v", tagsResponse.Name, imageName)
	}

	if len(tagsResponse.Tags) != 1 {
		t.Fatalf("expected some tags in response: %v", tagsResponse.Tags)
	}

	if tagsResponse.Tags[0] != tag {
		t.Fatalf("tag not as expected: %q != %q", tagsResponse.Tags[0], tag)
	}

	// ---------------
	// Delete by digest
	resp, err = httpDelete(manifestDigestURL)
	checkErr(t, err, "deleting manifest by digest")
	defer resp.Body.Close()

	checkResponse(t, "deleting manifest with tag", resp, http.StatusAccepted)
	checkHeaders(t, resp, http.Header{
		"Content-Length": []string{"0"},
	})

	// Ensure that the tag is not listed.
	resp, err = http.Get(tagsURL)
	if err != nil {
		t.Fatalf("unexpected error getting unknown tags: %v", err)
	}
	defer resp.Body.Close()

	dec = json.NewDecoder(resp.Body)
	if err := dec.Decode(&tagsResponse); err != nil {
		t.Fatalf("unexpected error decoding error response: %v", err)
	}

	if tagsResponse.Name != imageName.Name() {
		t.Fatalf("tags name should match image name: %v != %v", tagsResponse.Name, imageName)
	}

	if len(tagsResponse.Tags) != 0 {
		t.Fatalf("expected 0 tags in response: %v", tagsResponse.Tags)
	}
}

// Test mutation operations on a registry configured as a cache.  Ensure that they return
// appropriate errors.
func TestRegistryAsCacheMutationAPIs(t *testing.T) {
	env := newTestEnvMirror(t, withDelete)
	defer env.Shutdown()

	imageName, _ := reference.WithName("foo/bar")
	tag := "latest"
	tagRef, _ := reference.WithTag(imageName, tag)
	manifestURL, err := env.builder.BuildManifestURL(tagRef)
	if err != nil {
		t.Fatalf("unexpected error building base url: %v", err)
	}

	// Manifest upload
	m := &schema2.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     schema2.MediaTypeManifest,
		},
		Layers: []distribution.Descriptor{
			{
				Digest:    digest.FromString("fake-layer"),
				MediaType: schema2.MediaTypeLayer,
			},
		},
	}

	deserializedManifest, err := schema2.FromStruct(*m)
	require.NoError(t, err)

	resp := putManifest(t, "putting manifest", manifestURL, schema2.MediaTypeManifest, deserializedManifest)
	defer resp.Body.Close()
	checkResponse(t, "putting signed manifest to cache", resp, errcode.ErrorCodeUnsupported.Descriptor().HTTPStatusCode)

	// Manifest Delete
	resp, err = httpDelete(manifestURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	checkResponse(t, "deleting signed manifest from cache", resp, errcode.ErrorCodeUnsupported.Descriptor().HTTPStatusCode)

	// Blob upload initialization
	layerUploadURL, err := env.builder.BuildBlobUploadURL(imageName)
	if err != nil {
		t.Fatalf("unexpected error building layer upload url: %v", err)
	}

	resp, err = http.Post(layerUploadURL, "", nil)
	if err != nil {
		t.Fatalf("unexpected error starting layer push: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, fmt.Sprintf("starting layer push to cache %v", imageName), resp, errcode.ErrorCodeUnsupported.Descriptor().HTTPStatusCode)

	// Blob Delete
	ref, _ := reference.WithDigest(imageName, digestSha256EmptyTar)
	blobURL, _ := env.builder.BuildBlobURL(ref)
	resp, err = httpDelete(blobURL)
	require.NoError(t, err)
	defer resp.Body.Close()
	checkResponse(t, "deleting blob from cache", resp, errcode.ErrorCodeUnsupported.Descriptor().HTTPStatusCode)
}

func TestProxyManifestGetByTag(t *testing.T) {
	truthConfig := newConfig()

	imageName, _ := reference.WithName("foo/bar")
	tag := "latest"

	truthEnv := newTestEnvWithConfig(t, &truthConfig)
	defer truthEnv.Shutdown()
	// create a repository in the truth registry
	dgst := createRepository(t, truthEnv, imageName.Name(), tag)

	proxyConfig := newConfig()
	proxyConfig.Proxy.RemoteURL = truthEnv.server.URL

	proxyEnv := newTestEnvWithConfig(t, &proxyConfig)
	defer proxyEnv.Shutdown()

	digestRef, _ := reference.WithDigest(imageName, dgst)
	manifestDigestURL, err := proxyEnv.builder.BuildManifestURL(digestRef)
	checkErr(t, err, "building manifest url")

	resp, err := http.Get(manifestDigestURL)
	checkErr(t, err, "fetching manifest from proxy by digest")
	defer resp.Body.Close()

	tagRef, _ := reference.WithTag(imageName, tag)
	manifestTagURL, err := proxyEnv.builder.BuildManifestURL(tagRef)
	checkErr(t, err, "building manifest url")

	resp, err = http.Get(manifestTagURL)
	checkErr(t, err, "fetching manifest from proxy by tag (error check 1)")
	defer resp.Body.Close()
	checkResponse(t, "fetching manifest from proxy by tag (response check 1)", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Docker-Content-Digest": []string{dgst.String()},
	})

	// Create another manifest in the remote with the same image/tag pair
	newDigest := createRepository(t, truthEnv, imageName.Name(), tag)
	if dgst == newDigest {
		t.Fatalf("non-random test data")
	}

	// fetch it with the same proxy URL as before.  Ensure the updated content is at the same tag
	resp, err = http.Get(manifestTagURL)
	checkErr(t, err, "fetching manifest from proxy by tag (error check 2)")
	defer resp.Body.Close()
	checkResponse(t, "fetching manifest from proxy by tag (response check 2)", resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Docker-Content-Digest": []string{newDigest.String()},
	})
}

// In https://gitlab.com/gitlab-org/container-registry/-/issues/409 we have identified that currently it's possible to
// upload lists/indexes with invalid references (to layers/configs). Attempting to read these through the manifests API
// resulted in a 500 Internal Server Error. We have changed this in
// https://gitlab.com/gitlab-org/container-registry/-/issues/411 to return a 404 Not Found error instead while the root
// cause (allowing these invalid references to sneak in) is not addressed (#409).
func TestManifestAPI_Get_Config(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	// disable the database so writes only go to the filesystem
	env.config.Database.Enabled = false

	// create repository with a manifest
	repo, err := reference.WithName("foo/bar")
	require.NoError(t, err)
	deserializedManifest := seedRandomSchema2Manifest(t, env, repo.Name())

	// fetch config through manifest endpoint
	digestRef, err := reference.WithDigest(repo, deserializedManifest.Config().Digest)
	require.NoError(t, err)

	digestURL, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	res, err := http.Get(digestURL)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusNotFound, res.StatusCode)
}

func testPrometheusMetricsCollectionDoesNotPanic(t *testing.T, env *testEnv) {
	t.Helper()

	// we can test this with any HTTP request
	catalogURL, err := env.builder.BuildCatalogURL()
	require.NoError(t, err)

	resp, err := http.Get(catalogURL)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func Test_PrometheusMetricsCollectionDoesNotPanic(t *testing.T) {
	env := newTestEnv(t, withPrometheusMetrics())
	defer env.Shutdown()

	testPrometheusMetricsCollectionDoesNotPanic(t, env)
}

func Test_PrometheusMetricsCollectionDoesNotPanic_InMigrationMode(t *testing.T) {
	env := newTestEnv(t, withPrometheusMetrics(), withMigrationEnabled)
	defer env.Shutdown()

	testPrometheusMetricsCollectionDoesNotPanic(t, env)
}

func TestGitLabAPIBase_Get_404WhenDBIsNotEnabled(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()
	env.requireDB(t)

	baseURL, err := env.builder.BuildGitlabV1BaseURL()
	require.NoError(t, err)

	req, err := http.NewRequest("GET", baseURL, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Equal(t, "2", resp.Header.Get("Content-Length"))
	require.Equal(t, strings.TrimPrefix(version.Version, "v"), resp.Header.Get("Gitlab-Container-Registry-Version"))

	p, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, "{}", string(p))

	// Disable the database, the base URL should return 404.
	env.config.Database.Enabled = false

	req, err = http.NewRequest("GET", baseURL, nil)
	require.NoError(t, err)

	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, strings.TrimPrefix(version.Version, "v"), resp.Header.Get("Gitlab-Container-Registry-Version"))
}
