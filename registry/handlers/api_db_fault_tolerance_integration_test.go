//go:build integration && toxiproxy

package handlers_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/datastore/testutil"
	htestutil "github.com/docker/distribution/registry/internal/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file is intended to test the HTTP API fault tolerance under adverse network conditions related to the metadata
// database, using Shopify Toxiproxy as intermediary between the registry and its DB. Fine grain tests of the handlers
// internal behavior (e.g., Schema 1 support, content negotiation, etc.) are out of scope. Here we're mainly concerned
// with ensuring that all HTTP handlers and methods are handling failure scenarios properly.

var (
	toxiClient    *toxiproxy.Client
	toxiproxyHost string
)

func init() {
	toxiproxyHost = os.Getenv("TOXIPROXY_HOST")
	if toxiproxyHost == "" {
		panic("TOXIPROXY_HOST environment variable not set")
	}
	port := os.Getenv("TOXIPROXY_PORT")
	if port == "" {
		panic("TOXIPROXY_PORT environment variable not set")
	}
	toxiClient = toxiproxy.NewClient(net.JoinHostPort(toxiproxyHost, port))
	if err := toxiClient.ResetState(); err != nil {
		panic(fmt.Errorf("failed to reset toxiproxy: %w", err))
	}
}

type dbProxy struct {
	proxy *toxiproxy.Proxy
	t     *testing.T
}

func (p dbProxy) HostAndPort() (string, int) {
	p.t.Helper()

	_, portStr, err := net.SplitHostPort(p.proxy.Listen)
	require.NoError(p.t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(p.t, err)

	return toxiproxyHost, port
}

func (p dbProxy) Enable() {
	p.t.Helper()
	require.NoError(p.t, p.proxy.Enable())
}

func (p dbProxy) Disable() {
	p.t.Helper()
	require.NoError(p.t, p.proxy.Disable())
}

func (p dbProxy) Delete() {
	p.t.Helper()
	require.NoError(p.t, p.proxy.Delete())
}

func (p dbProxy) AddToxic(typeName string, attrs toxiproxy.Attributes) *toxiproxy.Toxic {
	p.t.Helper()
	toxic, err := p.proxy.AddToxic("", typeName, "", 1, attrs)
	require.NoError(p.t, err)
	return toxic
}

func (p dbProxy) RemoveToxic(toxic *toxiproxy.Toxic) {
	p.t.Helper()
	require.NoError(p.t, p.proxy.RemoveToxic(toxic.Name))
}

func newDBProxy(t *testing.T) *dbProxy {
	t.Helper()

	dsn, err := testutil.NewDSNFromEnv()
	require.NoError(t, err)
	p, err := toxiClient.CreateProxy("db", "", dsn.Address())
	require.NoError(t, err)

	return &dbProxy{p, t}
}

func TestDBFaultTolerance_ConnectionRefused_Catalog(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	u, err := env.builder.BuildCatalogURL()
	require.NoError(t, err)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertGetResponse(t, u, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertGetResponse(t, u, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionRefused_TagList(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "foo"
	name, err := reference.WithName(repoName)
	require.NoError(t, err)
	u, err := env.builder.BuildTagsURL(name)
	require.NoError(t, err)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertGetResponse(t, u, http.StatusServiceUnavailable)
	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertGetResponse(t, u, http.StatusNotFound)
}

func TestDBFaultTolerance_ConnectionRefused_TagDelete(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDelete)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"

	// query API with proxy disabled, should fail
	// create the repo, otherwise the request will halt on the filesystem search, which precedes the DB search
	createRepository(t, env, repoName, tagName)
	dbProxy.Disable()
	assertTagDeleteResponse(t, env, repoName, tagName, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	createRepository(t, env, repoName, tagName)
	assertTagDeleteResponse(t, env, repoName, tagName, http.StatusAccepted)
}

func TestDBFaultTolerance_ConnectionRefused_BlobGet(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	// we can use a non-existing repo and blob, as reads are executed against the DB first
	repoName := "foo"
	dgst := digest.FromString(repoName)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertBlobGetResponse(t, env, repoName, dgst, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertBlobGetResponse(t, env, repoName, dgst, http.StatusNotFound)
}

func TestDBFaultTolerance_ConnectionRefused_BlobHead(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	// we can use a non-existing repo and blob, as reads are executed against the DB first
	repoName := "foo"
	dgst := digest.FromString(repoName)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertBlobHeadResponse(t, env, repoName, dgst, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertBlobHeadResponse(t, env, repoName, dgst, http.StatusNotFound)
}

func TestDBFaultTolerance_ConnectionRefused_BlobDelete(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDelete, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	// query API with proxy disabled, should fail
	// create the repo and blob, otherwise the request will halt on the filesystem search, which precedes the DB search
	args := createRepoWithBlob(t, env)
	dbProxy.Disable()
	assertBlobDeleteResponse(t, env, args.imageName.String(), args.layerDigest, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	args = createRepoWithBlob(t, env)
	assertBlobDeleteResponse(t, env, args.imageName.String(), args.layerDigest, http.StatusAccepted)
}

func TestDBFaultTolerance_ConnectionRefused_BlobPut(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	args := makeBlobArgs(t)
	assertBlobPutResponse(t, env, args.imageName.String(), args.layerDigest, args.layerFile, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	args = makeBlobArgs(t)
	assertBlobPutResponse(t, env, args.imageName.String(), args.layerDigest, args.layerFile, http.StatusCreated)
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func TestDBFaultTolerance_ConnectionRefused_BlobPostMount(t *testing.T) {
	testDBFaultTolerance_ConnectionRefused_BlobPostMountImpl(t)
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func TestDBFaultTolerance_ConnectionRefused_BlobPostMount_WithCentralRepositoryCache(t *testing.T) {
	testDBFaultTolerance_ConnectionRefused_BlobPostMountImpl(t, withRedisCache(htestutil.RedisServer(t).Addr()))
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func testDBFaultTolerance_ConnectionRefused_BlobPostMountImpl(t *testing.T, opts ...configOpt) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	opts = append(opts, withDBHostAndPort(dbProxy.HostAndPort()))
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	args := createRepoWithBlob(t, env)
	destRepo := "foo"

	// query API with proxy disabled, should fall back to starting a regular
	// upload, rather than a blob mount.
	dbProxy.Disable()
	assertBlobPostMountResponse(t, env, args.imageName.String(), destRepo, args.layerDigest, http.StatusAccepted)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertBlobPostMountResponse(t, env, args.imageName.String(), destRepo, args.layerDigest, http.StatusCreated)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestGetByDigest(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestGetByDigestResponse(t, env, repoName, m, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertManifestGetByDigestResponse(t, env, repoName, m, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestGetByTag(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "test/repo"
	tagName := "latest"
	seedRandomSchema2Manifest(t, env, repoName, putByTag(tagName))

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestGetByTagResponse(t, env, repoName, tagName, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertManifestGetByTagResponse(t, env, repoName, tagName, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestHeadByDigest(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestHeadByDigestResponse(t, env, repoName, m, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertManifestHeadByDigestResponse(t, env, repoName, m, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestHeadByTag(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "test/repo"
	tagName := "latest"
	seedRandomSchema2Manifest(t, env, repoName, putByTag(tagName))

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestHeadByTagResponse(t, env, repoName, tagName, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertManifestHeadByTagResponse(t, env, repoName, tagName, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestPutByDigest(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestPutByDigestResponse(t, env, repoName, m, m.MediaType, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertManifestPutByDigestResponse(t, env, repoName, m, m.MediaType, http.StatusCreated)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestPutByTag(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	m := seedRandomSchema2Manifest(t, env, repoName, putByTag(tagName))

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestPutByTagResponse(t, env, repoName, m, m.MediaType, tagName, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	assertManifestPutByTagResponse(t, env, repoName, m, m.MediaType, tagName, http.StatusCreated)
}

func TestDBFaultTolerance_ConnectionRefused_ManifestDelete(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDelete, withDBHostAndPort(dbProxy.HostAndPort()))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with proxy disabled, should fail
	dbProxy.Disable()
	assertManifestDeleteResponse(t, env, repoName, m, http.StatusServiceUnavailable)

	// query API with proxy re-enabled, should succeed
	dbProxy.Enable()
	m = seedRandomSchema2Manifest(t, env, repoName, putByDigest)
	assertManifestDeleteResponse(t, env, repoName, m, http.StatusAccepted)
}

func TestDBFaultTolerance_ConnectionTimeout_Catalog(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	u, err := env.builder.BuildCatalogURL()
	require.NoError(t, err)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertGetResponse(t, u, http.StatusServiceUnavailable)
	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertGetResponse(t, u, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionTimeout_TagList(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "foo"
	name, err := reference.WithName(repoName)
	require.NoError(t, err)
	u, err := env.builder.BuildTagsURL(name)
	require.NoError(t, err)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertGetResponse(t, u, http.StatusServiceUnavailable)
	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertGetResponse(t, u, http.StatusNotFound)
}

func TestDBFaultTolerance_ConnectionTimeout_TagDelete(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second), withDelete)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"

	// query API with timeout, should fail
	// create the repo, otherwise the request will halt on the filesystem search, which precedes the DB search
	createRepository(t, env, repoName, tagName)
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertTagDeleteResponse(t, env, repoName, tagName, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	createRepository(t, env, repoName, tagName)
	assertTagDeleteResponse(t, env, repoName, tagName, http.StatusAccepted)
}

func TestDBFaultTolerance_ConnectionTimeout_BlobGet(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	// we can use a non-existing repo and blob, as reads are executed against the DB first
	repoName := "foo"
	dgst := digest.FromString(repoName)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertBlobGetResponse(t, env, repoName, dgst, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertBlobGetResponse(t, env, repoName, dgst, http.StatusNotFound)
}

func TestDBFaultTolerance_ConnectionTimeout_BlobHead(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	// we can use a non-existing repo and blob, as reads are executed against the DB first
	repoName := "foo"
	dgst := digest.FromString(repoName)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertBlobHeadResponse(t, env, repoName, dgst, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertBlobHeadResponse(t, env, repoName, dgst, http.StatusNotFound)
}

func TestDBFaultTolerance_ConnectionTimeout_BlobDelete(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDelete, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	// query API with timeout, should fail
	// create the repo and blob, otherwise the request will halt on the filesystem search, which precedes the DB search
	args := createRepoWithBlob(t, env)
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertBlobDeleteResponse(t, env, args.imageName.String(), args.layerDigest, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	args = createRepoWithBlob(t, env)
	assertBlobDeleteResponse(t, env, args.imageName.String(), args.layerDigest, http.StatusAccepted)
}

func TestDBFaultTolerance_ConnectionTimeout_BlobPut(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	args := makeBlobArgs(t)
	assertBlobPutResponse(t, env, args.imageName.String(), args.layerDigest, args.layerFile, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	args = makeBlobArgs(t)
	assertBlobPutResponse(t, env, args.imageName.String(), args.layerDigest, args.layerFile, http.StatusCreated)
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func TestDBFaultTolerance_ConnectionTimeout_BlobPostMount(t *testing.T) {
	testDBFaultTolerance_ConnectionTimeout_BlobPostMount(t)
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func TestDBFaultTolerance_ConnectionTimeout_BlobPostMount_WithCentralRepositoryCache(t *testing.T) {
	testDBFaultTolerance_ConnectionTimeout_BlobPostMount(t, withRedisCache(htestutil.RedisServer(t).Addr()))
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func testDBFaultTolerance_ConnectionTimeout_BlobPostMount(t *testing.T, opts ...configOpt) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	opts = append(opts, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	args := createRepoWithBlob(t, env)
	destRepo := "foo"

	// query API with timeout, should fall back to starting a regular
	// upload, rather than a blob mount.
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertBlobPostMountResponse(t, env, args.imageName.String(), destRepo, args.layerDigest, http.StatusAccepted)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertBlobPostMountResponse(t, env, args.imageName.String(), destRepo, args.layerDigest, http.StatusCreated)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestGetByDigest(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestGetByDigestResponse(t, env, repoName, m, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertManifestGetByDigestResponse(t, env, repoName, m, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestGetByTag(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "test/repo"
	tagName := "latest"
	seedRandomSchema2Manifest(t, env, repoName, putByTag(tagName))

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestGetByTagResponse(t, env, repoName, tagName, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertManifestGetByTagResponse(t, env, repoName, tagName, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestHeadByDigest(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestHeadByDigestResponse(t, env, repoName, m, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertManifestHeadByDigestResponse(t, env, repoName, m, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestHeadByTag(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "test/repo"
	tagName := "latest"
	seedRandomSchema2Manifest(t, env, repoName, putByTag(tagName))

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestHeadByTagResponse(t, env, repoName, tagName, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertManifestHeadByTagResponse(t, env, repoName, tagName, http.StatusOK)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestPutByDigest(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestPutByDigestResponse(t, env, repoName, m, m.MediaType, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertManifestPutByDigestResponse(t, env, repoName, m, m.MediaType, http.StatusCreated)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestPutByTag(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	m := seedRandomSchema2Manifest(t, env, repoName, putByTag(tagName))

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestPutByTagResponse(t, env, repoName, m, m.MediaType, tagName, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	assertManifestPutByTagResponse(t, env, repoName, m, m.MediaType, tagName, http.StatusCreated)
}

func TestDBFaultTolerance_ConnectionTimeout_ManifestDelete(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	env := newTestEnv(t, withDelete, withDBHostAndPort(dbProxy.HostAndPort()), withDBConnectTimeout(1*time.Second))
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	// query API with timeout, should fail
	toxic := dbProxy.AddToxic("timeout", toxiproxy.Attributes{"timeout": 2000})
	assertManifestDeleteResponse(t, env, repoName, m, http.StatusServiceUnavailable)

	// query API with no timeout, should succeed
	dbProxy.RemoveToxic(toxic)
	m = seedRandomSchema2Manifest(t, env, repoName, putByDigest)
	assertManifestDeleteResponse(t, env, repoName, m, http.StatusAccepted)
}

func TestDBFaultTolerance_ConnectionPoolSaturation(t *testing.T) {
	dbProxy := newDBProxy(t)
	defer dbProxy.Delete()

	// simulate connection pool with up to 10 open connections
	poolMaxSize := 10
	env := newTestEnv(t, withDBHostAndPort(dbProxy.HostAndPort()), withDBPoolMaxOpen(poolMaxSize))
	defer env.Shutdown()
	require.Equal(t, poolMaxSize, env.app.DBStats().MaxOpenConnections)

	// simulate latency of 500ms+0..100ms for every connection
	toxic := dbProxy.AddToxic("latency", toxiproxy.Attributes{"latency": 500, "jitter": 100})
	defer dbProxy.RemoveToxic(toxic)

	// Connection pooling is handled by database/sql behind the scenes, so there is no app specific logic (besides
	// configuring db.SetMaxOpenConns), therefore using the catalog endpoint (or any other) as example is enough to
	// assert the behavior.
	u, err := env.builder.BuildCatalogURL()
	require.NoError(t, err)

	var wg sync.WaitGroup
	// spawn 10 times more clients than max pool open connections
	for i := 0; i < 10*poolMaxSize; i++ {
		wg.Add(1)
		t.Run(fmt.Sprintf("client %d", i), func(t *testing.T) {
			go func() {
				// If there are no available connections, database/sql should queue connection requests until they
				// can be assigned, so all requests should succeed.
				assert.NoError(t, assertGetResponseErr(u, http.StatusOK))
				wg.Done()
			}()
		})
	}
	// the connection pool should be saturated by now
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, poolMaxSize, env.app.DBStats().OpenConnections)
	wg.Wait()
	// the connection pool should be free by now
	require.Zero(t, env.app.DBStats().OpenConnections)
}

func TestDBFaultTolerance_ConnectionLeak_Catalog(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	u, err := env.builder.BuildCatalogURL()
	require.NoError(t, err)

	// there should be no open/in use/idle connections at this point
	assertNoDBConnections(t, env)

	done := asyncDo(func() { assertGetResponse(t, u, http.StatusOK) })
	// eventually there should be one DB connection open and in use
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	// there should be no open/in use/idle connections at this point
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_TagList(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	createRepository(t, env, repoName, tagName)
	name, err := reference.WithName(repoName)
	require.NoError(t, err)
	u, err := env.builder.BuildTagsURL(name)
	require.NoError(t, err)

	assertNoDBConnections(t, env)

	done := asyncDo(func() { assertGetResponse(t, u, http.StatusOK) })
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_TagDelete(t *testing.T) {
	env := newTestEnv(t, withDelete)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	createRepository(t, env, repoName, tagName)

	assertNoDBConnections(t, env)

	done := asyncDo(func() { assertTagDeleteResponse(t, env, repoName, tagName, http.StatusAccepted) })
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_BlobGet(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	blobArgs := createRepoWithBlob(t, env)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertBlobGetResponse(t, env, blobArgs.imageName.String(), blobArgs.layerDigest, http.StatusOK)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_BlobHead(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	blobArgs := createRepoWithBlob(t, env)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertBlobHeadResponse(t, env, blobArgs.imageName.String(), blobArgs.layerDigest, http.StatusOK)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_BlobDelete(t *testing.T) {
	env := newTestEnv(t, withDelete)
	defer env.Shutdown()

	blobArgs := createRepoWithBlob(t, env)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertBlobDeleteResponse(t, env, blobArgs.imageName.String(), blobArgs.layerDigest, http.StatusAccepted)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_BlobPut(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		args := makeBlobArgs(t)
		assertBlobPutResponse(t, env, args.imageName.String(), args.layerDigest, args.layerFile, http.StatusCreated)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 10*time.Second)

	<-done
	assertNoDBConnections(t, env)
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func TestDBFaultTolerance_ConnectionLeak_BlobPostMount(t *testing.T) {
	testDBFaultTolerance_ConnectionLeak_BlobPostMountImpl(t)
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func TestDBFaultTolerance_ConnectionLeak_BlobPostMount_WithCentralRepositoryCache(t *testing.T) {
	testDBFaultTolerance_ConnectionLeak_BlobPostMountImpl(t, withRedisCache(htestutil.RedisServer(t).Addr()))
}

// nolint: revive // var-naming - I do not see any other way to make this name readable
func testDBFaultTolerance_ConnectionLeak_BlobPostMountImpl(t *testing.T, opts ...configOpt) {
	env := newTestEnv(t, opts...)
	defer env.Shutdown()

	blobArgs := createRepoWithBlob(t, env)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertBlobPostMountResponse(t, env, blobArgs.imageName.String(), "bar", blobArgs.layerDigest, http.StatusCreated)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestGetByDigest(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestGetByDigestResponse(t, env, repoName, m, http.StatusOK)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestGetByTag(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	createRepository(t, env, repoName, tagName)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestGetByTagResponse(t, env, repoName, tagName, http.StatusOK)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestHeadByDigest(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestHeadByDigestResponse(t, env, repoName, m, http.StatusOK)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestHeadByTag(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	createRepository(t, env, repoName, tagName)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestHeadByTagResponse(t, env, repoName, tagName, http.StatusOK)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestPutByDigest(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestPutByDigestResponse(t, env, repoName, m, m.MediaType, http.StatusCreated)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestPutByTag(t *testing.T) {
	env := newTestEnv(t)
	defer env.Shutdown()

	repoName := "foo"
	tagName := "latest"
	m := seedRandomSchema2Manifest(t, env, repoName)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestPutByTagResponse(t, env, repoName, m, m.MediaType, tagName, http.StatusCreated)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

func TestDBFaultTolerance_ConnectionLeak_ManifestDelete(t *testing.T) {
	env := newTestEnv(t, withDelete)
	defer env.Shutdown()

	repoName := "foo"
	m := seedRandomSchema2Manifest(t, env, repoName, putByDigest)

	assertNoDBConnections(t, env)

	done := asyncDo(func() {
		assertManifestDeleteResponse(t, env, repoName, m, http.StatusAccepted)
	})
	assertEventuallyOpenAndInUseDBConnections(t, env, 1, 1, 100*time.Millisecond)

	<-done
	assertNoDBConnections(t, env)
}

// nolint:unparam //(`open` always receives `1`)
func assertEventuallyOpenAndInUseDBConnections(t *testing.T, env *testEnv, open, inUse int, deadline time.Duration) {
	t.Helper()
	require.Eventually(t, func() bool {
		stats := env.app.DBStats()
		return stats.OpenConnections == open && stats.InUse == inUse
	}, deadline, 1*time.Millisecond)
}

func assertNoDBConnections(t *testing.T, env *testEnv) {
	t.Helper()
	stats := env.app.DBStats()
	require.Zero(t, stats.OpenConnections)
	require.Zero(t, stats.InUse)
	require.Zero(t, stats.Idle)
}
