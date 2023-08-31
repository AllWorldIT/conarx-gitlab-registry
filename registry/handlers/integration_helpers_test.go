//go:build integration

package handlers_test

import (
	"bytes"
	"context"
	"crypto"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/notifications"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/api/urls"
	"github.com/docker/distribution/registry/auth/token"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/migrations"
	datastoretestutil "github.com/docker/distribution/registry/datastore/testutil"
	registryhandlers "github.com/docker/distribution/registry/handlers"
	rtestutil "github.com/docker/distribution/registry/internal/testutil"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	_ "github.com/docker/distribution/registry/storage/driver/filesystem"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	_ "github.com/docker/distribution/registry/storage/driver/testdriver"
	"github.com/docker/distribution/testutil"

	"github.com/docker/libtrust"
	gorillahandlers "github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/labkit/correlation"
	"gitlab.com/gitlab-org/labkit/metrics/sqlmetrics"
)

func init() {
	factory.Register("schema1Preseededinmemorydriver", &schema1PreseededInMemoryDriverFactory{})

	// http.DefaultClient does not have a timeout, so we need to configure it here
	http.DefaultClient.Timeout = time.Second * 10
}

type configOpt func(*configuration.Configuration)

type cacheClient interface {
	// FlushCache removes all cached data in the cache
	FlushCache() error
}

func withDelete(config *configuration.Configuration) {
	config.Storage["delete"] = configuration.Parameters{"enabled": true}
}

func withAccessLog(config *configuration.Configuration) {
	config.Log.AccessLog.Disabled = false
}

func withReadOnly(config *configuration.Configuration) {
	if _, ok := config.Storage["maintenance"]; !ok {
		config.Storage["maintenance"] = configuration.Parameters{}
	}

	config.Storage["maintenance"]["readonly"] = map[interface{}]interface{}{"enabled": true}
}

func withoutManifestURLValidation(config *configuration.Configuration) {
	config.Validation.Manifests.URLs.Allow = []string{".*"}
}

func withSillyAuth(config *configuration.Configuration) {
	if config.Auth == nil {
		config.Auth = make(map[string]configuration.Parameters)
	}

	config.Auth["silly"] = configuration.Parameters{"realm": "test-realm", "service": "test-service"}
}

func withFSDriver(path string) configOpt {
	return func(config *configuration.Configuration) {
		config.Storage["filesystem"] = configuration.Parameters{"rootdirectory": path}
	}
}

func withSchema1PreseededInMemoryDriver(config *configuration.Configuration) {
	config.Storage["schema1Preseededinmemorydriver"] = configuration.Parameters{}
}

func withDBDisabled(config *configuration.Configuration) {
	config.Database.Enabled = false
}

func withDBHostAndPort(host string, port int) configOpt {
	return func(config *configuration.Configuration) {
		config.Database.Host = host
		config.Database.Port = port
	}
}

func withDBConnectTimeout(d time.Duration) configOpt {
	return func(config *configuration.Configuration) {
		config.Database.ConnectTimeout = d
	}
}

func withDBPoolMaxOpen(n int) configOpt {
	return func(config *configuration.Configuration) {
		config.Database.Pool.MaxOpen = n
	}
}

func withPrometheusMetrics() configOpt {
	return func(config *configuration.Configuration) {
		config.HTTP.Debug.Addr = ":"
		config.HTTP.Debug.Prometheus.Enabled = true
	}
}

func withReferenceLimit(n int) configOpt {
	return func(config *configuration.Configuration) {
		config.Validation.Manifests.ReferenceLimit = n
	}
}

func withPayloadSizeLimit(n int) configOpt {
	return func(config *configuration.Configuration) {
		config.Validation.Manifests.PayloadSizeLimit = n
	}
}

func withRedisCache(srvAddr string) configOpt {
	return func(config *configuration.Configuration) {
		config.Redis.Cache.Enabled = true
		config.Redis.Cache.Addr = srvAddr
	}
}

type issuerProps struct {
	Realm      string
	Service    string
	Issuer     string
	ExpireFunc func() int64
}

func defaultIssuerProps() issuerProps {
	return issuerProps{
		Realm:   "test-realm",
		Service: "test-service",
		Issuer:  "test-issuer",
		// this issuer grants token that expires after 1 hour
		ExpireFunc: func() int64 { return time.Now().Add(time.Hour).Unix() },
	}
}
func withTokenAuth(rootCertPath string, issProps issuerProps) configOpt {
	return func(config *configuration.Configuration) {
		config.Auth = configuration.Auth{
			"token": {
				"realm":          issProps.Realm,
				"service":        issProps.Service,
				"issuer":         issProps.Issuer,
				"rootcertbundle": rootCertPath,
				"autoredirect":   false,
			},
		}
	}
}

func withHTTPPrefix(s string) configOpt {
	return func(config *configuration.Configuration) {
		config.HTTP.Prefix = s
	}
}

var headerConfig = http.Header{
	"X-Content-Type-Options": []string{"nosniff"},
}

type tagsAPIResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// digestSha256EmptyTar is the canonical sha256 digest of empty data
const digestSha256EmptyTar = "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func newConfig(opts ...configOpt) configuration.Configuration {
	config := &configuration.Configuration{
		Storage: configuration.Storage{
			"maintenance": configuration.Parameters{
				"uploadpurging": map[interface{}]interface{}{"enabled": false},
			},
		},
	}
	config.HTTP.Headers = headerConfig

	if os.Getenv("REGISTRY_DATABASE_ENABLED") == "true" {
		dsn, err := datastoretestutil.NewDSNFromEnv()
		if err != nil {
			panic(fmt.Sprintf("error creating dsn: %v", err))
		}

		config.Database = configuration.Database{
			Enabled:     true,
			Host:        dsn.Host,
			Port:        dsn.Port,
			User:        dsn.User,
			Password:    dsn.Password,
			DBName:      dsn.DBName,
			SSLMode:     dsn.SSLMode,
			SSLCert:     dsn.SSLCert,
			SSLKey:      dsn.SSLKey,
			SSLRootCert: dsn.SSLRootCert,
		}
	}

	for _, o := range opts {
		o(config)
	}

	// If no driver was configured, default to test driver, if multiple drivers
	// were configured, this will panic.
	if config.Storage.Type() == "" {
		config.Storage["testdriver"] = configuration.Parameters{}
	}

	return *config
}

func skipDatabaseNotEnabled(tb testing.TB) {
	tb.Helper()

	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		tb.Skip("skipping test because the metadata database is not enabled")
	}
}

var (
	preseededSchema1RepoPath = "schema1/preseeded"
	preseededSchema1TagName  = "schema1preseededtag"
	preseededSchema1Digest   digest.Digest
)

// schema1PreseededInMemoryDriverFactory implements the factory.StorageDriverFactory interface.
type schema1PreseededInMemoryDriverFactory struct{}

// Create returns a shared instance of the inmemory storage driver with a
// preseeded schema1 manifest. This allows us to test GETs against schema1
// manifests even though we are unable to PUT schema1 manifests via the API.
func (factory *schema1PreseededInMemoryDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	d := inmemory.New()

	unsignedManifest := &schema1.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 1,
		},
		Name:    preseededSchema1RepoPath,
		Tag:     preseededSchema1TagName,
		History: []schema1.History{},
	}

	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		return nil, err
	}

	sm, err := schema1.Sign(unsignedManifest, pk)
	if err != nil {
		return nil, err
	}

	dgst := digest.FromBytes(sm.Canonical)
	preseededSchema1Digest = dgst

	manifestTagCurrentPath := filepath.Clean(fmt.Sprintf("/docker/registry/v2/repositories/%s/_manifests/tags/%s/current/link", preseededSchema1RepoPath, preseededSchema1TagName))
	manifestRevisionLinkPath := filepath.Clean(fmt.Sprintf("/docker/registry/v2/repositories/%s/_manifests/revisions/sha256/%s/link", preseededSchema1RepoPath, dgst.Hex()))
	blobDataPath := filepath.Clean(fmt.Sprintf("/docker/registry/v2/blobs/sha256/%s/%s/data", dgst.Hex()[0:2], dgst.Hex()))

	ctx := context.Background()

	d.PutContent(ctx, manifestTagCurrentPath, []byte(dgst))
	d.PutContent(ctx, manifestRevisionLinkPath, []byte(dgst))
	d.PutContent(ctx, blobDataPath, sm.Canonical)

	return d, nil
}

type testEnv struct {
	pk          libtrust.PrivateKey
	ctx         context.Context
	config      *configuration.Configuration
	app         *registryhandlers.App
	server      *httptest.Server
	builder     *urls.Builder
	db          *datastore.DB
	ns          *rtestutil.NotificationServer
	cacheClient cacheClient
}

func (e *testEnv) requireDB(t *testing.T) {
	if !e.config.Database.Enabled {
		t.Skip("skipping test because the metadata database is not enabled")
	}
}

func newTestEnv(t *testing.T, opts ...configOpt) *testEnv {
	config := newConfig(opts...)

	return newTestEnvWithConfig(t, &config)
}

func newTestEnvWithConfig(t *testing.T, config *configuration.Configuration) *testEnv {
	ctx := context.Background()

	// The API test needs access to the database only to clean it up during
	// shutdown so that environments come up with a fresh copy of the database.
	var db *datastore.DB
	var err error
	if config.Database.Enabled {
		db, err = datastoretestutil.NewDBFromConfig(config)
		if err != nil {
			t.Fatal(err)
		}
		m := migrations.NewMigrator(db.DB)
		if _, err = m.Up(); err != nil {
			t.Fatal(err)
		}

		// online GC workers are noisy and not required for the API test, so we disable them globally here
		config.GC.Disabled = true

		if config.GC.ReviewAfter != 0 {
			d := config.GC.ReviewAfter
			// -1 means no review delay, so set it to 0 here
			if d == -1 {
				d = 0
			}
			s := datastore.NewGCSettingsStore(db)
			if _, err := s.UpdateAllReviewAfterDefaults(ctx, d); err != nil {
				t.Fatal(err)
			}
		}
	}

	// The API test needs access to the redis only to clean it up during
	// shutdown so that environments come up with a fresh cache.
	var redis cacheClient
	if config.Redis.Cache.Enabled {
		redis, err = datastoretestutil.NewRedisClientFromConfig(config)
		if err != nil {
			t.Fatal(err)
		}
	}

	var notifServer *rtestutil.NotificationServer
	if len(config.Notifications.Endpoints) == 1 {
		notifServer = rtestutil.NewNotificationServer(t, config.Database.Enabled)
		// ensure URL is set properly with mock server URL
		config.Notifications.Endpoints[0].URL = notifServer.URL
	}

	app, err := registryhandlers.NewApp(ctx, config)
	require.NoError(t, err)
	handler := correlation.InjectCorrelationID(app, correlation.WithPropagation())

	var out io.Writer
	if config.Log.AccessLog.Disabled {
		out = io.Discard
	} else {
		out = os.Stderr
	}
	server := httptest.NewServer(gorillahandlers.CombinedLoggingHandler(out, handler))
	builder, err := urls.NewBuilderFromString(server.URL+config.HTTP.Prefix, false)
	require.NoError(t, err)

	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		t.Fatalf("unexpected error generating private key: %v", err)
	}

	return &testEnv{
		pk:          pk,
		ctx:         ctx,
		config:      config,
		app:         app,
		server:      server,
		builder:     builder,
		db:          db,
		ns:          notifServer,
		cacheClient: redis,
	}
}

func (t *testEnv) Shutdown() {
	t.server.CloseClientConnections()
	t.server.Close()

	if t.config.Database.Enabled {
		if err := t.app.GracefulShutdown(t.ctx); err != nil {
			panic(err)
		}

		if err := datastoretestutil.TruncateAllTables(t.db); err != nil {
			panic(err)
		}

		if err := t.db.Close(); err != nil {
			panic(err)
		}

		// Needed for idempotency, so that shutdowns may be defer'd without worry.
		t.config.Database.Enabled = false
	}

	if t.config.Redis.Cache.Enabled {
		if err := t.cacheClient.FlushCache(); err != nil {
			panic(err)
		}

		// Needed for idempotency, so that shutdowns may be defer'd without worry.
		t.config.Redis.Cache.Enabled = false
	}

	// The Prometheus DBStatsCollector is registered within handlers.NewApp (it is the only place we can do so).
	// Therefore, if metrics are enabled, we must unregister this collector it when the env is shutdown. Otherwise,
	// prometheus.MustRegister will panic on a subsequent test with metrics enabled.
	if t.config.HTTP.Debug.Prometheus.Enabled {
		collector := sqlmetrics.NewDBStatsCollector(t.config.Database.DBName, t.db)
		prometheus.Unregister(collector)
	}
}

type manifestOpts struct {
	manifestURL        string
	putManifest        bool
	assertNotification bool
	withoutMediaType   bool
	// Non-optional values which be passed through by the testing func for ease of use.
	repoPath string
}

type manifestOptsFunc func(*testing.T, *testEnv, *manifestOpts)

func putByTag(tagName string) manifestOptsFunc {
	return func(t *testing.T, env *testEnv, opts *manifestOpts) {
		opts.manifestURL = buildManifestTagURL(t, env, opts.repoPath, tagName)
		opts.putManifest = true
	}
}

func putByDigest(t *testing.T, env *testEnv, opts *manifestOpts) {
	opts.putManifest = true
}

func withAssertNotification(t *testing.T, env *testEnv, opts *manifestOpts) {
	opts.assertNotification = true
}

func withoutMediaType(_ *testing.T, _ *testEnv, opts *manifestOpts) {
	opts.withoutMediaType = true
}

func schema2Config() ([]byte, distribution.Descriptor) {
	payload := []byte(`{
		"architecture": "amd64",
		"history": [
			{
				"created": "2015-10-31T22:22:54.690851953Z",
				"created_by": "/bin/sh -c #(nop) ADD file:a3bc1e842b69636f9df5256c49c5374fb4eef1e281fe3f282c65fb853ee171c5 in /"
			},
			{
				"created": "2015-10-31T22:22:55.613815829Z",
				"created_by": "/bin/sh -c #(nop) CMD [\"sh\"]"
			}
		],
		"rootfs": {
			"diff_ids": [
				"sha256:c6f988f4874bb0add23a778f753c65efe992244e148a1d2ec2a8b664fb66bbd1",
				"sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
			],
			"type": "layers"
		}
	}`)

	return payload, distribution.Descriptor{
		Size:      int64(len(payload)),
		MediaType: schema2.MediaTypeImageConfig,
		Digest:    digest.FromBytes(payload),
	}
}

// seedRandomSchema2Manifest generates a random schema2 manifest and puts its config and layers.
func seedRandomSchema2Manifest(t *testing.T, env *testEnv, repoPath string, opts ...manifestOptsFunc) *schema2.DeserializedManifest {
	t.Helper()

	if env.ns != nil {
		opts = append(opts, withAssertNotification)
	}

	config := &manifestOpts{
		repoPath: repoPath,
	}

	for _, o := range opts {
		o(t, env, config)
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

	// Create and push up 2 random layers.
	manifest.Layers = make([]distribution.Descriptor, 2)

	for i := range manifest.Layers {
		rs, dgst, size := createRandomSmallLayer()

		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

		manifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: schema2.MediaTypeLayer,
			Size:      size,
		}
	}

	deserializedManifest, err := schema2.FromStruct(*manifest)
	require.NoError(t, err)

	if config.putManifest {
		manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

		if config.manifestURL == "" {
			config.manifestURL = manifestDigestURL
		}

		resp := putManifest(t, "putting manifest no error", config.manifestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))

		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)
		dgst := digest.FromBytes(payload)
		require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

		if config.assertNotification {
			expectedEvent := buildEventManifestPush(schema2.MediaTypeManifest, config.repoPath, "", dgst, int64(len(payload)))
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}

	return deserializedManifest
}

func createRandomSmallLayer() (io.ReadSeeker, digest.Digest, int64) {
	size := rand.Int63n(20)
	b := make([]byte, size)
	rand.Read(b)

	dgst := digest.FromBytes(b)
	rs := bytes.NewReader(b)

	return rs, dgst, size
}

func ociConfig() ([]byte, distribution.Descriptor) {
	payload := []byte(`{
    "created": "2015-10-31T22:22:56.015925234Z",
    "author": "Alyssa P. Hacker <alyspdev@example.com>",
    "architecture": "amd64",
    "os": "linux",
    "config": {
        "User": "alice",
        "ExposedPorts": {
            "8080/tcp": {}
        },
        "Env": [
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "FOO=oci_is_a",
            "BAR=well_written_spec"
        ],
        "Entrypoint": [
            "/bin/my-app-binary"
        ],
        "Cmd": [
            "--foreground",
            "--config",
            "/etc/my-app.d/default.cfg"
        ],
        "Volumes": {
            "/var/job-result-data": {},
            "/var/log/my-app-logs": {}
        },
        "WorkingDir": "/home/alice",
        "Labels": {
            "com.example.project.git.url": "https://example.com/project.git",
            "com.example.project.git.commit": "45a939b2999782a3f005621a8d0f29aa387e1d6b"
        }
    },
    "rootfs": {
      "diff_ids": [
        "sha256:c6f988f4874bb0add23a778f753c65efe992244e148a1d2ec2a8b664fb66bbd1",
        "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
      ],
      "type": "layers"
    },
    "history": [
      {
        "created": "2015-10-31T22:22:54.690851953Z",
        "created_by": "/bin/sh -c #(nop) ADD file:a3bc1e842b69636f9df5256c49c5374fb4eef1e281fe3f282c65fb853ee171c5 in /"
      },
      {
        "created": "2015-10-31T22:22:55.613815829Z",
        "created_by": "/bin/sh -c #(nop) CMD [\"sh\"]",
        "empty_layer": true
      }
    ]
}`)

	return payload, distribution.Descriptor{
		Size:      int64(len(payload)),
		MediaType: v1.MediaTypeImageConfig,
		Digest:    digest.FromBytes(payload),
	}
}

// seedRandomOCIManifest generates a random oci manifest and puts its config and layers.
func seedRandomOCIManifest(t *testing.T, env *testEnv, repoPath string, opts ...manifestOptsFunc) *ocischema.DeserializedManifest {
	t.Helper()

	if env.ns != nil {
		opts = append(opts, withAssertNotification)
	}

	config := &manifestOpts{
		repoPath: repoPath,
	}

	for _, o := range opts {
		o(t, env, config)
	}

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	manifest := &ocischema.Manifest{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			MediaType:     v1.MediaTypeImageManifest,
		},
	}

	// Create a manifest config and push up its content.
	cfgPayload, cfgDesc := ociConfig()
	uploadURLBase, _ := startPushLayer(t, env, repoRef)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
	manifest.Config = cfgDesc

	// Create and push up 2 random layers.
	manifest.Layers = make([]distribution.Descriptor, 2)

	for i := range manifest.Layers {
		rs, dgst, size := createRandomSmallLayer()

		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs)

		manifest.Layers[i] = distribution.Descriptor{
			Digest:    dgst,
			MediaType: v1.MediaTypeImageLayer,
			Size:      size,
		}
	}

	deserializedManifest, err := ocischema.FromStruct(*manifest)
	require.NoError(t, err)

	if config.putManifest {
		manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

		if config.manifestURL == "" {
			config.manifestURL = manifestDigestURL
		}

		resp := putManifest(t, "putting manifest no error", config.manifestURL, v1.MediaTypeImageManifest, deserializedManifest)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))

		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)
		dgst := digest.FromBytes(payload)
		require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

		if config.assertNotification {
			expectedEvent := buildEventManifestPush(v1.MediaTypeImageManifest, config.repoPath, "", dgst, int64(len(payload)))
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}

	return deserializedManifest
}

// randomPlatformSpec generates a random platfromSpec. Arch and OS combinations
// may not strictly be valid for the Go runtime.
func randomPlatformSpec() manifestlist.PlatformSpec {
	rand.Seed(time.Now().Unix())

	architectures := []string{"amd64", "arm64", "ppc64le", "mips64", "386"}
	oses := []string{"aix", "darwin", "linux", "freebsd", "plan9"}

	return manifestlist.PlatformSpec{
		Architecture: architectures[rand.Intn(len(architectures))],
		OS:           oses[rand.Intn(len(oses))],
		// Optional values.
		OSVersion:  "",
		OSFeatures: nil,
		Variant:    "",
		Features:   nil,
	}
}

// seedRandomOCIImageIndex generates a random oci image index and puts its images.
func seedRandomOCIImageIndex(t *testing.T, env *testEnv, repoPath string, opts ...manifestOptsFunc) *manifestlist.DeserializedManifestList {
	t.Helper()

	if env.ns != nil {
		opts = append(opts, withAssertNotification)
	}

	config := &manifestOpts{
		repoPath: repoPath,
	}

	for _, o := range opts {
		o(t, env, config)
	}

	ociImageIndex := &manifestlist.ManifestList{
		Versioned: manifest.Versioned{
			SchemaVersion: 2,
			// MediaType field for OCI image indexes is reserved to maintain compatibility and can be blank:
			// https://github.com/opencontainers/image-spec/blob/master/image-index.md#image-index-property-descriptions
			MediaType: "",
		},
	}

	// Create and push up 2 random OCI images.
	ociImageIndex.Manifests = make([]manifestlist.ManifestDescriptor, 2)

	for i := range ociImageIndex.Manifests {
		deserializedManifest := seedRandomOCIManifest(t, env, repoPath, putByDigest)

		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)
		dgst := digest.FromBytes(payload)

		ociImageIndex.Manifests[i] = manifestlist.ManifestDescriptor{
			Descriptor: distribution.Descriptor{
				Digest:    dgst,
				MediaType: v1.MediaTypeImageManifest,
			},
			Platform: randomPlatformSpec(),
		}
	}

	mediaType := v1.MediaTypeImageIndex
	if config.withoutMediaType {
		mediaType = ""
	}
	deserializedManifest, err := manifestlist.FromDescriptorsWithMediaType(ociImageIndex.Manifests, mediaType)
	require.NoError(t, err)

	if config.putManifest {
		manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

		if config.manifestURL == "" {
			config.manifestURL = manifestDigestURL
		}

		resp := putManifest(t, "putting oci image index no error", config.manifestURL, v1.MediaTypeImageIndex, deserializedManifest)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))

		_, payload, err := deserializedManifest.Payload()
		require.NoError(t, err)
		dgst := digest.FromBytes(payload)
		require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))

		if config.assertNotification {
			expectedEvent := buildEventManifestPush(v1.MediaTypeImageIndex, config.repoPath, "", dgst, int64(len(payload)))
			env.ns.AssertEventNotification(t, expectedEvent)
		}
	}

	return deserializedManifest
}

func buildEventManifestPush(mediaType, repoPath, tagName string, dgst digest.Digest, size int64) notifications.Event {
	return notifications.Event{
		Action: "push",
		Target: notifications.Target{
			Descriptor: distribution.Descriptor{
				MediaType: mediaType,
				Digest:    dgst,
				Size:      size,
			},
			Repository: repoPath,
			Tag:        tagName,
		},
	}
}

func buildEventManifestPull(mediaType, repoPath string, dgst digest.Digest, size int64) notifications.Event {
	return notifications.Event{
		Action: "pull",
		Target: notifications.Target{
			Descriptor: distribution.Descriptor{
				MediaType: mediaType,
				Digest:    dgst,
				Size:      size,
			},
			Repository: repoPath,
		},
	}
}

func buildEventManifestDeleteByDigest(mediaType, repoPath string, dgst digest.Digest) notifications.Event {
	return buildEventManifestDelete(mediaType, repoPath, "", dgst)
}

func buildEventManifestDeleteByTag(mediaType, repoPath, tag string) notifications.Event {
	return buildEventManifestDelete(mediaType, repoPath, tag, "")
}

func buildEventManifestDelete(mediaType, repoPath, tagName string, dgst digest.Digest) notifications.Event {
	return notifications.Event{
		Action: "delete",
		Target: notifications.Target{
			Descriptor: distribution.Descriptor{
				MediaType: mediaType,
				Digest:    dgst,
			},
			Repository: repoPath,
			Tag:        tagName,
		},
	}
}

func buildManifestTagURL(t *testing.T, env *testEnv, repoPath, tagName string) string {
	t.Helper()

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	tagRef, err := reference.WithTag(repoRef, tagName)
	require.NoError(t, err)

	tagURL, err := env.builder.BuildManifestURL(tagRef)
	require.NoError(t, err)

	return tagURL
}

func buildManifestDigestURL(t *testing.T, env *testEnv, repoPath string, manifest distribution.Manifest) string {
	t.Helper()

	repoRef, err := reference.WithName(repoPath)
	require.NoError(t, err)

	_, payload, err := manifest.Payload()
	require.NoError(t, err)

	dgst := digest.FromBytes(payload)

	digestRef, err := reference.WithDigest(repoRef, dgst)
	require.NoError(t, err)

	digestURL, err := env.builder.BuildManifestURL(digestRef)
	require.NoError(t, err)

	return digestURL
}

func shuffledCopy(s []string) []string {
	shuffled := make([]string, len(s))
	copy(shuffled, s)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled
}

func putManifest(t *testing.T, msg, url, contentType string, v interface{}) *http.Response {
	var body []byte

	switch m := v.(type) {
	case *schema1.SignedManifest:
		_, pl, err := m.Payload()
		if err != nil {
			t.Fatalf("error getting payload: %v", err)
		}
		body = pl
	case *manifestlist.DeserializedManifestList:
		_, pl, err := m.Payload()
		if err != nil {
			t.Fatalf("error getting payload: %v", err)
		}
		body = pl
	default:
		var err error
		body, err = json.MarshalIndent(v, "", "   ")
		if err != nil {
			t.Fatalf("unexpected error marshaling %v: %v", v, err)
		}
	}

	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("error creating request for %s: %v", msg, err)
	}

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("error doing put request while %s: %v", msg, err)
	}

	return resp
}

func startPushLayer(t *testing.T, env *testEnv, name reference.Named) (location string, uuid string) {
	t.Helper()

	layerUploadURL, err := env.builder.BuildBlobUploadURL(name)
	if err != nil {
		t.Fatalf("unexpected error building layer upload url: %v", err)
	}

	u, err := url.Parse(layerUploadURL)
	if err != nil {
		t.Fatalf("error parsing layer upload URL: %v", err)
	}

	base, err := url.Parse(env.server.URL)
	if err != nil {
		t.Fatalf("error parsing server URL: %v", err)
	}

	layerUploadURL = base.ResolveReference(u).String()
	resp, err := http.Post(layerUploadURL, "", nil)
	if err != nil {
		t.Fatalf("unexpected error starting layer push: %v", err)
	}

	defer resp.Body.Close()

	checkResponse(t, fmt.Sprintf("pushing starting layer push %v", name.String()), resp, http.StatusAccepted)

	u, err = url.Parse(resp.Header.Get("Location"))
	if err != nil {
		t.Fatalf("error parsing location header: %v", err)
	}

	uuid = path.Base(u.Path)
	checkHeaders(t, resp, http.Header{
		"Location":           []string{"*"},
		"Content-Length":     []string{"0"},
		"Docker-Upload-UUID": []string{uuid},
	})

	return resp.Header.Get("Location"), uuid
}

// doPushLayer pushes the layer content returning the url on success returning
// the response. If you're only expecting a successful response, use pushLayer.
func doPushLayer(t *testing.T, ub *urls.Builder, name reference.Named, dgst digest.Digest, uploadURLBase string, body io.Reader) (*http.Response, error) {
	u, err := url.Parse(uploadURLBase)
	if err != nil {
		t.Fatalf("unexpected error parsing pushLayer url: %v", err)
	}

	u.RawQuery = url.Values{
		"_state": u.Query()["_state"],
		"digest": []string{dgst.String()},
	}.Encode()

	uploadURL := u.String()

	// Just do a monolithic upload
	req, err := http.NewRequest(http.MethodPut, uploadURL, body)
	if err != nil {
		t.Fatalf("unexpected error creating new request: %v", err)
	}

	return http.DefaultClient.Do(req)
}

// pushLayer pushes the layer content returning the url on success.
func pushLayer(t *testing.T, ub *urls.Builder, name reference.Named, dgst digest.Digest, uploadURLBase string, body io.Reader) string {
	digester := digest.Canonical.Digester()

	resp, err := doPushLayer(t, ub, name, dgst, uploadURLBase, io.TeeReader(body, digester.Hash()))
	if err != nil {
		t.Fatalf("unexpected error doing push layer request: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "putting monolithic chunk", resp, http.StatusCreated)

	if err != nil {
		t.Fatalf("error generating sha256 digest of body")
	}

	sha256Dgst := digester.Digest()

	ref, _ := reference.WithDigest(name, sha256Dgst)
	expectedLayerURL, err := ub.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf("error building expected layer url: %v", err)
	}

	checkHeaders(t, resp, http.Header{
		"Location":              []string{expectedLayerURL},
		"Content-Length":        []string{"0"},
		"Docker-Content-Digest": []string{sha256Dgst.String()},
	})

	return resp.Header.Get("Location")
}

func finishUpload(t *testing.T, ub *urls.Builder, name reference.Named, uploadURLBase string, dgst digest.Digest) string {
	resp, err := doPushLayer(t, ub, name, dgst, uploadURLBase, nil)
	if err != nil {
		t.Fatalf("unexpected error doing push layer request: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "putting monolithic chunk", resp, http.StatusCreated)

	ref, _ := reference.WithDigest(name, dgst)
	expectedLayerURL, err := ub.BuildBlobURL(ref)
	if err != nil {
		t.Fatalf("error building expected layer url: %v", err)
	}

	checkHeaders(t, resp, http.Header{
		"Location":              []string{expectedLayerURL},
		"Content-Length":        []string{"0"},
		"Docker-Content-Digest": []string{dgst.String()},
	})

	return resp.Header.Get("Location")
}

func doPushChunk(t *testing.T, uploadURLBase string, body io.Reader) (*http.Response, digest.Digest, error) {
	u, err := url.Parse(uploadURLBase)
	if err != nil {
		t.Fatalf("unexpected error parsing pushLayer url: %v", err)
	}

	u.RawQuery = url.Values{
		"_state": u.Query()["_state"],
	}.Encode()

	uploadURL := u.String()

	digester := digest.Canonical.Digester()

	req, err := http.NewRequest(http.MethodPatch, uploadURL, io.TeeReader(body, digester.Hash()))
	if err != nil {
		t.Fatalf("unexpected error creating new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)

	return resp, digester.Digest(), err
}

func pushChunk(t *testing.T, ub *urls.Builder, name reference.Named, uploadURLBase string, body io.Reader, length int64) (string, digest.Digest) {
	resp, dgst, err := doPushChunk(t, uploadURLBase, body)
	if err != nil {
		t.Fatalf("unexpected error doing push layer request: %v", err)
	}
	defer resp.Body.Close()

	checkResponse(t, "putting chunk", resp, http.StatusAccepted)

	if err != nil {
		t.Fatalf("error generating sha256 digest of body")
	}

	checkHeaders(t, resp, http.Header{
		"Range":          []string{fmt.Sprintf("0-%d", length-1)},
		"Content-Length": []string{"0"},
	})

	return resp.Header.Get("Location"), dgst
}

func checkResponse(t *testing.T, msg string, resp *http.Response, expectedStatus int) {
	t.Helper()

	if resp.StatusCode != expectedStatus {
		t.Logf("unexpected status %s: %v != %v", msg, resp.StatusCode, expectedStatus)
		maybeDumpResponse(t, resp)

		t.FailNow()
	}

	// We expect the headers included in the configuration, unless the
	// status code is 405 (Method Not Allowed), which means the handler
	// doesn't even get called.
	if resp.StatusCode != 405 && !reflect.DeepEqual(resp.Header["X-Content-Type-Options"], []string{"nosniff"}) {
		t.Logf("missing or incorrect header X-Content-Type-Options %s", msg)
		maybeDumpResponse(t, resp)

		t.FailNow()
	}
}

// checkBodyHasErrorCodes ensures the body is an error body and has the
// expected error codes, returning the error structure, the json slice and a
// count of the errors by code.
func checkBodyHasErrorCodes(t *testing.T, msg string, resp *http.Response, errorCodes ...errcode.ErrorCode) (errcode.Errors, []byte, map[errcode.ErrorCode]int) {
	t.Helper()

	p, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var errs errcode.Errors
	err = json.Unmarshal(p, &errs)
	require.NoError(t, err)

	require.NotEmpty(t, errs, "expected errors in response")

	// TODO(stevvooe): Shoot. The error setup is not working out. The content-
	// type headers are being set after writing the status code.
	// if resp.Header.Get("Content-Type") != "application/json" {
	//	t.Fatalf("unexpected content type: %v != 'application/json'",
	//		resp.Header.Get("Content-Type"))
	// }

	expected := map[errcode.ErrorCode]struct{}{}
	counts := map[errcode.ErrorCode]int{}

	// Initialize map with zeros for expected
	for _, code := range errorCodes {
		expected[code] = struct{}{}
		counts[code] = 0
	}

	for _, e := range errs {
		err, ok := e.(errcode.ErrorCoder)
		require.Truef(t, ok, "not an ErrorCoder: %#v", e)

		_, ok = expected[err.ErrorCode()]
		require.Truef(t, ok, "unexpected error code %v encountered during %s: %s ", err.ErrorCode(), msg, p)

		counts[err.ErrorCode()]++
	}

	// Ensure that counts of expected errors were all non-zero
	for code := range expected {
		require.NotZerof(t, counts[code], "expected error code %v not encountered during %s: %s", code, msg, p)
	}

	return errs, p, counts
}

func maybeDumpResponse(t *testing.T, resp *http.Response) {
	t.Helper()

	if d, err := httputil.DumpResponse(resp, true); err != nil {
		t.Logf("error dumping response: %v", err)
	} else {
		t.Logf("response:\n%s", string(d))
	}
}

// matchHeaders checks that the response has at least the headers. If not, the
// test will fail. If a passed in header value is "*", any non-zero value will
// suffice as a match.
func checkHeaders(t *testing.T, resp *http.Response, headers http.Header) {
	for k, vs := range headers {
		if resp.Header.Get(k) == "" {
			t.Fatalf("response missing header %q", k)
		}

		for _, v := range vs {
			if v == "*" {
				// Just ensure there is some value.
				if len(resp.Header[http.CanonicalHeaderKey(k)]) > 0 {
					continue
				}
			}

			for _, hv := range resp.Header[http.CanonicalHeaderKey(k)] {
				if hv != v {
					t.Fatalf("%+v %v header value not matched in response: %q != %q", resp.Header, k, hv, v)
				}
			}
		}
	}
}

func checkAllowedMethods(t *testing.T, url string, allowed []string) {
	resp, err := httpOptions(url)
	msg := "checking allowed methods"
	checkErr(t, err, msg)

	defer resp.Body.Close()

	checkResponse(t, msg, resp, http.StatusOK)
	checkHeaders(t, resp, http.Header{
		"Allow": allowed,
	})
}

func checkErr(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("unexpected error %s: %v", msg, err)
	}
}

func createRepository(t *testing.T, env *testEnv, repoPath string, tag string) digest.Digest {
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath, putByTag(tag))

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)

	return digest.FromBytes(payload)
}

func createRepositoryWithMultipleIdenticalTags(t *testing.T, env *testEnv, repoPath string, tags []string) (digest.Digest, digest.Digest, string, int64) {
	deserializedManifest := seedRandomSchema2Manifest(t, env, repoPath)

	_, payload, err := deserializedManifest.Payload()
	require.NoError(t, err)
	dgst := digest.FromBytes(payload)

	// upload a manifest per tag
	for _, tag := range tags {
		manifestTagURL := buildManifestTagURL(t, env, repoPath, tag)
		manifestDigestURL := buildManifestDigestURL(t, env, repoPath, deserializedManifest)

		resp := putManifest(t, "putting manifest no error", manifestTagURL, schema2.MediaTypeManifest, deserializedManifest.Manifest)
		defer resp.Body.Close()
		require.Equal(t, http.StatusCreated, resp.StatusCode)
		require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		require.Equal(t, manifestDigestURL, resp.Header.Get("Location"))
		require.Equal(t, dgst.String(), resp.Header.Get("Docker-Content-Digest"))
	}

	return dgst, deserializedManifest.Config().Digest, schema2.MediaTypeManifest, deserializedManifest.TotalSize()
}

func httpDelete(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	//	defer resp.Body.Close()
	return resp, err
}

func httpOptions(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodOptions, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, err
}

type blobArgs struct {
	imageName   reference.Named
	layerFile   io.ReadSeeker
	layerDigest digest.Digest
}

func makeBlobArgs(t *testing.T) blobArgs {
	layerFile, layerDigest, err := testutil.CreateRandomTarFile()
	require.NoError(t, err)

	args := blobArgs{
		layerFile:   layerFile,
		layerDigest: layerDigest,
	}
	args.imageName, err = reference.WithName("foo/bar")
	require.NoError(t, err)

	return args
}

func makeBlobArgsWithRepoName(t *testing.T, repoName string) blobArgs {
	layerFile, layerDigest, err := testutil.CreateRandomTarFile()
	require.NoError(t, err)

	args := blobArgs{
		layerFile:   layerFile,
		layerDigest: layerDigest,
	}
	args.imageName, err = reference.WithName(repoName)
	require.NoError(t, err)

	return args
}

func asyncDo(f func()) chan struct{} {
	done := make(chan struct{})
	go func() {
		f()
		close(done)
	}()
	return done
}

func createRepoWithBlob(t *testing.T, env *testEnv) (blobArgs, string) {
	t.Helper()

	args := makeBlobArgs(t)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	return args, blobURL
}

func createNamedRepoWithBlob(t *testing.T, env *testEnv, repoName string) (blobArgs, string) {
	t.Helper()

	args := makeBlobArgsWithRepoName(t, repoName)
	uploadURLBase, _ := startPushLayer(t, env, args.imageName)
	blobURL := pushLayer(t, env.builder, args.imageName, args.layerDigest, uploadURLBase, args.layerFile)

	return args, blobURL
}

func assertGetResponse(t *testing.T, url string, expectedStatus int) {
	t.Helper()

	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertHeadResponse(t *testing.T, url string, expectedStatus int) {
	t.Helper()

	resp, err := http.Head(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertPutResponse(t *testing.T, url string, body io.Reader, headers http.Header, expectedStatus int) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPut, url, body)
	require.NoError(t, err)
	for k, vv := range headers {
		req.Header.Set(k, strings.Join(vv, ","))
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertPostResponse(t *testing.T, url string, body io.Reader, headers http.Header, expectedStatus int) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, url, body)
	require.NoError(t, err)
	for k, vv := range headers {
		req.Header.Set(k, strings.Join(vv, ","))
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertDeleteResponse(t *testing.T, url string, expectedStatus int) {
	t.Helper()

	resp, err := httpDelete(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertTagDeleteResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	named, err := reference.WithTag(tmp, tagName)
	require.NoError(t, err)
	u, err := env.builder.BuildTagURL(named)
	require.NoError(t, err)

	assertDeleteResponse(t, u, expectedStatus)
}

func assertBlobGetResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertGetResponse(t, u, expectedStatus)
}

func assertBlobHeadResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertHeadResponse(t, u, expectedStatus)
}

func assertBlobDeleteResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertDeleteResponse(t, u, expectedStatus)
}

func assertBlobPutResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, body io.ReadSeeker, expectedStatus int) {
	t.Helper()

	name, err := reference.WithName(repoName)
	require.NoError(t, err)

	baseURL, _ := startPushLayer(t, env, name)
	u, err := url.Parse(baseURL)
	require.NoError(t, err)
	u.RawQuery = url.Values{
		"_state": u.Query()["_state"],
		"digest": []string{dgst.String()},
	}.Encode()

	assertPutResponse(t, u.String(), body, nil, expectedStatus)
}

func assertBlobPostMountResponse(t *testing.T, env *testEnv, srcRepoName, destRepoName string, dgst digest.Digest, expectedStatus int) {
	t.Helper()

	name, err := reference.WithName(destRepoName)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobUploadURL(name, url.Values{
		"mount": []string{dgst.String()},
		"from":  []string{srcRepoName},
	})
	require.NoError(t, err)

	assertPostResponse(t, u, nil, nil, expectedStatus)
}

func assertManifestGetByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertGetResponse(t, u, expectedStatus)
}

func assertManifestGetByTagResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	assertGetResponse(t, u, expectedStatus)
}

func assertManifestHeadByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertHeadResponse(t, u, expectedStatus)
}

func assertManifestHeadByTagResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	assertHeadResponse(t, u, expectedStatus)
}

func assertManifestPutByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, mediaType string, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	_, body, err := m.Payload()
	require.NoError(t, err)

	assertPutResponse(t, u, bytes.NewReader(body), http.Header{"Content-Type": []string{mediaType}}, expectedStatus)
}

func assertManifestPutByTagResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, mediaType, tagName string, expectedStatus int) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	_, body, err := m.Payload()
	require.NoError(t, err)

	assertPutResponse(t, u, bytes.NewReader(body), http.Header{"Content-Type": []string{mediaType}}, expectedStatus)
}

func assertManifestDeleteResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertDeleteResponse(t, u, expectedStatus)
}

func seedMultipleRepositoriesWithTaggedManifest(t *testing.T, env *testEnv, tagName string, repoPaths []string) {
	t.Helper()

	for _, path := range repoPaths {
		seedRandomSchema2Manifest(t, env, path, putByTag(tagName))
	}
}

func generateAuthToken(t *testing.T, user string, access []*token.ResourceActions, issuer issuerProps, signingKey libtrust.PrivateKey) string {
	t.Helper()

	var rawJWK json.RawMessage
	rawJWK, err := signingKey.PublicKey().MarshalJSON()
	require.NoError(t, err, "unable to marshal signing key to JSON")

	joseHeader := &token.Header{
		Type:       "JWT",
		SigningAlg: "ES256",
		RawJWK:     &rawJWK,
	}

	randomBytes := make([]byte, 15)
	_, err = rand.Read(randomBytes)
	require.NoError(t, err, "unable to read random bytes for jwt")

	claimSet := &token.ClaimSet{
		Issuer:     issuer.Issuer,
		Subject:    user,
		AuthType:   "gitlab_test",
		Audience:   issuer.Service,
		Expiration: issuer.ExpireFunc(),
		NotBefore:  time.Now().Unix(),
		IssuedAt:   time.Now().Unix(),
		JWTID:      base64.URLEncoding.EncodeToString(randomBytes),
		Access:     access,
	}

	var joseHeaderBytes, claimSetBytes []byte

	joseHeaderBytes, err = json.Marshal(joseHeader)
	require.NoError(t, err, "unable to marshal jose header")

	claimSetBytes, err = json.Marshal(claimSet)
	require.NoError(t, err, "unable to marshal claim set")

	encodedJoseHeader := joseBase64UrlEncode(joseHeaderBytes)
	encodedClaimSet := joseBase64UrlEncode(claimSetBytes)
	encodingToSign := fmt.Sprintf("%s.%s", encodedJoseHeader, encodedClaimSet)

	var signatureBytes []byte
	signatureBytes, _, err = signingKey.Sign(strings.NewReader(encodingToSign), crypto.SHA256)
	require.NoError(t, err, "unable to sign jwt payload")

	signature := joseBase64UrlEncode(signatureBytes)
	tokenString := fmt.Sprintf("%s.%s", encodingToSign, signature)

	return tokenString
}

// joseBase64UrlEncode encodes the given data using the standard base64 url
// encoding format but with all trailing '=' characters omitted in accordance
// with the jose specification.
// http://tools.ietf.org/html/draft-ietf-jose-json-web-signature-31#section-2
func joseBase64UrlEncode(b []byte) string {
	return strings.TrimRight(base64.URLEncoding.EncodeToString(b), "=")
}

// authTokenProvider manages the procurement of authorization tokens
// by holding the necessary private key value and public cert path needed to generate/validate a token.
type authTokenProvider struct {
	t          *testing.T
	certPath   string
	privateKey libtrust.PrivateKey
}

// NewAuthTokenProvider creates an authTokenProvider that manages the procurement of authorization tokens
// by holding the necessary private key value and cert path needed to generate/validate a token.
func NewAuthTokenProvider(t *testing.T) *authTokenProvider {
	t.Helper()

	path, privKey, err := rtestutil.WriteTempRootCerts()
	t.Cleanup(func() {
		err := os.Remove(path)
		require.NoError(t, err)
	})
	require.NoError(t, err)

	return &authTokenProvider{
		t:          t,
		certPath:   path,
		privateKey: privKey,
	}
}

// TokenWithActions generates a token for a specified set of actions
func (a *authTokenProvider) TokenWithActions(tra []*token.ResourceActions) string {
	return generateAuthToken(a.t, "test-user", tra, defaultIssuerProps(), a.privateKey)
}

// RequestWithAuthActions wraps a request with a bearer authorization header
// using a standard JWT generated from the provided resource actions
func (a *authTokenProvider) RequestWithAuthActions(r *http.Request, tra []*token.ResourceActions) *http.Request {
	clonedReq := r.Clone(r.Context())
	clonedReq.Header.Add("Authorization", fmt.Sprintf("Bearer %s", a.TokenWithActions(tra)))
	return clonedReq
}

// RequestWithAuthToken wraps a request with a bearer authorization header
// using a provided token string
func (a *authTokenProvider) RequestWithAuthToken(r *http.Request, token string) *http.Request {
	clonedReq := r.Clone(r.Context())
	clonedReq.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	return clonedReq
}

// CertPath returns the cert location for the token provider
func (a *authTokenProvider) CertPath() string {
	return a.certPath
}

// fullAccessToken grants a GitLab rails admin token for a specified repository
func fullAccessToken(repositoryName string) []*token.ResourceActions {
	return []*token.ResourceActions{
		{Type: "repository", Name: repositoryName, Actions: []string{"pull", "push"}},
		{Type: "repository", Name: repositoryName + "/*", Actions: []string{"pull"}},
	}
}

// fullAccessTokenWithProjectMeta grants a GitLab rails admin token for a specified repository and project path
func fullAccessTokenWithProjectMeta(projectPath, repositoryName string) []*token.ResourceActions {
	return []*token.ResourceActions{
		{Type: "repository", Name: repositoryName, Actions: []string{"pull", "push"}, Meta: &token.Meta{ProjectPath: projectPath}},
		{Type: "repository", Name: repositoryName + "/*", Actions: []string{"pull"}},
	}
}

// requireRenameTTLInRange makes sure that the rename operation TTL is within an acceptable range of an expected duration
func requireRenameTTLInRange(t *testing.T, actualTTL time.Time, expectedTTLDuration time.Duration) {
	t.Helper()
	lowerBound := time.Now()
	upperBound := time.Now().Add(expectedTTLDuration)
	require.WithinRange(t, actualTTL, lowerBound, upperBound,
		"rename TTL of %s should be between %s and %s",
		actualTTL.String(), lowerBound.String(), upperBound.String())
}
