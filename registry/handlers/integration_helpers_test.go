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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/internal/feature"
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
	iredis "github.com/docker/distribution/registry/internal/redis"
	internaltestutil "github.com/docker/distribution/registry/internal/testutil"
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
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/labkit/correlation"
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

func withWebhookNotifications(notifCfg configuration.Notifications) configOpt {
	return func(config *configuration.Configuration) {
		config.Notifications = notifCfg
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

		if os.Getenv("REGISTRY_DATABASE_LOADBALANCING_ENABLED") == "true" {
			// service discovery takes precedence over fixed hosts
			if os.Getenv("REGISTRY_DATABASE_LOADBALANCING_RECORD") != "" {
				nameserver := os.Getenv("REGISTRY_DATABASE_LOADBALANCING_NAMESERVER")
				tmpPort := os.Getenv("REGISTRY_DATABASE_LOADBALANCING_PORT")
				record := os.Getenv("REGISTRY_DATABASE_LOADBALANCING_RECORD")

				if nameserver == "" || tmpPort == "" || record == "" {
					panic("REGISTRY_DATABASE_LOADBALANCING_NAMESERVER, " +
						"REGISTRY_DATABASE_LOADBALANCING_PORT and REGISTRY_DATABASE_LOADBALANCING_RECORD required for " +
						"enabling DB load balancing with service discovery")
				}
				port, err := strconv.Atoi(tmpPort)
				if err != nil {
					panic(fmt.Sprintf("invalid REGISTRY_DATABASE_LOADBALANCING_PORT: %q", tmpPort))
				}

				config.Database.LoadBalancing = configuration.DatabaseLoadBalancing{
					Enabled:    true,
					Nameserver: nameserver,
					Port:       port,
					Record:     record,
				}
			} else if hosts := os.Getenv("REGISTRY_DATABASE_LOADBALANCING_HOSTS"); hosts != "" {
				config.Database.LoadBalancing = configuration.DatabaseLoadBalancing{
					Enabled: true,
					Hosts:   strings.Split(hosts, ","),
				}
			}
		}

		if os.Getenv("REGISTRY_REDIS_CACHE_ENABLED") == "true" {
			config.Redis.Cache = configuration.RedisCommon{
				Enabled:  true,
				Addr:     os.Getenv("REGISTRY_REDIS_CACHE_ADDR"),
				Username: os.Getenv("REGISTRY_REDIS_CACHE_USERNAME"),
				Password: os.Getenv("REGISTRY_REDIS_CACHE_PASSWORD"),
			}
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

func skipRedisCacheEnabled(tb testing.TB) {
	tb.Helper()

	if os.Getenv("REGISTRY_REDIS_CACHE_ENABLED") == "true" {
		tb.Skip("skipping test because Redis cache is enabled")
	}
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
	pk           libtrust.PrivateKey
	ctx          context.Context
	config       *configuration.Configuration
	app          *registryhandlers.App
	server       *httptest.Server
	builder      *urls.Builder
	db           datastore.LoadBalancer
	ns           *rtestutil.NotificationServer
	cacheClient  cacheClient
	shutdownOnce *sync.Once
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
	ctx := testutil.NewContextWithLogger(t)

	// The API test needs access to the database only to clean it up during
	// shutdown so that environments come up with a fresh copy of the database.
	var db datastore.LoadBalancer
	var err error
	if config.Database.Enabled {
		db, err = datastoretestutil.NewDBFromConfig(config)
		if err != nil {
			t.Fatal(err)
		}
		m := migrations.NewMigrator(db.Primary())
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
			s := datastore.NewGCSettingsStore(db.Primary())
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
		pk:           pk,
		ctx:          ctx,
		config:       config,
		app:          app,
		server:       server,
		builder:      builder,
		db:           db,
		ns:           notifServer,
		cacheClient:  redis,
		shutdownOnce: new(sync.Once),
	}
}

func (t *testEnv) Shutdown() {
	t.shutdownOnce.Do(func() {
		t.server.CloseClientConnections()
		t.server.Close()

		if err := t.app.GracefulShutdown(t.ctx); err != nil {
			panic(err)
		}

		if t.config.Database.Enabled {
			if err := datastoretestutil.TruncateAllTables(t.db.Primary()); err != nil {
				panic(err)
			}

			if err := t.db.Close(); err != nil {
				panic(err)
			}
		}

		if t.config.Redis.Cache.Enabled {
			if err := t.cacheClient.FlushCache(); err != nil {
				panic(err)
			}
		}
	})
}

type subjectManifest interface {
	Config() distribution.Descriptor
	Payload() (string, []byte, error)
}

type manifestOpts struct {
	manifestURL        string
	putManifest        bool
	assertNotification bool
	withoutMediaType   bool
	authToken          string
	artifactType       string
	subjectManifest
	// Non-optional values which be passed through by the testing func for ease of use.
	repoPath string
}

func (m manifestOpts) hasSubject() bool {
	return m.subjectManifest != nil
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

func withArtifactType(at string) manifestOptsFunc {
	return func(t *testing.T, env *testEnv, opts *manifestOpts) {
		opts.artifactType = at
	}
}

func withSubject(subject subjectManifest) manifestOptsFunc {
	return func(t *testing.T, env *testEnv, opts *manifestOpts) {
		opts.subjectManifest = subject
	}
}

func withAuthToken(token string) manifestOptsFunc {
	return func(t *testing.T, env *testEnv, opts *manifestOpts) {
		opts.authToken = token
	}
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

	var requestOpts []requestOpt
	if config.authToken != "" {
		requestOpts = append(requestOpts, witAuthToken(config.authToken))
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
	uploadURLBase, _ := startPushLayer(t, env, repoRef, requestOpts...)
	pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload), requestOpts...)
	manifest.Config = cfgDesc

	// Create and push up 2 random layers.
	manifest.Layers = make([]distribution.Descriptor, 2)

	for i := range manifest.Layers {
		rs, dgst, size := createRandomSmallLayer()

		uploadURLBase, _ := startPushLayer(t, env, repoRef, requestOpts...)
		pushLayer(t, env.builder, repoRef, dgst, uploadURLBase, rs, requestOpts...)

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

		resp := putManifest(t, "putting manifest no error", config.manifestURL, schema2.MediaTypeManifest, deserializedManifest.Manifest, requestOpts...)
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
	// NOTE(prozlach): It is crucial to not to make the size of the layer too
	// small as this will lead to flakes, as there is only one sha for layer
	// size 0, handfull of shas for layer with size 1, etc... 128-196 bytes
	// gives enough entropy to make tests reliable.
	size := 128 + rand.Int63n(64)
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

	manifest.ArtifactType = config.artifactType

	// Use the config from the subject manifest, if present;
	// otherwise, create a manifest config and push up its content.
	if config.hasSubject() {
		manifest.Config = config.subjectManifest.Config()
	} else {
		cfgPayload, cfgDesc := ociConfig()
		uploadURLBase, _ := startPushLayer(t, env, repoRef)
		pushLayer(t, env.builder, repoRef, cfgDesc.Digest, uploadURLBase, bytes.NewReader(cfgPayload))
		manifest.Config = cfgDesc
	}

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

	// Set subject descriptor
	if config.hasSubject() {
		_, payload, err := config.subjectManifest.Payload()
		require.NoError(t, err)

		manifest.Subject = &distribution.Descriptor{
			Digest:    digest.FromBytes(payload),
			MediaType: v1.MediaTypeImageManifest,
			Size:      int64(len(payload)),
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

func buildEventManifestDeleteByTag(mediaType, repoPath, tag string, opts ...eventOpt) notifications.Event {
	return buildEventManifestDelete(mediaType, repoPath, tag, "", opts...)
}

func buildEventRepositoryRename(repoTargetPath string, rename notifications.Rename, opts ...eventOpt) notifications.Event {
	event := notifications.Event{
		Action: "rename",
		Target: notifications.Target{
			Repository: repoTargetPath,
			Rename:     &rename,
		},
	}

	for _, opt := range opts {
		opt(&event)
	}

	return event
}

type eventOpt func(event *notifications.Event)

func buildEventManifestDelete(mediaType, repoPath, tagName string, dgst digest.Digest, opts ...eventOpt) notifications.Event {
	event := notifications.Event{
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

	for _, opt := range opts {
		opt(&event)
	}

	return event
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
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled
}

func putManifestRequest(t *testing.T, msg, url, contentType string, v interface{}) *http.Request {
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

	return req
}

func putManifest(t *testing.T, msg, url, contentType string, v interface{}, requestopts ...requestOpt) *http.Response {
	req := putManifestRequest(t, msg, url, contentType, v)
	req = newRequest(req, requestopts...)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("error doing put request while %s: %v", msg, err)
	}

	return resp
}

func startPushLayerRequest(t *testing.T, env *testEnv, name reference.Named) *http.Request {
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
	req, err := http.NewRequest(http.MethodPost, layerUploadURL, nil)
	if err != nil {
		t.Fatalf("unexpected error creating new request: %v", err)
	}
	req.Header.Set("Content-Type", "")

	return req
}

func startPushLayer(t *testing.T, env *testEnv, name reference.Named, requestopts ...requestOpt) (location, uuid string) {
	t.Helper()

	req := startPushLayerRequest(t, env, name)
	req = newRequest(req, requestopts...)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error starting layer push: %v", err)
	}

	defer resp.Body.Close()

	checkResponse(t, fmt.Sprintf("pushing starting layer push %v", name.String()), resp, http.StatusAccepted)

	u, err := url.Parse(resp.Header.Get("Location"))
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

func doPushLayerRequest(t *testing.T, ub *urls.Builder, name reference.Named, dgst digest.Digest, uploadURLBase string, body io.Reader) *http.Request {
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
	return req
}

// doPushLayer pushes the layer content returning the url on success returning
// the response. If you're only expecting a successful response, use pushLayer.
func doPushLayer(t *testing.T, ub *urls.Builder, name reference.Named, dgst digest.Digest, uploadURLBase string, body io.Reader, requestopts ...requestOpt) (*http.Response, error) {
	req := doPushLayerRequest(t, ub, name, dgst, uploadURLBase, body)
	req = newRequest(req, requestopts...)
	return http.DefaultClient.Do(req)
}

// pushLayer pushes the layer content returning the url on success.
func pushLayer(t *testing.T, ub *urls.Builder, name reference.Named, dgst digest.Digest, uploadURLBase string, body io.Reader, requestopts ...requestOpt) string {
	digester := digest.Canonical.Digester()

	resp, err := doPushLayer(t, ub, name, dgst, uploadURLBase, io.TeeReader(body, digester.Hash()), requestopts...)
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

func finishUpload(t *testing.T, ub *urls.Builder, name reference.Named, uploadURLBase string, dgst digest.Digest, requestopts ...requestOpt) string {
	resp, err := doPushLayer(t, ub, name, dgst, uploadURLBase, nil, requestopts...)
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

func doPushChunkRequest(t *testing.T, uploadURLBase string, body io.Reader) *http.Request {
	u, err := url.Parse(uploadURLBase)
	if err != nil {
		t.Fatalf("unexpected error parsing pushLayer url: %v", err)
	}

	u.RawQuery = url.Values{
		"_state": u.Query()["_state"],
	}.Encode()

	uploadURL := u.String()

	req, err := http.NewRequest(http.MethodPatch, uploadURL, body)
	if err != nil {
		t.Fatalf("unexpected error creating new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return req
}

func doPushChunk(t *testing.T, uploadURLBase string, body io.Reader, requestopts ...requestOpt) (*http.Response, digest.Digest, error) {
	// NOTE(prozlach): There is an issue/bug in golang:
	// https://github.com/golang/go/issues/51907
	// It prevents us from using the same request body reader for both digest
	// calculation and making the request as there is no guarantee that the
	// request body will be fully read after the call to Do(). We workaround it
	// by simply draining the requests body into the local buffer while
	// calculating the body and then pass the buffer to http request call.
	buf := new(bytes.Buffer)

	digester := digest.Canonical.Digester()
	multiWriter := io.MultiWriter(buf, digester.Hash())

	if _, err := io.Copy(multiWriter, body); err != nil {
		t.Fatalf("unexpected error while copying request body: %v", err)
	}

	req := doPushChunkRequest(t, uploadURLBase, buf)
	req = newRequest(req, requestopts...)
	resp, err := http.DefaultClient.Do(req)

	return resp, digester.Digest(), err
}

func pushChunk(t *testing.T, ub *urls.Builder, name reference.Named, uploadURLBase string, body io.Reader, length int64, requestopts ...requestOpt) (string, digest.Digest) {
	resp, dgst, err := doPushChunk(t, uploadURLBase, body, requestopts...)
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

func checkErr(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("unexpected error %s: %v", msg, err)
	}
}

func createRepository(t *testing.T, env *testEnv, repoPath, tag string) digest.Digest {
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

func assertGetResponse(t *testing.T, url string, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	req, err := http.NewRequest(http.MethodGet, url, nil)
	require.NoError(t, err)
	for _, o := range opts {
		o(req)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, expectedStatus, resp.StatusCode)
}

func assertHeadResponse(t *testing.T, url string, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	req, err := http.NewRequest(http.MethodHead, url, nil)
	require.NoError(t, err)
	for _, o := range opts {
		o(req)
	}
	resp, err := http.DefaultClient.Do(req)
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
	u, err := env.builder.BuildManifestURL(named)
	require.NoError(t, err)

	assertDeleteResponse(t, u, expectedStatus)
}

func assertBlobGetResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertGetResponse(t, u, expectedStatus, opts...)
}

func assertBlobHeadResponse(t *testing.T, env *testEnv, repoName string, dgst digest.Digest, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	tmp, err := reference.WithName(repoName)
	require.NoError(t, err)
	ref, err := reference.WithDigest(tmp, dgst)
	require.NoError(t, err)
	u, err := env.builder.BuildBlobURL(ref)
	require.NoError(t, err)

	assertHeadResponse(t, u, expectedStatus, opts...)
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

func assertManifestGetByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertGetResponse(t, u, expectedStatus, opts...)
}

func assertManifestGetByTagResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	assertGetResponse(t, u, expectedStatus, opts...)
}

func assertManifestHeadByDigestResponse(t *testing.T, env *testEnv, repoName string, m distribution.Manifest, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	u := buildManifestDigestURL(t, env, repoName, m)
	assertHeadResponse(t, u, expectedStatus, opts...)
}

func assertManifestHeadByTagResponse(t *testing.T, env *testEnv, repoName, tagName string, expectedStatus int, opts ...requestOpt) {
	t.Helper()

	u := buildManifestTagURL(t, env, repoName, tagName)
	assertHeadResponse(t, u, expectedStatus, opts...)
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

	wg := new(sync.WaitGroup)
	// NOTE(prozlach): concurency controll, value chosen arbitraly
	semaphore := make(chan struct{}, 20)

	for _, path := range repoPaths {
		wg.Add(1)
		go func(path string) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			seedRandomSchema2Manifest(t, env, path, putByTag(tagName))
		}(path)
	}
	wg.Wait()
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
		AuthType:   authUserType,
		Audience:   issuer.Service,
		Expiration: issuer.ExpireFunc(),
		NotBefore:  time.Now().Unix(),
		IssuedAt:   time.Now().Unix(),
		JWTID:      base64.URLEncoding.EncodeToString(randomBytes),
		Access:     access,
		User:       authUserJWT,
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

const (
	authUsername = "test-user"
	authUserType = "gitlab_test"
	authUserJWT  = "user-jwt"
)

// TokenWithActions generates a token for a specified set of actions
func (a *authTokenProvider) TokenWithActions(tra []*token.ResourceActions) string {
	return generateAuthToken(a.t, authUsername, tra, defaultIssuerProps(), a.privateKey)
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

// fullAccessNamespaceTokenWithProjectMeta grants a token used to rename a project's repositories namespace
func fullAccessNamespaceTokenWithProjectMeta(projectPath, namespace string) []*token.ResourceActions {
	return []*token.ResourceActions{
		{Type: "repository", Name: projectPath, Actions: []string{"pull", "push"}, Meta: &token.Meta{ProjectPath: projectPath}},
		{Type: "repository", Name: projectPath + "/*", Actions: []string{"pull"}, Meta: &token.Meta{ProjectPath: projectPath}},
		{Type: "repository", Name: namespace + "/*", Actions: []string{"push"}, Meta: &token.Meta{ProjectPath: projectPath}},
	}
}

// deleteAccessToken grants a delete action scope token for the specified repository
func deleteAccessToken(repositoryName string) []*token.ResourceActions {
	return []*token.ResourceActions{
		{Type: "repository", Name: repositoryName, Actions: []string{"delete"}},
	}
}

// deleteAccessTokenWithProjectMeta grants a delete action scope token for the specified repository and project path
func deleteAccessTokenWithProjectMeta(projectPath, repositoryName string) []*token.ResourceActions {
	return []*token.ResourceActions{
		{Type: "repository", Name: repositoryName, Actions: []string{"delete"}, Meta: &token.Meta{ProjectPath: projectPath}},
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

// acquireProjectLease enacts a project lease for `projectPath` in the `redisCache` for time `TTL` duration
func acquireProjectLease(t *testing.T, redisCache *iredis.Cache, projectPath string, TTL time.Duration) {
	t.Helper()
	// enact a lease on the project path which will be used to block all
	// write operations to the existing repositories in the given GitLab project.

	// create the lease store
	plStore, err := datastore.NewProjectLeaseStore(datastore.NewCentralProjectLeaseCache(redisCache))
	require.NoError(t, err)

	// invalidate the key if it already exists
	err = plStore.Invalidate(context.Background(), projectPath)
	require.NoError(t, err)

	// create a lease that expires in less than TTL duration .
	err = plStore.Set(context.Background(), projectPath, TTL)
	require.NoError(t, err)
}

// releaseProjectLease releases an existing project lease for `projectPath` in the `redisCache`
func releaseProjectLease(t *testing.T, redisCache *iredis.Cache, projectPath string) {
	t.Helper()
	plStore, err := datastore.NewProjectLeaseStore(datastore.NewCentralProjectLeaseCache(redisCache))
	require.NoError(t, err)
	err = plStore.Invalidate(context.Background(), projectPath)
	require.NoError(t, err)
}

type requestOpt func(r *http.Request)

func witAuthToken(token string) requestOpt {
	return func(r *http.Request) {
		r.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))
	}
}

func witContentRangeHeader(contentRange string) requestOpt {
	return func(r *http.Request) {
		r.Header.Add("Content-Range", contentRange)
	}
}

func newRequest(request *http.Request, opts ...requestOpt) *http.Request {
	for _, o := range opts {
		o(request)
	}
	return request
}

// setupValidRenameEnv will setup redis & use token based authorization for all proceeding requests.
// Redis & token based authorization are the two main registry configurations required to use any rename functionality
// (i.e enacting a rename or checking for an ongoing rename) when operating with the metadata database.
// This function will also set the OngoingRenameCheck FF to true
func setupValidRenameEnv(t *testing.T, opts ...configOpt) (*testEnv, internaltestutil.RedisCacheController, *authTokenProvider) {
	redisController := internaltestutil.NewRedisCacheController(t, 0)
	tokenProvider := NewAuthTokenProvider(t)
	env := newTestEnv(t, append(opts, withRedisCache(redisController.Addr()), withTokenAuth(tokenProvider.CertPath(), defaultIssuerProps()))...)
	// Enable the rename lease check environment variable
	t.Setenv(feature.OngoingRenameCheck.EnvVariable, "true")
	return env, redisController, tokenProvider
}
