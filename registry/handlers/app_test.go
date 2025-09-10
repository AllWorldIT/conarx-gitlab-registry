package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/internal/feature"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	"github.com/docker/distribution/registry/api/urls"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/auth"
	_ "github.com/docker/distribution/registry/auth/silly"
	"github.com/docker/distribution/registry/datastore"
	dmocks "github.com/docker/distribution/registry/datastore/mocks"
	imocks "github.com/docker/distribution/registry/internal/mocks"
	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/docker/distribution/registry/storage"
	memorycache "github.com/docker/distribution/registry/storage/cache/memory"
	_ "github.com/docker/distribution/registry/storage/driver/filesystem"
	"github.com/docker/distribution/registry/storage/driver/testdriver"
	dtestutil "github.com/docker/distribution/testutil"
)

// TestAppDistribtionDispatcher builds an application with a test dispatcher and
// ensures that requests are properly dispatched and the handlers are
// constructed. This only tests the dispatch mechanism. The underlying
// dispatchers must be tested individually.
func TestAppDistribtionDispatcher(t *testing.T) {
	driver := testdriver.New()
	ctx := dtestutil.NewContextWithLogger(t)
	registry, err := storage.NewRegistry(ctx, driver, storage.BlobDescriptorCacheProvider(memorycache.NewInMemoryBlobDescriptorCacheProvider()), storage.EnableDelete, storage.EnableRedirect)
	require.NoError(t, err, "error creating registry")
	app := &App{
		Config:         &configuration.Configuration{},
		Context:        ctx,
		router:         &metaRouter{distribution: v2.Router()},
		driver:         driver,
		registry:       registry,
		dualRedisCache: iredis.NewDualCache(nil, nil),
	}

	require.NoError(t, app.initMetaRouter())

	server := httptest.NewServer(app)
	defer server.Close()
	distributionRouter := v2.Router()

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err, "error parsing server url")

	varCheckingDispatcher := func(expectedVars map[string]string) dispatchFunc {
		return func(ctx *Context, _ *http.Request) http.Handler {
			// Always checks the same name context
			assert.Equal(t, ctx.Repository.Named().Name(), getName(ctx), "unexpected name")

			// Check that we have all that is expected
			for expectedK, expectedV := range expectedVars {
				assert.Equalf(t, expectedV, ctx.Value(expectedK), "unexpected %s in context vars", expectedK)
			}

			// Check that we only have variables that are expected
			for k, v := range ctx.Value("vars").(map[string]string) {
				_, ok := expectedVars[k]

				assert.True(t, ok, "unexpected key %q in vars with value %q", k, v)
			}

			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
		}
	}

	// unflatten a list of variables, suitable for gorilla/mux, to a map[string]string
	unflatten := func(vars []string) map[string]string {
		m := make(map[string]string)
		for i := 0; i < len(vars)-1; i += 2 {
			m[vars[i]] = vars[i+1]
		}

		return m
	}

	for _, testcase := range []struct {
		endpoint string
		vars     []string
	}{
		{
			endpoint: v2.RouteNameManifest,
			vars: []string{
				"name", "foo/bar",
				"reference", "sometag",
			},
		},
		{
			endpoint: v2.RouteNameTags,
			vars: []string{
				"name", "foo/bar",
			},
		},
		{
			endpoint: v2.RouteNameBlobUpload,
			vars: []string{
				"name", "foo/bar",
			},
		},
		{
			endpoint: v2.RouteNameBlobUploadChunk,
			vars: []string{
				"name", "foo/bar",
				"uuid", "theuuid",
			},
		},
	} {
		app.registerDistribution(testcase.endpoint, varCheckingDispatcher(unflatten(testcase.vars)))
		route := distributionRouter.GetRoute(testcase.endpoint).Host(serverURL.Host)
		u, err := route.URL(testcase.vars...)
		require.NoError(t, err)

		resp, err := http.Get(u.String())
		require.NoError(t, err)

		err = resp.Body.Close()
		require.NoError(t, err)

		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
}

func testConfig() *configuration.Configuration {
	return &configuration.Configuration{
		Storage: configuration.Storage{
			"testdriver": nil,
			"maintenance": configuration.Parameters{"uploadpurging": map[any]any{
				"enabled": false,
			}},
		},
		Auth: configuration.Auth{
			"silly": {
				"realm":   "realm-test",
				"service": "service-test",
			},
		},
	}
}

// TestNewApp covers the creation of an application via NewApp with a
// configuration.
func TestNewApp(t *testing.T) {
	ctx := dtestutil.NewContextWithLogger(t)
	config := testConfig()

	// Mostly, with this test, given a sane configuration, we are simply
	// ensuring that NewApp doesn't panic. We might want to tweak this
	// behavior.
	app, err := NewApp(ctx, config)
	require.NoError(t, err)

	server := httptest.NewServer(app)
	defer server.Close()
	builder, err := urls.NewBuilderFromString(server.URL, false)
	require.NoError(t, err, "error creating urlbuilder")

	baseURL, err := builder.BuildBaseURL()
	require.NoError(t, err, "error creating baseURL")

	// Just hit the app and make sure we get a 401 Unauthorized error.
	req, err := http.Get(baseURL)
	require.NoError(t, err, "unexpected error during GET")
	defer req.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, req.StatusCode, "unexpected status code during request")

	assert.Equal(t, "application/json", req.Header.Get("Content-Type"), "unexpected content-type")

	expectedAuthHeader := "Bearer realm=\"realm-test\",service=\"service-test\""
	assert.Equal(t, expectedAuthHeader, req.Header.Get("WWW-Authenticate"), "unexpected WWW-Authenticate header")

	var errs errcode.Errors
	dec := json.NewDecoder(req.Body)
	require.NoError(t, dec.Decode(&errs), "error decoding error response")

	err2, ok := errs[0].(errcode.ErrorCoder)
	require.True(t, ok, "not an ErrorCoder")
	assert.Equal(t, errcode.ErrorCodeUnauthorized, err2.ErrorCode(), "unexpected error code")
}

// Test the access record accumulator
func TestAppendAccessRecords(t *testing.T) {
	repo := "testRepo"

	expectedResource := auth.Resource{
		Type: "repository",
		Name: repo,
	}

	expectedPullRecord := auth.Access{
		Resource: expectedResource,
		Action:   "pull",
	}
	expectedPushRecord := auth.Access{
		Resource: expectedResource,
		Action:   "push",
	}
	expectedDeleteRecord := auth.Access{
		Resource: expectedResource,
		Action:   "delete",
	}

	records := make([]auth.Access, 0)
	result := appendAccessRecords(records, http.MethodGet, repo)
	expectedResult := []auth.Access{expectedPullRecord}
	assert.Equal(t, expectedResult, result, "actual access record differs from expected")

	records = make([]auth.Access, 0)
	result = appendAccessRecords(records, http.MethodHead, repo)
	expectedResult = []auth.Access{expectedPullRecord}
	assert.Equal(t, expectedResult, result, "actual access record differs from expected")

	records = make([]auth.Access, 0)
	result = appendAccessRecords(records, http.MethodPost, repo)
	expectedResult = []auth.Access{expectedPullRecord, expectedPushRecord}
	assert.Equal(t, expectedResult, result, "actual access record differs from expected")

	records = make([]auth.Access, 0)
	result = appendAccessRecords(records, http.MethodPut, repo)
	expectedResult = []auth.Access{expectedPullRecord, expectedPushRecord}
	assert.Equal(t, expectedResult, result, "actual access record differs from expected")

	records = make([]auth.Access, 0)
	result = appendAccessRecords(records, http.MethodPatch, repo)
	expectedResult = []auth.Access{expectedPullRecord, expectedPushRecord}
	assert.Equal(t, expectedResult, result, "actual access record differs from expected")

	records = make([]auth.Access, 0)
	result = appendAccessRecords(records, http.MethodDelete, repo)
	expectedResult = []auth.Access{expectedDeleteRecord}
	assert.Equal(t, expectedResult, result, "actual access record differs from expected")
}

// TestGitlabAPI_GetRepositoryDetailsAccessRecords ensures that only users will pull permissions for repository x can invoke the
// `GET /gitlab/v1/repositories/x` endpoint.
func TestGitlabAPI_GetRepositoryDetailsAccessRecords(t *testing.T) {
	ctx := dtestutil.NewContextWithLogger(t)
	config := testConfig()

	app, err := NewApp(ctx, config)
	require.NoError(t, err)

	server := httptest.NewServer(app)
	defer server.Close()

	repo, err := reference.WithName("test/repo")
	require.NoError(t, err)

	repo, err = reference.WithTag(repo, "latest")
	require.NoError(t, err)

	builder, err := urls.NewBuilderFromString(server.URL, false)
	require.NoError(t, err)

	u, err := builder.BuildGitlabV1RepositoryURL(repo)
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	expectedAuthHeader := `Bearer realm="realm-test",service="service-test",scope="repository:test/repo:pull"`
	require.Equal(t, expectedAuthHeader, resp.Header.Get("WWW-Authenticate"))
}

// TestGitlabAPI_GetRepositoryDetails_SelfWithDescendantsAccessRecords ensures that only users with pull permissions
// for repositories `<name>` (base) and `<name>/*` (descendants) can invoke the `GET /gitlab/v1/repositories/<name>`
// endpoint with the `size` query param set to `self_with_descendants`.
func TestGitlabAPI_GetRepositoryDetails_SelfWithDescendantsAccessRecords(t *testing.T) {
	ctx := dtestutil.NewContextWithLogger(t)
	config := testConfig()

	app, err := NewApp(ctx, config)
	require.NoError(t, err)

	server := httptest.NewServer(app)
	defer server.Close()

	repo, err := reference.WithName("test/repo")
	require.NoError(t, err)

	repo, err = reference.WithTag(repo, "latest")
	require.NoError(t, err)

	builder, err := urls.NewBuilderFromString(server.URL, false)
	require.NoError(t, err)

	u, err := builder.BuildGitlabV1RepositoryURL(repo, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	expectedAuthHeader := `Bearer realm="realm-test",service="service-test",scope="repository:test/repo:pull repository:test/repo/*:pull"`
	require.Equal(t, expectedAuthHeader, resp.Header.Get("WWW-Authenticate"))
}

// TestGitlabAPI_GetStatistics_AuthRequired ensures that only users with admin permissions,
// denoted by the `*` action, can access the `GET /gitlab/v1/statistics/` endpoint.
func TestGitlabAPI_GetStatistics_AuthRequired(t *testing.T) {
	ctx := dtestutil.NewContextWithLogger(t)
	config := testConfig()

	app, err := NewApp(ctx, config)
	require.NoError(t, err)

	server := httptest.NewServer(app)
	defer server.Close()

	builder, err := urls.NewBuilderFromString(server.URL, false)
	require.NoError(t, err)

	u, err := builder.BuildGitlabV1StatisticsURL()
	require.NoError(t, err)

	resp, err := http.Get(u)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	expectedAuthHeader := `Bearer realm="realm-test",service="service-test",scope="registry:statistics:*"`
	assert.Equal(t, expectedAuthHeader, resp.Header.Get("WWW-Authenticate"))
}

func Test_updateOnlineGCSettings_SkipIfDatabaseDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)

	config := &configuration.Configuration{}

	// no expectations were set on mocks, so this asserts that no methods are called
	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_SkipIfGCDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			Disabled: true,
		},
	}

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_SkipIfAllGCWorkersDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			Blobs: configuration.GCBlobs{
				Disabled: true,
			},
			Manifests: configuration.GCManifests{
				Disabled: true,
			},
		},
	}

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_SkipIfReviewAfterNotSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
	}

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

var storeMock *dmocks.MockGCSettingsStore

func mockSettingsStore(tb testing.TB, ctrl *gomock.Controller) {
	tb.Helper()

	storeMock = dmocks.NewMockGCSettingsStore(ctrl)
	bkp := gcSettingsStoreConstructor
	gcSettingsStoreConstructor = func(datastore.Queryer) datastore.GCSettingsStore { return storeMock }

	tb.Cleanup(func() { gcSettingsStoreConstructor = bkp })
}

func Test_updateOnlineGCSettings(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := imocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			ReviewAfter: 10 * time.Minute,
		},
	}

	// use fixed time for reproducible rand seeds (used to generate jitter durations)
	now := time.Time{}
	r := rand.New(rand.NewChaCha8(dtestutil.SeedFromUnixNano(now.UnixNano())))
	expectedJitter := time.Duration(r.Int64N(onlineGCUpdateJitterMaxSeconds)) * time.Second

	startTime := now.Add(1 * time.Millisecond)

	gomock.InOrder(
		clockMock.EXPECT().Now().Return(now).Times(1),       // base for jitter
		clockMock.EXPECT().Sleep(expectedJitter).Times(1),   // jitter sleep
		clockMock.EXPECT().Now().Return(startTime).Times(1), // start time snapshot
		storeMock.EXPECT().UpdateAllReviewAfterDefaults(
			testutil.IsContextWithDeadline{Deadline: startTime.Add(onlineGCUpdateTimeout)},
			config.GC.ReviewAfter,
		).Return(true, nil).Times(1),
		clockMock.EXPECT().Since(startTime).Return(1*time.Millisecond).Times(1), // elapsed time
	)

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_NoReviewDelay(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := imocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			ReviewAfter: -1,
		},
	}

	gomock.InOrder(
		// The value of the input arguments were already tested in Test_updateOnlineGCSettings, so here we can focus on
		// testing the UpdateAllReviewAfterDefaults call result.
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		storeMock.EXPECT().UpdateAllReviewAfterDefaults(
			gomock.Any(),
			time.Duration(0), // -1 was converted to 0
		).Return(true, nil).Times(1),
		clockMock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).Times(1),
	)

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_NoRowsUpdated(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := imocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			ReviewAfter: 10 * time.Minute,
		},
	}

	gomock.InOrder(
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		storeMock.EXPECT().UpdateAllReviewAfterDefaults(gomock.Any(), gomock.Any()).
			Return(false, nil).Times(1),
		clockMock.EXPECT().Since(gomock.Any()).Return(time.Duration(0)).Times(1),
	)

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := imocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			ReviewAfter: 10 * time.Minute,
		},
	}

	fakeErr := errors.New("foo")
	gomock.InOrder(
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		storeMock.EXPECT().UpdateAllReviewAfterDefaults(gomock.Any(), gomock.Any()).
			Return(false, fakeErr).Times(1),
	)

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.EqualError(t, err, fakeErr.Error())
}

func Test_updateOnlineGCSettings_Timeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dmocks.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := imocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
		GC: configuration.GC{
			ReviewAfter: 10 * time.Minute,
		},
	}

	gomock.InOrder(
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		clockMock.EXPECT().Sleep(gomock.Any()).Times(1),
		clockMock.EXPECT().Now().Return(time.Time{}).Times(1),
		storeMock.EXPECT().UpdateAllReviewAfterDefaults(gomock.Any(), gomock.Any()).
			Return(false, context.Canceled).Times(1),
	)

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.EqualError(t, err, context.Canceled.Error())
}

// TestGitlabAPI_LogsCFRayID ensures that the CF_ray Id
// is logged if it exists in the request header
// `GET /gitlab/v1/` endpoint.
func TestGitlabAPI_LogsCFRayID(t *testing.T) {
	testcases := []struct {
		name          string
		headers       map[string]string
		checkContains func(buf bytes.Buffer) bool
	}{
		{
			name:    "a request with a CF-ray header",
			headers: map[string]string{"CF-Ray": "value"},
			checkContains: func(buf bytes.Buffer) bool {
				return assert.Contains(t, buf.String(), "CF-RAY=value")
			},
		},
		{
			name:    "a request with a CF-ray header but empty value",
			headers: map[string]string{"CF-Ray": ""},
			checkContains: func(buf bytes.Buffer) bool {
				return assert.NotContains(t, buf.String(), "CF-RAY=value") &&
					assert.Contains(t, buf.String(), "CF-RAY= ")
			},
		},
		{
			name:    "a request without a CF-ray header",
			headers: map[string]string{"Not-CF-Ray": "value"},
			checkContains: func(buf bytes.Buffer) bool {
				return assert.NotContains(t, buf.String(), "CF-RAY")
			},
		},
	}
	t.Logf("Running Test %s", t.Name())
	for _, test := range testcases {
		ctx := context.TODO()
		config := testConfig()

		// use a logger that writes to a buffer instead of stdout
		var buf bytes.Buffer
		ctx = dcontext.WithLogger(ctx, bufferStreamLogger(&buf))

		app, err := NewApp(ctx, config)
		require.NoError(t, err)

		server := httptest.NewServer(app)
		// nolint: revive // defer
		defer server.Close()

		builder, err := urls.NewBuilderFromString(server.URL, false)
		require.NoError(t, err)

		baseURL, err := builder.BuildGitlabV1BaseURL()
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodGet, baseURL, nil)
		require.NoError(t, err)
		for headerKey, headerVal := range test.headers {
			req.Header.Add(headerKey, headerVal)
		}

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		// nolint: revive // defer
		defer resp.Body.Close()
		test.checkContains(buf)
	}
}

// TestGitlabAPI_LogsCFRayID ensures that the CF_ray Id
// is logged if it exists in the request header
// `GET /v2/` endpoint.
func TestDistributionAPI_LogsCFRayID(t *testing.T) {
	testcases := []struct {
		name          string
		headers       map[string]string
		checkContains func(buf bytes.Buffer) bool
	}{
		{
			name:    "a request with a CF-ray header",
			headers: map[string]string{"CF-Ray": "value"},
			checkContains: func(buf bytes.Buffer) bool {
				return assert.Contains(t, buf.String(), "CF-RAY=value")
			},
		},
		{
			name:    "a request with a CF-ray header but empty value",
			headers: map[string]string{"CF-Ray": ""},
			checkContains: func(buf bytes.Buffer) bool {
				return assert.NotContains(t, buf.String(), "CF-RAY=value") &&
					assert.Contains(t, buf.String(), "CF-RAY= ")
			},
		},
		{
			name:    "a request without a CF-ray header",
			headers: map[string]string{"Not-CF-Ray": "value"},
			checkContains: func(buf bytes.Buffer) bool {
				return assert.NotContains(t, buf.String(), "CF-RAY")
			},
		},
	}
	t.Logf("Running Test %s", t.Name())
	for _, test := range testcases {
		ctx := context.TODO()
		config := testConfig()

		// use a logger that writes to a buffer instead of stdout
		var buf bytes.Buffer
		ctx = dcontext.WithLogger(ctx, bufferStreamLogger(&buf))

		app, err := NewApp(ctx, config)
		require.NoError(t, err)

		server := httptest.NewServer(app)
		// nolint: revive // defer
		defer server.Close()

		builder, err := urls.NewBuilderFromString(server.URL, false)
		require.NoError(t, err)

		baseURL, err := builder.BuildBaseURL()
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodGet, baseURL, nil)
		require.NoError(t, err)
		for headerKey, headerVal := range test.headers {
			req.Header.Add(headerKey, headerVal)
		}

		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		// nolint: revive // defer
		defer resp.Body.Close()
		test.checkContains(buf)
	}
}

func bufferStreamLogger(buf *bytes.Buffer) *logrus.Entry {
	fields := logrus.Fields{}
	fields["test"] = true
	logger := logrus.StandardLogger().WithFields(fields)
	logger.Logger.Level = logrus.DebugLevel
	logger.Logger.SetOutput(buf)

	return logger.WithFields(fields)
}

func Test_startDBPoolRefresh_StartupJitter(t *testing.T) {
	ctrl := gomock.NewController(t)
	clockMock := imocks.NewMockClock(ctrl)
	testutil.StubClock(t, &systemClock, clockMock)

	// use fixed time for reproducible rand seeds (used to generate jitter durations)
	now := time.Time{}
	r := rand.New(rand.NewChaCha8(dtestutil.SeedFromUnixNano(now.UnixNano())))
	expectedJitter := time.Duration(r.IntN(dlbPeriodicTaskJitterMaxSeconds)) * time.Second

	// Create the load balancer with the required options
	lbMock := dmocks.NewMockLoadBalancer(ctrl)
	ctx := dtestutil.NewContextWithLogger(t)

	var wg sync.WaitGroup
	wg.Add(1) // We expect one goroutine to be run

	// Start the DB replica checking and mark the WaitGroup as done when finished
	gomock.InOrder(
		clockMock.EXPECT().Now().Return(now).Times(1),     // base for jitter
		clockMock.EXPECT().Sleep(expectedJitter).Times(1), // jitter sleep
		lbMock.EXPECT().StartPoolRefresh(ctx).Return(nil).Times(1).Do(func(_ context.Context) {
			wg.Done() // Mark the goroutine as done
		}),
	)

	startDBPoolRefresh(ctx, lbMock)

	// Wait for the goroutine to complete before checking expectations
	wg.Wait()
}

func TestStatusRecordingResponseWriter(t *testing.T) {
	bodyContent := "default response"

	tests := []struct {
		name         string
		writeHeader  bool
		customStatus int
		expectedCode int
		expectedBody string
	}{
		{
			name:         "default status without WriteHeader",
			writeHeader:  false,
			expectedCode: http.StatusOK,
			expectedBody: bodyContent,
		},
		{
			name:         "explicit WriteHeader with custom status",
			writeHeader:  true,
			customStatus: http.StatusCreated,
			expectedCode: http.StatusCreated,
			expectedBody: bodyContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			srw := newStatusRecordingResponseWriter(recorder)

			if tt.writeHeader {
				srw.WriteHeader(tt.customStatus)
			}
			srw.Write([]byte(bodyContent))

			assert.Equal(t, tt.expectedCode, srw.statusCode)
			// nolint: bodyclose // not required here
			assert.Equal(t, tt.expectedCode, recorder.Result().StatusCode)
			assert.Equal(t, tt.expectedBody, recorder.Body.String())
		})
	}
}

func TestRecordLSNMiddleware(t *testing.T) {
	driver := testdriver.New()
	ctx := context.Background()
	registry, err := storage.NewRegistry(ctx, driver)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := dmocks.NewMockLoadBalancer(ctrl)

	app := &App{
		Config: &configuration.Configuration{
			Database: configuration.Database{
				Enabled: true,
				LoadBalancing: configuration.DatabaseLoadBalancing{
					Enabled: true,
				},
			},
		},
		Context: ctx,
		// doesn't matter which router we use (distribution or GitLab's), we're only testing the middleware internals
		router:         &metaRouter{distribution: v2.Router()},
		driver:         driver,
		registry:       registry,
		db:             mockDB,
		dualRedisCache: iredis.NewDualCache(nil, nil),
	}
	require.NoError(t, app.initMetaRouter())

	server := httptest.NewServer(app)
	defer server.Close()
	router := v2.Router()
	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	testDispatcher := func(expectedStatus int) dispatchFunc {
		return func(*Context, *http.Request) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(expectedStatus)
			})
		}
	}

	for _, testcase := range []struct {
		name            string
		endpoint        string
		method          string
		vars            []string
		status          int
		shouldRecordLSN bool
	}{
		{
			name:     "target repository and success status and write method",
			endpoint: v2.RouteNameManifest,
			method:   http.MethodDelete,
			vars: []string{
				"name", "foo/bar",
				"reference", "sometag",
			},
			status:          http.StatusOK,
			shouldRecordLSN: true,
		},
		{
			name:     "target repository and success status and read method",
			endpoint: v2.RouteNameManifest,
			method:   http.MethodGet,
			vars: []string{
				"name", "foo/bar",
				"reference", "sometag",
			},
			status: http.StatusOK,
		},
		{
			name:     "target repository and error status and write method",
			endpoint: v2.RouteNameManifest,
			method:   http.MethodDelete,
			vars: []string{
				"name", "foo/bar",
				"reference", "sometag",
			},
			status: http.StatusBadRequest,
		},
		{
			name:     "target repository and error status and read method",
			endpoint: v2.RouteNameManifest,
			method:   http.MethodGet,
			vars: []string{
				"name", "foo/bar",
				"reference", "sometag",
			},
			status: http.StatusBadRequest,
		},
		{
			// there are no real write endpoints without a target repository, but just for future-proofing...
			name:     "no target repository and success status and write method",
			endpoint: v2.RouteNameBase,
			method:   http.MethodPost,
			status:   http.StatusOK,
		},
		{
			name:     "no target repository and success status and read method",
			endpoint: v2.RouteNameBase,
			method:   http.MethodGet,
			status:   http.StatusOK,
		},
		{
			name:     "no target repository and error status and write method",
			endpoint: v2.RouteNameBase,
			method:   http.MethodPost,
			status:   http.StatusInternalServerError,
		},
		{
			name:     "no target repository and error status and read method",
			endpoint: v2.RouteNameBase,
			method:   http.MethodGet,
			status:   http.StatusInternalServerError,
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			app.registerDistribution(testcase.endpoint, testDispatcher(testcase.status))
			route := router.GetRoute(testcase.endpoint).Host(serverURL.Host)
			u, err := route.URL(testcase.vars...)
			require.NoError(t, err)

			req, err := http.NewRequest(testcase.method, u.String(), nil)
			require.NoError(t, err)

			if testcase.shouldRecordLSN {
				mockDB.EXPECT().RecordLSN(gomock.Any(), gomock.Any()).Times(1)
			}

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, testcase.status, resp.StatusCode)
		})
	}
}

func TestNewApp_Locks_Errors(t *testing.T) {
	ctx := context.Background()
	config := testConfig()
	delete(config.Storage, "testdriver")

	tcs := map[string]struct {
		rootdir         string
		databaseEnabled bool
		expectedError   error
	}{
		"database in use": {
			rootdir: "../datastore/testdata/fixtures/importer/lockfile-db-in-use",
			// disabling the database when database-in-use exists should error out
			databaseEnabled: false,
			expectedError:   ErrDatabaseInUse,
		},
		"filesystem in use": {
			rootdir: "../datastore/testdata/fixtures/importer/happy-path",
			// enabling the database when filesystem-in-use exists should error out
			databaseEnabled: true,
			expectedError:   ErrFilesystemInUse,
		},
		// we cannot test the scenario where the FF is disabled
		// because it requires proper DB configuration and restoring of lockfiles
		// this is meant to be a unit test rather than an integration test, so
		// we can skip this test while the FF_ENFORCE_LOCKFILES exists
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			config.Storage["filesystem"] = map[string]any{
				"rootdirectory": tc.rootdir,
			}
			config.Database.Enabled = tc.databaseEnabled

			// Temporary use of FF while other tests are updated and fixed
			// see https://gitlab.com/gitlab-org/container-registry/-/issues/1335
			t.Setenv(feature.EnforceLockfiles.EnvVariable, "true")

			_, err := NewApp(ctx, config)
			require.ErrorIs(t, err, tc.expectedError)
		})
	}
}

// Do not lock the filesystem when the _manifests/ directory is empty
// https://gitlab.com/gitlab-org/container-registry/-/issues/1523
func TestNewApp_Locks_NoManifestsInFilesystem(t *testing.T) {
	ctx := context.Background()
	config := testConfig()
	delete(config.Storage, "testdriver")

	tcs := []struct {
		name      string
		createDir bool
	}{
		{
			name:      "docker directory exists but it's empty",
			createDir: true,
		},
		{
			// tests when Enumerate returns storagedriver.PathNotFoundError
			name:      "docker directory does not exist",
			createDir: false,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			if tc.createDir {
				require.NoError(t, os.MkdirAll(filepath.Join(tmpDir, "docker/registry/v2/repositories"), os.FileMode(0o755)))
			}

			config.Storage["filesystem"] = map[string]any{
				"rootdirectory": tmpDir,
			}

			// Temporary use of FF while other tests are updated and fixed
			// see https://gitlab.com/gitlab-org/container-registry/-/issues/1335
			t.Setenv(feature.EnforceLockfiles.EnvVariable, "true")

			_, err := NewApp(ctx, config)
			require.NoError(t, err)

			require.NoFileExists(t, filepath.Join(tmpDir, "docker/registry/lockfiles/filesystem-in-use"))
		})
	}
}

// TestDispatcherGitlab_RepoCacheInitialization ensures that ctx.repoCache is properly initialized
// when the database is enabled in the GitLab v1 API dispatcher.
func TestDispatcherGitlab_RepoCacheInitialization(t *testing.T) {
	driver := testdriver.New()
	ctx := dtestutil.NewContextWithLogger(t)
	registry, err := storage.NewRegistry(ctx, driver)
	require.NoError(t, err)

	testCases := []struct {
		name         string
		redisCache   bool
		expectedType any
	}{
		{
			name:         "with redis cache",
			redisCache:   true,
			expectedType: datastore.NewCentralRepositoryCache(&iredis.Cache{}),
		},
		{
			name:         "without redis cache",
			redisCache:   false,
			expectedType: datastore.NewSingleRepositoryCache(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			app := &App{
				Config: &configuration.Configuration{
					Database: configuration.Database{
						Enabled: true,
					},
				},
				Context:  ctx,
				driver:   driver,
				registry: registry,
				// Bypass authorization logic
				accessController: nil,
				dualRedisCache:   iredis.NewDualCache(nil, nil),
			}

			if tc.redisCache {
				// Create a mock Redis cache
				app.dualRedisCache = iredis.NewDualCache(&iredis.Cache{}, nil)
			}

			// Initialize the app's router to get proper GitLab v1 routes
			require.NoError(t, app.initMetaRouter())

			// Create a test dispatcher that captures the context
			var capturedContext *Context
			testDispatcher := func(ctx *Context, _ *http.Request) http.Handler {
				capturedContext = ctx
				return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				})
			}

			// Register our test dispatcher for the GitLab v1 base route
			app.registerGitlab(v1.Base, testDispatcher)

			// Create a test request to the GitLab v1 base endpoint
			req := httptest.NewRequest(http.MethodGet, v1.Base.Path, nil)
			w := httptest.NewRecorder()

			// Use the app's router to serve the request, which will properly set up route context
			app.ServeHTTP(w, req)

			// Verify the context was captured
			require.NotNil(t, capturedContext, "context should be captured")

			// Check repoCache initialization
			require.NotNil(t, capturedContext.repoCache, "repoCache should not be nil when database is enabled")
			// Verify the type of cache based on Redis availability
			require.IsType(t, tc.expectedType, capturedContext.repoCache, "repoCache should be of expected type")
		})
	}
}

func TestApp_initializeMigrationCountMetric(t *testing.T) {
	ctx := dtestutil.NewContextWithLogger(t)

	t.Run("initializes migration count metrics successfully", func(t *testing.T) {
		app := &App{
			Context: ctx,
		}

		// Create a minimal DB instance for testing
		// Since the method only uses the DB to create migrators that count files,
		// we can use a DB with nil sql.DB (the migrators don't actually query the database)
		testDB := &datastore.DB{
			DB:  nil, // This is fine since migrators only count migration files
			DSN: &datastore.DSN{Host: "test", DBName: "test"},
		}

		// This should not panic and should complete successfully
		require.NotPanics(t, func() {
			app.setMigrationCountMetric(testDB)
		})
	})
}
