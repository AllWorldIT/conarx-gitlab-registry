package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/api/urls"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/auth"
	_ "github.com/docker/distribution/registry/auth/silly"
	"github.com/docker/distribution/registry/datastore"
	dbmock "github.com/docker/distribution/registry/datastore/mocks"
	storemock "github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/internal/mocks"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/docker/distribution/registry/storage"
	memorycache "github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver/testdriver"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

// TestAppDistribtionDispatcher builds an application with a test dispatcher and
// ensures that requests are properly dispatched and the handlers are
// constructed. This only tests the dispatch mechanism. The underlying
// dispatchers must be tested individually.
func TestAppDistribtionDispatcher(t *testing.T) {
	driver := testdriver.New()
	ctx := context.Background()
	registry, err := storage.NewRegistry(ctx, driver, storage.BlobDescriptorCacheProvider(memorycache.NewInMemoryBlobDescriptorCacheProvider()), storage.EnableDelete, storage.EnableRedirect)
	if err != nil {
		t.Fatalf("error creating registry: %v", err)
	}
	app := &App{
		Config:             &configuration.Configuration{},
		Context:            ctx,
		distributionRouter: v2.Router(),
		driver:             driver,
		registry:           registry,
	}
	server := httptest.NewServer(app)
	defer server.Close()
	distributionRouter := v2.Router()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("error parsing server url: %v", err)
	}

	varCheckingDispatcher := func(expectedVars map[string]string) dispatchFunc {
		return func(ctx *Context, r *http.Request) http.Handler {
			// Always checks the same name context
			if ctx.Repository.Named().Name() != getName(ctx) {
				t.Fatalf("unexpected name: %q != %q", ctx.Repository.Named().Name(), "foo/bar")
			}

			// Check that we have all that is expected
			for expectedK, expectedV := range expectedVars {
				if ctx.Value(expectedK) != expectedV {
					t.Fatalf("unexpected %s in context vars: %q != %q", expectedK, ctx.Value(expectedK), expectedV)
				}
			}

			// Check that we only have variables that are expected
			for k, v := range ctx.Value("vars").(map[string]string) {
				_, ok := expectedVars[k]

				if !ok { // name is checked on context
					// We have an unexpected key, fail
					t.Fatalf("unexpected key %q in vars with value %q", k, v)
				}
			}

			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
		}
	}

	// unflatten a list of variables, suitable for gorilla/mux, to a map[string]string
	unflatten := func(vars []string) map[string]string {
		m := make(map[string]string)
		for i := 0; i < len(vars)-1; i = i + 2 {
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

		require.Equal(t, http.StatusOK, resp.StatusCode)
	}
}

// TestNewApp covers the creation of an application via NewApp with a
// configuration.
func TestNewApp(t *testing.T) {
	ctx := context.Background()
	config := configuration.Configuration{
		Storage: configuration.Storage{
			"testdriver": nil,
			"maintenance": configuration.Parameters{"uploadpurging": map[interface{}]interface{}{
				"enabled": false,
			}},
		},
		Auth: configuration.Auth{
			// For now, we simply test that new auth results in a viable
			// application.
			"silly": {
				"realm":   "realm-test",
				"service": "service-test",
			},
		},
	}

	// Mostly, with this test, given a sane configuration, we are simply
	// ensuring that NewApp doesn't panic. We might want to tweak this
	// behavior.
	app := NewApp(ctx, &config)

	server := httptest.NewServer(app)
	defer server.Close()
	builder, err := urls.NewBuilderFromString(server.URL, false)
	if err != nil {
		t.Fatalf("error creating urlbuilder: %v", err)
	}

	baseURL, err := builder.BuildBaseURL()
	if err != nil {
		t.Fatalf("error creating baseURL: %v", err)
	}

	// TODO(stevvooe): The rest of this test might belong in the API tests.

	// Just hit the app and make sure we get a 401 Unauthorized error.
	req, err := http.Get(baseURL)
	if err != nil {
		t.Fatalf("unexpected error during GET: %v", err)
	}
	defer req.Body.Close()

	if req.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected status code during request: %v", err)
	}

	if req.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("unexpected content-type: %v != %v", req.Header.Get("Content-Type"), "application/json")
	}

	expectedAuthHeader := "Bearer realm=\"realm-test\",service=\"service-test\""
	if e, a := expectedAuthHeader, req.Header.Get("WWW-Authenticate"); e != a {
		t.Fatalf("unexpected WWW-Authenticate header: %q != %q", e, a)
	}

	var errs errcode.Errors
	dec := json.NewDecoder(req.Body)
	if err := dec.Decode(&errs); err != nil {
		t.Fatalf("error decoding error response: %v", err)
	}

	err2, ok := errs[0].(errcode.ErrorCoder)
	if !ok {
		t.Fatalf("not an ErrorCoder: %#v", errs[0])
	}
	if err2.ErrorCode() != errcode.ErrorCodeUnauthorized {
		t.Fatalf("unexpected error code: %v != %v", err2.ErrorCode(), errcode.ErrorCodeUnauthorized)
	}
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

	records := []auth.Access{}
	result := appendAccessRecords(records, "GET", repo)
	expectedResult := []auth.Access{expectedPullRecord}
	if ok := reflect.DeepEqual(result, expectedResult); !ok {
		t.Fatalf("Actual access record differs from expected")
	}

	records = []auth.Access{}
	result = appendAccessRecords(records, "HEAD", repo)
	expectedResult = []auth.Access{expectedPullRecord}
	if ok := reflect.DeepEqual(result, expectedResult); !ok {
		t.Fatalf("Actual access record differs from expected")
	}

	records = []auth.Access{}
	result = appendAccessRecords(records, "POST", repo)
	expectedResult = []auth.Access{expectedPullRecord, expectedPushRecord}
	if ok := reflect.DeepEqual(result, expectedResult); !ok {
		t.Fatalf("Actual access record differs from expected")
	}

	records = []auth.Access{}
	result = appendAccessRecords(records, "PUT", repo)
	expectedResult = []auth.Access{expectedPullRecord, expectedPushRecord}
	if ok := reflect.DeepEqual(result, expectedResult); !ok {
		t.Fatalf("Actual access record differs from expected")
	}

	records = []auth.Access{}
	result = appendAccessRecords(records, "PATCH", repo)
	expectedResult = []auth.Access{expectedPullRecord, expectedPushRecord}
	if ok := reflect.DeepEqual(result, expectedResult); !ok {
		t.Fatalf("Actual access record differs from expected")
	}

	records = []auth.Access{}
	result = appendAccessRecords(records, "DELETE", repo)
	expectedResult = []auth.Access{expectedDeleteRecord}
	if ok := reflect.DeepEqual(result, expectedResult); !ok {
		t.Fatalf("Actual access record differs from expected")
	}
}

// TestAppendRepositoryImportAccessRecords ensures that only admins may trigger imports
// via the GitLab v1 API.
func TestAppendRepositoryImportAccessRecords(t *testing.T) {

	// The appendRepositoryImportAccessRecords function depends on
	// mux.CurrentRoute, which only inside a handler when a proper request is
	// being made so we need to test against an instantiated app via HTTP.
	ctx := context.Background()
	config := configuration.Configuration{
		Storage: configuration.Storage{
			"testdriver": nil,
			"maintenance": configuration.Parameters{"uploadpurging": map[interface{}]interface{}{
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

	app := NewApp(ctx, &config)

	server := httptest.NewServer(app)
	defer server.Close()

	repo, err := reference.WithName("test/repo")
	require.NoError(t, err)

	repo, err = reference.WithTag(repo, "latest")
	require.NoError(t, err)

	builder, err := urls.NewBuilderFromString(server.URL, false)
	require.NoError(t, err)

	url, err := builder.BuildGitlabV1RepositoryImportURL(repo)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPut, url, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	expectedAuthHeader := `Bearer realm="realm-test",service="service-test",scope="repository:test/repo:pull repository:test/repo:push registry:import:*"`
	require.Equal(t, expectedAuthHeader, resp.Header.Get("WWW-Authenticate"))

	// Ensure import scopes are not attached to other repository scoped requests.
	url, err = builder.BuildManifestURL(repo)
	require.NoError(t, err)

	resp, err = http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	expectedAuthHeader = `Bearer realm="realm-test",service="service-test",scope="repository:test/repo:pull"`
	require.Equal(t, expectedAuthHeader, resp.Header.Get("WWW-Authenticate"))
}

func Test_updateOnlineGCSettings_SkipIfDatabaseDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dbmock.NewMockHandler(ctrl)

	config := &configuration.Configuration{}

	// no expectations were set on mocks, so this asserts that no methods are called
	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

func Test_updateOnlineGCSettings_SkipIfGCDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dbmock.NewMockHandler(ctrl)

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
	dbMock := dbmock.NewMockHandler(ctrl)

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
	dbMock := dbmock.NewMockHandler(ctrl)

	config := &configuration.Configuration{
		Database: configuration.Database{
			Enabled: true,
		},
	}

	err := updateOnlineGCSettings(context.Background(), dbMock, config)
	require.NoError(t, err)
}

var storeMock *storemock.MockGCSettingsStore

func mockSettingsStore(tb testing.TB, ctrl *gomock.Controller) {
	tb.Helper()

	storeMock = storemock.NewMockGCSettingsStore(ctrl)
	bkp := gcSettingsStoreConstructor
	gcSettingsStoreConstructor = func(db datastore.Queryer) datastore.GCSettingsStore { return storeMock }

	tb.Cleanup(func() { gcSettingsStoreConstructor = bkp })
}

func Test_updateOnlineGCSettings(t *testing.T) {
	ctrl := gomock.NewController(t)
	dbMock := dbmock.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := mocks.NewMockClock(ctrl)
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
	rand.Seed(now.UnixNano())
	expectedJitter := time.Duration(rand.Intn(onlineGCUpdateJitterMaxSeconds)) * time.Second

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
	dbMock := dbmock.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := mocks.NewMockClock(ctrl)
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
	dbMock := dbmock.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := mocks.NewMockClock(ctrl)
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
	dbMock := dbmock.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := mocks.NewMockClock(ctrl)
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
	dbMock := dbmock.NewMockHandler(ctrl)
	mockSettingsStore(t, ctrl)

	clockMock := mocks.NewMockClock(ctrl)
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
