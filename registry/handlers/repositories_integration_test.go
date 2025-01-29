//go:build integration && handlers_test

package handlers

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gorilla "github.com/gorilla/handlers"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/labkit/correlation"
	"go.uber.org/mock/gomock"

	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/urls"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/mocks"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/datastore/testutil"
	dtestutil "github.com/docker/distribution/testutil"
)

// testEnv is a drastically simplified version of the implementation in handlers_test (integration_helpers_test.go). The
// sole purpose of this is to allow stubbing the unexported App.db, so that we can simulate PosgtreSQL errors. This
// could not be done within api_gitlab_integration_test.go, which would be the obvious place, because that's part of
// handlers_test, and we can't mutate App.db there. Exporting App.db to the outside is far from ideal. Extracting the
// testEnv implementation from handlers_test into a separate (parent) package could be done, but that's a major change.
// Plus, the setup of a testEnv for regular integration tests is far more complex than what we need here. For these
// reasons, and because the (so far) tested functionality is supposed to be a temporary workaround, we decided to create
// this simplified implementation and will evaluate a possible refactoring in the future if/when needed.
type testEnv struct {
	t          *testing.T
	app        *App
	urlBuilder *urls.Builder
}

func (e testEnv) mockDB() sqlmock.Sqlmock {
	ctrl := gomock.NewController(e.t)
	mockBalancer := mocks.NewMockLoadBalancer(ctrl)

	db, mock, err := sqlmock.New()
	require.NoError(e.t, err)

	e.t.Cleanup(func() { db.Close() })

	primary := &datastore.DB{DB: db}
	replica := &datastore.DB{DB: db}

	mockBalancer.EXPECT().Primary().Return(primary).AnyTimes()
	mockBalancer.EXPECT().UpToDateReplica(gomock.Any(), gomock.Any()).Return(replica).AnyTimes()
	mockBalancer.EXPECT().TypeOf(primary).Return(datastore.HostTypePrimary).AnyTimes()
	mockBalancer.EXPECT().TypeOf(replica).Return(datastore.HostTypeReplica).AnyTimes()

	e.app.db = mockBalancer

	return mock
}

func newTestEnv(t *testing.T) *testEnv {
	dbDSN, err := testutil.NewDSNFromEnv()
	require.NoError(t, err)

	// minimum required configuration for the existing tests, plus turning off background jobs to avoid noise
	cfg := &configuration.Configuration{
		Storage: configuration.Storage{
			"testdriver": configuration.Parameters{},
			"maintenance": configuration.Parameters{
				"uploadpurging": map[any]any{"enabled": false},
			},
		},
		GC: configuration.GC{Disabled: true},
		Database: configuration.Database{
			Enabled:     true,
			Host:        dbDSN.Host,
			Port:        dbDSN.Port,
			User:        dbDSN.User,
			Password:    dbDSN.Password,
			DBName:      dbDSN.DBName,
			SSLMode:     dbDSN.SSLMode,
			SSLCert:     dbDSN.SSLCert,
			SSLKey:      dbDSN.SSLKey,
			SSLRootCert: dbDSN.SSLRootCert,
		},
	}

	ctx := dtestutil.NewContextWithLogger(t)
	app, err := NewApp(ctx, cfg)
	require.NoError(t, err)

	handler := correlation.InjectCorrelationID(app, correlation.WithPropagation())
	server := httptest.NewServer(gorilla.CombinedLoggingHandler(os.Stderr, handler))
	builder, err := urls.NewBuilderFromString(server.URL, false)
	require.NoError(t, err)

	return &testEnv{
		t:          t,
		app:        app,
		urlBuilder: builder,
	}
}

func TestGitlabAPI_GetRepository_SizeWithDescendantsTimeout(t *testing.T) {
	if os.Getenv("REGISTRY_DATABASE_ENABLED") != "true" {
		t.Skip("skipping test because the metadata database is not enabled")
	}

	env := newTestEnv(t)

	// expected fake repository attributes
	want := &models.Repository{
		ID:          1,
		NamespaceID: 1,
		Name:        "foo",
		Path:        "foo",
		ParentID:    sql.NullInt64{},
		CreatedAt:   time.Now(),
		UpdatedAt:   sql.NullTime{},
		DeletedAt:   sql.NullTime{},
	}
	wantSizePrecise := int64(12345)
	wantSizeEstimate := int64(12678)

	// get size with descendants of a base repository
	baseRepoRef, err := reference.WithName(want.Path)
	require.NoError(t, err)
	u, err := env.urlBuilder.BuildGitlabV1RepositoryURL(baseRepoRef, url.Values{
		"size": []string{"self_with_descendants"},
	})
	require.NoError(t, err)

	// fake PG query timeout error
	pgTimeoutErr := &pgconn.PgError{Code: pgerrcode.QueryCanceled}

	tests := []struct {
		name             string
		preciseQueryErr  error
		estimateQueryErr error
	}{
		{
			name:            "fallback to estimate on new precise timeout",
			preciseQueryErr: pgTimeoutErr,
		},
		{
			name: "fallback to estimate on previous precise timeout",
			// Although datastore.ErrSizeHasTimedOut is not returned by the SQL driver, we wrap the error in the
			// SizeWithDescendants method, and we use errors.Is on the handler, so this works just fine.
			preciseQueryErr: datastore.ErrSizeHasTimedOut,
		},
		{
			name: "do not fallback to estimate on precise success",
		},
		{
			name:             "return error on estimate failure",
			preciseQueryErr:  pgTimeoutErr,
			estimateQueryErr: pgTimeoutErr,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbMock := env.mockDB()

			// first query will be the search by path
			rows := sqlmock.NewRows([]string{
				"id",
				"top_level_namespace_id",
				"name",
				"path",
				"parent_id",
				"created_at",
				"updated_at",
				"last_published_at",
			}).AddRow(want.ID, want.NamespaceID, want.Name, want.Path, want.ParentID, want.CreatedAt, want.UpdatedAt, want.LastPublishedAt)

			// There are plenty `\n` and `\t` on the actual queries, and they are pretty long, so a full match is hard to digest
			// and unnecessary because there is a limited known set of queries involved. So we simplify and use a partial match.
			dbMock.ExpectQuery("SELECT(.+)FROM(.+)repositories(.+)WHERE(.+)path = ?(.+)").
				WithArgs(want.Path).WillReturnRows(rows)

			// next query is the precise size calculation (`WITH RECURSIVE cte AS` is only present on the precise size query)
			exp := dbMock.ExpectQuery(`SELECT(.+)coalesce\(sum\(q.size\), 0\)(.+)FROM(.+)WITH RECURSIVE cte AS(.+)WHERE(.+)m.top_level_namespace_id = ?`).
				WithArgs(want.NamespaceID)

			if tt.preciseQueryErr != nil {
				exp.WillReturnError(tt.preciseQueryErr)
			} else {
				exp.WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(wantSizePrecise))
			}

			// Next we must see the app fallback to the simplified estimate query (`FROM ( SELECT DISTINCT ON (digest)` is only
			// present on that query) if the main one failed.
			if tt.preciseQueryErr != nil {
				exp = dbMock.ExpectQuery(`SELECT(.+)coalesce\(sum\(q.size\), 0\)(.+)FROM \( SELECT DISTINCT ON \(digest\)(.+)WHERE(.+)top_level_namespace_id = ?`).
					WithArgs(want.NamespaceID)
				if tt.estimateQueryErr != nil {
					exp.WillReturnError(tt.estimateQueryErr)
				} else {
					exp.WillReturnRows(sqlmock.NewRows([]string{"size"}).AddRow(wantSizeEstimate))
				}
			}

			resp, err := http.Get(u)
			require.NoError(t, err)
			defer resp.Body.Close()

			// we make sure that all expectations were met, including that no additional queries were executed
			require.NoError(t, dbMock.ExpectationsWereMet())

			// validate API response
			if tt.estimateQueryErr != nil {
				require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
			} else {
				require.Equal(t, http.StatusOK, resp.StatusCode)

				got := RepositoryAPIResponse{}
				p, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				err = json.Unmarshal(p, &got)
				require.NoError(t, err)

				require.NotNil(t, got.Size)

				if tt.preciseQueryErr != nil {
					require.Equal(t, wantSizeEstimate, *got.Size)
					require.Equal(t, sizePrecisionUntagged, got.SizePrecision)
				} else {
					require.Equal(t, wantSizePrecise, *got.Size)
					require.Equal(t, sizePrecisionDefault, got.SizePrecision)
				}
			}
		})
	}
}
