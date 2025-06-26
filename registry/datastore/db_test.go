package datastore_test

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/docker/distribution/registry/datastore"
	"github.com/stretchr/testify/require"
)

func TestDSN_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arg  datastore.DSN
		out  string
	}{
		{name: "empty", arg: datastore.DSN{}, out: ""},
		{
			name: "full",
			arg: datastore.DSN{
				Host:           "127.0.0.1",
				Port:           5432,
				User:           "registry",
				Password:       "secret",
				DBName:         "registry_production",
				SSLMode:        "require",
				SSLCert:        "/path/to/client.crt",
				SSLKey:         "/path/to/client.key",
				SSLRootCert:    "/path/to/root.crt",
				ConnectTimeout: 5 * time.Second,
			},
			out: "host=127.0.0.1 port=5432 user=registry password=secret dbname=registry_production sslmode=require sslcert=/path/to/client.crt sslkey=/path/to/client.key sslrootcert=/path/to/root.crt connect_timeout=5",
		},
		{
			name: "with zero port",
			arg: datastore.DSN{
				Port: 0,
			},
			out: "",
		},
		{
			name: "with spaces",
			arg: datastore.DSN{
				Password: "jw8s 0F4",
			},
			out: `password=jw8s\ 0F4`,
		},
		{
			name: "with quotes",
			arg: datastore.DSN{
				Password: "jw8s'0F4",
			},
			out: `password=jw8s\'0F4`,
		},
		{
			name: "with other special characters",
			arg: datastore.DSN{
				Password: "jw8s%^@0F4",
			},
			out: "password=jw8s%^@0F4",
		},
		{
			name: "with zero connection timeout",
			arg: datastore.DSN{
				ConnectTimeout: 0,
			},
			out: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, tt.arg.String())
		})
	}
}

func TestDSN_Address(t *testing.T) {
	tests := []struct {
		name string
		arg  datastore.DSN
		out  string
	}{
		{name: "empty", arg: datastore.DSN{}, out: ":0"},
		{name: "no port", arg: datastore.DSN{Host: "127.0.0.1"}, out: "127.0.0.1:0"},
		{name: "no host", arg: datastore.DSN{Port: 5432}, out: ":5432"},
		{name: "full", arg: datastore.DSN{Host: "127.0.0.1", Port: 5432}, out: "127.0.0.1:5432"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, tt.arg.Address())
		})
	}
}

// expectSingleRowQuery asserts that a query on the mock database returns a single row with the specified value for the
// given column, or returns an error if specified. It takes the mock database, the query string, the response row column
// name, the expected response row column value, an error (if any), and the query arguments as input.
func expectSingleRowQuery(db sqlmock.Sqlmock, query, column string, value driver.Value, err error, args ...driver.Value) {
	if err != nil {
		db.ExpectQuery(query).
			WithArgs(args...).
			WillReturnError(err)
	} else {
		db.ExpectQuery(query).
			WithArgs(args...).
			WillReturnRows(sqlmock.NewRows([]string{column}).AddRow(value))
	}
}

func TestDB_Address(t *testing.T) {
	tests := []struct {
		name string
		arg  datastore.DB
		out  string
	}{
		{name: "nil DSN", arg: datastore.DB{}, out: ""},
		{name: "empty DSN", arg: datastore.DB{DSN: &datastore.DSN{}}, out: ":0"},
		{name: "DSN with no port", arg: datastore.DB{DSN: &datastore.DSN{Host: "127.0.0.1"}}, out: "127.0.0.1:0"},
		{name: "DSN with no host", arg: datastore.DB{DSN: &datastore.DSN{Port: 5432}}, out: ":5432"},
		{name: "full DSN", arg: datastore.DB{DSN: &datastore.DSN{Host: "127.0.0.1", Port: 5432}}, out: "127.0.0.1:5432"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.out, tt.arg.Address())
		})
	}
}

func TestQueryBuilder_Build(t *testing.T) {
	testCases := []struct {
		name           string
		query          string
		args           []any
		expectedSQL    string
		expectedParams []any
		expectError    bool
	}{
		{
			name:           "empty query",
			query:          "",
			args:           make([]any, 0),
			expectedSQL:    "",
			expectedParams: make([]any, 0),
		},
		{
			name:           "single placeholder",
			query:          "SELECT * FROM users WHERE id = ?",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users WHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:           "leading and trailing space is removed",
			query:          " SELECT * FROM users WHERE id = ? ",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users WHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:           "multiple placeholders",
			query:          "SELECT * FROM users WHERE id = ? AND name = ?",
			args:           []any{1, "John Doe"},
			expectedSQL:    "SELECT * FROM users WHERE id = $1 AND name = $2",
			expectedParams: []any{1, "John Doe"},
		},
		{
			name:           "placeholders with spaces",
			query:          "SELECT * FROM users WHERE id = ? AND name = ?",
			args:           []any{1, "John Doe"},
			expectedSQL:    "SELECT * FROM users WHERE id = $1 AND name = $2",
			expectedParams: []any{1, "John Doe"},
		},
		{
			name:           "query with newline",
			query:          "SELECT * FROM users WHERE id = ?\n",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users WHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:           "query without arguments",
			query:          "SELECT * FROM users WHERE id = 10",
			args:           make([]any, 0),
			expectedSQL:    "SELECT * FROM users WHERE id = 10",
			expectedParams: make([]any, 0),
		},
		{
			name:           "query with multiple newlines",
			query:          "SELECT * FROM users\nWHERE id = ?\n",
			args:           []any{1},
			expectedSQL:    "SELECT * FROM users\nWHERE id = $1",
			expectedParams: []any{1},
		},
		{
			name:        "errors on mismatched placeholder count",
			query:       "SELECT * FROM users WHERE id = ? AND name = ?",
			args:        []any{1},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qb := datastore.NewQueryBuilder()

			err := qb.Build(tc.query, tc.args...)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.Equal(t, tc.expectedSQL, qb.SQL())
				require.Equal(t, tc.expectedParams, qb.Params())
			}
		})
	}
}

func TestQueryBuilder_MultipleBuildCalls(t *testing.T) {
	t.Parallel()
	qb := datastore.NewQueryBuilder()
	qb.Build("SELECT * FROM users WHERE id = ?", 1)
	qb.Build("AND name = ?", "John Doe")
	require.Equal(t, "SELECT * FROM users WHERE id = $1 AND name = $2", qb.SQL())
	require.Equal(t, []any{1, "John Doe"}, qb.Params())
}

func TestQueryBuilder_WrapIntoSubqueryOf(t *testing.T) {
	testCases := []struct {
		name           string
		query          string
		args           []any
		wrapQuery      string
		expectedSQL    string
		expectedParams []any
		expectError    bool
	}{
		{
			name:           "basic subquery",
			query:          "SELECT * FROM users WHERE id = ?",
			args:           []any{1},
			wrapQuery:      "SELECT * FROM orders WHERE user_id IN (%s)",
			expectedSQL:    "SELECT * FROM orders WHERE user_id IN (SELECT * FROM users WHERE id = $1)",
			expectedParams: []any{1},
		},
		{
			name:           "subquery with multiple placeholders",
			query:          "SELECT * FROM users WHERE id = ? AND name = ?",
			args:           []any{1, "John Doe"},
			wrapQuery:      "SELECT * FROM orders WHERE user_id IN (%s)",
			expectedSQL:    "SELECT * FROM orders WHERE user_id IN (SELECT * FROM users WHERE id = $1 AND name = $2)",
			expectedParams: []any{1, "John Doe"},
		},
		{
			name:        "subquery without placeholder",
			query:       "SELECT * FROM users WHERE id = ? AND name = ?",
			args:        []any{1, "John Doe"},
			wrapQuery:   "SELECT * FROM orders WHERE user_id IN ?",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			qb := datastore.NewQueryBuilder()

			err := qb.Build(tc.query, tc.args...)
			require.NoError(t, err)

			err = qb.WrapIntoSubqueryOf(tc.wrapQuery)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.Equal(t, tc.expectedSQL, qb.SQL())
				require.Equal(t, tc.expectedParams, qb.Params())
			}
		})
	}
}
