package datastore

import (
	"fmt"
	"testing"

	"github.com/docker/distribution/registry/datastore/models"
	"github.com/stretchr/testify/require"
)

func Test_sqlPartialMatch(t *testing.T) {
	tests := []struct {
		name string
		arg  string
		want string
	}{
		{
			name: "empty string",
			want: "%%",
		},
		{
			name: "no metacharacters",
			arg:  "foo",
			want: "%foo%",
		},
		{
			name: "percentage wildcard",
			arg:  "a%b%c",
			want: `%a\%b\%c%`,
		},
		{
			name: "underscore wildcard",
			arg:  "a_b_c",
			want: `%a\_b\_c%`,
		},
		{
			name: "other special characters",
			arg:  "a-b.c",
			want: `%a-b.c%`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sqlPartialMatch(tt.arg); got != tt.want {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func Test_tagsDetailPaginatedQuery(t *testing.T) {
	r := &models.Repository{ID: 123, NamespaceID: 456}
	baseArgs := []any{r.NamespaceID, r.ID, sqlPartialMatch("")}

	baseQuery := `SELECT
			t.name,
			encode(m.digest, 'hex') AS digest,
			encode(m.configuration_blob_digest, 'hex') AS config_digest,
			mt.media_type,
			m.total_size,
			t.created_at,
			t.updated_at,
			GREATEST(t.created_at, t.updated_at) as published_at
		FROM
			tags AS t
			JOIN manifests AS m ON m.top_level_namespace_id = t.top_level_namespace_id
				AND m.repository_id = t.repository_id
				AND m.id = t.manifest_id
			JOIN media_types AS mt ON mt.id = m.media_type_id
		WHERE
			t.top_level_namespace_id = $1
			AND t.repository_id = $2
		  	AND t.name LIKE $3`

	tcs := map[string]struct {
		filters       FilterParams
		expectedQuery string
		expectedArgs  []any
	}{
		"no filters": {
			filters: FilterParams{MaxEntries: 5},
			expectedQuery: baseQuery + `
			ORDER BY name asc LIMIT $4`,
			expectedArgs: append(baseArgs, 5),
		},
		"no filters order by published_at": {
			filters: FilterParams{MaxEntries: 5, OrderBy: "published_at"},
			expectedQuery: baseQuery + `
			ORDER BY published_at asc, name asc LIMIT $4`,
			expectedArgs: append(baseArgs, 5),
		},
		"last entry asc": {
			filters: FilterParams{MaxEntries: 5, LastEntry: "abc"},
			expectedQuery: baseQuery + `
			AND t.name > $4
		ORDER BY
			name asc
		LIMIT $5`,
			expectedArgs: append(baseArgs, "abc", 5),
		},
		"last entry desc": {
			filters: FilterParams{MaxEntries: 5, LastEntry: "abc", SortOrder: OrderDesc},
			expectedQuery: baseQuery + `
			AND t.name < $4
		ORDER BY
			name desc
		LIMIT $5`,
			expectedArgs: append(baseArgs, "abc", 5),
		},
		"last entry order by published_at asc": {
			filters: FilterParams{MaxEntries: 5, LastEntry: "abc", PublishedAt: "TIMESTAMP"},
			expectedQuery: baseQuery + `
			AND (GREATEST(t.created_at, t.updated_at), t.name) > ($4, $5)
		ORDER BY
			published_at asc,
			t.name asc
		LIMIT $6`,
			expectedArgs: append(baseArgs, "TIMESTAMP", "abc", 5),
		},
		"last entry order by published_at desc": {
			filters: FilterParams{MaxEntries: 5, LastEntry: "abc", PublishedAt: "TIMESTAMP", SortOrder: OrderDesc},
			expectedQuery: baseQuery + `
			AND (GREATEST(t.created_at, t.updated_at), t.name) < ($4, $5)
		ORDER BY
			published_at desc,
			t.name desc
		LIMIT $6`,
			expectedArgs: append(baseArgs, "TIMESTAMP", "abc", 5),
		},
		"before entry asc": {
			filters: FilterParams{MaxEntries: 5, BeforeEntry: "abc"},
			expectedQuery: func() string {
				q := baseQuery + `
			AND t.name < $4
		ORDER BY
			name desc
		LIMIT $5`

				return fmt.Sprintf(`SElECT * FROM (%s) AS tags ORDER BY tags.name ASC`, q)
			}(),
			expectedArgs: append(baseArgs, "abc", 5),
		},
		"before entry desc": {
			filters: FilterParams{MaxEntries: 5, BeforeEntry: "abc", SortOrder: OrderDesc},
			expectedQuery: func() string {
				q := baseQuery + `
			AND t.name > $4
		ORDER BY
			name asc
		LIMIT $5`

				return fmt.Sprintf(`SElECT * FROM (%s) AS tags ORDER BY tags.name DESC`, q)
			}(),
			expectedArgs: append(baseArgs, "abc", 5),
		},
		"before entry order by published_at asc": {
			filters: FilterParams{MaxEntries: 5, BeforeEntry: "abc", PublishedAt: "TIMESTAMP"},
			expectedQuery: func() string {
				q := baseQuery + `
			AND (GREATEST(t.created_at, t.updated_at), t.name) < ($4, $5)
		ORDER BY
			published_at desc,
			t.name desc
		LIMIT $6`

				return fmt.Sprintf(`SElECT * FROM (%s) AS tags ORDER BY tags.name ASC`, q)
			}(),
			expectedArgs: append(baseArgs, "TIMESTAMP", "abc", 5),
		},
		"before entry order by published_at desc": {
			filters: FilterParams{MaxEntries: 5, BeforeEntry: "abc", PublishedAt: "TIMESTAMP", SortOrder: OrderDesc},
			expectedQuery: func() string {
				q := baseQuery + `
			AND (GREATEST(t.created_at, t.updated_at), t.name) > ($4, $5)
		ORDER BY
			published_at asc,
			t.name asc
		LIMIT $6`

				return fmt.Sprintf(`SElECT * FROM (%s) AS tags ORDER BY tags.name DESC`, q)
			}(),
			expectedArgs: append(baseArgs, "TIMESTAMP", "abc", 5),
		},
		"publised_at asc": {
			filters: FilterParams{MaxEntries: 5, PublishedAt: "TIMESTAMP"},
			expectedQuery: baseQuery + `
			AND GREATEST(t.created_at,t.updated_at) >= $4
		ORDER BY
			published_at asc,
			t.name asc
		LIMIT $5`,
			expectedArgs: append(baseArgs, "TIMESTAMP", 5),
		},
		"publised_at desc": {
			filters: FilterParams{MaxEntries: 5, PublishedAt: "TIMESTAMP", SortOrder: OrderDesc},
			expectedQuery: func() string {
				q := baseQuery + `
			AND GREATEST(t.created_at,t.updated_at) <= $4
		ORDER BY
			published_at asc,
			t.name asc
		LIMIT $5`

				return fmt.Sprintf(`SELECT * FROM (%s) AS tags ORDER BY tags.name DESC`, q)
			}(),
			expectedArgs: append(baseArgs, "TIMESTAMP", 5),
		},
	}

	for tn, tc := range tcs {
		t.Run(tn, func(t *testing.T) {
			q, args := tagsDetailPaginatedQuery(r, tc.filters)
			require.Equal(t, tc.expectedQuery, q)
			require.ElementsMatch(t, tc.expectedArgs, args)
		})
	}
}
