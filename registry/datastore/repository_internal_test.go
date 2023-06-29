package datastore

import (
	"testing"

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
