package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestContainsAny(t *testing.T) {
	tests := []struct {
		name     string
		ss       []string
		q        []string
		expected bool
	}{
		{
			name:     "empty q, empty ss",
			ss:       make([]string, 0),
			q:        make([]string, 0),
			expected: false,
		},
		{
			name:     "empty q, non-empty ss",
			ss:       []string{"a", "b", "c"},
			q:        make([]string, 0),
			expected: false,
		},
		{
			name:     "q equals ss",
			ss:       []string{"a"},
			q:        []string{"a"},
			expected: true,
		},
		{
			name:     "q and ss do not overlap",
			ss:       []string{"a", "b", "c"},
			q:        []string{"d", "e", "f"},
			expected: false,
		},
		{
			name:     "q and ss overlap",
			ss:       []string{"a", "b"},
			q:        []string{"a", "c"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := containsAny(tt.ss, tt.q)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
