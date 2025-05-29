package token

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudienceList_Unmarshal(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		expected    AudienceList
		shouldError bool
	}{
		{
			name:        "single string",
			value:       `"audience"`,
			expected:    AudienceList{"audience"},
			shouldError: false,
		},
		{
			name:        "array of strings",
			value:       `["audience1", "audience2"]`,
			expected:    AudienceList{"audience1", "audience2"},
			shouldError: false,
		},
		{
			name:        "unicode strings don't get normalized",
			value:       `["金", "金"]`,
			expected:    AudienceList{"\xef\xa4\x8a", "\xe9\x87\x91"},
			shouldError: false,
		},
		{
			name:        "duplicate elements are allowed",
			value:       `["audience1", "audience1"]`,
			expected:    AudienceList{"audience1", "audience1"},
			shouldError: false,
		},
		{
			name:        "null",
			value:       `null`,
			expected:    nil,
			shouldError: false,
		},
		{
			name:        "empty array",
			value:       `[]`,
			expected:    nil,
			shouldError: false,
		},
		{
			name:        "array with single empty string",
			value:       `[""]`,
			expected:    AudienceList{""},
			shouldError: false,
		},
		{
			name:        "malformed JSON",
			value:       `{`,
			expected:    nil,
			shouldError: true,
		},
		{
			name:        "array with mixed string and non-string values",
			value:       `["audience1", 1234]`,
			expected:    nil,
			shouldError: true,
		},
		{
			name:        "non-string number",
			value:       `1234`,
			expected:    nil,
			shouldError: true,
		},
		{
			name:        "array with too many elements",
			value:       `["aud1", "aud2", "aud3", "aud4", "aud5", "aud6", "aud7", "aud8", "aud9", "aud10", "aud11"]`,
			expected:    nil,
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual AudienceList
			err := json.Unmarshal([]byte(tt.value), &actual)
			if tt.shouldError {
				require.Zero(t, actual)
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestAudienceList_Marshal(t *testing.T) {
	tests := []struct {
		name     string
		value    AudienceList
		expected []byte
	}{
		{
			name:     "single element array",
			value:    AudienceList{"audience"},
			expected: []byte(`["audience"]`),
		},
		{
			name:     "multiple element array",
			value:    AudienceList{"audience1", "audience2"},
			expected: []byte(`["audience1","audience2"]`),
		},
		{
			name:     "array with string numbers",
			value:    AudienceList{"1234", "5678"},
			expected: []byte(`["1234","5678"]`),
		},
		{
			name:     "empty array",
			value:    AudienceList{},
			expected: []byte(`[]`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := json.Marshal(tt.value)

			require.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
