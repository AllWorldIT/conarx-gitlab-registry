package notifications

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTranslateBackoffParams(t *testing.T) {
	tests := []struct {
		name            string
		threshold       int
		backoffTime     time.Duration
		expectedRetries int
	}{
		{
			name:            "Zero threshold",
			threshold:       0,
			backoffTime:     1 * time.Second,
			expectedRetries: 10,
		},
		{
			name:            "1s backoff with low threshold",
			threshold:       3,
			backoffTime:     1 * time.Second,
			expectedRetries: 10,
		},
		{
			name:            "1s backoff with high threshold",
			threshold:       15,
			backoffTime:     1 * time.Second,
			expectedRetries: 15,
		},
		{
			name:            "Very small backoff",
			threshold:       5,
			backoffTime:     10 * time.Millisecond,
			expectedRetries: 10,
		},
		{
			name:            "100ms backoff",
			threshold:       3,
			backoffTime:     100 * time.Millisecond,
			expectedRetries: 10,
		},
		{
			name:            "500ms backoff",
			threshold:       5,
			backoffTime:     500 * time.Millisecond,
			expectedRetries: 10,
		},
		{
			name:            "2s backoff",
			threshold:       5,
			backoffTime:     2 * time.Second,
			expectedRetries: 10,
		},
		{
			name:            "3s backoff",
			threshold:       5,
			backoffTime:     3 * time.Second,
			expectedRetries: 11,
		},
		{
			name:            "10s backoff",
			threshold:       2,
			backoffTime:     10 * time.Second,
			expectedRetries: 17,
		},
		{
			name:            "30s backoff - hits MaxElapsedTime cap",
			threshold:       1,
			backoffTime:     30 * time.Second,
			expectedRetries: 15,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(tt *testing.T) {
			gotRetries := translateBackoffParams(tc.threshold, tc.backoffTime)

			require.Equal(tt, tc.expectedRetries, gotRetries)
			require.GreaterOrEqual(tt, gotRetries, tc.threshold)
		})
	}
}
