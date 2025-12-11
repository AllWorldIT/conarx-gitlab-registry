package token

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithEgressMetadata(t *testing.T) {
	testNamespaceID := int64(12345)
	testProjectID := int64(67890)

	testCases := []struct {
		name                string
		accesses            []*ResourceActions
		expectedNamespaceID int64
		expectedProjectID   int64
	}{
		{
			name: "pull action",
			accesses: []*ResourceActions{
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID,
						ProjectID:   testProjectID,
					},
					Actions: []string{"pull"},
				},
			},
			expectedNamespaceID: testNamespaceID,
			expectedProjectID:   testProjectID,
		},
		{
			name: "multiple actions",
			accesses: []*ResourceActions{
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID,
						ProjectID:   testProjectID,
					},
					Actions: []string{"pull", "push"},
				},
			},
			expectedNamespaceID: testNamespaceID,
			expectedProjectID:   testProjectID,
		},
		{
			name: "no pull action",
			accesses: []*ResourceActions{
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID,
						ProjectID:   testProjectID,
					},
					Actions: []string{"delete", "push"},
				},
			},
		},
		{
			name: "multiple accesses with pull action",
			accesses: []*ResourceActions{
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID,
						ProjectID:   testProjectID,
					},
					Actions: []string{"pull", "push"},
				},
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID + 1,
						ProjectID:   testProjectID + 1,
					},
					Actions: []string{"pull"},
				},
			},
			expectedNamespaceID: testNamespaceID + 1,
			expectedProjectID:   testProjectID + 1,
		},
		{
			name: "multiple accesses without pull action",
			accesses: []*ResourceActions{
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID,
						ProjectID:   testProjectID,
					},
					Actions: []string{"delete", "push"},
				},
				{
					Meta: &Meta{
						NamespaceID: testNamespaceID + 1,
						ProjectID:   testProjectID + 1,
					},
					Actions: []string{"delete"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			// Create a new context with egress metadata
			ctx := context.Background()
			egressCtx := WithEgressMetadata(ctx, tc.accesses)

			// Assert that the namespace and project IDs are(n't) correctly set and retrieved from the context
			nid, ok := egressCtx.Value(EgressNamespaceIdKey).(int64)
			require.True(tt, ok)
			require.Equal(tt, tc.expectedNamespaceID, nid)
			pid, ok := egressCtx.Value(EgressProjectIdKey).(int64)
			require.True(tt, ok)
			require.Equal(tt, tc.expectedProjectID, pid)

			// Test fallback to parent context for an unknown key
			fallbackKey := "unknown.key"
			fallbackValue := "fallback"
			// nolint: revive // context-keys-type
			parentCtx := context.WithValue(ctx, fallbackKey, fallbackValue)
			egressCtxWithFallback := WithEgressMetadata(parentCtx, tc.accesses)
			val, ok := egressCtxWithFallback.Value(fallbackKey).(string)
			require.True(tt, ok)
			require.Equal(tt, fallbackValue, val)
		})
	}
}
