package migrations

import (
	"testing"

	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/require"
)

func TestValidatePostDeployMigrationOrder(t *testing.T) {
	tests := []struct {
		name             string
		sortedMigrations []*migrate.Migration
		pendingPDSet     map[string]struct{}
		expectedFailure  *PostDeployOrderFailure
		expectedValid    bool
	}{
		{
			name: "All migrations in correct order",
			sortedMigrations: []*migrate.Migration{
				{Id: "001"}, {Id: "002"}, {Id: "003"}, {Id: "PDM-004"},
			},
			pendingPDSet:    map[string]struct{}{"PDM-004": {}},
			expectedFailure: nil,
			expectedValid:   true,
		},
		{
			name: "Post-deployment migration appears before a non-post-deployment migration",
			sortedMigrations: []*migrate.Migration{
				{Id: "001"}, {Id: "PDM-002"}, {Id: "003"},
			},
			pendingPDSet: map[string]struct{}{"PDM-002": {}},
			expectedFailure: &PostDeployOrderFailure{
				MigrationID:        "PDM-002",
				SafeToMigrateLimit: 1,
			},
			expectedValid: false,
		},
		{
			name:             "No post-deployment migrations",
			sortedMigrations: []*migrate.Migration{{Id: "001"}, {Id: "002"}, {Id: "003"}},
			pendingPDSet:     make(map[string]struct{}),
			expectedFailure:  nil,
			expectedValid:    true,
		},
		{
			name: "Only post-deployment migrations",
			sortedMigrations: []*migrate.Migration{
				{Id: "PDM-001"}, {Id: "PDM-002"}, {Id: "PDM-003"},
			},
			pendingPDSet:    map[string]struct{}{"PDM-001": {}, "PDM-002": {}, "PDM-003": {}},
			expectedFailure: nil,
			expectedValid:   true,
		},
		{
			name: "Mixed migrations but in correct order",
			sortedMigrations: []*migrate.Migration{
				{Id: "001"}, {Id: "002"}, {Id: "PDM-003"}, {Id: "PDM-004"},
			},
			pendingPDSet:    map[string]struct{}{"PDM-003": {}, "PDM-004": {}},
			expectedFailure: nil,
			expectedValid:   true,
		},
		{
			name: "Multiple post-deployment migrations with a non-post-deployment migration in between (violation)",
			sortedMigrations: []*migrate.Migration{
				{Id: "001"}, {Id: "002"}, {Id: "PDM-002"}, {Id: "003"}, {Id: "PDM-004"},
			},
			pendingPDSet: map[string]struct{}{"PDM-002": {}, "PDM-004": {}},
			expectedFailure: &PostDeployOrderFailure{
				MigrationID:        "PDM-002",
				SafeToMigrateLimit: 2,
			},
			expectedValid: false,
		},
		{
			name:             "Edge case: Single migration (NPD)",
			sortedMigrations: []*migrate.Migration{{Id: "001"}},
			pendingPDSet:     make(map[string]struct{}),
			expectedFailure:  nil,
			expectedValid:    true,
		},
		{
			name:             "Edge case: Single migration (PDM)",
			sortedMigrations: []*migrate.Migration{{Id: "PDM-001"}},
			pendingPDSet:     map[string]struct{}{"PDM-001": {}},
			expectedFailure:  nil,
			expectedValid:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			failure, valid := validatePostDeployMigrationOrder(tc.sortedMigrations, tc.pendingPDSet)
			require.Equal(t, tc.expectedValid, valid)
			if !valid {
				require.Equal(t, tc.expectedFailure.MigrationID, failure.MigrationID)
				require.Equal(t, tc.expectedFailure.SafeToMigrateLimit, failure.SafeToMigrateLimit)
			}
		})
	}
}

func TestClassifyPendingMigrations(t *testing.T) {
	tests := []struct {
		name           string
		migrationLimit int
		appliedRecords []*migrate.MigrationRecord
		allMigrations  []*Migration
		expectedPDSet  map[string]struct{}
		expectedSorted []string
	}{
		{
			name:           "All migrations pending, no limit",
			migrationLimit: 0,
			appliedRecords: make([]*migrate.MigrationRecord, 0),
			allMigrations: []*Migration{
				{
					Migration:      &migrate.Migration{Id: "001"},
					PostDeployment: false,
				},
				{
					Migration:      &migrate.Migration{Id: "PDM-002"},
					PostDeployment: true,
				},
				{
					Migration:      &migrate.Migration{Id: "003"},
					PostDeployment: false,
				},
			},
			expectedPDSet:  map[string]struct{}{"PDM-002": {}},
			expectedSorted: []string{"001", "003", "PDM-002"},
		},
		{
			name:           "Some migrations already applied",
			migrationLimit: 0,
			appliedRecords: []*migrate.MigrationRecord{
				{Id: "001"},
			},
			allMigrations: []*Migration{
				{
					Migration:      &migrate.Migration{Id: "001"},
					PostDeployment: false,
				},
				{
					Migration:      &migrate.Migration{Id: "PDM-002"},
					PostDeployment: true,
				},
				{
					Migration:      &migrate.Migration{Id: "003"},
					PostDeployment: false,
				},
			},
			expectedPDSet:  map[string]struct{}{"PDM-002": {}},
			expectedSorted: []string{"003", "PDM-002"},
		},
		{
			name:           "Migration limit reached",
			migrationLimit: 1,
			appliedRecords: make([]*migrate.MigrationRecord, 0),
			allMigrations: []*Migration{
				{
					Migration:      &migrate.Migration{Id: "001"},
					PostDeployment: false,
				},
				{
					Migration:      &migrate.Migration{Id: "002"},
					PostDeployment: false,
				},
				{
					Migration:      &migrate.Migration{Id: "003"},
					PostDeployment: false,
				},
			},
			expectedPDSet:  make(map[string]struct{}),
			expectedSorted: []string{"001"}, // stops after first non-PDM due to limit
		},
		{
			name:           "Migration limit reached PDM",
			migrationLimit: 1,
			appliedRecords: make([]*migrate.MigrationRecord, 0),
			allMigrations: []*Migration{
				{
					Migration:      &migrate.Migration{Id: "001"},
					PostDeployment: true,
				},
				{
					Migration:      &migrate.Migration{Id: "002"},
					PostDeployment: false,
				},
				{
					Migration:      &migrate.Migration{Id: "003"},
					PostDeployment: false,
				},
			},
			expectedPDSet:  map[string]struct{}{"001": {}},
			expectedSorted: []string{"001", "002"}, // stops after first non-PDM due to limit
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sorted, pdSet := classifyPendingMigrations(tc.migrationLimit, tc.appliedRecords, tc.allMigrations)

			require.Equal(t, tc.expectedPDSet, pdSet)

			// extract sorted migration IDs
			var sortedIDs []string
			for _, m := range sorted {
				sortedIDs = append(sortedIDs, m.Id)
			}

			require.Equal(t, tc.expectedSorted, sortedIDs)
		})
	}
}
