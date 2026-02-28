package bbm

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/stretchr/testify/require"
)

func TestShouldSubBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		job          *models.BackgroundMigrationJob
		subBatchSize int
		expected     bool
	}{
		{
			name:         "disabled when sub-batch size is zero",
			job:          &models.BackgroundMigrationJob{StartID: 1, EndID: 10, BatchSize: 20},
			subBatchSize: 0,
			expected:     false,
		},
		{
			name:         "disabled for nil job",
			job:          nil,
			subBatchSize: 5,
			expected:     false,
		},
		{
			name:         "disabled when start equals end",
			job:          &models.BackgroundMigrationJob{StartID: 10, EndID: 10, BatchSize: 20},
			subBatchSize: 5,
			expected:     false,
		},
		{
			name: "disabled for null batching strategy",
			job: &models.BackgroundMigrationJob{
				StartID:          1,
				EndID:            100,
				BatchSize:        20,
				BatchingStrategy: models.NullBatchingBBMStrategy,
			},
			subBatchSize: 5,
			expected:     false,
		},
		{
			name:         "disabled when sub-batch size is not smaller than batch size",
			job:          &models.BackgroundMigrationJob{StartID: 1, EndID: 100, BatchSize: 20},
			subBatchSize: 20,
			expected:     false,
		},
		{
			name:         "enabled when sub-batch size is smaller than batch size",
			job:          &models.BackgroundMigrationJob{StartID: 1, EndID: 100, BatchSize: 20},
			subBatchSize: 5,
			expected:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.expected, shouldSubBatch(tc.job, tc.subBatchSize))
		})
	}
}

func TestExecuteWorkInSubBatches_NoSubBatching(t *testing.T) {
	t.Parallel()

	job := &models.BackgroundMigrationJob{
		BBMID:            1,
		JobName:          "test-job",
		PaginationTable:  "repositories",
		PaginationColumn: "id",
		StartID:          1,
		EndID:            50,
		BatchSize:        50,
	}

	var calls int
	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, table, column string, start, end, batchSize int) error {
			calls++
			require.Equal(t, job.PaginationTable, table)
			require.Equal(t, job.PaginationColumn, column)
			require.Equal(t, job.StartID, start)
			require.Equal(t, job.EndID, end)
			require.Equal(t, job.BatchSize, batchSize)
			return nil
		},
	}

	findEnd := func(_ context.Context, _, _ string, _, _, _ int) (int, error) {
		t.Fatal("findEnd should not be called when sub-batching is disabled")
		return 0, nil
	}

	err := executeWorkInSubBatches(context.Background(), nil, log.GetLogger(), work, job, 0, findEnd)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestExecuteWorkInSubBatches_NoSubBatching_Error(t *testing.T) {
	t.Parallel()

	job := &models.BackgroundMigrationJob{
		JobName:          "test-job",
		PaginationTable:  "repositories",
		PaginationColumn: "id",
		StartID:          1,
		EndID:            50,
		BatchSize:        50,
	}
	expectedErr := errors.New("work failed")
	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
			return expectedErr
		},
	}

	err := executeWorkInSubBatches(context.Background(), nil, log.GetLogger(), work, job, 0, nil)
	require.ErrorIs(t, err, expectedErr)
	require.NotContains(t, err.Error(), "sub_batch=1")
}

func TestExecuteWorkInSubBatches_SubBatchingHappyPath(t *testing.T) {
	t.Parallel()

	job := &models.BackgroundMigrationJob{
		BBMID:            99,
		JobName:          "test-job",
		PaginationTable:  "repositories",
		PaginationColumn: "id",
		StartID:          1,
		EndID:            25,
		BatchSize:        50,
	}
	subBatchSize := 10

	type call struct {
		start int
		end   int
		size  int
	}
	var calls []call
	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, _, _ string, start, end, batchSize int) error {
			calls = append(calls, call{start: start, end: end, size: batchSize})
			return nil
		},
	}

	findEnd := func(_ context.Context, _, _ string, start, last, size int) (int, error) {
		end := start + size - 1
		if end > last {
			end = last
		}
		return end, nil
	}

	err := executeWorkInSubBatches(context.Background(), nil, log.GetLogger(), work, job, subBatchSize, findEnd)
	require.NoError(t, err)
	require.Equal(t, []call{
		{start: 1, end: 10, size: 10},
		{start: 11, end: 20, size: 10},
		{start: 21, end: 25, size: 10},
	}, calls)
}

func TestExecuteWorkInSubBatches_FindEndError(t *testing.T) {
	t.Parallel()

	job := &models.BackgroundMigrationJob{
		JobName:   "test-job",
		StartID:   1,
		EndID:     25,
		BatchSize: 50,
	}

	expectedErr := errors.New("boom")
	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
			t.Fatal("work should not run when finding sub-batch end fails")
			return nil
		},
	}
	findEnd := func(_ context.Context, _, _ string, _, _, _ int) (int, error) {
		return 0, expectedErr
	}

	err := executeWorkInSubBatches(context.Background(), nil, log.GetLogger(), work, job, 10, findEnd)
	require.Error(t, err)
	require.ErrorContains(t, err, "sub_batch=1")
	require.ErrorContains(t, err, "finding sub-batch end")
	require.ErrorIs(t, err, expectedErr)
}

func TestExecuteWorkInSubBatches_EndBeforeStart(t *testing.T) {
	t.Parallel()

	job := &models.BackgroundMigrationJob{
		JobName:   "test-job",
		StartID:   11,
		EndID:     20,
		BatchSize: 50,
	}

	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
			t.Fatal("work should not run when end < start")
			return nil
		},
	}
	findEnd := func(_ context.Context, _, _ string, _, _, _ int) (int, error) {
		return 10, nil
	}

	err := executeWorkInSubBatches(context.Background(), nil, log.GetLogger(), work, job, 10, findEnd)
	require.ErrorIs(t, err, ErrJobEndpointNotFound)
	require.ErrorContains(t, err, "sub_batch=1")
}

func TestExecuteWorkInSubBatches_WorkErrorStopsProcessing(t *testing.T) {
	t.Parallel()

	job := &models.BackgroundMigrationJob{
		JobName:   "test-job",
		StartID:   1,
		EndID:     25,
		BatchSize: 50,
	}

	expectedErr := errors.New("work failed")
	var calls int
	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
			calls++
			if calls == 2 {
				return expectedErr
			}
			return nil
		},
	}
	findEnd := func(_ context.Context, _, _ string, start, last, size int) (int, error) {
		end := start + size - 1
		if end > last {
			end = last
		}
		return end, nil
	}

	err := executeWorkInSubBatches(context.Background(), nil, log.GetLogger(), work, job, 10, findEnd)
	require.ErrorIs(t, err, expectedErr)
	require.ErrorContains(t, err, "sub_batch=2")
	require.Equal(t, 2, calls)
}

func TestExecuteWorkInSubBatches_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	job := &models.BackgroundMigrationJob{
		JobName:   "test-job",
		StartID:   1,
		EndID:     25,
		BatchSize: 50,
	}

	work := Work{
		Name: job.JobName,
		Do: func(_ context.Context, _ datastore.Handler, _, _ string, _, _, _ int) error {
			t.Fatal("work should not run when context is canceled")
			return nil
		},
	}
	findEnd := func(_ context.Context, _, _ string, _, _, _ int) (int, error) {
		t.Fatal("findEnd should not run when context is canceled")
		return 0, nil
	}

	err := executeWorkInSubBatches(ctx, nil, log.GetLogger(), work, job, 10, findEnd)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "sub_batch=1")
}
