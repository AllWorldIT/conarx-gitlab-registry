package bbm

import (
	"context"
	"fmt"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/bbm/metrics"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
)

type jobEndFinder func(ctx context.Context, table, column string, start, last, batchSize int) (int, error)

// executeWorkInSubBatches executes the work in sub-batches if the job is configured to do so.
// if sub-batching is not enabled, the work is executed in a single batch.
func executeWorkInSubBatches(
	ctx context.Context,
	db datastore.Handler,
	logger log.Logger,
	work Work,
	job *models.BackgroundMigrationJob,
	subBatchSize int,
	findEnd jobEndFinder,
) error {
	if !shouldSubBatch(job, subBatchSize) {
		return work.Do(ctx, db, job.PaginationTable, job.PaginationColumn, job.StartID, job.EndID, job.BatchSize)
	}

	start := job.StartID
	var subBatchCount int
	for start <= job.EndID {
		subBatchCount++
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sub_batch=%d: %w", subBatchCount, err)
		}
		end, err := findEnd(ctx, job.PaginationTable, job.PaginationColumn, start, job.EndID, subBatchSize)
		if err != nil {
			return fmt.Errorf("sub_batch=%d finding sub-batch end: %w", subBatchCount, err)
		}
		if end < start {
			return fmt.Errorf("sub_batch=%d: %w", subBatchCount, ErrJobEndpointNotFound)
		}

		logger.WithFields(log.Fields{
			"sub_batch_count": subBatchCount,
			"sub_batch_start": start,
			"sub_batch_end":   end,
			"sub_batch_size":  subBatchSize,
		}).Info("sub-batch execution started")

		report := metrics.SubBatch(job.JobName, fmt.Sprint(job.BBMID))
		err = work.Do(ctx, db, job.PaginationTable, job.PaginationColumn, start, end, subBatchSize)
		report()
		if err != nil {
			return fmt.Errorf("sub_batch=%d executing sub-batch: %w", subBatchCount, err)
		}
		if end == job.EndID {
			break
		}

		start = end + 1
	}

	return nil
}

func shouldSubBatch(job *models.BackgroundMigrationJob, subBatchSize int) bool {
	if subBatchSize <= 0 {
		return false
	}
	if job == nil || job.StartID >= job.EndID {
		return false
	}
	// Null strategy jobs do not carry deterministic start/end markers.
	if job.BatchingStrategy == models.NullBatchingBBMStrategy {
		return false
	}

	return subBatchSize < job.BatchSize
}
