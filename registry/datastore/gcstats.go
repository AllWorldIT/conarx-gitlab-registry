package datastore

import (
	"context"
	"fmt"
	"time"

	"github.com/docker/distribution/registry/datastore/models"
)

// GCStats holds garbage collection statistics for both blob and manifest queues.
type GCStats struct {
	Blobs     BlobQueueStats     `json:"blobs"`
	Manifests ManifestQueueStats `json:"manifests"`
}

// ReviewTask contains common fields shared across all GC task samples.
type ReviewTask struct {
	ReviewAfter time.Time     `json:"review_after"`
	ReviewCount int           `json:"review_count,omitempty"`
	Event       string        `json:"event"`
	Overdue     time.Duration `json:"overdue,omitempty"`
}

// BlobQueueStats holds statistics for the blob review queue.
type BlobQueueStats struct {
	PendingRemoval BlobPendingStats   `json:"pending_removal"`
	LongOverdue    BlobOverdueStats   `json:"long_overdue"`
	HighRetry      BlobHighRetryStats `json:"high_retry"`
}

// BlobPendingStats holds count and samples for blobs pending removal.
type BlobPendingStats struct {
	Count   int                 `json:"count"`
	Samples []BlobPendingSample `json:"samples"`
}

// BlobOverdueStats holds count and samples for long overdue blob tasks.
type BlobOverdueStats struct {
	Count   int                 `json:"count"`
	Samples []BlobOverdueSample `json:"samples"`
}

// BlobHighRetryStats holds count and samples for high retry blob tasks.
type BlobHighRetryStats struct {
	Count   int                   `json:"count"`
	Samples []BlobHighRetrySample `json:"samples"`
}

// BlobPendingSample represents a blob task pending removal.
type BlobPendingSample struct {
	Digest string `json:"digest"`
	ReviewTask
}

// BlobOverdueSample represents a long overdue blob task.
type BlobOverdueSample struct {
	Digest string `json:"digest"`
	ReviewTask
}

// BlobHighRetrySample represents a blob task with high retry count.
type BlobHighRetrySample struct {
	Digest string `json:"digest"`
	ReviewTask
}

// ManifestQueueStats holds statistics for the manifest review queue.
type ManifestQueueStats struct {
	PendingRemoval ManifestPendingStats   `json:"pending_removal"`
	LongOverdue    ManifestOverdueStats   `json:"long_overdue"`
	HighRetry      ManifestHighRetryStats `json:"high_retry"`
}

// ManifestPendingStats holds count and samples for manifests pending removal.
type ManifestPendingStats struct {
	Count   int                     `json:"count"`
	Samples []ManifestPendingSample `json:"samples"`
}

// ManifestOverdueStats holds count and samples for long overdue manifest tasks.
type ManifestOverdueStats struct {
	Count   int                     `json:"count"`
	Samples []ManifestOverdueSample `json:"samples"`
}

// ManifestHighRetryStats holds count and samples for high retry manifest tasks.
type ManifestHighRetryStats struct {
	Count   int                       `json:"count"`
	Samples []ManifestHighRetrySample `json:"samples"`
}

// ManifestPendingSample represents a manifest task pending removal.
type ManifestPendingSample struct {
	RepositoryID int64 `json:"repository_id"`
	ManifestID   int64 `json:"manifest_id"`
	ReviewTask
}

// ManifestOverdueSample represents a long overdue manifest task.
type ManifestOverdueSample struct {
	RepositoryID int64 `json:"repository_id"`
	ManifestID   int64 `json:"manifest_id"`
	ReviewTask
}

// ManifestHighRetrySample represents a manifest task with high retry count.
type ManifestHighRetrySample struct {
	RepositoryID int64 `json:"repository_id"`
	ManifestID   int64 `json:"manifest_id"`
	ReviewTask
}

// GCStatsStore defines the interface for retrieving garbage collection statistics.
type GCStatsStore interface {
	GetGCStats(ctx context.Context) (GCStats, error)
}

type manifestFinderCounter interface {
	FindAll(ctx context.Context, opts ...GCTaskFilterOption) ([]*models.GCManifestTask, error)
	Count(ctx context.Context, opts ...GCTaskFilterOption) (int, error)
}

type blobFinderCounter interface {
	FindAll(ctx context.Context, opts ...GCTaskFilterOption) ([]*models.GCBlobTask, error)
	Count(ctx context.Context, opts ...GCTaskFilterOption) (int, error)
}

type gcStatsStore struct {
	manifests   manifestFinderCounter
	blobs       blobFinderCounter
	limit       int
	retryCount  int
	reviewDelay time.Duration

	now time.Time
}

// NewGCStatsStore creates a new GCStatsStore instance.
func NewGCStatsStore(manifestStore GCManifestTaskStore, blobStore GCBlobTaskStore, reviewDelay time.Duration, retryCount, limit int) GCStatsStore {
	return &gcStatsStore{
		manifests:   manifestStore,
		blobs:       blobStore,
		limit:       limit,
		retryCount:  retryCount,
		reviewDelay: reviewDelay,
	}
}

// GetGCStats retrieves garbage collection statistics for blobs and manifests.
func (s *gcStatsStore) GetGCStats(ctx context.Context) (GCStats, error) {
	s.now = time.Now().UTC()

	blobPendingSamples, err := s.getBlobPendingSamples(ctx)
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching blob pending samples: %w", err)
	}

	blobPendingCount, err := s.blobs.Count(ctx, WithGCTasksReviewAfterLessThan(s.now))
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching blob pending count: %w", err)
	}

	blobLongOverdueSamples, err := s.getBlobOverdueSamples(ctx)
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching blob long overdue samples: %w", err)
	}

	blobLongOverdueCount, err := s.blobs.Count(ctx, WithGCTasksReviewAfterLessThan(s.now.Add(-s.reviewDelay)))
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching blob long overdue count: %w", err)
	}

	blobHighRetrySamples, err := s.getBlobHighRetrySamples(ctx)
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching blob high retry samples: %w", err)
	}

	blobHighRetryCount, err := s.blobs.Count(ctx, WithGCTasksReviewCountGreaterThan(s.retryCount))
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching blob high retry count: %w", err)
	}

	manifestPendingSamples, err := s.getManifestPendingSamples(ctx)
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching manifest pending samples: %w", err)
	}

	manifestPendingCount, err := s.manifests.Count(ctx, WithGCTasksReviewAfterLessThan(s.now))
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching manifest pending count: %w", err)
	}

	manifestLongOverdueSamples, err := s.getManifestOverdueSamples(ctx)
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching manifest long overdue samples: %w", err)
	}

	manifestLongOverdueCount, err := s.manifests.Count(ctx, WithGCTasksReviewAfterLessThan(s.now.Add(-s.reviewDelay)))
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching manifest long overdue count: %w", err)
	}

	manifestHighRetrySamples, err := s.getManifestHighRetrySamples(ctx)
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching manifest high retry samples: %w", err)
	}

	manifestHighRetryCount, err := s.manifests.Count(ctx, WithGCTasksReviewCountGreaterThan(s.retryCount))
	if err != nil {
		return GCStats{}, fmt.Errorf("fetching manifest high retry count: %w", err)
	}

	return GCStats{
		Blobs: BlobQueueStats{
			PendingRemoval: BlobPendingStats{
				Count:   blobPendingCount,
				Samples: blobPendingSamples,
			},
			LongOverdue: BlobOverdueStats{
				Count:   blobLongOverdueCount,
				Samples: blobLongOverdueSamples,
			},
			HighRetry: BlobHighRetryStats{
				Count:   blobHighRetryCount,
				Samples: blobHighRetrySamples,
			},
		},
		Manifests: ManifestQueueStats{
			PendingRemoval: ManifestPendingStats{
				Count:   manifestPendingCount,
				Samples: manifestPendingSamples,
			},
			LongOverdue: ManifestOverdueStats{
				Count:   manifestLongOverdueCount,
				Samples: manifestLongOverdueSamples,
			},
			HighRetry: ManifestHighRetryStats{
				Count:   manifestHighRetryCount,
				Samples: manifestHighRetrySamples,
			},
		},
	}, nil
}

func (s *gcStatsStore) getBlobPendingSamples(ctx context.Context) ([]BlobPendingSample, error) {
	rawBlobTasks, err := s.blobs.FindAll(ctx, WithGCTasksReviewAfterLessThan(s.now), WithGCTasksLimit(s.limit))
	if err != nil {
		return nil, fmt.Errorf("fetching blob tasks: %w", err)
	}

	// Convert rawBlobTasks to []BlobPendingSamples
	samples := make([]BlobPendingSample, 0, len(rawBlobTasks))
	for _, task := range rawBlobTasks {
		samples = append(samples, BlobPendingSample{
			Digest:     task.Digest.String(),
			ReviewTask: rawBlobTaskToReviewTask(task),
		})
	}
	return samples, nil
}

func (s *gcStatsStore) getBlobOverdueSamples(ctx context.Context) ([]BlobOverdueSample, error) {
	rawBlobTasks, err := s.blobs.FindAll(ctx, WithGCTasksReviewAfterLessThan(s.now.Add(-s.reviewDelay)), WithGCTasksLimit(s.limit))
	if err != nil {
		return nil, fmt.Errorf("fetching long overdue blob tasks: %w", err)
	}

	samples := make([]BlobOverdueSample, 0, len(rawBlobTasks))
	for _, task := range rawBlobTasks {
		samples = append(samples, BlobOverdueSample{
			Digest:     task.Digest.String(),
			ReviewTask: rawBlobTaskToReviewTask(task),
		})
	}
	return samples, nil
}

func (s *gcStatsStore) getBlobHighRetrySamples(ctx context.Context) ([]BlobHighRetrySample, error) {
	rawBlobTasks, err := s.blobs.FindAll(ctx, WithGCTasksReviewCountGreaterThan(s.retryCount), WithGCTasksLimit(s.limit))
	if err != nil {
		return nil, fmt.Errorf("fetching high retry blob tasks: %w", err)
	}

	samples := make([]BlobHighRetrySample, 0, len(rawBlobTasks))
	for _, task := range rawBlobTasks {
		samples = append(samples, BlobHighRetrySample{
			Digest:     task.Digest.String(),
			ReviewTask: rawBlobTaskToReviewTask(task),
		})
	}

	return samples, nil
}

func (s *gcStatsStore) getManifestPendingSamples(ctx context.Context) ([]ManifestPendingSample, error) {
	rawManifestTasks, err := s.manifests.FindAll(ctx, WithGCTasksReviewAfterLessThan(s.now), WithGCTasksLimit(s.limit))
	if err != nil {
		return nil, fmt.Errorf("fetching manifest tasks: %w", err)
	}

	samples := make([]ManifestPendingSample, 0, len(rawManifestTasks))
	for _, task := range rawManifestTasks {
		samples = append(samples, ManifestPendingSample{
			RepositoryID: task.RepositoryID,
			ManifestID:   task.ManifestID,
			ReviewTask:   rawManifestTaskToReviewTask(task),
		})
	}
	return samples, nil
}

func (s *gcStatsStore) getManifestOverdueSamples(ctx context.Context) ([]ManifestOverdueSample, error) {
	rawManifestTasks, err := s.manifests.FindAll(ctx, WithGCTasksReviewAfterLessThan(s.now.Add(-s.reviewDelay)), WithGCTasksLimit(s.limit))
	if err != nil {
		return nil, fmt.Errorf("fetching long overdue manifest tasks: %w", err)
	}

	samples := make([]ManifestOverdueSample, 0, len(rawManifestTasks))
	for _, task := range rawManifestTasks {
		samples = append(samples, ManifestOverdueSample{
			RepositoryID: task.RepositoryID,
			ManifestID:   task.ManifestID,
			ReviewTask:   rawManifestTaskToReviewTask(task),
		})
	}
	return samples, nil
}

func (s *gcStatsStore) getManifestHighRetrySamples(ctx context.Context) ([]ManifestHighRetrySample, error) {
	rawManifestTasks, err := s.manifests.FindAll(ctx, WithGCTasksReviewCountGreaterThan(s.retryCount), WithGCTasksLimit(s.limit))
	if err != nil {
		return nil, fmt.Errorf("fetching high retry manifest tasks: %w", err)
	}

	samples := make([]ManifestHighRetrySample, 0, len(rawManifestTasks))
	for _, task := range rawManifestTasks {
		samples = append(samples, ManifestHighRetrySample{
			RepositoryID: task.RepositoryID,
			ManifestID:   task.ManifestID,
			ReviewTask:   rawManifestTaskToReviewTask(task),
		})
	}
	return samples, nil
}

func rawManifestTaskToReviewTask(task *models.GCManifestTask) ReviewTask {
	return ReviewTask{
		ReviewAfter: task.ReviewAfter,
		ReviewCount: task.ReviewCount,
		Event:       task.Event,
		Overdue:     time.Since(task.ReviewAfter),
	}
}

func rawBlobTaskToReviewTask(task *models.GCBlobTask) ReviewTask {
	return ReviewTask{
		ReviewAfter: task.ReviewAfter,
		ReviewCount: task.ReviewCount,
		Event:       task.Event,
		Overdue:     time.Since(task.ReviewAfter),
	}
}
