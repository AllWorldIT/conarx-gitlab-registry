package datastore

import "time"

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

// GetMockGCStats returns mock GC statistics for development and testing.
// TODO: Replace with real database queries.
func GetMockGCStats(limit int) GCStats {
	now := time.Now().UTC()

	// Mock blob pending samples
	blobPendingSamples := []BlobPendingSample{
		{Digest: "sha256:a3ed95caeb02ffe68cdd9fd84406680ae93d633cb16422d00e8a7c22e", ReviewTask: ReviewTask{ReviewAfter: now.Add(-2 * time.Hour), Event: "blob_upload"}},
		{Digest: "sha256:b4f5e6d7c8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2", ReviewTask: ReviewTask{ReviewAfter: now.Add(-4 * time.Hour), Event: "manifest_delete"}},
		{Digest: "sha256:c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3", ReviewTask: ReviewTask{ReviewAfter: now.Add(-6 * time.Hour), Event: "layer_delete"}},
	}

	// Mock blob overdue samples
	blobOverdueSamples := []BlobOverdueSample{
		{Digest: "sha256:d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4", ReviewTask: ReviewTask{ReviewAfter: now.Add(-5 * 24 * time.Hour), Event: "blob_upload", Overdue: 4 * 24 * time.Hour}},
		{Digest: "sha256:e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5", ReviewTask: ReviewTask{ReviewAfter: now.Add(-3 * 24 * time.Hour), Event: "layer_delete", Overdue: 2 * 24 * time.Hour}},
	}

	// Mock blob high retry samples
	blobHighRetrySamples := []BlobHighRetrySample{
		{Digest: "sha256:f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6", ReviewTask: ReviewTask{ReviewAfter: now.Add(1 * time.Hour), ReviewCount: 15, Event: "blob_upload"}},
		{Digest: "sha256:a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7", ReviewTask: ReviewTask{ReviewAfter: now.Add(2 * time.Hour), ReviewCount: 12, Event: "manifest_delete"}},
	}

	// Mock manifest pending samples
	manifestPendingSamples := []ManifestPendingSample{
		{RepositoryID: 1001, ManifestID: 12345, ReviewTask: ReviewTask{ReviewAfter: now.Add(-1 * time.Hour), Event: "tag_delete"}},
		{RepositoryID: 1002, ManifestID: 67890, ReviewTask: ReviewTask{ReviewAfter: now.Add(-3 * time.Hour), Event: "manifest_upload"}},
		{RepositoryID: 1003, ManifestID: 11111, ReviewTask: ReviewTask{ReviewAfter: now.Add(-5 * time.Hour), Event: "tag_switch"}},
		{RepositoryID: 2001, ManifestID: 22222, ReviewTask: ReviewTask{ReviewAfter: now.Add(-7 * time.Hour), Event: "manifest_list_delete"}},
	}

	// Mock manifest overdue samples
	manifestOverdueSamples := []ManifestOverdueSample{
		{RepositoryID: 3001, ManifestID: 33333, ReviewTask: ReviewTask{ReviewAfter: now.Add(-4 * 24 * time.Hour), Event: "tag_delete", Overdue: 3 * 24 * time.Hour}},
		{RepositoryID: 3002, ManifestID: 44444, ReviewTask: ReviewTask{ReviewAfter: now.Add(-2 * 24 * time.Hour), Event: "manifest_delete", Overdue: 1 * 24 * time.Hour}},
	}

	// Mock manifest high retry samples
	manifestHighRetrySamples := []ManifestHighRetrySample{
		{RepositoryID: 4001, ManifestID: 55555, ReviewTask: ReviewTask{ReviewAfter: now.Add(30 * time.Minute), ReviewCount: 18, Event: "tag_delete"}},
		{RepositoryID: 4002, ManifestID: 66666, ReviewTask: ReviewTask{ReviewAfter: now.Add(45 * time.Minute), ReviewCount: 11, Event: "manifest_upload"}},
	}

	// Apply limit to samples consistently
	// limit <= 0 means no limit (show all samples)
	if limit > 0 {
		if limit < len(blobPendingSamples) {
			blobPendingSamples = blobPendingSamples[:limit]
		}
		if limit < len(blobOverdueSamples) {
			blobOverdueSamples = blobOverdueSamples[:limit]
		}
		if limit < len(blobHighRetrySamples) {
			blobHighRetrySamples = blobHighRetrySamples[:limit]
		}
		if limit < len(manifestPendingSamples) {
			manifestPendingSamples = manifestPendingSamples[:limit]
		}
		if limit < len(manifestOverdueSamples) {
			manifestOverdueSamples = manifestOverdueSamples[:limit]
		}
		if limit < len(manifestHighRetrySamples) {
			manifestHighRetrySamples = manifestHighRetrySamples[:limit]
		}
	}

	return GCStats{
		Blobs: BlobQueueStats{
			PendingRemoval: BlobPendingStats{
				Count:   42,
				Samples: blobPendingSamples,
			},
			LongOverdue: BlobOverdueStats{
				Count:   5,
				Samples: blobOverdueSamples,
			},
			HighRetry: BlobHighRetryStats{
				Count:   2,
				Samples: blobHighRetrySamples,
			},
		},
		Manifests: ManifestQueueStats{
			PendingRemoval: ManifestPendingStats{
				Count:   128,
				Samples: manifestPendingSamples,
			},
			LongOverdue: ManifestOverdueStats{
				Count:   8,
				Samples: manifestOverdueSamples,
			},
			HighRetry: ManifestHighRetryStats{
				Count:   3,
				Samples: manifestHighRetrySamples,
			},
		},
	}
}
