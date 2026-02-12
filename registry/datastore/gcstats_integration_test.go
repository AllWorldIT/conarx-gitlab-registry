//go:build integration

package datastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/docker/distribution/registry/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGCStatsStoreGetStats(t *testing.T) {
	reloadGCManifestTaskFixtures(t)
	reloadGCBlobTaskFixtures(t)

	// Grab the location from the first review task for use in the expected results.
	s := datastore.NewGCManifestTaskStore(suite.db)
	rr, err := s.FindAll(suite.ctx)
	require.NoError(t, err)

	local := rr[0].ReviewAfter.Location()

	tests := []struct {
		name        string
		reviewDelay time.Duration
		retryCount  int
		limit       int
		want        datastore.GCStats
	}{
		{
			name:        "gcstats formatted as expected",
			reviewDelay: 24 * time.Hour,
			retryCount:  1,
			limit:       10,
			want: datastore.GCStats{
				Blobs: datastore.BlobQueueStats{
					PendingRemoval: datastore.BlobPendingStats{
						Count: 3,
						Samples: []datastore.BlobPendingSample{
							{Digest: "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 3, 17, 57, 23, 405516000, local), ReviewCount: 1, Event: "layer_delete", Overdue: time.Since(time.Date(2020, 3, 3, 17, 57, 23, 405516000, local))}},
							{Digest: "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 5, 20, 5, 35, 338639000, local), ReviewCount: 0, Event: "blob_upload", Overdue: time.Since(time.Date(2020, 3, 5, 20, 5, 35, 338639000, local))}},
							{Digest: "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 5, 20, 5, 35, 338639000, local), ReviewCount: 3, Event: "blob_upload", Overdue: time.Since(time.Date(2020, 3, 5, 20, 5, 35, 338639000, local))}},
						},
					},
					LongOverdue: datastore.BlobOverdueStats{
						Count: 3,
						Samples: []datastore.BlobOverdueSample{
							{Digest: "sha256:9ead3a93fc9c9dd8f35221b1f22b155a513815b7b00425d6645b34d98e83b073", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 3, 17, 57, 23, 405516000, local), ReviewCount: 1, Event: "layer_delete", Overdue: time.Since(time.Date(2020, 3, 3, 17, 57, 23, 405516000, local))}},
							{Digest: "sha256:c9b1b535fdd91a9855fb7f82348177e5f019329a58c53c47272962dd60f71fc9", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 5, 20, 5, 35, 338639000, local), ReviewCount: 0, Event: "blob_upload", Overdue: time.Since(time.Date(2020, 3, 5, 20, 5, 35, 338639000, local))}},
							{Digest: "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 5, 20, 5, 35, 338639000, local), ReviewCount: 3, Event: "blob_upload", Overdue: time.Since(time.Date(2020, 3, 5, 20, 5, 35, 338639000, local))}},
						},
					},
					HighRetry: datastore.BlobHighRetryStats{
						Count: 1,
						Samples: []datastore.BlobHighRetrySample{
							{Digest: "sha256:6b0937e234ce911b75630b744fb12836fe01bda5f7db203927edbb1390bc7e21", ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 5, 20, 5, 35, 338639000, local), ReviewCount: 3, Event: "blob_upload", Overdue: time.Since(time.Date(2020, 3, 5, 20, 5, 35, 338639000, local))}},
						},
					},
				},
				Manifests: datastore.ManifestQueueStats{
					PendingRemoval: datastore.ManifestPendingStats{
						Count: 3,
						Samples: []datastore.ManifestPendingSample{
							{RepositoryID: 3, ManifestID: 1, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 3, 17, 50, 26, 461745000, local), ReviewCount: 0, Event: "tag_switch", Overdue: time.Since(time.Date(2020, 3, 3, 17, 50, 26, 461745000, local))}},
							{RepositoryID: 4, ManifestID: 7, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 4, 3, 18, 45, 4, 470711000, local), ReviewCount: 2, Event: "manifest_upload", Overdue: time.Since(time.Date(2020, 4, 3, 18, 45, 4, 470711000, local))}},
							{RepositoryID: 4, ManifestID: 4, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 6, 11, 9, 11, 23, 655121000, local), ReviewCount: 0, Event: "manifest_list_delete", Overdue: time.Since(time.Date(2020, 6, 11, 9, 11, 23, 655121000, local))}},
						},
					},
					LongOverdue: datastore.ManifestOverdueStats{
						Count: 3,
						Samples: []datastore.ManifestOverdueSample{
							{RepositoryID: 3, ManifestID: 1, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 3, 3, 17, 50, 26, 461745000, local), ReviewCount: 0, Event: "tag_switch", Overdue: time.Since(time.Date(2020, 3, 3, 17, 50, 26, 461745000, local))}},
							{RepositoryID: 4, ManifestID: 7, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 4, 3, 18, 45, 4, 470711000, local), ReviewCount: 2, Event: "manifest_upload", Overdue: time.Since(time.Date(2020, 4, 3, 18, 45, 4, 470711000, local))}},
							{RepositoryID: 4, ManifestID: 4, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 6, 11, 9, 11, 23, 655121000, local), ReviewCount: 0, Event: "manifest_list_delete", Overdue: time.Since(time.Date(2020, 6, 11, 9, 11, 23, 655121000, local))}},
						},
					},
					HighRetry: datastore.ManifestHighRetryStats{
						Count: 1,
						Samples: []datastore.ManifestHighRetrySample{
							{RepositoryID: 4, ManifestID: 7, ReviewTask: datastore.ReviewTask{ReviewAfter: time.Date(2020, 4, 3, 18, 45, 4, 470711000, local), ReviewCount: 2, Event: "manifest_upload", Overdue: time.Since(time.Date(2020, 4, 3, 18, 45, 4, 470711000, local))}},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := datastore.NewGCStatsStore(datastore.NewGCManifestTaskStore(suite.db), datastore.NewGCBlobTaskStore(suite.db), test.reviewDelay, test.retryCount, test.limit)
			got, err := s.GetGCStats(context.Background())
			require.NoError(t, err)

			assert.Equal(t, test.want.Blobs.PendingRemoval.Count, got.Blobs.PendingRemoval.Count)
			assert.Equal(t, test.want.Blobs.LongOverdue.Count, got.Blobs.LongOverdue.Count)
			assert.Equal(t, test.want.Blobs.HighRetry.Count, got.Blobs.HighRetry.Count)

			// Compare the overdue field within a delta then zero out the overdue
			// durations since these will vary depending on when the test struct is
			// defined vs the actual struct is constructed. With the field zeroed out
			// the whole slices can be conveniently compared.
			for i := range got.Blobs.PendingRemoval.Samples {
				assert.InDelta(t, test.want.Blobs.PendingRemoval.Samples[i].ReviewTask.Overdue.Seconds(), got.Blobs.PendingRemoval.Samples[i].ReviewTask.Overdue.Seconds(), float64(20*time.Second))

				got.Blobs.PendingRemoval.Samples[i].ReviewTask.Overdue = 0
				test.want.Blobs.PendingRemoval.Samples[i].ReviewTask.Overdue = 0
			}
			for i := range got.Blobs.LongOverdue.Samples {
				assert.InDelta(t, test.want.Blobs.LongOverdue.Samples[i].ReviewTask.Overdue.Seconds(), got.Blobs.LongOverdue.Samples[i].ReviewTask.Overdue.Seconds(), float64(20*time.Second))

				got.Blobs.LongOverdue.Samples[i].ReviewTask.Overdue = 0
				test.want.Blobs.LongOverdue.Samples[i].ReviewTask.Overdue = 0
			}
			for i := range got.Blobs.HighRetry.Samples {
				assert.InDelta(t, test.want.Blobs.HighRetry.Samples[i].ReviewTask.Overdue.Seconds(), got.Blobs.HighRetry.Samples[i].ReviewTask.Overdue.Seconds(), float64(20*time.Second))

				got.Blobs.HighRetry.Samples[i].ReviewTask.Overdue = 0
				test.want.Blobs.HighRetry.Samples[i].ReviewTask.Overdue = 0
			}
			for i := range got.Manifests.PendingRemoval.Samples {
				assert.InDelta(t, test.want.Manifests.PendingRemoval.Samples[i].ReviewTask.Overdue.Seconds(), got.Manifests.PendingRemoval.Samples[i].ReviewTask.Overdue.Seconds(), float64(20*time.Second))

				got.Manifests.PendingRemoval.Samples[i].ReviewTask.Overdue = 0
				test.want.Manifests.PendingRemoval.Samples[i].ReviewTask.Overdue = 0
			}
			for i := range got.Manifests.LongOverdue.Samples {
				assert.InDelta(t, test.want.Manifests.LongOverdue.Samples[i].ReviewTask.Overdue.Seconds(), got.Manifests.LongOverdue.Samples[i].ReviewTask.Overdue.Seconds(), float64(20*time.Second))

				got.Manifests.LongOverdue.Samples[i].ReviewTask.Overdue = 0
				test.want.Manifests.LongOverdue.Samples[i].ReviewTask.Overdue = 0
			}
			for i := range got.Manifests.HighRetry.Samples {
				assert.InDelta(t, test.want.Manifests.HighRetry.Samples[i].ReviewTask.Overdue.Seconds(), got.Manifests.HighRetry.Samples[i].ReviewTask.Overdue.Seconds(), float64(20*time.Second))

				got.Manifests.HighRetry.Samples[i].ReviewTask.Overdue = 0
				test.want.Manifests.HighRetry.Samples[i].ReviewTask.Overdue = 0
			}

			assert.ElementsMatch(t, test.want.Blobs.PendingRemoval.Samples, got.Blobs.PendingRemoval.Samples)
			assert.ElementsMatch(t, test.want.Blobs.LongOverdue.Samples, got.Blobs.LongOverdue.Samples)
			assert.ElementsMatch(t, test.want.Blobs.HighRetry.Samples, got.Blobs.HighRetry.Samples)

			assert.ElementsMatch(t, test.want.Manifests.PendingRemoval.Samples, got.Manifests.PendingRemoval.Samples)
			assert.ElementsMatch(t, test.want.Manifests.LongOverdue.Samples, got.Manifests.LongOverdue.Samples)
			assert.ElementsMatch(t, test.want.Manifests.HighRetry.Samples, got.Manifests.HighRetry.Samples)
		})
	}
}
