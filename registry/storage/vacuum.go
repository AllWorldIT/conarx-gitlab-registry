//go:generate mockgen -package mocks -destination mocks/vacuum.go . BlobRemover

package storage

import (
	"context"
	"math"
	"path"
	"time"

	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
	"github.com/sirupsen/logrus"
)

// vacuum contains functions for cleaning up repositories and blobs on the storage backend.
// These functions will only reliably work on strongly consistent
// storage systems.
// https://en.wikipedia.org/wiki/Consistency_model

// NewVacuum creates a new Vacuum
func NewVacuum(driver driver.StorageDeleter) *Vacuum {
	return &Vacuum{driver: driver}
}

// BlobRemover defines methods that Vacuum must implement to delete blobs from the storage backend.
// This allows mocking Vacuum for online garbage collection, where blob removal is the only required operation.
type BlobRemover interface {
	RemoveBlob(ctx context.Context, dgst digest.Digest) error
	RemoveBlobs(ctx context.Context, dgsts []digest.Digest) error
}

// Vacuum removes content from the filesystem. Implements BlobRemover.
type Vacuum struct {
	driver driver.StorageDeleter
}

// RemoveBlob removes a blob from the filesystem
func (v Vacuum) RemoveBlob(ctx context.Context, dgst digest.Digest) error {
	blobPath, err := pathFor(blobPathSpec{digest: dgst})
	if err != nil {
		return err
	}

	dcontext.GetLogger(ctx).WithFields(logrus.Fields{
		"digest": dgst,
		"path":   blobPath,
	}).Info("deleting blob")
	if err := v.driver.Delete(ctx, blobPath); err != nil {
		return err
	}

	return nil
}

// RemoveBlobs removes a list of blobs from the filesystem. This is used exclusively by the garbage collector and
// the intention is to leverage on bulk delete requests whenever supported by the storage backend.
func (v Vacuum) RemoveBlobs(ctx context.Context, dgsts []digest.Digest) error {
	start := time.Now()
	blobPaths := make([]string, 0, len(dgsts))

	log := dcontext.GetLogger(ctx)
	for _, d := range dgsts {
		// get the full path of the blob's data file
		p, err := pathFor(blobDataPathSpec{digest: d})
		if err != nil {
			return err
		}
		dcontext.GetLogger(ctx).WithFields(logrus.Fields{
			"digest": d,
			"path":   p,
		}).Debug("preparing to delete blob")
		blobPaths = append(blobPaths, p)
	}

	total := len(blobPaths)
	log.WithField("count", total).Info("deleting blobs")

	count, err := v.driver.DeleteFiles(ctx, blobPaths)

	l := log.WithFields(logrus.Fields{
		"count":      count,
		"duration_s": time.Since(start).Seconds(),
	})
	if count < total {
		l.Warn("blobs partially deleted")
	} else {
		l.Info("blobs deleted")
	}

	return err
}

func (v Vacuum) removeManifestsBatch(ctx context.Context, batchNo int, mm []ManifestDel) error {
	log := dcontext.GetLogger(ctx)
	defer func() {
		if r := recover(); r != nil {
			log.WithFields(logrus.Fields{
				"batch_number": batchNo,
				"r":            r,
			}).Error("recovered batch deletion, attempting next one")
		}
	}()

	start := time.Now()
	var manifestLinks, tagLinks, allLinks []string
	for _, m := range mm {
		// get manifest revision link full path
		// Note: we're skipping `storage.pathFor` on purpose inside this method due to major performance concerns, see:
		// https://gitlab.com/gitlab-org/container-registry/-/merge_requests/101#3-skipping-storagepathfor-92f7ca45
		p := "/docker/registry/v2/repositories/" + m.Name + "/_manifests/revisions/" + m.Digest.Algorithm().String() + "/" + m.Digest.Hex() + "/link"
		manifestLinks = append(manifestLinks, p)

		for _, t := range m.Tags {
			// get tag index link full path
			p := "/docker/registry/v2/repositories/" + m.Name + "/_manifests/tags/" + t + "/index/" + m.Digest.Algorithm().String() + "/" + m.Digest.Hex() + "/link"
			tagLinks = append(tagLinks, p)
		}
	}

	allLinks = append(manifestLinks, tagLinks...)
	total := len(allLinks)
	if total == 0 {
		return nil
	}

	log.WithFields(logrus.Fields{
		"batch_number": batchNo,
		"manifests":    len(manifestLinks),
		"tags":         len(tagLinks),
		"total":        total,
	}).Info("deleting batch")

	count, err := v.driver.DeleteFiles(ctx, allLinks)

	l := log.WithFields(logrus.Fields{
		"batch_number": batchNo,
		"count":        count,
		"duration_s":   time.Since(start).Seconds(),
	})
	if count < total {
		l.Warn("batch partially deleted")
	} else {
		l.Info("batch deleted")
	}

	return err
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RemoveManifests removes a series of manifests from the filesystem. Unlike RemoveManifest, this bundles all related
// tag index and manifest link files in a single driver.DeleteFiles request. The link files full path is used instead of
// their parent directory path (which always contains a single file, the link itself). For really large repositories the
// amount of manifests and tags eligible for deletion can be really high, which would generate a considerable amount of
// memory pressure. For this reason, manifests eligible for deletion are processed in batches of maxBatchSize, allowing
// the Go GC to kick in and free the space required to save their full paths between batches.
func (v Vacuum) RemoveManifests(ctx context.Context, mm []ManifestDel) error {
	start := time.Now()

	maxBatchSize := 100
	totalToDelete := len(mm)
	totalBatches := math.Ceil(float64(totalToDelete) / float64(maxBatchSize))

	log := dcontext.GetLogger(ctx)
	log.WithFields(logrus.Fields{
		"batch_count":    totalBatches,
		"batch_max_size": maxBatchSize,
	}).Info("deleting manifest metadata in batches")

	batchNo := 0
	for i := 0; i < totalToDelete; i += maxBatchSize {
		batchNo++
		log.WithFields(logrus.Fields{
			"batch_number": batchNo,
			"batch_total":  totalBatches,
		}).Info("preparing batch")

		batch := mm[i:min(i+maxBatchSize, totalToDelete)]
		if err := v.removeManifestsBatch(ctx, batchNo, batch); err != nil {
			return err
		}
	}

	log.WithFields(logrus.Fields{
		"duration_s": time.Since(start).Seconds(),
	}).Info("manifest metadata deleted")

	return nil
}

// RemoveRepository removes a repository directory from the
// filesystem
func (v Vacuum) RemoveRepository(ctx context.Context, repoName string) error {
	rootForRepository, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return err
	}
	repoDir := path.Join(rootForRepository, repoName)
	dcontext.GetLogger(ctx).WithFields(logrus.Fields{"name": repoName, "path": repoDir}).Info("deleting repository")
	err = v.driver.Delete(ctx, repoDir)
	if err != nil {
		return err
	}

	return nil
}
