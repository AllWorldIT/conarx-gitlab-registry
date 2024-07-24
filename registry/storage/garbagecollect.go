package storage

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/hashicorp/go-multierror"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
	"golang.org/x/sync/errgroup"
)

// GCOpts contains options for garbage collector
type GCOpts struct {
	DryRun                  bool
	RemoveUntagged          bool
	MaxParallelManifestGets int
}

// ManifestDel contains manifest structure which will be deleted
type ManifestDel struct {
	Name   string
	Digest digest.Digest
	Tags   []string
}

// syncManifestDelContainer provides thead-safe appends of ManifestDel
type syncManifestDelContainer struct {
	sync.Mutex
	manifestDels []ManifestDel
}

func (c *syncManifestDelContainer) append(md ManifestDel) {
	c.Lock()
	defer c.Unlock()

	c.manifestDels = append(c.manifestDels, md)
}

// syncDigestSet provides thread-safe set operations on digests.
type syncDigestSet struct {
	sync.Mutex
	members map[digest.Digest]struct{}
}

func newSyncDigestSet() syncDigestSet {
	return syncDigestSet{sync.Mutex{}, make(map[digest.Digest]struct{})}
}

// idempotently adds a digest to the set.
func (s *syncDigestSet) add(d digest.Digest) {
	s.Lock()
	defer s.Unlock()

	s.members[d] = struct{}{}
}

// contains reports the digest's membership within the set.
func (s *syncDigestSet) contains(d digest.Digest) bool {
	s.Lock()
	defer s.Unlock()

	_, ok := s.members[d]

	return ok
}

// len returns the number of members within the set.
func (s *syncDigestSet) len() int {
	s.Lock()
	defer s.Unlock()

	return len(s.members)
}

// MarkAndSweep performs a mark and sweep of registry data
func MarkAndSweep(ctx context.Context, storageDriver driver.StorageDriver, registry distribution.Namespace, opts GCOpts) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"stage":   "mark",
		"driver":  storageDriver.Name(),
		"dry_run": opts.DryRun,
	})

	// Always log this info message at the end of MarkAndSweep, even if there was an error or the process was interrupted.
	defer l.Info("zero-downtime continuous garbage collection is now available as part of a beta feature. Please see the feedback issue for more information: https://gitlab.com/gitlab-org/gitlab/-/issues/423459")

	// Check that the database does **not** manage this filesystem.
	// We should both log and exit during the failure cases becase we send the
	// final error to stderr and this is not always captured in user reports.
	dbLock := DatabaseInUseLocker{Driver: storageDriver}
	locked, err := dbLock.IsLocked(ctx)
	if err != nil {
		l.WithError(err).Error("unable to check for database in use lock, exiting")
		return err
	}
	if locked {
		dbLockPath, err := dbLock.path()
		if err != nil {
			// This error should not be possible, but just in case.
			l.WithError(err).Error("failed to get database in use lock path")
		}
		l.WithFields(log.Fields{
			"lock_path": dbLockPath,
		}).Error("this filesystem is managed by the metadata database, and offline garbage collection is no longer possible, if you are not using the database anymore, remove the file at the lock_path in this log message")
		return fmt.Errorf("database managed filesystem, cannot continue")
	}

	if opts.MaxParallelManifestGets < 1 {
		opts.MaxParallelManifestGets = 1
	}

	repositoryEnumerator, ok := registry.(distribution.RepositoryEnumerator)
	if !ok {
		return fmt.Errorf("converting Namespace to RepositoryEnumerator")
	}

	// mark
	markStart := time.Now()
	l.Info("starting mark stage")

	// check registry root repository path exists, if path is non existent log warning
	// message indicating gc does not need to be run and exit early without errors.
	repositoriesRoot, _ := pathFor(repositoriesRootPathSpec{})
	if _, err := storageDriver.Stat(ctx, repositoriesRoot); err != nil {
		if errors.As(err, &driver.PathNotFoundError{}) {
			l.WithError(err).Warn("skipping garbage collection")
			return nil
		}
		return fmt.Errorf("checking root path: %w", err)
	}

	markSet := newSyncDigestSet()
	manifestArr := syncManifestDelContainer{sync.Mutex{}, make([]ManifestDel, 0)}

	err = repositoryEnumerator.Enumerate(ctx, func(repoName string) error {
		rLog := l.WithFields(log.Fields{"repository": repoName})
		rLog.Info("marking repository")

		taggedManifests := newSyncDigestSet()
		unTaggedManifests := newSyncDigestSet()
		referencedManifests := newSyncDigestSet()

		var err error
		named, err := reference.WithName(repoName)
		if err != nil {
			return fmt.Errorf("parsing repo name %s: %w", repoName, err)
		}
		repository, err := registry.Repository(ctx, named)
		if err != nil {
			return fmt.Errorf("constructing repository: %w", err)
		}

		manifestService, err := repository.Manifests(ctx)
		if err != nil {
			return fmt.Errorf("constructing manifest service: %w", err)
		}

		manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
		if !ok {
			return fmt.Errorf("converting ManifestService into ManifestEnumerator")
		}

		t, ok := repository.Tags(ctx).(*tagStore)
		if !ok {
			return fmt.Errorf("converting tagService into tagStore")
		}

		cachedTagStore := newCachedTagStore(t)

		// Since we're removing untagged images, retrieving all tags primes the
		// cache. This isn't strictly necessary, but it prevents a potentially large
		// amount of goroutines being spawned only to wait for priming to complete
		// and allows us to report the number of primed tags.
		if opts.RemoveUntagged {
			primeStart := time.Now()
			rLog.Info("priming tags cache")

			allTags, err := cachedTagStore.All(ctx)
			if err != nil {
				switch err := err.(type) {
				case distribution.ErrRepositoryUnknown:
					// Ignore path not found error on missing tags folder
				default:
					return fmt.Errorf("retrieving tags %w", err)
				}
			}

			rLog.WithFields(log.Fields{
				"tags_primed": len(allTags),
				"duration_s":  time.Since(primeStart).Seconds(),
			}).Info("tags cache primed")
		}

		err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
			if opts.RemoveUntagged {
				// fetch all tags where this manifest is the latest one
				tags, err := cachedTagStore.Lookup(ctx, distribution.Descriptor{Digest: dgst})
				if err != nil {
					return fmt.Errorf("retrieving tags for digest %v: %w", dgst, err)
				}
				if len(tags) == 0 {
					unTaggedManifests.add(dgst)
					return nil
				}
				rLog.WithFields(log.Fields{
					"referenced_by": "tag",
					"tag_count":     len(tags),
					"digest":        dgst,
				}).Info("marking manifest metadata for repository")
				taggedManifests.add(dgst)
				return nil
			}
			referencedManifests.add(dgst)
			return nil
		})
		if err != nil {
			// In certain situations such as unfinished uploads, deleting all
			// tags in S3 or removing the _manifests folder manually, this
			// error may be of type PathNotFound.
			//
			// In these cases we can continue marking other manifests safely.
			//
			// If we encounter a MultiError, check each underlying error, returning
			// nil only if all errors are of type PathNotFound.
			switch err := err.(type) {
			case *multierror.Error:
				for _, e := range err.Errors {
					if _, ok := e.(driver.PathNotFoundError); !ok {
						return err
					}
				}
			case driver.PathNotFoundError:
			default:
				return err
			}
		}

		semaphore := make(chan struct{}, opts.MaxParallelManifestGets)

		if opts.RemoveUntagged {
			g, ctx := errgroup.WithContext(ctx)

			for dgst := range taggedManifests.members {
				semaphore <- struct{}{}
				manifestDigest := dgst

				g.Go(func() error {
					defer func() {
						<-semaphore
					}()

					rLog.WithFields(log.Fields{
						"referenced_by": "tag",
						"digest_type":   "manifest",
						"digest":        manifestDigest,
					}).Info("marking blob")
					markSet.add(manifestDigest)

					manifest, err := manifestService.Get(ctx, manifestDigest)
					if err != nil {
						return fmt.Errorf("retrieving manifest for digest %v: %w", manifestDigest, err)
					}

					if manifestList, ok := manifest.(*manifestlist.DeserializedManifestList); ok {
						// Docker buildx incorrectly uses OCI Image Indexes to store lists
						// of layer blobs, account for this so that garbage collection
						// can preserve these blobs and won't break later down the line
						// when we try to get these digests as manifests due to
						// the fallback behavior introduced in
						// https://github.com/distribution/distribution/pull/864
						splitRef := mlcompat.References(manifestList)

						// Normal manifest list with only manifest references, add these
						// to the set of referenced manifests.
						for _, r := range splitRef.Manifests {
							rLog.WithFields(log.Fields{
								"digest_type":   "manifest",
								"referenced_by": "manifest_list",
								"digest":        r.Digest,
								"mediatype":     r.MediaType,
								"parent_digest": manifestDigest,
							}).Info("marking manifest")

							referencedManifests.add(r.Digest)
						}

						// Do some extra logging here for invalid manifest lists and provide
						// the list of tags associated with this manifest.
						if mlcompat.ContainsBlobs(manifestList) {
							tags, err := cachedTagStore.Lookup(ctx, distribution.Descriptor{Digest: manifestDigest})
							if err != nil {
								return fmt.Errorf("retrieving tags for digest %v: %w", manifestDigest, err)
							}
							rLog.WithFields(log.Fields{
								"mediatype": manifestList.Versioned.MediaType,
								"digest":    manifestDigest,
								"tags":      tags,
							}).Warn("nonconformant manifest list with layer references, please report this to GitLab")
						}

						// Mark the manifest list layer references as normal blobs.
						for _, r := range splitRef.Blobs {
							rLog.WithFields(log.Fields{
								"digest_type":   "layer",
								"referenced_by": "manifest_list",
								"digest":        r.Digest,
								"mediatype":     r.MediaType,
								"parent_digest": manifestDigest,
							}).Info("marking blob")
							markSet.add(r.Digest)
						}
					} else {
						for _, descriptor := range manifest.References() {
							rLog.WithFields(log.Fields{
								"referenced_by": "manifest",
								"digest_type":   "layer",
								"digest":        descriptor.Digest,
								"mediatype":     descriptor.MediaType,
								"parent_digest": manifestDigest,
							}).Info("marking blob")
							markSet.add(descriptor.Digest)
						}
					}
					return nil
				})
			}

			if err := g.Wait(); err != nil {
				return fmt.Errorf("marking tagged manifests: %w", err)
			}

			for dgst := range unTaggedManifests.members {
				if referencedManifests.contains(dgst) {
					continue
				}

				rLog.WithFields(log.Fields{"digest": dgst}).Info("manifest metadata will be deleted from repository")
				// Fetch all tags from repository: all of these tags could contain the
				// manifest in history which means that we need check (and delete) those
				// references when deleting the manifest.
				allTags, err := cachedTagStore.All(ctx)
				if err != nil {
					switch err := err.(type) {
					case distribution.ErrRepositoryUnknown:
						// Ignore path not found error on missing tags folder
					default:
						return fmt.Errorf("retrieving tags %w", err)
					}
				}
				manifestArr.append(ManifestDel{Name: repoName, Digest: dgst, Tags: allTags})
			}
		}

		refType := "tagOrManifest"
		// If we're removing untagged, any manifests left at this point were only
		// referenced by a manifest list.
		if opts.RemoveUntagged {
			refType = "manifest_list"
		}

		markLog := rLog.WithFields(log.Fields{"referenced_by": refType})

		g, ctx := errgroup.WithContext(ctx)

		for dgst := range referencedManifests.members {
			semaphore <- struct{}{}
			d := dgst

			g.Go(func() error {
				defer func() {
					<-semaphore
				}()

				// Mark the manifest's blob
				markLog.WithFields(log.Fields{"digest_type": "manifest", "digest": d}).Info("marking blob")
				markSet.add(d)

				manifest, err := manifestService.Get(ctx, d)
				if err != nil {
					// If the manifest is not present, then we should continue,
					// allowing its layers to be garbage collected.
					errUnknownRev := &distribution.ErrManifestUnknownRevision{}
					if errors.As(err, errUnknownRev) {
						return nil
					}
					return fmt.Errorf("retrieving manifest for digest %v: %w", d, err)
				}

				for _, descriptor := range manifest.References() {
					markLog.WithFields(log.Fields{"digest_type": "layer", "digest": descriptor.Digest}).Info("marking blob")
					markSet.add(descriptor.Digest)
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return fmt.Errorf("marking referenced manifests: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("marking blobs: %w", err)
	}

	blobService := registry.Blobs()
	deleteSet := newSyncDigestSet()

	l.Info("finding blobs eligible for deletion. This may take some time...")

	sizeChan := make(chan int64)
	sizeDone := make(chan struct{})
	var totalSizeBytes int64
	go func() {
		for size := range sizeChan {
			totalSizeBytes += size
		}
		sizeDone <- struct{}{}
	}()

	err = blobService.Enumerate(ctx, func(desc distribution.Descriptor) error {
		// check if digest is in markSet. If not, delete it!
		if !markSet.contains(desc.Digest) {
			l.WithFields(log.Fields{"digest": desc.Digest, "size_bytes": desc.Size}).Info("blob eligible for deletion")

			sizeChan <- desc.Size
			deleteSet.add(desc.Digest)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("enumerating blobs: %w", err)
	}

	close(sizeChan)
	<-sizeDone

	l.WithFields(log.Fields{
		"blobs_marked":               markSet.len(),
		"blobs_to_delete":            deleteSet.len(),
		"manifests_to_delete":        len(manifestArr.manifestDels),
		"storage_use_estimate_bytes": totalSizeBytes,
		"duration_s":                 time.Since(markStart).Seconds(),
	}).Info("mark stage complete")

	// sweep
	if opts.DryRun {
		return nil
	}
	sweepStart := time.Now()
	l = l.WithFields(log.Fields{"stage": "sweep"})
	l.Info("starting sweep stage")

	vacuum := NewVacuum(storageDriver)

	if len(manifestArr.manifestDels) > 0 {
		if err := vacuum.RemoveManifests(ctx, manifestArr.manifestDels); err != nil {
			return fmt.Errorf("deleting manifests: %w", err)
		}
	}

	// Lock and unlock manually and access members directly to reduce lock operations.
	deleteSet.Lock()
	defer deleteSet.Unlock()

	dgsts := make([]digest.Digest, 0, len(deleteSet.members))
	for dgst := range deleteSet.members {
		dgsts = append(dgsts, dgst)
	}
	if len(dgsts) > 0 {
		if err := vacuum.RemoveBlobs(ctx, dgsts); err != nil {
			return fmt.Errorf("deleting blobs: %w", err)
		}
	}
	l.WithFields(log.Fields{"duration_s": time.Since(sweepStart).Seconds()}).Info("sweep stage complete")

	return err
}
