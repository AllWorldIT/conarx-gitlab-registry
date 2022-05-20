package datastore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/reference"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/internal/migration"
	"github.com/docker/distribution/registry/internal/migration/metrics"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	ErrImportCanceled       = errors.New("repository import has been canceled")
	ErrPreImportCanceled    = errors.New("repository pre import has been canceled")
	errNegativeTestingDelay = errors.New("negative testing delay")
	errManifestSkip         = errors.New("the manifest is invalid and its (pre)import should be skipped")
)

// Importer populates the registry database with filesystem metadata. This is only meant to be used for an initial
// one-off migration, starting with an empty database.
type Importer struct {
	registry            distribution.Namespace
	blobTransferService distribution.BlobTransferService
	db                  *DB
	repositoryStore     RepositoryStore
	manifestStore       ManifestStore
	tagStore            TagStore
	blobStore           BlobStore

	importDanglingManifests bool
	importDanglingBlobs     bool
	requireEmptyDatabase    bool
	dryRun                  bool
	tagConcurrency          int
	rowCount                bool
	testingDelay            time.Duration
}

// ImporterOption provides functional options for the Importer.
type ImporterOption func(*Importer)

// WithImportDanglingManifests configures the Importer to import all manifests
// rather than only tagged manifests.
func WithImportDanglingManifests(imp *Importer) {
	imp.importDanglingManifests = true
}

// WithImportDanglingBlobs configures the Importer to import all blobs
// rather than only blobs referenced by manifests.
func WithImportDanglingBlobs(imp *Importer) {
	imp.importDanglingBlobs = true
}

// WithRequireEmptyDatabase configures the Importer to stop import unless the
// database being imported to is empty.
func WithRequireEmptyDatabase(imp *Importer) {
	imp.requireEmptyDatabase = true
}

// WithDryRun configures the Importer to use a single transacton which is rolled
// back and the end of an import cycle.
func WithDryRun(imp *Importer) {
	imp.dryRun = true
}

// WithRowCount configures the Importer to count and log the number of rows across the most relevant database tables
// on (pre)import completion.
func WithRowCount(imp *Importer) {
	imp.rowCount = true
}

// WithBlobTransferService configures the Importer to use the passed BlobTransferService.
func WithBlobTransferService(bts distribution.BlobTransferService) ImporterOption {
	return func(imp *Importer) {
		imp.blobTransferService = bts
	}
}

// WithTagConcurrency configures the Importer to retrieve the details of n tags
// concurrently.
func WithTagConcurrency(n int) ImporterOption {
	return func(imp *Importer) {
		imp.tagConcurrency = n
	}
}

// WithTestSlowImport configures the Importer to sleep at the end of the import
// for the given duration. This is useful for testing, but should never be
// enabled on production environments.
func WithTestSlowImport(d time.Duration) ImporterOption {
	return func(imp *Importer) {
		imp.testingDelay = d
	}
}

// NewImporter creates a new Importer.
func NewImporter(db *DB, registry distribution.Namespace, opts ...ImporterOption) *Importer {
	imp := &Importer{
		registry:       registry,
		db:             db,
		tagConcurrency: 1,
	}

	for _, o := range opts {
		o(imp)
	}

	imp.loadStores(imp.db)

	return imp
}

func (imp *Importer) beginTx(ctx context.Context) (Transactor, error) {
	tx, err := imp.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	imp.loadStores(tx)

	return tx, nil
}

func (imp *Importer) loadStores(db Queryer) {
	imp.manifestStore = NewManifestStore(db)
	imp.blobStore = NewBlobStore(db)
	imp.repositoryStore = NewRepositoryStore(db)
	imp.tagStore = NewTagStore(db)
}

func (imp *Importer) findOrCreateDBManifest(ctx context.Context, dbRepo *models.Repository, m *models.Manifest) (*models.Manifest, error) {
	dbManifest, err := imp.repositoryStore.FindManifestByDigest(ctx, dbRepo, m.Digest)
	if err != nil {
		return nil, fmt.Errorf("searching for manifest: %w", err)
	}

	if dbManifest == nil {
		if err := imp.manifestStore.Create(ctx, m); err != nil {
			return nil, fmt.Errorf("creating manifest: %w", err)
		}
		dbManifest = m
	}

	return dbManifest, nil
}

func (imp *Importer) importLayer(ctx context.Context, dbRepo *models.Repository, dbLayer *models.Blob) error {
	if err := imp.blobStore.CreateOrFind(ctx, dbLayer); err != nil {
		return fmt.Errorf("creating layer blob: %w", err)
	}

	if err := imp.repositoryStore.LinkBlob(ctx, dbRepo, dbLayer.Digest); err != nil {
		return fmt.Errorf("linking layer blob to repository: %w", err)
	}

	if err := imp.transferBlob(ctx, dbLayer.Digest, dbLayer.Size, metrics.BlobTypeLayer); err != nil {
		return err
	}

	return nil
}

func (imp *Importer) importLayers(ctx context.Context, dbRepo *models.Repository, fsRepo distribution.Repository, fsLayers []distribution.Descriptor) ([]*models.Blob, error) {
	total := len(fsLayers)
	metrics.LayerCount(total)

	var dbLayers []*models.Blob

	for i, fsLayer := range fsLayers {
		l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
			"repository": dbRepo.Path,
			"digest":     fsLayer.Digest,
			"media_type": fsLayer.MediaType,
			"size":       fsLayer.Size,
		})
		ctx = log.WithLogger(ctx, l)
		l.WithFields(log.Fields{"total": total, "count": i + 1}).Info("importing layer")

		if _, err := fsRepo.Blobs(ctx).Stat(ctx, fsLayer.Digest); err != nil {
			if errors.Is(err, distribution.ErrBlobUnknown) {
				l.Warn("blob is not linked to repository, skipping blob import")
				continue
			}
			if errors.Is(err, digest.ErrDigestInvalidFormat) {
				l.WithError(err).Warn("broken layer link, skipping manifest import")
				return dbLayers, errManifestSkip
			}
			return dbLayers, fmt.Errorf("checking for access to blob with digest %s on repository %s: %w", fsLayer.Digest, fsRepo.Named().Name(), err)
		}

		layer := &models.Blob{MediaType: fsLayer.MediaType, Digest: fsLayer.Digest, Size: fsLayer.Size}

		if err := imp.importLayer(ctx, dbRepo, layer); err != nil {
			return dbLayers, err
		}
		dbLayers = append(dbLayers, layer)
	}

	return dbLayers, nil
}

func (imp *Importer) transferBlob(ctx context.Context, d digest.Digest, size int64, t metrics.BlobType) error {
	if imp.dryRun || imp.blobTransferService == nil {
		return nil
	}

	start := time.Now()
	var noop bool
	if err := imp.blobTransferService.Transfer(ctx, d); err != nil {
		if !errors.Is(err, distribution.ErrBlobExists) {
			return fmt.Errorf("transferring blob with digest %s: %w", d, err)
		}
		noop = true
	}

	duration := time.Since(start).Seconds()
	metrics.BlobTransfer(duration, float64(size), t)
	log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"noop":       noop,
		"digest":     d,
		"duration_s": duration,
		"blob_type":  t.String(),
	}).Info("blob transfer complete")

	return nil
}

func (imp *Importer) importManifestV2(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository, m distribution.ManifestV2, dgst digest.Digest, payload []byte, nonConformant bool) (*models.Manifest, error) {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": dbRepo.Path})

	// get configuration blob payload
	blobStore := fsRepo.Blobs(ctx)
	configPayload, err := blobStore.Get(ctx, m.Config().Digest)
	if err != nil {
		if errors.Is(err, digest.ErrDigestInvalidFormat) {
			l.WithError(err).Warn("broken configuration layer link, skipping")
			return nil, errManifestSkip
		}
		return nil, fmt.Errorf("obtaining configuration payload: %w", err)
	}

	dbConfigBlob := &models.Blob{
		MediaType: m.Config().MediaType,
		Digest:    m.Config().Digest,
		Size:      m.Config().Size,
	}

	l = l.WithFields(log.Fields{
		"digest":         dbConfigBlob.Digest,
		"media_type":     dbConfigBlob.MediaType,
		"size":           dbConfigBlob.Size,
		"non_conformant": nonConformant,
	})
	l.Info("importing configuration")
	ctx = log.WithLogger(ctx, l)

	if err = imp.blobStore.CreateOrFind(ctx, dbConfigBlob); err != nil {
		return nil, err
	}

	if err = imp.transferBlob(ctx, m.Config().Digest, m.Config().Size, metrics.BlobTypeConfig); err != nil {
		return nil, err
	}

	// link configuration to repository
	if err := imp.repositoryStore.LinkBlob(ctx, dbRepo, dbConfigBlob.Digest); err != nil {
		return nil, fmt.Errorf("associating configuration blob with repository: %w", err)
	}

	// Import manifest layers stored locally on the registry.
	dbLayers, err := imp.importLayers(ctx, dbRepo, fsRepo, m.DistributableLayers())
	if err != nil {
		return nil, fmt.Errorf("importing layers: %w", err)
	}

	// find or create DB manifest
	dbManifest, err := imp.findOrCreateDBManifest(ctx, dbRepo, &models.Manifest{
		NamespaceID:   dbRepo.NamespaceID,
		RepositoryID:  dbRepo.ID,
		TotalSize:     m.TotalSize(),
		SchemaVersion: m.Version().SchemaVersion,
		MediaType:     m.Version().MediaType,
		Digest:        dgst,
		Payload:       payload,
		NonConformant: nonConformant,
		Configuration: &models.Configuration{
			MediaType: dbConfigBlob.MediaType,
			Digest:    dbConfigBlob.Digest,
			Payload:   configPayload,
		},
	})
	if err != nil {
		return nil, err
	}

	// Link imported layers to the manifest.
	for _, dbLayer := range dbLayers {
		if err := imp.manifestStore.AssociateLayerBlob(ctx, dbManifest, dbLayer); err != nil {
			return nil, fmt.Errorf("associating layer blob with manifest: %w", err)
		}
	}

	return dbManifest, nil
}

func (imp *Importer) importManifestList(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository, ml *manifestlist.DeserializedManifestList, dgst digest.Digest) (*models.Manifest, error) {
	if mlcompat.LikelyBuildxCache(ml) {
		_, payload, err := ml.Payload()
		if err != nil {
			return nil, err
		}

		// convert to OCI manifest and process as if it was one
		m, err := mlcompat.OCIManifestFromBuildkitIndex(ml)
		if err != nil {
			return nil, fmt.Errorf("converting buildkit index to manifest: %w", err)
		}

		// Note that `payload` is not the deserialized manifest list (`ml`) payload but rather the index payload, untouched.
		manifestV2, err := imp.importManifestV2(ctx, fsRepo, dbRepo, m, dgst, payload, true)
		if err != nil {
			return nil, err
		}

		return manifestV2, nil
	}

	_, payload, err := ml.Payload()
	if err != nil {
		return nil, fmt.Errorf("parsing payload: %w", err)
	}

	// Media type can be either Docker (`application/vnd.docker.distribution.manifest.list.v2+json`) or OCI (empty).
	// We need to make it explicit if empty, otherwise we're not able to distinguish between media types.
	mediaType := ml.MediaType
	if mediaType == "" {
		mediaType = v1.MediaTypeImageIndex
	}

	// create manifest list on DB
	dbManifestList, err := imp.findOrCreateDBManifest(ctx, dbRepo, &models.Manifest{
		NamespaceID:   dbRepo.NamespaceID,
		RepositoryID:  dbRepo.ID,
		SchemaVersion: ml.SchemaVersion,
		MediaType:     mediaType,
		Digest:        dgst,
		Payload:       payload,
	})
	if err != nil {
		return nil, fmt.Errorf("creating manifest list in database: %w", err)
	}

	manifestService, err := fsRepo.Manifests(ctx)
	if err != nil {
		return nil, fmt.Errorf("constructing manifest service: %w", err)
	}

	// import manifests in list
	total := len(ml.Manifests)
	for i, m := range ml.Manifests {
		l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
			"repository": dbRepo.Path,
			"digest":     m.Digest.String(),
			"count":      i + 1,
			"total":      total,
		})
		fsManifest, err := manifestService.Get(ctx, m.Digest)
		if err != nil {
			return nil, fmt.Errorf("retrieving referenced manifest %q from filesystem: %w", m.Digest, err)
		}

		l.WithFields(log.Fields{"type": fmt.Sprintf("%T", fsManifest)}).Info("importing manifest referenced in list")

		dbManifest, err := imp.importManifest(ctx, fsRepo, dbRepo, fsManifest, m.Digest)
		if err != nil {
			if errors.Is(err, distribution.ErrSchemaV1Unsupported) {
				l.WithError(err).Warn("skipping v1 manifest")
				continue
			}
			return nil, err
		}

		if err := imp.manifestStore.AssociateManifest(ctx, dbManifestList, dbManifest); err != nil {
			return nil, err
		}
	}

	return dbManifestList, nil
}

func (imp *Importer) importManifest(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository, m distribution.Manifest, dgst digest.Digest) (*models.Manifest, error) {
	switch fsManifest := m.(type) {
	case *schema1.SignedManifest:
		return nil, distribution.ErrSchemaV1Unsupported
	case distribution.ManifestV2:
		_, payload, err := m.Payload()
		if err != nil {
			return nil, fmt.Errorf("getting manifest payload: %w", err)
		}

		return imp.importManifestV2(ctx, fsRepo, dbRepo, fsManifest, dgst, payload, false)
	default:
		return nil, fmt.Errorf("unknown manifest class")
	}
}

func (imp *Importer) importManifests(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository) error {
	manifestService, err := fsRepo.Manifests(ctx)
	if err != nil {
		return fmt.Errorf("constructing manifest service: %w", err)
	}
	manifestEnumerator, ok := manifestService.(distribution.ManifestEnumerator)
	if !ok {
		return fmt.Errorf("converting ManifestService into ManifestEnumerator")
	}

	index := 0
	err = manifestEnumerator.Enumerate(ctx, func(dgst digest.Digest) error {
		index++

		l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
			"repository": dbRepo.Path,
			"digest":     dgst,
			"count":      index,
		})

		m, err := getFsManifest(ctx, manifestService, dgst, l)
		if err != nil {
			if errors.Is(err, errManifestSkip) {
				return nil
			}
			return err
		}

		l = l.WithFields(log.Fields{"type": fmt.Sprintf("%T", m)})

		switch fsManifest := m.(type) {
		case *manifestlist.DeserializedManifestList:
			l.Info("importing manifest list")
			_, err = imp.importManifestList(ctx, fsRepo, dbRepo, fsManifest, dgst)
		default:
			l.Info("importing manifest")
			_, err = imp.importManifest(ctx, fsRepo, dbRepo, fsManifest, dgst)
			if errors.Is(err, distribution.ErrSchemaV1Unsupported) {
				l.WithError(err).Warn("skipping v1 manifest import")
				return nil
			}
		}

		return err
	})

	return err
}

// getFsManifest retrieves a manifest from the filesystem. In case the manifest is empty, the corresponding revision
// is unknown (rare unexpected errors, likely due to a past bug or data corruption) or it's an unsupported v1 schema,
// it simply logs a warning message and returns a nil distribution.Manifest and errManifestSkip error to the caller.
// In such case, the import of this manifest should be skipped, and an appropriate warn log message is emitted within
// this function.
func getFsManifest(ctx context.Context, manifestService distribution.ManifestService, dgst digest.Digest, l log.Logger) (distribution.Manifest, error) {
	m, err := manifestService.Get(ctx, dgst)
	if err != nil {
		if errors.As(err, &distribution.ErrManifestEmpty{}) {
			// This manifest is empty, which means it's unrecoverable, and therefore we should simply log, leave it
			// behind and continue
			l.WithError(err).Warn("empty manifest payload, skipping")
			return nil, errManifestSkip
		}
		if errors.As(err, &distribution.ErrManifestUnknownRevision{}) {
			// This manifest does not have a corresponding revision on the filesystem (unexpected) and as such,
			// attempting to pull if from the API (on the old code path) will return a not found error (even though the
			// manifest does exist). We should preserve whatever is the behavior on the old code path, so pulling this
			// manifest should also fail on the new code path. Therefore, just log and skip.
			l.WithError(err).Warn("unknown manifest revision, skipping")
			return nil, errManifestSkip
		}
		if errors.Is(err, distribution.ErrSchemaV1Unsupported) {
			// v1 schema manifests are no longer supported (both writes and reads), so just log a warning and skip
			l.WithError(err).Warn("unsupported v1 manifest, skipping")
			return nil, errManifestSkip
		}
		if errors.Is(err, digest.ErrDigestInvalidFormat) {
			// The manifest link is corrupted. Although its payload may still be present in common blob storage, this
			// manifest is no longer accessible from the outside in the scope of the current repository. For security
			// reasons we should not repair the broken link and therefore just log a warning and skip.
			l.WithError(err).Warn("broken manifest link, skipping")
			return nil, errManifestSkip
		}
		return nil, fmt.Errorf("retrieving manifest %q from filesystem: %w", dgst, err)
	}

	return m, nil
}

type tagLookupResponse struct {
	name string
	desc distribution.Descriptor
	err  error
}

func (imp *Importer) importTags(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": dbRepo.Path})

	manifestService, err := fsRepo.Manifests(ctx)
	if err != nil {
		return fmt.Errorf("constructing manifest service: %w", err)
	}

	tagService := fsRepo.Tags(ctx)
	fsTags, err := tagService.All(ctx)
	if err != nil {
		if errors.As(err, &distribution.ErrRepositoryUnknown{}) {
			// No `tags` folder, so no tags and therefore nothing to import. Just handle this gracefully and return.
			// The import will be completed successfully.
			return nil
		}
		return fmt.Errorf("reading tags: %w", err)
	}

	total := len(fsTags)
	metrics.TagCount(metrics.ImportTypeFinal, total)
	semaphore := make(chan struct{}, imp.tagConcurrency)
	tagResChan := make(chan *tagLookupResponse)

	l.WithFields(log.Fields{"total": total}).Info("importing tags")

	// Start a goroutine to concurrently dispatch tag details lookup, up to the configured tag concurrency at once.
	go func() {
		var wg sync.WaitGroup
		for _, tag := range fsTags {
			semaphore <- struct{}{}
			wg.Add(1)

			select {
			case <-ctx.Done():
				// Exit earlier if a tag lookup or import failed.
				return
			default:
			}

			go func(t string) {
				defer func() {
					<-semaphore
					wg.Done()
				}()

				desc, err := tagService.Get(ctx, t)
				tagResChan <- &tagLookupResponse{t, desc, err}
			}(tag)
		}

		wg.Wait()
		close(tagResChan)
	}()

	// Consume the tag lookup details serially. In the ideal case, we only need
	// retrieve the manifest from the database and associate it with a tag. This
	// is fast enough that concurrency really isn't warranted here as well.
	var i int
	for tRes := range tagResChan {
		i++
		fsTag := tRes.name
		desc := tRes.desc
		err := tRes.err

		l := l.WithFields(log.Fields{"tag_name": fsTag, "count": i, "total": total, "digest": desc.Digest})
		l.Info("importing tag")

		if err != nil {
			l := l.WithError(err)

			if errors.As(err, &distribution.ErrTagUnknown{}) {
				// The tag link is missing, log a warning and skip.
				l.Warn("missing tag link, skipping")
				continue
			}
			if errors.Is(err, digest.ErrDigestInvalidFormat) {
				// The tag link is corrupted, log a warning and skip.
				l.Warn("broken tag link, skipping")
				continue
			}

			return fmt.Errorf("reading tag details: %w", err)
		}

		// Find corresponding manifest in DB or filesystem.
		var dbManifest *models.Manifest
		dbManifest, err = imp.repositoryStore.FindManifestByDigest(ctx, dbRepo, desc.Digest)
		if err != nil {
			return fmt.Errorf("finding tagged manifest in database: %w", err)
		}
		if dbManifest == nil {
			m, err := getFsManifest(ctx, manifestService, desc.Digest, l)
			if err != nil {
				if errors.Is(err, errManifestSkip) {
					continue
				}
				return err
			}

			switch fsManifest := m.(type) {
			case *manifestlist.DeserializedManifestList:
				l.Info("importing manifest list")
				dbManifest, err = imp.importManifestList(ctx, fsRepo, dbRepo, fsManifest, desc.Digest)
			default:
				l.Info("importing manifest")
				dbManifest, err = imp.importManifest(ctx, fsRepo, dbRepo, fsManifest, desc.Digest)
			}
			if err != nil {
				if errors.Is(err, distribution.ErrSchemaV1Unsupported) {
					l.WithError(err).Warn("skipping v1 manifest import")
					continue
				}
				if errors.Is(err, errManifestSkip) {
					l.WithError(err).Warn("skipping manifest import")
					continue
				}
				return fmt.Errorf("importing manifest: %w", err)
			}
		}

		dbTag := &models.Tag{Name: fsTag, RepositoryID: dbRepo.ID, ManifestID: dbManifest.ID, NamespaceID: dbRepo.NamespaceID}
		if err := imp.tagStore.CreateOrUpdate(ctx, dbTag); err != nil {
			l.WithError(err).Error("creating tag")
		}
	}

	return nil
}

func (imp *Importer) importRepository(ctx context.Context, path string) error {
	named, err := reference.WithName(path)
	if err != nil {
		return fmt.Errorf("parsing repository name: %w", err)
	}
	fsRepo, err := imp.registry.Repository(ctx, named)
	if err != nil {
		return fmt.Errorf("constructing filesystem repository: %w", err)
	}

	// Find or create repository.
	var dbRepo *models.Repository

	if dbRepo, err = imp.repositoryStore.CreateOrFindByPath(ctx, path); err != nil {
		return fmt.Errorf("creating or finding repository in database: %w", err)
	}

	if imp.importDanglingManifests {
		// import all repository manifests
		if err := imp.importManifests(ctx, fsRepo, dbRepo); err != nil {
			return fmt.Errorf("importing manifests: %w", err)
		}
	}

	// import repository tags and associated manifests
	if err := imp.importTags(ctx, fsRepo, dbRepo); err != nil {
		return fmt.Errorf("importing tags: %w", err)
	}

	return nil
}

func (imp *Importer) preImportTaggedManifests(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository) error {
	tagService := fsRepo.Tags(ctx)
	fsTags, err := tagService.All(ctx)
	if err != nil {
		if errors.As(err, &distribution.ErrRepositoryUnknown{}) {
			// No `tags` folder, so no tags and therefore nothing to import. Just handle this gracefully and return.
			// The pre-import will be completed successfully.
			return nil
		}
		return fmt.Errorf("reading tags: %w", err)
	}

	total := len(fsTags)
	metrics.TagCount(metrics.ImportTypePre, total)

	doneManifests := map[digest.Digest]struct{}{}

	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": dbRepo.Path, "total": total})
	l.Info("processing tags")

	for i, fsTag := range fsTags {
		l := l.WithFields(log.Fields{"tag_name": fsTag, "count": i + 1})
		l.Info("processing tag")

		// read tag details from the filesystem
		desc, err := tagService.Get(ctx, fsTag)
		if err != nil {
			if errors.As(err, &distribution.ErrTagUnknown{}) {
				// this tag was either deleted since all tags were listed or the link was missing already, log and skip
				l.WithError(err).Warn("missing tag link, skipping")
				continue
			}
			if errors.Is(err, digest.ErrDigestInvalidFormat) {
				// the tag link is corrupted, just log a warning and skip
				l.WithError(err).Warn("broken tag link, skipping")
				continue
			}
			return fmt.Errorf("reading tag %q from filesystem: %w", fsTag, err)
		}

		// We should always fully pre-import a manifest (the manifest itself and its references) at least once per
		// pre-import run to avoid running into https://gitlab.com/gitlab-org/container-registry/-/issues/652. However,
		// there is no need to re-import the same manifest multiple times per pre-import (e.g. the same manifest with
		// multiple tags). Therefore, we keep a list of pre-imported manifests per run and only pre-import each once.
		if _, ok := doneManifests[desc.Digest]; ok {
			// for precaution, just double check that it does indeed exist on the database
			dbManifest, err := imp.repositoryStore.FindManifestByDigest(ctx, dbRepo, desc.Digest)
			if err != nil {
				return fmt.Errorf("finding tagged manifests in database: %w", err)
			}
			if dbManifest == nil {
				return fmt.Errorf("previously pre-imported manifest %q not found in database", desc.Digest.String())
			}
		} else {
			if err := imp.preImportManifest(ctx, fsRepo, dbRepo, desc.Digest); err != nil {
				if errors.Is(err, errManifestSkip) {
					continue
				}
				return fmt.Errorf("pre importing manifest: %w", err)
			}
			doneManifests[desc.Digest] = struct{}{}
		}
	}

	return nil
}

func (imp *Importer) preImportManifest(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository, dgst digest.Digest) error {
	manifestService, err := fsRepo.Manifests(ctx)
	if err != nil {
		return fmt.Errorf("constructing manifest service: %w", err)
	}

	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": dbRepo.Path, "digest": dgst})

	m, err := getFsManifest(ctx, manifestService, dgst, l)
	if err != nil {
		return err
	}

	switch fsManifest := m.(type) {
	case *manifestlist.DeserializedManifestList:
		l.Info("pre-importing manifest list")
		if _, err := imp.importManifestList(ctx, fsRepo, dbRepo, fsManifest, dgst); err != nil {
			return fmt.Errorf("pre importing manifest list: %w", err)
		}
	default:
		l.Info("pre-importing manifest")
		if _, err := imp.importManifest(ctx, fsRepo, dbRepo, fsManifest, dgst); err != nil {
			if errors.Is(err, distribution.ErrSchemaV1Unsupported) {
				l.WithError(err).Warn("skipping v1 manifest import")
				return nil
			}
			if errors.Is(err, errManifestSkip) {
				l.WithError(err).Warn("skipping manifest import")
				return nil
			}
			return fmt.Errorf("pre importing manifest: %w", err)
		}
	}

	return nil
}

func (imp *Importer) countRows(ctx context.Context) (map[string]int, error) {
	numRepositories, err := imp.repositoryStore.Count(ctx)
	if err != nil {
		return nil, err
	}
	numManifests, err := imp.manifestStore.Count(ctx)
	if err != nil {
		return nil, err
	}
	numBlobs, err := imp.blobStore.Count(ctx)
	if err != nil {
		return nil, err
	}
	numTags, err := imp.tagStore.Count(ctx)
	if err != nil {
		return nil, err
	}

	count := map[string]int{
		"repositories": numRepositories,
		"manifests":    numManifests,
		"blobs":        numBlobs,
		"tags":         numTags,
	}

	return count, nil
}

func (imp *Importer) isDatabaseEmpty(ctx context.Context) (bool, error) {
	counters, err := imp.countRows(ctx)
	if err != nil {
		return false, err
	}

	for _, c := range counters {
		if c > 0 {
			return false, nil
		}
	}

	return true, nil
}

// ImportAll populates the registry database with metadata from all repositories in the storage backend.
func (imp *Importer) ImportAll(ctx context.Context) error {
	var tx Transactor
	var err error

	// Add pre_import field to all subsequent logging.
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"pre_import": false, "dry_run": imp.dryRun})
	ctx = log.WithLogger(ctx, l)

	// Create a single transaction and roll it back at the end for dry runs.
	if imp.dryRun {
		tx, err = imp.beginTx(ctx)
		if err != nil {
			return fmt.Errorf("beginning dry run transaction: %w", err)
		}
		defer tx.Rollback()
	}

	start := time.Now()
	l.Info("starting metadata import")

	if imp.requireEmptyDatabase {
		empty, err := imp.isDatabaseEmpty(ctx)
		if err != nil {
			return fmt.Errorf("checking if database is empty: %w", err)
		}
		if !empty {
			return errors.New("non-empty database")
		}
	}

	if imp.importDanglingBlobs {
		var index int
		blobStart := time.Now()
		l.Info("importing all blobs")
		err := imp.registry.Blobs().Enumerate(ctx, func(desc distribution.Descriptor) error {
			index++
			l := l.WithFields(log.Fields{"digest": desc.Digest, "count": index, "size": desc.Size})
			l.Info("importing blob")

			dbBlob, err := imp.blobStore.FindByDigest(ctx, desc.Digest)
			if err != nil {
				return fmt.Errorf("checking for existence of blob: %w", err)
			}

			if dbBlob == nil {
				if err := imp.blobStore.Create(ctx, &models.Blob{MediaType: "application/octet-stream", Digest: desc.Digest, Size: desc.Size}); err != nil {
					return fmt.Errorf("creating blob in database: %w", err)
				}
			}

			// Even if we found the blob in the database, try to transfer in case it's
			// not present in blob storage on the transfer side.
			if err = imp.transferBlob(ctx, desc.Digest, desc.Size, metrics.BlobTypeUnknown); err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("importing blobs: %w", err)
		}

		blobEnd := time.Since(blobStart).Seconds()
		l.WithFields(log.Fields{"duration_s": blobEnd}).Info("blob import complete")
	}

	repositoryEnumerator, ok := imp.registry.(distribution.RepositoryEnumerator)
	if !ok {
		return errors.New("error building repository enumerator")
	}

	index := 0
	err = repositoryEnumerator.Enumerate(ctx, func(path string) error {
		if !imp.dryRun {
			tx, err = imp.beginTx(ctx)
			if err != nil {
				return fmt.Errorf("beginning repository transaction: %w", err)
			}
			defer tx.Rollback()
		}

		index++
		repoStart := time.Now()
		l := l.WithFields(log.Fields{"repository": path, "count": index})
		l.Info("importing repository")

		if err := imp.importRepository(ctx, path); err != nil {
			l.WithError(err).Error("error importing repository")
			// if the storage driver failed to find a repository path (usually due to missing `_manifests/revisions`
			// or `_manifests/tags` folders) continue to the next one, otherwise stop as the error is unknown.
			if !(errors.As(err, &driver.PathNotFoundError{}) || errors.As(err, &distribution.ErrRepositoryUnknown{})) {
				return err
			}
			return nil
		}

		repoEnd := time.Since(repoStart).Seconds()
		l.WithFields(log.Fields{"duration_s": repoEnd}).Info("repository import complete")

		if !imp.dryRun {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit repository transaction: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	// This should only delay during testing.
	time.Sleep(imp.testingDelay)

	if !imp.dryRun {
		// reset stores to use the main connection handler instead of the last (committed/rolled back) transaction
		imp.loadStores(imp.db)
	}

	if imp.rowCount {
		counters, err := imp.countRows(ctx)
		if err != nil {
			l.WithError(err).Error("counting table rows")
		}

		logCounters := make(map[string]interface{}, len(counters))
		for t, n := range counters {
			logCounters[t] = n
		}
		l = l.WithFields(logCounters)
	}

	t := time.Since(start).Seconds()
	l.WithFields(log.Fields{"duration_s": t}).Info("metadata import complete")

	return err
}

// Import populates the registry database with metadata from a specific repository in the storage backend.
func (imp *Importer) Import(ctx context.Context, path string) error {
	tx, err := imp.beginTx(ctx)
	if err != nil {
		return fmt.Errorf("begin repository transaction: %w", err)
	}
	defer tx.Rollback()

	// Add specific log fields to all subsequent log entries.
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"pre_import": false,
		"dry_run":    imp.dryRun,
		"component":  "importer",
	})
	ctx = log.WithLogger(ctx, l)

	start := time.Now()
	l = l.WithFields(log.Fields{"repository": path})
	l.Info("starting metadata import")

	if imp.requireEmptyDatabase {
		empty, err := imp.isDatabaseEmpty(ctx)
		if err != nil {
			return fmt.Errorf("checking if database is empty: %w", err)
		}
		if !empty {
			return errors.New("non-empty database")
		}
	}

	l.Info("importing repository")
	if err := imp.importRepository(ctx, path); err != nil {
		l.WithError(err).Error("error importing repository")
		return err
	}

	// This should only delay during testing.
	timer := time.NewTimer(imp.testingDelay)
	select {
	case <-timer.C:
		// do nothing
		l.Debug("done waiting for slow import test")
	case <-ctx.Done():
		return nil
	}

	if imp.rowCount {
		counters, err := imp.countRows(ctx)
		if err != nil {
			l.WithError(err).Error("counting table rows")
		}

		logCounters := make(map[string]interface{}, len(counters))
		for t, n := range counters {
			logCounters[t] = n
		}
		l = l.WithFields(logCounters)
	}

	t := time.Since(start).Seconds()
	l.WithFields(log.Fields{"duration_s": t}).Info("metadata import complete")
	if imp.dryRun {
		return err
	}

	err = imp.checkStatusAndCommitTx(ctx, path, tx)
	if err != nil {
		if errors.Is(err, ErrImportCanceled) {
			l.Warn("import was canceled before committing transaction")
			return err
		}

		l.WithError(err).Error("committing transaction for final import")
	}

	return err
}

func (imp *Importer) checkStatusAndCommitTx(ctx context.Context, path string, tx Transactor) error {
	if err := imp.checkStatus(ctx, path); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit repository transaction: %w", err)
	}

	return nil
}

func (imp *Importer) checkStatus(ctx context.Context, path string) error {
	dbRepo, err := imp.repositoryStore.FindByPath(ctx, path)
	if err != nil {
		return fmt.Errorf("getting repository after import completed: %w", err)
	}

	if dbRepo == nil {
		return v2.ErrorCodeNameUnknown
	}

	switch dbRepo.MigrationStatus {
	case migration.RepositoryStatusImportCanceled:
		return ErrImportCanceled
	case migration.RepositoryStatusPreImportCanceled:
		return ErrPreImportCanceled
	}

	return nil
}

// PreImport populates repository data without including any tag information.
// Running pre-import can reduce the runtime of an Import against the same
// repository and, with online garbage collection enabled, does not require a
// repository to be read-only.
func (imp *Importer) PreImport(ctx context.Context, path string) error {
	var tx Transactor
	var err error

	// Add specific log fields to all subsequent log entries.
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"pre_import": true,
		"component":  "importer",
	})
	ctx = log.WithLogger(ctx, l)

	// Create a single transaction and roll it back at the end for dry runs.
	if imp.dryRun {
		tx, err = imp.beginTx(ctx)
		if err != nil {
			return fmt.Errorf("begin dry run transaction: %w", err)
		}
		defer tx.Rollback()
	}

	if imp.requireEmptyDatabase {
		empty, err := imp.isDatabaseEmpty(ctx)
		if err != nil {
			return fmt.Errorf("checking if database is empty: %w", err)
		}
		if !empty {
			return errors.New("non-empty database")
		}
	}

	start := time.Now()
	l = l.WithFields(log.Fields{"repository": path})
	l.Info("starting repository pre-import")

	named, err := reference.WithName(path)
	if err != nil {
		return fmt.Errorf("parsing repository name: %w", err)
	}
	fsRepo, err := imp.registry.Repository(ctx, named)
	if err != nil {
		return fmt.Errorf("constructing filesystem repository: %w", err)
	}

	dbRepo, err := imp.repositoryStore.CreateOrFindByPath(ctx, path)
	if err != nil {
		return fmt.Errorf("creating or finding repository in database: %w", err)
	}

	if err = imp.preImportTaggedManifests(ctx, fsRepo, dbRepo); err != nil {
		return fmt.Errorf("pre importing tagged manifests: %w", err)
	}

	if imp.testingDelay < 0 {
		return errNegativeTestingDelay
	}

	// This should only delay during testing.
	timer := time.NewTimer(imp.testingDelay)
	select {
	case <-timer.C:
		// do nothing
		l.Debug("done waiting for slow pre import test")
	case <-ctx.Done():
		return nil
	}

	if !imp.dryRun {
		// reset stores to use the main connection handler instead of the last (committed/rolled back) transaction
		imp.loadStores(imp.db)
	}

	if imp.rowCount {
		counters, err := imp.countRows(ctx)
		if err != nil {
			l.WithError(err).Error("counting table rows")
		}

		logCounters := make(map[string]interface{}, len(counters))
		for t, n := range counters {
			logCounters[t] = n
		}
		l = l.WithFields(logCounters)
	}

	if err := imp.checkStatus(ctx, path); err != nil {
		if errors.Is(err, ErrPreImportCanceled) {
			l.Warn("pre import was canceled before completion")
		}
		return err
	}

	t := time.Since(start).Seconds()
	l.WithFields(log.Fields{"duration_s": t}).Info("pre-import complete")

	return nil
}
