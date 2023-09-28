package datastore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/jackc/pgconn"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/api/googleapi"
)

var (
	errNegativeTestingDelay = errors.New("negative testing delay")
	errManifestSkip         = errors.New("the manifest is invalid and its (pre)import should be skipped")
)

const mtOctetStream = "application/octet-stream"

// Importer populates the registry database with filesystem metadata. This is only meant to be used for an initial
// one-off migration, starting with an empty database.
type Importer struct {
	registry        distribution.Namespace
	db              *DB
	repositoryStore RepositoryStore
	manifestStore   ManifestStore
	tagStore        TagStore
	blobStore       BlobStore

	importDanglingManifests bool
	importDanglingBlobs     bool
	requireEmptyDatabase    bool
	dryRun                  bool
	tagConcurrency          int
	rowCount                bool
	testingDelay            time.Duration
	preImportRetryTimeout   time.Duration
}

// ImporterOption provides functional options for the Importer.
type ImporterOption func(*Importer)

// WithImportDanglingManifests configures the Importer to import all manifests
// rather than only tagged manifests.
//
// Deprecated: WithImportDanglingManifests is a legacy option that is no longer used
// in the import command made available to the user.
func WithImportDanglingManifests(imp *Importer) {
	imp.importDanglingManifests = true
}

// WithImportDanglingBlobs configures the Importer to import all blobs
// rather than only blobs referenced by manifests.
//
// Deprecated: WithImportDanglingBlobs is a legacy option that is no longer used
// in the import command made available to the user.
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

// WithPreImportRetryTimeout configures the Importer with a retry timeout for certain operations
// due to network connection errors or DB timeouts. See shouldRetryManifestPreImport for more details.
func WithPreImportRetryTimeout(d time.Duration) ImporterOption {
	return func(imp *Importer) {
		imp.preImportRetryTimeout = d
	}
}

// NewImporter creates a new Importer.
func NewImporter(db *DB, registry distribution.Namespace, opts ...ImporterOption) *Importer {
	imp := &Importer{
		registry:       registry,
		db:             db,
		tagConcurrency: 1,
		// default manifest pre import retry timeout
		preImportRetryTimeout: time.Minute,
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

	return nil
}

func (imp *Importer) importLayers(ctx context.Context, dbRepo *models.Repository, fsRepo distribution.Repository, fsLayers []distribution.Descriptor) ([]*models.Blob, error) {
	total := len(fsLayers)

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

		// Use the generic octet stream media type for common blob storage, but set
		// the original fs media type on the *models.Blob object populated by importLayer.
		// This way, when the layers are associated with the manifest, the
		// manifest-layer associations record the layer media type in the manifest JSON.
		layer := &models.Blob{MediaType: mtOctetStream, Digest: fsLayer.Digest, Size: fsLayer.Size}
		if err := imp.importLayer(ctx, dbRepo, layer); err != nil {
			return dbLayers, err
		}
		layer.MediaType = fsLayer.MediaType

		dbLayers = append(dbLayers, layer)
	}

	return dbLayers, nil
}

func (imp *Importer) importManifestV2(ctx context.Context, fsRepo distribution.Repository, dbRepo *models.Repository, m distribution.ManifestV2, dgst digest.Digest, payload []byte, nonConformant bool) (*models.Manifest, error) {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": dbRepo.Path})

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

	configPayload, err := getConfigPayload(ctx, m.Config(), fsRepo)
	if err != nil {
		return nil, err
	}

	// Use the generic octet stream media type for common blob storage, but set
	// the original media type on the *models.Blob object populated by CreateOrFind.
	// This way, when the configuration is stored with the manifest, the media
	// type will match what is present in the manifest JSON.
	dbConfigBlob.MediaType = mtOctetStream
	if err := imp.blobStore.CreateOrFind(ctx, dbConfigBlob); err != nil {
		return nil, err
	}
	dbConfigBlob.MediaType = m.Config().MediaType

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

// getConfigPayload will read the configuration payload from fsRepo only if the manifest configuration size is
// smaller than ConfigSizeLimit
func getConfigPayload(ctx context.Context, m distribution.Descriptor, fsRepo distribution.Repository) ([]byte, error) {
	if m.Size > ConfigSizeLimit {
		return nil, nil
	}

	l := log.GetLogger(log.WithContext(ctx))

	// get configuration blob payload
	blobStore := fsRepo.Blobs(ctx)
	configPayload, err := blobStore.Get(ctx, m.Digest)
	if err != nil {
		if errors.Is(err, digest.ErrDigestInvalidFormat) {
			l.WithError(err).Warn("broken configuration layer link, skipping")
			return nil, errManifestSkip
		}
		if errors.Is(err, distribution.ErrBlobUnknown) {
			// This error might happen if the config blob is not present on common, so
			// this might shadow that as a simple "config unlinked" problem. However,
			// we haven't seen such an error before, and even if we do, we can't bring
			// such a blob back to life. So we simply skip here regardless.
			l.WithError(err).Warn("configuration blob not linked, skipping")
			return nil, errManifestSkip
		}
		if errors.Is(err, digest.ErrDigestInvalidFormat) {
			l.WithError(err).Warn("broken config link, skipping")
			return nil, errManifestSkip
		}
		return nil, fmt.Errorf("obtaining configuration payload: %w", err)
	}

	return configPayload, nil
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
		fsManifest, err := getFsManifest(ctx, manifestService, m.Digest, l)
		if err != nil {
			if errors.Is(err, errManifestSkip) {
				// Skipping the import of this broken referenced manifest will lead to a partially broken list. We could
				// skip the import of the referencing list as well, but it's already broken on the old registry
				// (filesystem metadata) so it's preferable to keep the pull behavior consistent across old and new.
				continue
			}
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
		return nil, fmt.Errorf("unknown manifest class digest=%s repository=%s", dgst, dbRepo.Path)
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
		log.GetLogger(log.WithContext(ctx)).Warn("beginning legacy dangling manifests import")
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
			switch {
			case errors.Is(err, distribution.ErrSchemaV1Unsupported):
				l.WithError(err).Warn("skipping v1 manifest import")
				return errManifestSkip
			case errors.Is(err, errManifestSkip):
				l.WithError(err).Warn("skipping manifest import")
				return err
			default:
				if shouldRetryManifestPreImport(err) {
					return imp.retryImportManifestWithBackoff(l, fsRepo, fsManifest, dbRepo, dgst)
				}
			}

			return err
		}
	}

	return nil
}

func (imp *Importer) retryImportManifestWithBackoff(l log.Logger, fsRepo distribution.Repository, fsManifest distribution.Manifest, dbRepo *models.Repository, dgst digest.Digest) error {
	backOff := backoff.NewExponentialBackOff()
	backOff.InitialInterval = 100 * time.Millisecond
	backOff.MaxElapsedTime = imp.preImportRetryTimeout
	count := 0

	operation := func() error {
		count++
		l = l.WithFields(log.Fields{"retry_count": count})
		l.Info("retrying pre import manifest")

		// use a new context with an extended deadline just for this retry
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if _, retryErr := imp.importManifest(ctx, fsRepo, dbRepo, fsManifest, dgst); retryErr != nil {
			err := fmt.Errorf("retrying pre import manifest %s: %w", dgst, retryErr)
			l.WithError(err).Warn("pre import retry failed")
			return err
		}

		return nil
	}

	return backoff.Retry(operation, backOff)
}

// shouldRetryManifestPreImport checks the returned error from importManifest and decides
// whether the manifest pre import should be retried based on the types of errors.
func shouldRetryManifestPreImport(err error) bool {
	// check for generic network timeouts
	var netError net.Error
	if errors.As(err, &netError) && netError.Timeout() {
		return true
	}

	// check for connection reset errors
	var netOpError *net.OpError
	if errors.As(err, &netOpError) {
		var syscallErr *os.SyscallError
		if errors.As(err, &syscallErr) && syscallErr.Err == syscall.ECONNRESET {
			return true
		}
	}

	// check DB connection errors and see if it's safe to retry
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgconn.SafeToRetry(pgErr) {
		return true
	}

	// special condition for GCS 503 responses used for gitlab.com, we should find a
	// generic way to expose this error for all drivers https://gitlab.com/gitlab-org/container-registry/-/issues/707
	var gcsErr *googleapi.Error
	if errors.As(err, &gcsErr) {
		if gcsErr.Code == http.StatusServiceUnavailable {
			return true
		}
	}

	return false
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
//
// Deprecated: ImportAll is the original implementation and should no longer be used, use FullImport instead.
func (imp *Importer) ImportAll(ctx context.Context) error {
	var tx Transactor
	var err error

	// Add pre_import field to all subsequent logging.
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"pre_import": false, "dry_run": imp.dryRun, "legacy": true})
	ctx = log.WithLogger(ctx, l)
	l.Warn("this is the legacy full import method, do not use on production registries")

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
		if err := imp.importBlobs(ctx); err != nil {
			return fmt.Errorf("importing blobs: %w", err)
		}
	}

	if err := imp.importAllRepositories(ctx); err != nil {
		return err
	}

	// This should only delay during testing.
	time.Sleep(imp.testingDelay)

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

type step int

const (
	unknown step = iota // Always first to catch uninitialized values.
	preImport
	repoImport
	commonBlobs
)

// doImport manages which import steps to run and ensure pre and post import
// tasks are handled consistently across import steps. Included import steps are
// always ran in the following order: pre import, repository import, common blobs.
// The function signature requires at least one step to be included, but allows multiple.
func (imp *Importer) doImport(ctx context.Context, required step, steps ...step) error {
	var (
		tx                Transactor
		err               error
		pre, repos, blobs bool
	)

	// Assign each valid step to a boolean value. This ensures that we only run a
	// particular step once and in the correct order.
	steps = append(steps, required)
	for _, s := range steps {
		switch s {
		case preImport:
			pre = true
		case repoImport:
			repos = true
		case commonBlobs:
			blobs = true
		default:
			return fmt.Errorf("unknown import step: %v", s)
		}
	}

	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"pre_import":        pre,
		"repository_import": repos,
		"common_blobs":      blobs,
		"dry_run":           imp.dryRun,
	})
	ctx = log.WithLogger(ctx, l)

	if imp.requireEmptyDatabase {
		empty, err := imp.isDatabaseEmpty(ctx)
		if err != nil {
			return fmt.Errorf("checking if database is empty: %w", err)
		}
		if !empty {
			return errors.New("non-empty database")
		}
	}

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

	if pre {
		if err := imp.preImportAllRepositories(ctx); err != nil {
			return fmt.Errorf("pre importing all repositories: %w", err)
		}
	}
	if repos {
		if err := imp.importAllRepositories(ctx); err != nil {
			return fmt.Errorf("importing all repositories: %w", err)
		}
	}
	if blobs {
		if err := imp.importBlobs(ctx); err != nil {
			return fmt.Errorf("importing blobs: %w", err)
		}
	}

	t := time.Since(start).Seconds()

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

	l.WithFields(log.Fields{"duration_s": t}).Info("metadata import complete")

	return err
}

// FullImport populates the registry database with metadata from all repositories in the storage backend.
func (imp *Importer) FullImport(ctx context.Context) error {
	return imp.doImport(ctx, preImport, repoImport, commonBlobs)
}

// PreImportAll populates repository data without including any tag information.
// This command is safe to run without read-only mode enabled on the registry.
func (imp *Importer) PreImportAll(ctx context.Context) error {
	return imp.doImport(ctx, preImport)
}

// ImportAllRepositories populates all repository data, when used after a pre import
// cycle, this data will largely include only tags, but if a tag is not
// associated with an existing manifest, all metadata associated with that
// manifest will be imported. This command must only be used when read-only
// mode is enabled on the registry.
func (imp *Importer) ImportAllRepositories(ctx context.Context) error {
	return imp.doImport(ctx, repoImport)
}

// ImportBlobs populates the registry database with metadata from all blobs in the storage backend.
func (imp *Importer) ImportBlobs(ctx context.Context) error {
	return imp.doImport(ctx, commonBlobs)
}

func (imp *Importer) preImportAllRepositories(ctx context.Context) error {
	repositoryEnumerator, ok := imp.registry.(distribution.RepositoryEnumerator)
	if !ok {
		return errors.New("building repository enumerator")
	}

	index := 0
	return repositoryEnumerator.Enumerate(ctx, func(path string) error {
		index++
		repoStart := time.Now()
		l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": path, "count": index})
		l.Info("pre importing repository")

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
			l.WithError(err).Error("pre importing tagged manifests")
			// if the storage driver failed to find a repository path (usually due to missing `_manifests/revisions`
			// or `_manifests/tags` folders) continue to the next one, otherwise stop as the error is unknown.
			if !(errors.As(err, &driver.PathNotFoundError{}) || errors.As(err, &distribution.ErrRepositoryUnknown{})) {
				return fmt.Errorf("pre importing tagged manifests: %w", err)
			}
			return nil
		}

		repoEnd := time.Since(repoStart).Seconds()
		l.WithFields(log.Fields{"duration_s": repoEnd}).Info("repository pre import complete")

		return nil
	})
}

func (imp *Importer) importBlobs(ctx context.Context) error {
	var index int
	start := time.Now()
	l := log.GetLogger(log.WithContext(ctx))
	l.Info("importing all blobs")

	if err := imp.registry.Blobs().Enumerate(ctx, func(desc distribution.Descriptor) error {
		index++
		l.WithFields(log.Fields{"digest": desc.Digest, "count": index, "size": desc.Size}).Info("importing blob")

		dbBlob, err := imp.blobStore.FindByDigest(ctx, desc.Digest)
		if err != nil {
			return fmt.Errorf("checking for existence of blob: %w", err)
		}

		if dbBlob == nil {
			if err := imp.blobStore.Create(ctx, &models.Blob{MediaType: mtOctetStream, Digest: desc.Digest, Size: desc.Size}); err != nil {
				return fmt.Errorf("creating blob in database: %w", err)
			}
		}

		return nil
	}); err != nil {
		return err
	}

	end := time.Since(start).Seconds()
	l.WithFields(log.Fields{"duration_s": end}).Info("blob import complete")

	return nil
}

func (imp *Importer) importAllRepositories(ctx context.Context) error {
	var tx Transactor
	var err error

	repositoryEnumerator, ok := imp.registry.(distribution.RepositoryEnumerator)
	if !ok {
		return errors.New("error building repository enumerator")
	}

	index := 0
	return repositoryEnumerator.Enumerate(ctx, func(path string) error {
		if !imp.dryRun {
			tx, err = imp.beginTx(ctx)
			if err != nil {
				return fmt.Errorf("beginning repository transaction: %w", err)
			}
			defer tx.Rollback()
		}

		index++
		start := time.Now()
		l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": path, "count": index})
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

		end := time.Since(start).Seconds()
		l.WithFields(log.Fields{"duration_s": end}).Info("repository import complete")

		if !imp.dryRun {
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("commit repository transaction: %w", err)
			}

			// reset stores to use the main connection handler instead of the last (committed/rolled back) transaction
			imp.loadStores(imp.db)
		}

		return nil
	})
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

	if err = tx.Commit(); err != nil {
		l.WithError(err).Error("committing transaction for final import")
	}

	return err
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

	t := time.Since(start).Seconds()
	l.WithFields(log.Fields{"duration_s": t}).Info("pre-import complete")

	return nil
}
