package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/notifications/meta"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/uuid"
	"github.com/opencontainers/go-digest"
)

// linkPathFunc describes a function that can resolve a link based on the
// repository name and digest.
type linkPathFunc func(name string, dgst digest.Digest) (string, error)

// linkedBlobStore provides a full BlobService that namespaces the blobs to a
// given repository. Effectively, it manages the links in a given repository
// that grant access to the global blob store.
type linkedBlobStore struct {
	*blobStore
	registry               *registry
	blobServer             distribution.BlobServer
	blobAccessController   distribution.BlobDescriptorService
	repository             distribution.Repository
	ctx                    context.Context // only to be used where context can't come through method args
	deleteEnabled          bool
	resumableDigestEnabled bool

	// when set to true, it will not write blob link paths to filesystem,
	// but still allow blob puts to common blob store
	useDatabase bool

	// linkPath allows one to control the repository blob link set to which
	// the blob store dispatches. This allows linked blobs stored at different
	// paths to be operated on with the same logic.
	linkPath linkPathFunc

	// linkDirectoryPathSpec locates the root directories in which one might find links
	linkDirectoryPathSpec pathSpec
}

var _ distribution.BlobStore = &linkedBlobStore{}

func (lbs *linkedBlobStore) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	return lbs.blobAccessController.Stat(ctx, dgst)
}

func (lbs *linkedBlobStore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	canonical, err := lbs.Stat(ctx, dgst) // access check
	if err != nil {
		return nil, err
	}

	return lbs.blobStore.Get(ctx, canonical.Digest)
}

func (lbs *linkedBlobStore) Open(ctx context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	canonical, err := lbs.Stat(ctx, dgst) // access check
	if err != nil {
		return nil, err
	}

	return lbs.blobStore.Open(ctx, canonical.Digest)
}

func (lbs *linkedBlobStore) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (*meta.Blob, error) {
	var d digest.Digest

	// skip checking the storage backend when using the database
	if lbs.useDatabase {
		d = dgst
	} else {
		canonical, err := lbs.Stat(ctx, dgst) // access check
		if err != nil {
			return nil, err
		}

		if canonical.MediaType != "" {
			// Set the repository local content type.
			w.Header().Set("Content-Type", canonical.MediaType)
		}
		d = canonical.Digest
	}

	return lbs.blobServer.ServeBlob(ctx, w, r, d)
}

func (lbs *linkedBlobStore) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	dgst := digest.FromBytes(p)
	// Place the data in the blob store first.
	desc, err := lbs.blobStore.Put(ctx, mediaType, p)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error putting into main store: %v", err)
		return distribution.Descriptor{}, err
	}

	if err := lbs.blobAccessController.SetDescriptor(ctx, dgst, desc); err != nil {
		return distribution.Descriptor{}, err
	}

	return desc, lbs.linkBlob(ctx, desc)
}

type optionFunc func(any) error

func (f optionFunc) Apply(v any) error {
	return f(v)
}

// WithMountFrom returns a BlobCreateOption which designates that the blob should be
// mounted from the given canonical reference.
func WithMountFrom(ref reference.Canonical) distribution.BlobCreateOption {
	return optionFunc(func(v any) error {
		opts, ok := v.(*distribution.CreateOptions)
		if !ok {
			return fmt.Errorf("unexpected options type: %T", v)
		}

		opts.Mount.ShouldMount = true
		opts.Mount.From = ref

		return nil
	})
}

// WithMountFromStat returns a BlobCreateOption which designates that the blob
// should be mounted from the given canonical reference and that the repository
// access check has already been performed.
func WithMountFromStat(ref reference.Canonical, stat *distribution.Descriptor) distribution.BlobCreateOption {
	return optionFunc(func(v any) error {
		opts, ok := v.(*distribution.CreateOptions)
		if !ok {
			return fmt.Errorf("unexpected options type: %T", v)
		}

		opts.Mount.ShouldMount = true
		opts.Mount.From = ref
		opts.Mount.Stat = stat

		return nil
	})
}

// Writer begins a blob write session, returning a handle.
func (lbs *linkedBlobStore) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	dcontext.GetLogger(ctx).Debug("(*linkedBlobStore).Writer")

	var opts distribution.CreateOptions

	for _, option := range options {
		err := option.Apply(&opts)
		if err != nil {
			return nil, err
		}
	}

	if opts.Mount.ShouldMount {
		desc, err := lbs.mount(ctx, opts.Mount.From, opts.Mount.From.Digest(), opts.Mount.Stat)
		if err == nil {
			// Mount successful, no need to initiate an upload session
			return nil, distribution.ErrBlobMounted{From: opts.Mount.From, Descriptor: desc}
		}
	}

	u := uuid.Generate().String()
	startedAt := time.Now().UTC()

	p, err := pathFor(uploadDataPathSpec{
		name: lbs.repository.Named().Name(),
		id:   u,
	})
	if err != nil {
		return nil, err
	}

	startedAtPath, err := pathFor(uploadStartedAtPathSpec{
		name: lbs.repository.Named().Name(),
		id:   u,
	})
	if err != nil {
		return nil, err
	}

	// Write a startedat file for this upload
	if err := lbs.blobStore.driver.PutContent(ctx, startedAtPath, []byte(startedAt.Format(time.RFC3339))); err != nil {
		return nil, err
	}

	return lbs.newBlobUpload(ctx, u, p, startedAt, false)
}

func (lbs *linkedBlobStore) Resume(ctx context.Context, id string) (distribution.BlobWriter, error) {
	dcontext.GetLogger(ctx).Debug("(*linkedBlobStore).Resume")

	startedAtPath, err := pathFor(uploadStartedAtPathSpec{
		name: lbs.repository.Named().Name(),
		id:   id,
	})
	if err != nil {
		return nil, err
	}

	startedAtBytes, err := lbs.blobStore.driver.GetContent(ctx, startedAtPath)
	if err != nil {
		if errors.As(err, new(driver.PathNotFoundError)) {
			return nil, distribution.ErrBlobUploadUnknown
		}

		return nil, err
	}

	startedAt, err := time.Parse(time.RFC3339, string(startedAtBytes))
	if err != nil {
		return nil, err
	}

	p, err := pathFor(uploadDataPathSpec{
		name: lbs.repository.Named().Name(),
		id:   id,
	})
	if err != nil {
		return nil, err
	}

	return lbs.newBlobUpload(ctx, id, p, startedAt, true)
}

func (lbs *linkedBlobStore) Delete(ctx context.Context, dgst digest.Digest) error {
	if !lbs.deleteEnabled {
		return distribution.ErrUnsupported
	}

	// Ensure the blob is available for deletion
	_, err := lbs.blobAccessController.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	err = lbs.blobAccessController.Clear(ctx, dgst)
	if err != nil {
		return err
	}

	return nil
}

// Enumerate will traverse the repository looking for link files. When one is found, the content is parsed (descriptor) and
// sent to ingestor. If a link file is corrupted (e.g. 0B in size or invalid digest) it is ignored, and the walk continues.
// The ingestor function must be thread-safe.
func (lbs *linkedBlobStore) Enumerate(ctx context.Context, ingestor func(distribution.Descriptor) error) error {
	rootPath, err := pathFor(lbs.linkDirectoryPathSpec)
	if err != nil {
		return err
	}
	return lbs.driver.WalkParallel(ctx, rootPath, func(fileInfo driver.FileInfo) error {
		// exit early if directory...
		if fileInfo.IsDir() {
			return nil
		}
		filePath := fileInfo.Path()

		// check if it's a link
		_, fileName := path.Split(filePath)
		if fileName != "link" {
			return nil
		}

		// read the digest found in link
		d, err := lbs.blobStore.readlink(ctx, filePath)
		if err != nil {
			// ignore if the link file is empty or doesn't contain a valid checksum (the GC should erase the blobs during the sweep stage)
			if errors.Is(err, digest.ErrDigestInvalidFormat) {
				dcontext.GetLoggerWithField(ctx, "path", filePath).Warnf("invalid link file, ignoring")
				return nil
			}
			return err
		}

		// ensure this conforms to the linkPathFns
		_, err = lbs.Stat(ctx, d)
		if err != nil {
			// we expect this error to occur so we move on
			if err == distribution.ErrBlobUnknown {
				return nil
			}
			return err
		}

		err = ingestor(distribution.Descriptor{
			Size:   fileInfo.Size(),
			Digest: d,
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func (lbs *linkedBlobStore) mount(ctx context.Context, sourceRepo reference.Named, dgst digest.Digest, sourceStat *distribution.Descriptor) (distribution.Descriptor, error) {
	var stat distribution.Descriptor
	if sourceStat == nil {
		// look up the blob info from the sourceRepo if not already provided
		repo, err := lbs.registry.Repository(ctx, sourceRepo)
		if err != nil {
			return distribution.Descriptor{}, err
		}
		stat, err = repo.Blobs(ctx).Stat(ctx, dgst)
		if err != nil {
			return distribution.Descriptor{}, err
		}
	} else {
		// use the provided blob info
		stat = *sourceStat
	}

	desc := distribution.Descriptor{
		Size: stat.Size,

		// NOTE(stevvooe): The central blob store firewalls media types from
		// other users. The caller should look this up and override the value
		// for the specific repository.
		MediaType: "application/octet-stream",
		Digest:    dgst,
	}
	return desc, lbs.linkBlob(ctx, desc)
}

// newBlobUpload allocates a new upload controller with the given state.
func (lbs *linkedBlobStore) newBlobUpload(ctx context.Context, u, targetPath string, startedAt time.Time, doAppend bool) (distribution.BlobWriter, error) {
	fw, err := lbs.driver.Writer(ctx, targetPath, doAppend)
	if err != nil {
		return nil, err
	}

	bw := &blobWriter{
		ctx:                    ctx,
		blobStore:              lbs,
		id:                     u,
		startedAt:              startedAt,
		digester:               digest.Canonical.Digester(),
		fileWriter:             fw,
		driver:                 lbs.driver,
		path:                   targetPath,
		resumableDigestEnabled: lbs.resumableDigestEnabled,
	}

	return bw, nil
}

// linkBlob links a valid, written blob into the registry under the named
// repository for the upload controller.
func (lbs *linkedBlobStore) linkBlob(ctx context.Context, canonical distribution.Descriptor, aliases ...digest.Digest) error {
	if lbs.useDatabase {
		return nil
	}

	dgsts := append([]digest.Digest{canonical.Digest}, aliases...)

	// Don't make duplicate links.
	seenDigests := make(map[digest.Digest]struct{}, len(dgsts))

	for _, dgst := range dgsts {
		if _, seen := seenDigests[dgst]; seen {
			continue
		}
		seenDigests[dgst] = struct{}{}

		blobLinkPath, err := lbs.linkPath(lbs.repository.Named().Name(), dgst)
		if err != nil {
			return err
		}

		if err := lbs.blobStore.link(ctx, blobLinkPath, canonical.Digest); err != nil {
			return err
		}
	}

	return nil
}

type linkedBlobStatter struct {
	*blobStore
	repository distribution.Repository

	// linkPath allows one to control the repository blob link set to which
	// the blob store dispatches. This allows linked blobs stored at different
	// paths to be operated on with the same logic.
	linkPath linkPathFunc
}

var _ distribution.BlobDescriptorService = &linkedBlobStatter{}

func (lbs *linkedBlobStatter) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	blobLinkPath, err := lbs.linkPath(lbs.repository.Named().Name(), dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	target, err := lbs.blobStore.readlink(ctx, blobLinkPath)
	if err != nil {
		if errors.As(err, new(driver.PathNotFoundError)) {
			return distribution.Descriptor{}, distribution.ErrBlobUnknown
		}

		return distribution.Descriptor{}, err
	}

	if target != dgst {
		// Track when we are doing cross-digest domain lookups. ie, sha512 to sha256.
		dcontext.GetLogger(ctx).Warnf("looking up blob with canonical target: %v -> %v", dgst, target)
	}

	return lbs.blobStore.statter.Stat(ctx, target)
}

func (lbs *linkedBlobStatter) Clear(ctx context.Context, dgst digest.Digest) (err error) {
	blobLinkPath, err := lbs.linkPath(lbs.repository.Named().Name(), dgst)
	if err != nil {
		return err
	}

	return lbs.blobStore.driver.Delete(ctx, blobLinkPath)
}

func (*linkedBlobStatter) SetDescriptor(_ context.Context, _ digest.Digest, _ distribution.Descriptor) error {
	// The canonical descriptor for a blob is set at the commit phase of upload
	return nil
}

// blobLinkPath provides the path to the blob link, also known as layers.
func blobLinkPath(name string, dgst digest.Digest) (string, error) {
	return pathFor(layerLinkPathSpec{name: name, digest: dgst})
}

// manifestRevisionLinkPath provides the path to the manifest revision link.
func manifestRevisionLinkPath(name string, dgst digest.Digest) (string, error) {
	return pathFor(manifestRevisionLinkPathSpec{name: name, revision: dgst})
}
