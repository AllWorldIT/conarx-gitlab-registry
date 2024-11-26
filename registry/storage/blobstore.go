package storage

import (
	"context"
	"errors"
	"fmt"
	"path"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// blobStore implements the read side of the blob store interface over a
// driver without enforcing per-repository membership. This object is
// intentionally a leaky abstraction, providing utility methods that support
// creating and traversing backend links.
type blobStore struct {
	driver  driver.StorageDriver
	statter distribution.BlobStatter
}

var _ distribution.BlobProvider = &blobStore{}

// Get implements the BlobReadService.Get call.
func (bs *blobStore) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	bp, err := bs.path(dgst)
	if err != nil {
		return nil, err
	}

	p, err := getContent(ctx, bs.driver, bp)
	if err != nil {
		if errors.As(err, new(driver.PathNotFoundError)) {
			return nil, distribution.ErrBlobUnknown
		}

		return nil, err
	}

	return p, nil
}

func (bs *blobStore) Open(ctx context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return nil, err
	}

	p, err := bs.path(desc.Digest)
	if err != nil {
		return nil, err
	}

	fr := newFileReader(ctx, bs.driver, p, desc.Size)
	return fr, nil
}

// Put stores the content p in the blob store, calculating the digest. If the
// content is already present, only the digest will be returned. This should
// only be used for small objects, such as manifests. This implemented as a convenience for other Put implementations
func (bs *blobStore) Put(ctx context.Context, _ string, p []byte) (distribution.Descriptor, error) {
	dgst := digest.FromBytes(p)
	desc, err := bs.statter.Stat(ctx, dgst)
	if err == nil {
		// content already present
		return desc, nil
	} else if err != distribution.ErrBlobUnknown {
		dcontext.GetLogger(ctx).Errorf("blobStore: error stating content (%v): %v", dgst, err)
		// real error, return it
		return distribution.Descriptor{}, err
	}

	bp, err := bs.path(dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	return distribution.Descriptor{
		Size: int64(len(p)),

		// NOTE(stevvooe): The central blob store firewalls media types from
		// other users. The caller should look this up and override the value
		// for the specific repository.
		MediaType: "application/octet-stream",
		Digest:    dgst,
	}, bs.driver.PutContent(ctx, bp, p)
}

// Enumerate will travers the repository, calling the ingester function once
// per descriptor encountered. The ingester function must be thread-safe.
func (bs *blobStore) Enumerate(ctx context.Context, ingester func(descriptor distribution.Descriptor) error) error {
	specPath, err := pathFor(blobsPathSpec{})
	if err != nil {
		return err
	}

	return bs.driver.WalkParallel(ctx, specPath, func(fileInfo driver.FileInfo) error {
		// skip directories
		if fileInfo.IsDir() {
			return nil
		}

		currentPath := fileInfo.Path()
		// we only want to parse paths that end with /data
		_, fileName := path.Split(currentPath)
		if fileName != "data" {
			return nil
		}

		digest, err := digestFromPath(currentPath)
		if err != nil {
			return fmt.Errorf("failed to parse digest from path %s: %w", currentPath, err)
		}

		return ingester(distribution.Descriptor{
			Size:   fileInfo.Size(),
			Digest: digest,
		})
	})
}

// path returns the canonical path for the blob identified by digest. The blob
// may or may not exist.
func (*blobStore) path(dgst digest.Digest) (string, error) {
	bp, err := pathFor(blobDataPathSpec{
		digest: dgst,
	})
	if err != nil {
		return "", err
	}

	return bp, nil
}

// link links the path to the provided digest by writing the digest into the
// target file. Caller must ensure that the blob actually exists.
func (bs *blobStore) link(ctx context.Context, targetPath string, dgst digest.Digest) error {
	// The contents of the "link" file are the exact string contents of the
	// digest, which is specified in that package.
	return bs.driver.PutContent(ctx, targetPath, []byte(dgst))
}

// readlink returns the linked digest at path.
func (bs *blobStore) readlink(ctx context.Context, targetPath string) (digest.Digest, error) {
	content, err := bs.driver.GetContent(ctx, targetPath)
	if err != nil {
		return "", err
	}

	linked, err := digest.Parse(string(content))
	if err != nil {
		return "", err
	}

	return linked, nil
}

type blobStatter struct {
	driver driver.StorageDriver
}

var _ distribution.BlobDescriptorService = &blobStatter{}

// Stat implements BlobStatter.Stat by returning the descriptor for the blob
// in the main blob store. If this method returns successfully, there is
// strong guarantee that the blob exists and is available.
func (bs *blobStatter) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	p, err := pathFor(blobDataPathSpec{
		digest: dgst,
	})
	if err != nil {
		return distribution.Descriptor{}, err
	}

	fi, err := bs.driver.Stat(ctx, p)
	if err != nil {
		if errors.As(err, new(driver.PathNotFoundError)) {
			return distribution.Descriptor{}, distribution.ErrBlobUnknown
		}

		return distribution.Descriptor{}, err
	}

	if fi.IsDir() {
		// NOTE(stevvooe): This represents a corruption situation. Somehow, we
		// calculated a blob path and then detected a directory. We log the
		// error and then error on the side of not knowing about the blob.
		dcontext.GetLogger(ctx).Warnf("blob path should not be a directory: %q", p)
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}

	return distribution.Descriptor{
		Size: fi.Size(),

		// NOTE(stevvooe): The central blob store firewalls media types from
		// other users. The caller should look this up and override the value
		// for the specific repository.
		MediaType: "application/octet-stream",
		Digest:    dgst,
	}, nil
}

func (*blobStatter) Clear(_ context.Context, _ digest.Digest) error {
	return distribution.ErrUnsupported
}

func (*blobStatter) SetDescriptor(_ context.Context, _ digest.Digest, _ distribution.Descriptor) error {
	return distribution.ErrUnsupported
}
