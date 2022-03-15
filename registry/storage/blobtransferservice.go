package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// BlobTransferService copies blobs from a BlobProvider to a BlobWriter.
type BlobTransferService struct {
	// BlobProvider side is the source containing the blob(s).
	src driver.StorageDriver
	// BlobService side is the destination to which blob(s) will be transferred.
	dest driver.StorageDriver
}

// NewBlobTransferService ...
func NewBlobTransferService(source, destination driver.StorageDriver) (*BlobTransferService, error) {
	if source == nil {
		return nil, fmt.Errorf("source driver cannot be nil")
	}

	if destination == nil {
		return nil, fmt.Errorf("destination driver cannot be nil")
	}

	return &BlobTransferService{source, destination}, nil
}

// Transfer ...
func (s *BlobTransferService) Transfer(ctx context.Context, dgst digest.Digest) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"digest": dgst, "component": "blob transfer service"})
	blobDataPath, err := pathFor(blobDataPathSpec{digest: dgst})
	if err != nil {
		return distribution.ErrBlobTransferFailed{Digest: dgst, Reason: err}
	}

	if _, err = s.dest.Stat(ctx, blobDataPath); err != nil {
		switch err := err.(type) {
		case driver.PathNotFoundError:
			// Continue with transfer.
			break
		default:
			return err
		}
	} else {
		l.Info("blob already present, no need to transfer")
		// If the path exists, we can assume that the content has already
		// been uploaded, since the blob storage is content-addressable.
		// While it may be corrupted, detection of such corruption belongs
		// elsewhere.
		return nil
	}

	if err = s.src.TransferTo(ctx, s.dest, blobDataPath, blobDataPath); err != nil {
		tErr := distribution.ErrBlobTransferFailed{Digest: dgst, Reason: err}

		// Blob transfer encountered a problem after modifying destination, attempt to cleanup.
		if errors.As(err, &driver.PartialTransferError{}) {
			tErr.Cleanup = true
			delErr := s.dest.Delete(ctx, blobDataPath)
			if errors.As(delErr, &driver.PathNotFoundError{}) {
				// Destination path can be considered clean if it doesn't exist.
				delErr = nil
			}

			tErr.CleanupErr = delErr
		}

		return tErr
	}

	return nil
}
