package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/notifications/meta"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/internal/metrics"
	"github.com/opencontainers/go-digest"
)

const blobCacheControlMaxAge = 365 * 24 * time.Hour

type redirect struct {
	enabled     bool          // allows toggling redirects to storage backends for blob downloads
	expiryDelay time.Duration // allows setting a custom delay for the presigned URLs expiration (defaults to 20m)
}

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver   driver.StorageDriver
	statter  distribution.BlobStatter
	pathFn   func(dgst digest.Digest) (string, error)
	redirect redirect
}

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) (*meta.Blob, error) {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return nil, err
	}

	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return nil, err
	}

	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"size_bytes": desc.Size,
		"digest":     desc.Digest,
	})

	var redirect bool
	if bs.redirect.enabled {
		opts := map[string]interface{}{"method": r.Method}
		if bs.redirect.expiryDelay > 0 {
			opts["expiry"] = time.Now().Add(bs.redirect.expiryDelay)
		}

		injectCustomKeyOpts(ctx, opts, map[string]any{SizeBytesKey: desc.Size})

		// TODO: The `driver` needs to be able to infer the redirect url's provider name that is used to obtain a redirect url.
		// This is required for notification purposes see: https://gitlab.com/gitlab-org/container-registry/-/issues/961.
		// Currently only the cofigured driver's name can be inferred. Example: using `gcs` storage driver the redirect url's provider
		// should be either the driver itself (e.g `gcs`) OR a pre-configured redirect middleware (e.g `googlecdn`)
		redirectURL, err := bs.driver.URLFor(ctx, path, opts)
		switch {
		case err == nil:
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			redirect = true
			metrics.BlobDownload(redirect, desc.Size)
			if r.Method == http.MethodGet {
				l.WithFields(log.Fields{"redirect": redirect}).Info("blob downloaded")
			}
			return &meta.Blob{StorageBackend: bs.driver.Name(), Redirected: redirect}, nil

		case errors.As(err, new(driver.ErrUnsupportedMethod)):
			// Fallback to serving the content directly.
		default:
			// Some unexpected error.
			return nil, err
		}
	}

	br := newFileReader(ctx, bs.driver, path, desc.Size)
	if err != nil {
		return nil, err
	}
	defer br.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
	}

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, br)
	metrics.BlobDownload(redirect, desc.Size)
	if r.Method == http.MethodGet {
		l.WithFields(log.Fields{"redirect": redirect}).Info("blob downloaded")
	}

	return &meta.Blob{StorageBackend: bs.driver.Name(), Redirected: redirect}, nil
}
