package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/docker/distribution/registry/storage"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
)

// blobUploadDispatcher constructs and returns the blob upload handler for the
// given request context.
func blobUploadDispatcher(ctx *Context, _ *http.Request) http.Handler {
	buh := &blobUploadHandler{
		Context: ctx,
		UUID:    getUploadUUID(ctx),
	}

	handler := handlers.MethodHandler{
		http.MethodGet:  http.HandlerFunc(buh.HandleGetUploadStatus),
		http.MethodHead: http.HandlerFunc(buh.HandleGetUploadStatus),
	}

	if !ctx.readOnly {
		handler[http.MethodPost] = http.HandlerFunc(buh.StartBlobUpload)
		handler[http.MethodPatch] = http.HandlerFunc(buh.PatchBlobData)
		handler[http.MethodPut] = http.HandlerFunc(buh.PutBlobUploadComplete)
		handler[http.MethodDelete] = http.HandlerFunc(buh.CancelBlobUpload)
	}

	return buh.validateUpload(handler)
}

func (buh *blobUploadHandler) validateUpload(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if buh.UUID != "" {
			h := buh.ResumeBlobUpload(buh.Context, r)
			if h == nil {
				h = closeResources(handler, buh.Upload)
			}
			checkOngoingRename(h, buh.Context).ServeHTTP(w, r)
			return
		}
		checkOngoingRename(handler, buh.Context).ServeHTTP(w, r)
	})
}

// blobUploadHandler handles the http blob upload process.
type blobUploadHandler struct {
	*Context

	// UUID identifies the upload instance for the current request. Using UUID
	// to key blob writers since this implementation uses UUIDs.
	UUID string

	Upload distribution.BlobWriter

	State blobUploadState
}

func dbMountBlob(ctx context.Context, rStore datastore.RepositoryStore, fromRepoPath, toRepoPath string, d digest.Digest) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"source":      fromRepoPath,
		"destination": toRepoPath,
		"digest":      d,
	})
	l.Debug("cross repository blob mounting")

	// find source blob from source repository
	b, err := dbFindRepositoryBlob(ctx, rStore, distribution.Descriptor{Digest: d}, fromRepoPath)
	if err != nil {
		return err
	}
	destRepo, err := rStore.CreateOrFindByPath(ctx, toRepoPath)
	if err != nil {
		return err
	}

	// link blob (does nothing if already linked)
	return rStore.LinkBlob(ctx, destRepo, b.Digest)
}

// StartBlobUpload begins the blob upload process and allocates a server-side
// blob writer session, optionally mounting the blob from a separate repository.
func (buh *blobUploadHandler) StartBlobUpload(w http.ResponseWriter, r *http.Request) {
	var options []distribution.BlobCreateOption
	var rStore datastore.RepositoryStore
	if buh.useDatabase {
		var opts []datastore.RepositoryStoreOption
		if buh.GetRepoCache() != nil {
			opts = append(opts, datastore.WithRepositoryCache(buh.GetRepoCache()))
		}
		rStore = datastore.NewRepositoryStore(buh.db.Primary(), opts...)
	}

	fromRepo := r.FormValue("from")
	mountDigest := r.FormValue("mount")
	if mountDigest != "" && fromRepo != "" {
		opt, err := buh.createBlobMountOption(fromRepo, mountDigest, rStore)
		if opt != nil && err == nil {
			options = append(options, opt)
		}
	}

	blobs := buh.Repository.Blobs(buh)
	upload, err := blobs.Create(buh, options...)
	// nolint: revive // max-control-nesting
	if err != nil {
		var ebm distribution.ErrBlobMounted
		switch {
		case errors.As(err, &ebm):
			if buh.useDatabase {
				errIn := dbMountBlob(buh.Context, rStore, ebm.From.Name(), buh.Repository.Named().Name(), ebm.Descriptor.Digest)
				if errIn != nil {
					e := fmt.Errorf("failed to mount blob in database: %w", errIn)
					buh.Errors = append(buh.Errors, errcode.FromUnknownError(e))
					return
				}
			}
			errIn := buh.writeBlobCreatedHeaders(w, ebm.Descriptor)
			if errIn != nil {
				buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(errIn))
			}
		case errors.Is(err, distribution.ErrUnsupported):
			buh.Errors = append(buh.Errors, errcode.ErrorCodeUnsupported)
		default:
			buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		}
		return
	}

	buh.Upload = upload

	if err := buh.blobUploadResponse(w); err != nil {
		buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	w.Header().Set("Docker-Upload-UUID", buh.Upload.ID())
	w.WriteHeader(http.StatusAccepted)
}

// HandleGetUploadStatus returns the status of a given upload, identified by id.
func (buh *blobUploadHandler) HandleGetUploadStatus(w http.ResponseWriter, _ *http.Request) {
	if buh.Upload == nil {
		buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadUnknown)
		return
	}

	if err := buh.blobUploadResponse(w); err != nil {
		buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	w.Header().Set("Docker-Upload-UUID", buh.UUID)
	w.WriteHeader(http.StatusNoContent)
}

// PatchBlobData writes data to an upload.
func (buh *blobUploadHandler) PatchBlobData(w http.ResponseWriter, r *http.Request) {
	if buh.Upload == nil {
		buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadUnknown)
		return
	}

	ct := r.Header.Get("Content-Type")
	if ct != "" && ct != "application/octet-stream" {
		buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(fmt.Errorf("bad Content-Type")))
		return
	}

	chunkRange := r.Header.Get("Content-Range")

	if chunkRange != "" {
		startRange, _, err := parseContentRange(chunkRange)
		if err != nil {
			buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err.Error()))
			return
		}
		// chunks MUST be uploaded in order, with the first byte of a chunk
		// being the last chunk's end offset + 1 (which is equivalent to ` buh.State.Offset`)
		if startRange != buh.State.Offset {
			buh.Errors = append(buh.Errors, v2.ErrorCodeInvalidContentRange)
			return
		}
	}

	if err := copyFullPayload(buh, w, r, buh.Upload, -1, "blob PATCH"); err != nil {
		buh.Errors = append(buh.Errors, errcode.FromUnknownError(err))
		return
	}

	if err := buh.blobUploadResponse(w); err != nil {
		buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func dbPutBlobUploadComplete(ctx context.Context, db *datastore.DB, repoPath string, desc distribution.Descriptor, repoCache datastore.RepositoryCache) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning database transaction: %w", err)
	}
	defer tx.Rollback()

	// create or find blob
	bs := datastore.NewBlobStore(tx)
	b := &models.Blob{
		MediaType: desc.MediaType,
		Digest:    desc.Digest,
		Size:      desc.Size,
	}
	if err := bs.CreateOrFind(ctx, b); err != nil {
		return err
	}

	// create or find repository
	var opts []datastore.RepositoryStoreOption
	if repoCache != nil {
		opts = append(opts, datastore.WithRepositoryCache(repoCache))
	}
	rStore := datastore.NewRepositoryStore(tx, opts...)

	r, err := rStore.CreateOrFindByPath(ctx, repoPath)
	if err != nil {
		return err
	}

	// link blob to repository
	if err := rStore.LinkBlob(ctx, r, b.Digest); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing database transaction: %w", err)
	}

	return nil
}

// PutBlobUploadComplete takes the final request of a blob upload. The
// request may include all the blob data or no blob data. Any data
// provided is received and verified. If successful, the blob is linked
// into the blob store and 201 Created is returned with the canonical
// url of the blob.
func (buh *blobUploadHandler) PutBlobUploadComplete(w http.ResponseWriter, r *http.Request) {
	if buh.Upload == nil {
		buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadUnknown)
		return
	}

	dgstStr := r.FormValue("digest")

	if dgstStr == "" {
		// no digest? return error, but allow retry.
		buh.Errors = append(buh.Errors, v2.ErrorCodeDigestInvalid.WithDetail("digest missing"))
		return
	}

	dgst, err := digest.Parse(dgstStr)
	if err != nil {
		// no digest? return error, but allow retry.
		buh.Errors = append(buh.Errors, v2.ErrorCodeDigestInvalid.WithDetail("digest parsing failed"))
		return
	}

	if err := copyFullPayload(buh, w, r, buh.Upload, -1, "blob PUT"); err != nil {
		buh.Errors = append(buh.Errors, errcode.FromUnknownError(err))
		return
	}

	desc, err := buh.Upload.Commit(buh, distribution.Descriptor{
		Digest: dgst,
	})

	l := log.GetLogger(log.WithContext(buh))
	if err != nil {
		switch err := err.(type) {
		case distribution.ErrBlobInvalidDigest:
			buh.Errors = append(buh.Errors, v2.ErrorCodeDigestInvalid.WithDetail(err))
		case errcode.Error:
			buh.Errors = append(buh.Errors, err)
		default:
			switch err {
			case distribution.ErrAccessDenied:
				buh.Errors = append(buh.Errors, errcode.ErrorCodeDenied)
			case distribution.ErrUnsupported:
				buh.Errors = append(buh.Errors, errcode.ErrorCodeUnsupported)
			case distribution.ErrBlobInvalidLength, distribution.ErrBlobDigestUnsupported:
				buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadInvalid.WithDetail(err))
			default:
				l.WithError(err).Error("unknown error completing upload")
				buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			}
		}

		// Clean up the backend blob data if there was an error.
		if err := buh.Upload.Cancel(buh); err != nil {
			// If the cleanup fails, all we can do is observe and report.
			l.WithError(err).Error("error canceling upload after error")
		}

		return
	}

	if buh.useDatabase {
		if err := dbPutBlobUploadComplete(buh.Context, buh.db.Primary(), buh.Repository.Named().Name(), desc, buh.GetRepoCache()); err != nil {
			e := fmt.Errorf("failed to create blob in database: %w", err)
			buh.Errors = append(buh.Errors, errcode.FromUnknownError(e))
			return
		}
	}

	if err := buh.writeBlobCreatedHeaders(w, desc); err != nil {
		buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	l.WithFields(log.Fields{
		"media_type": desc.MediaType,
		"size_bytes": desc.Size,
		"digest":     desc.Digest,
	}).Info("blob uploaded")
}

// CancelBlobUpload cancels an in-progress upload of a blob.
func (buh *blobUploadHandler) CancelBlobUpload(w http.ResponseWriter, _ *http.Request) {
	if buh.Upload == nil {
		buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadUnknown)
		return
	}

	w.Header().Set("Docker-Upload-UUID", buh.UUID)
	if err := buh.Upload.Cancel(buh); err != nil {
		log.GetLogger(log.WithContext(buh)).WithError(err).Error("error encountered canceling upload")
		buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
	}

	w.WriteHeader(http.StatusNoContent)
}

func (buh *blobUploadHandler) ResumeBlobUpload(ctx *Context, r *http.Request) http.Handler {
	state, err := hmacKey(ctx.Config.HTTP.Secret).unpackUploadState(r.FormValue("_state"))
	if err != nil {
		return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			log.GetLogger(log.WithContext(ctx)).WithError(err).Info("error resolving upload")
			buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadInvalid.WithDetail(err))
		})
	}
	buh.State = state

	if state.Name != ctx.Repository.Named().Name() {
		return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
				"state_name": state.Name,
				"repository": buh.Repository.Named().Name(),
			}).Info("mismatched repository name in upload state")
			buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadInvalid.WithDetail(err))
		})
	}

	if state.UUID != buh.UUID {
		return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
				"state_uuid":  state.UUID,
				"upload_uuid": buh.UUID,
			}).Info("mismatched uuid in upload state")
			buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadInvalid.WithDetail(err))
		})
	}

	blobs := ctx.Repository.Blobs(buh)
	upload, err := blobs.Resume(buh, buh.UUID)
	if err != nil {
		log.GetLogger(log.WithContext(ctx)).WithError(err).Error("error resolving upload")
		if err == distribution.ErrBlobUploadUnknown {
			return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
				buh.Errors = append(buh.Errors, v2.ErrorCodeBlobUploadUnknown.WithDetail(err))
			})
		}

		return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			buh.Errors = append(buh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		})
	}
	buh.Upload = upload

	// The offset specified in the request's `state` query parameter is not useful when only querying for
	// the status of the current blob upload and hence does not need be validated against.
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		if size := upload.Size(); size != buh.State.Offset {
			defer upload.Close()
			log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
				"upload_size":  size,
				"state_offset": buh.State.Offset,
			}).Error("upload resumed at wrong offset")
			return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
				buh.Errors = append(buh.Errors, v2.ErrorCodeResumableBlobUploadInvalid.WithDetail(err))
			})
		}
	}
	return nil
}

// blobUploadResponse provides a standard request for uploading blobs and
// chunk responses. This sets the correct headers but the response status is
// left to the caller.
func (buh *blobUploadHandler) blobUploadResponse(w http.ResponseWriter) error {
	buh.State.Name = buh.Repository.Named().Name()
	buh.State.UUID = buh.Upload.ID()
	_ = buh.Upload.Close()
	buh.State.Offset = buh.Upload.Size()
	buh.State.StartedAt = buh.Upload.StartedAt()

	token, err := hmacKey(buh.Config.HTTP.Secret).packUploadState(buh.State)
	if err != nil {
		log.GetLogger(log.WithContext(buh)).WithError(err).Info("error building upload state token")
		return err
	}

	uploadURL, err := buh.urlBuilder.BuildBlobUploadChunkURL(
		buh.Repository.Named(), buh.Upload.ID(),
		url.Values{
			"_state": []string{token},
		})
	if err != nil {
		log.GetLogger(log.WithContext(buh)).WithError(err).Info("error building upload url")
		return err
	}

	endRange := buh.Upload.Size()
	if endRange > 0 {
		endRange--
	}

	w.Header().Set("Docker-Upload-UUID", buh.UUID)
	w.Header().Set("Location", uploadURL)

	w.Header().Set("Content-Length", "0")
	w.Header().Set("Range", fmt.Sprintf("0-%d", endRange))

	return nil
}

// mountBlob attempts to mount a blob from another repository by its digest. If
// successful, the blob is linked into the blob store and 201 Created is
// returned with the canonical url of the blob.
func (buh *blobUploadHandler) createBlobMountOption(fromRepo, mountDigest string, rStore datastore.RepositoryStore) (distribution.BlobCreateOption, error) {
	dgst, err := digest.Parse(mountDigest)
	if err != nil {
		return nil, err
	}

	ref, err := reference.WithName(fromRepo)
	if err != nil {
		return nil, err
	}

	canonical, err := reference.WithDigest(ref, dgst)
	if err != nil {
		return nil, err
	}

	if !buh.useDatabase {
		return storage.WithMountFrom(canonical), nil
	}

	// Check for blob access on the database and pass that information via the
	// BlobCreateOption.
	b, err := dbFindRepositoryBlob(buh, rStore, distribution.Descriptor{Digest: dgst}, ref.Name())
	if err != nil {
		return nil, err
	}

	return storage.WithMountFromStat(canonical, &distribution.Descriptor{Digest: b.Digest, Size: b.Size, MediaType: b.MediaType}), nil
}

// writeBlobCreatedHeaders writes the standard headers describing a newly
// created blob. A 201 Created is written as well as the canonical URL and
// blob digest.
func (buh *blobUploadHandler) writeBlobCreatedHeaders(w http.ResponseWriter, desc distribution.Descriptor) error {
	ref, err := reference.WithDigest(buh.Repository.Named(), desc.Digest)
	if err != nil {
		return err
	}
	blobURL, err := buh.urlBuilder.BuildBlobURL(ref)
	if err != nil {
		return err
	}

	w.Header().Set("Location", blobURL)
	w.Header().Set("Content-Length", "0")
	w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	w.WriteHeader(http.StatusCreated)
	return nil
}
