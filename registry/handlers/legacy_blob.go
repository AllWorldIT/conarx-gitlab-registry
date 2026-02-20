package handlers

import (
	"net/http"

	"github.com/docker/distribution/log"
)

// LegacyDeleteBlob unlinks a layer reference from the repository.
func (bh *blobHandler) LegacyDeleteBlob(w http.ResponseWriter, _ *http.Request) {
	log.GetLogger(log.WithContext(bh)).Debug("LegacyDeleteBlob")

	blobs := bh.Repository.Blobs(bh)
	err := blobs.Delete(bh, bh.Digest)
	if err != nil {
		bh.appendBlobDeleteError(err)
		return
	}

	w.Header().Set("Content-Length", "0")
	w.WriteHeader(http.StatusAccepted)
}
