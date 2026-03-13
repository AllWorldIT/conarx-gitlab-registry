package handlers

import (
	"errors"
	"io"
	"net/http"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/storage/driver"
)

// LegacyHandleGetCatalog returns a json list of repositories from the legacy filesystem backend.
// Basic pagination is supported via the last and n query parameters, but filtering is not.
func (ch *catalogHandler) LegacyHandleGetCatalog(w http.ResponseWriter, r *http.Request) {
	log.GetLogger(log.WithContext(ch)).Debug("LegacyHandleGetCatalog")

	lastEntry, maxEntries, ok := ch.parseCatalogParams(r)
	if !ok {
		return
	}

	repos := make([]string, maxEntries)
	filled, err := ch.App.registry.Repositories(ch.Context, repos, lastEntry)
	// Repositories returns io.EOF when the walk completes without filling the buffer (no more
	// entries), or PathNotFoundError when the repositories root does not exist (empty registry).
	// A nil error means the buffer was filled and more entries may exist.
	moreEntries := false
	if err == nil {
		moreEntries = true
	} else if !errors.Is(err, io.EOF) && !errors.As(err, new(driver.PathNotFoundError)) {
		ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	ch.writeCatalogResponse(w, r, repos[:filled], moreEntries, maxEntries)
}
