package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/docker/distribution/registry/datastore"

	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/gorilla/handlers"
)

const maximumReturnedEntries = 100

func catalogDispatcher(ctx *Context, r *http.Request) http.Handler {
	catalogHandler := &catalogHandler{
		Context: ctx,
	}

	return handlers.MethodHandler{
		"GET": http.HandlerFunc(catalogHandler.GetCatalog),
	}
}

type catalogHandler struct {
	*Context
}

type catalogAPIResponse struct {
	Repositories []string `json:"repositories"`
}

func dbGetCatalog(ctx context.Context, db datastore.Queryer, n int, last string) ([]string, bool, error) {
	rStore := datastore.NewRepositoryStore(db)
	rr, err := rStore.FindAllPaginated(ctx, n, last)
	if err != nil {
		return nil, false, err
	}

	repos := make([]string, 0, len(rr))
	for _, r := range rr {
		repos = append(repos, r.Path)
	}

	var moreEntries bool
	if len(rr) > 0 {
		n, err := rStore.CountAfterPath(ctx, rr[len(rr)-1].Path)
		if err != nil {
			return nil, false, err
		}
		moreEntries = n > 0
	}

	return repos, moreEntries, nil
}

func (ch *catalogHandler) GetCatalog(w http.ResponseWriter, r *http.Request) {
	var moreEntries = true

	q := r.URL.Query()
	lastEntry := q.Get("last")
	maxEntries, err := strconv.Atoi(q.Get("n"))
	if err != nil || maxEntries <= 0 {
		maxEntries = maximumReturnedEntries
	}

	var filled int
	var repos []string

	if ch.useDatabase {
		repos, moreEntries, err = dbGetCatalog(ch.Context, ch.db, maxEntries, lastEntry)
		if err != nil {
			ch.Errors = append(ch.Errors, errcode.FromUnknownError(err))
			return
		}
		filled = len(repos)
	} else {
		repos = make([]string, maxEntries)

		filled, err = ch.App.registry.Repositories(ch.Context, repos, lastEntry)
		_, pathNotFound := err.(driver.PathNotFoundError)

		if err == io.EOF || pathNotFound {
			moreEntries = false
		} else if err != nil {
			ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")

	// Add a link header if there are more entries to retrieve
	if moreEntries {
		lastEntry = repos[len(repos)-1]
		urlStr, err := createLinkEntry(r.URL.String(), maxEntries, lastEntry)
		if err != nil {
			ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
		w.Header().Set("Link", urlStr)
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(catalogAPIResponse{
		Repositories: repos[0:filled],
	}); err != nil {
		ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}

// Use the original URL from the request to create a new URL for
// the link header
func createLinkEntry(origURL string, maxEntries int, lastEntry string, extra ...url.Values) (string, error) {
	calledURL, err := url.Parse(origURL)
	if err != nil {
		return "", err
	}

	v := url.Values{}
	v.Add("n", strconv.Itoa(maxEntries))
	v.Add("last", lastEntry)

	for _, val := range extra {
		for k, vv := range val {
			v[k] = append(v[k], vv...)
		}
	}

	calledURL.RawQuery = v.Encode()

	calledURL.Fragment = ""
	urlStr := fmt.Sprintf("<%s>; rel=\"next\"", calledURL.String())

	return urlStr, nil
}
