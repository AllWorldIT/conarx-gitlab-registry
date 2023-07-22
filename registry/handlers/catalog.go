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

const (
	linkPrevious = "previous"
	linkNext     = "next"

	maximumReturnedEntries = 100
)

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

func dbGetCatalog(ctx context.Context, db datastore.Queryer, filters datastore.FilterParams) ([]string, bool, error) {
	rStore := datastore.NewRepositoryStore(db)
	rr, err := rStore.FindAllPaginated(ctx, filters)
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

	filters := datastore.FilterParams{
		LastEntry:  lastEntry,
		MaxEntries: maxEntries,
	}

	var filled int
	var repos []string

	if ch.useDatabase {
		repos, moreEntries, err = dbGetCatalog(ch.Context, ch.db, filters)
		if err != nil {
			ch.Errors = append(ch.Errors, errcode.FromUnknownError(err))
			return
		}
		filled = len(repos)
	} else {
		repos = make([]string, filters.MaxEntries)

		filled, err = ch.App.registry.Repositories(ch.Context, repos, filters.LastEntry)
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
		filters.LastEntry = repos[len(repos)-1]
		urlStr, err := createLinkEntry(r.URL.String(), filters)
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
func createLinkEntry(origURL string, filters datastore.FilterParams) (string, error) {
	var combinedURL string

	if filters.BeforeEntry != "" {
		beforeURL, err := generateLink(origURL, linkPrevious, filters)
		if err != nil {
			return "", err
		}
		combinedURL = beforeURL
	}

	if filters.LastEntry != "" {
		lastURL, err := generateLink(origURL, linkNext, filters)
		if err != nil {
			return "", err
		}

		if filters.BeforeEntry == "" {
			combinedURL = lastURL
		} else {
			// Put the "previous" URL first and then "next" as shown in the
			// RFC5988 examples https://datatracker.ietf.org/doc/html/rfc5988#section-5.5
			combinedURL = fmt.Sprintf("%s, %s", combinedURL, lastURL)
		}
	}

	return combinedURL, nil
}

func generateLink(originalURL, rel string, filters datastore.FilterParams) (string, error) {
	calledURL, err := url.Parse(originalURL)
	if err != nil {
		return "", err
	}

	qValues := url.Values{}
	qValues.Add(nQueryParamKey, strconv.Itoa(filters.MaxEntries))

	switch rel {
	case linkPrevious:
		qValues.Add(beforeQueryParamKey, filters.BeforeEntry)
	case linkNext:
		qValues.Add(lastQueryParamKey, filters.LastEntry)
	}

	if filters.Name != "" {
		qValues.Add(tagNameQueryParamKey, filters.Name)
	}

	calledURL.RawQuery = qValues.Encode()

	calledURL.Fragment = ""
	urlStr := fmt.Sprintf("<%s>; rel=\"%s\"", calledURL.String(), rel)

	return urlStr, nil
}
