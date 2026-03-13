package handlers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"

	"github.com/gorilla/handlers"
)

const (
	linkPrevious      = "previous"
	linkNext          = "next"
	encodingSeparator = "|"

	defaultMaximumReturnedEntries  = 100
	maximumReturnEntriesUpperLimit = 1000
)

func catalogDispatcher(ctx *Context, _ *http.Request) http.Handler {
	ch := &catalogHandler{
		Context: ctx,
	}

	getHandler := ch.LegacyHandleGetCatalog
	if ch.useDatabase {
		getHandler = ch.HandleGetCatalog
	}

	return handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(getHandler),
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

func (ch *catalogHandler) parseCatalogParams(r *http.Request) (string, int, bool) {
	q := r.URL.Query()
	lastEntry := q.Get("last")
	maxEntries, err := strconv.Atoi(q.Get("n"))
	if err != nil || maxEntries <= 0 {
		maxEntries = defaultMaximumReturnedEntries
	}
	if maxEntries > maximumReturnEntriesUpperLimit {
		ch.Errors = append(ch.Errors, v2.ErrorCodePaginationNumberInvalid.WithDetail(fmt.Sprintf("n must be no larger than %d", maximumReturnEntriesUpperLimit)))
		return "", 0, false
	}
	return lastEntry, maxEntries, true
}

// writeCatalogResponse sets the Content-Type and Link headers and writes the JSON catalog body.
func (ch *catalogHandler) writeCatalogResponse(w http.ResponseWriter, r *http.Request, repos []string, moreEntries bool, maxEntries int) {
	w.Header().Set("Content-Type", "application/json")

	// Add a link header if there are more entries to retrieve
	if moreEntries && len(repos) > 0 {
		filters := datastore.FilterParams{
			LastEntry:  repos[len(repos)-1],
			MaxEntries: maxEntries,
		}
		urlStr, err := createLinkEntry(r.URL.String(), filters, "", "")
		if err != nil {
			ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
		if urlStr != "" {
			w.Header().Set("Link", urlStr)
		}
	}

	if err := json.NewEncoder(w).Encode(catalogAPIResponse{Repositories: repos}); err != nil {
		ch.Errors = append(ch.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
	}
}

func (ch *catalogHandler) HandleGetCatalog(w http.ResponseWriter, r *http.Request) {
	log.GetLogger(log.WithContext(ch)).Debug("HandleGetCatalog")

	lastEntry, maxEntries, ok := ch.parseCatalogParams(r)
	if !ok {
		return
	}

	repos, moreEntries, err := dbGetCatalog(ch.Context, ch.db.Primary(), datastore.FilterParams{
		LastEntry:  lastEntry,
		MaxEntries: maxEntries,
	})
	if err != nil {
		ch.Errors = append(ch.Errors, errcode.FromUnknownError(err))
		return
	}

	ch.writeCatalogResponse(w, r, repos, moreEntries, maxEntries)
}

// Use the original URL from the request to create a new URL for
// the link header
func createLinkEntry(origURL string, filters datastore.FilterParams, publishedBefore, publishedLast string) (string, error) {
	var combinedURL string

	if filters.BeforeEntry != "" {
		beforeURL, err := generateLink(origURL, linkPrevious, filters, publishedBefore, publishedLast)
		if err != nil {
			return "", err
		}
		combinedURL = beforeURL
	}

	if filters.LastEntry != "" {
		lastURL, err := generateLink(origURL, linkNext, filters, publishedBefore, publishedLast)
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

func generateLink(originalURL, rel string, filters datastore.FilterParams, publishedBefore, publishedLast string) (string, error) {
	calledURL, err := url.Parse(originalURL)
	if err != nil {
		return "", err
	}

	qValues := url.Values{}
	qValues.Add(nQueryParamKey, strconv.Itoa(filters.MaxEntries))

	switch rel {
	case linkPrevious:
		before := filters.BeforeEntry
		if filters.OrderBy == publishedAtQueryParamKey && publishedBefore != "" {
			before = EncodeFilter(publishedBefore, filters.BeforeEntry)
		}
		qValues.Add(beforeQueryParamKey, before)
	case linkNext:
		last := filters.LastEntry
		if filters.OrderBy == publishedAtQueryParamKey && publishedLast != "" {
			last = EncodeFilter(publishedLast, filters.LastEntry)
		}
		qValues.Add(lastQueryParamKey, last)
	}

	if filters.Name != "" {
		qValues.Add(tagNameQueryParamKey, filters.Name)
	}

	orderBy := filters.OrderBy
	if orderBy != "" {
		if filters.SortOrder == datastore.OrderDesc {
			orderBy = "-" + orderBy
		}
		qValues.Add(sortQueryParamKey, orderBy)
	}

	calledURL.RawQuery = qValues.Encode()

	calledURL.Fragment = ""
	urlStr := fmt.Sprintf("<%s>; rel=\"%s\"", calledURL.String(), rel)

	return urlStr, nil
}

// EncodeFilter base64 encode by concatenating the published_at value with the tagName using an encodingSeparator
func EncodeFilter(publishedAt, tagName string) (v string) {
	return base64.StdEncoding.EncodeToString(
		[]byte(fmt.Sprintf("%s%s%s", publishedAt, encodingSeparator, tagName)),
	)
}

// DecodeFilter base64 filter using encodingSeparator to obtain the values for published_at and tag name
func DecodeFilter(encodedStr string) (a, b string, e error) {
	urlUnescaped, err := url.QueryUnescape(encodedStr)
	if err != nil {
		return "", "", err
	}

	bytes, err := base64.StdEncoding.DecodeString(urlUnescaped)
	if err != nil {
		return "", "", err
	}
	str := string(bytes)
	values := strings.Split(str, encodingSeparator)
	if len(values) != 2 {
		return "", "", fmt.Errorf("invalid encode value %q", str)
	}

	return values[0], values[1], nil
}
