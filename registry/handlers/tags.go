package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/datastore"
	"github.com/gorilla/handlers"
)

// tagsDispatcher constructs the tags handler api endpoint.
func tagsDispatcher(ctx *Context, _ *http.Request) http.Handler {
	tagsHandler := &tagsHandler{
		Context: ctx,
	}
	h := handlers.MethodHandler{
		http.MethodGet: http.HandlerFunc(tagsHandler.HandleGetTags),
	}
	return h
}

// tagsHandler handles requests for lists of tags under a repository name.
type tagsHandler struct {
	*Context
}

type tagsAPIResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func dbGetTags(
	ctx context.Context,
	db datastore.Queryer,
	rcache datastore.RepositoryCache,
	repoPath string,
	filters datastore.FilterParams,
) ([]string, bool, error) {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": repoPath, "limit": filters.MaxEntries, "marker": filters.LastEntry})
	l.Debug("finding tags in database")

	var opts []datastore.RepositoryStoreOption
	if rcache != nil {
		opts = append(opts, datastore.WithRepositoryCache(rcache))
	}
	rStore := datastore.NewRepositoryStore(db, opts...)
	r, err := rStore.FindByPath(ctx, repoPath)
	if err != nil {
		return nil, false, err
	}
	if r == nil {
		return nil, false, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"name": repoPath})
	}

	tt, err := rStore.TagsPaginated(ctx, r, filters)
	if err != nil {
		return nil, false, err
	}

	tags := make([]string, 0, len(tt))
	for _, t := range tt {
		tags = append(tags, t.Name)
	}

	var moreEntries bool
	if len(tt) > 0 {
		filters.LastEntry = tt[len(tt)-1].Name
		moreEntries, err = rStore.HasTagsAfterName(ctx, r, filters)
		if err != nil {
			return nil, false, err
		}
	}

	return tags, moreEntries, nil
}

// HandleGetTags returns a json list of tags for a specific image name.
func (th *tagsHandler) HandleGetTags(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// Pagination headers are currently only supported by the metadata database backend
	q := r.URL.Query()
	lastEntry := q.Get("last")
	maxEntries, err := strconv.Atoi(q.Get("n"))
	if err != nil || maxEntries <= 0 {
		maxEntries = defaultMaximumReturnedEntries
	}

	filters := datastore.FilterParams{
		LastEntry:  lastEntry,
		MaxEntries: maxEntries,
	}

	var tags []string
	var moreEntries bool

	if th.useDatabase {
		tags, moreEntries, err = dbGetTags(th.Context, th.db.Primary(), th.GetRepoCache(), th.Repository.Named().Name(), filters)
		if err != nil {
			th.Errors = append(th.Errors, errcode.FromUnknownError(err))
			return
		}
		if len(tags) == 0 {
			// If no tags are found, the current implementation (`else`) returns a nil slice instead of an empty one,
			// so we have to enforce the same behavior here, for consistency.
			tags = nil
		}
	} else {
		tagService := th.Repository.Tags(th)
		tags, err = tagService.All(th)
		if err != nil {
			switch err := err.(type) {
			case distribution.ErrRepositoryUnknown:
				th.Errors = append(th.Errors, v2.ErrorCodeNameUnknown.WithDetail(map[string]string{"name": th.Repository.Named().Name()}))
			default:
				th.Errors = append(th.Errors, errcode.FromUnknownError(err))
			}
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")

	// Add a link header if there are more entries to retrieve (only supported by the metadata database backend)
	if moreEntries {
		filters.LastEntry = tags[len(tags)-1]
		urlStr, err := createLinkEntry(r.URL.String(), filters, "", "")
		if err != nil {
			th.Errors = append(th.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
			return
		}
		if urlStr != "" {
			w.Header().Set("Link", urlStr)
		}
	}

	enc := json.NewEncoder(w)
	if err := enc.Encode(tagsAPIResponse{
		Name: th.Repository.Named().Name(),
		Tags: tags,
	}); err != nil {
		th.Errors = append(th.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}
}
