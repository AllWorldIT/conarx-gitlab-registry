// Package v1 provides API routes for GitLab specific features, which exposes
// additional functionality beyond that of the distribution v2 API. These routes
// depend on functionality provided by the metadata database.
package v1

import (
	"github.com/docker/distribution/reference"
	"github.com/gorilla/mux"
)

// Route is the name and path pair of a GitLab v1 API route.
type Route struct {
	Name string
	Path string
	// ID is the unique identifier for this route. Used for metrics purposes.
	ID string
}

var (
	// Base is the API route under which all other GitLab v1 API routes are found.
	Base = Route{
		Name: "base",
		Path: "/gitlab/v1/",
		ID:   "/gitlab/v1",
	}
	// Repositories is the API route for the repositories' entity.
	Repositories = Route{
		Name: "repositories",
		Path: Base.Path + "repositories/{name:" + reference.NameRegexp.String() + "}/",
		ID:   Base.Path + "repositories/{name}",
	}

	// RepositoryTags is the API route for the repository tags list endpoint.
	RepositoryTags = Route{
		Name: "repository-tags",
		Path: Base.Path + "repositories/{name:" + reference.NameRegexp.String() + "}/tags/list/",
		ID:   Base.Path + "repositories/{name}/tags/list",
	}
	// RepositoryTagDetail is the API route for the repository tag details endpoint.
	RepositoryTagDetail = Route{
		Name: "repository-tag-detail",
		Path: Base.Path + "repositories/{name:" + reference.NameRegexp.String() + "}/tags/detail/{tagName:" + reference.TagRegexp.String() + "}/",
		ID:   Base.Path + "repositories/{name}/tags/detail/{tagName}",
	}
	// SubRepositories is the API route for the sub-repositories list.
	SubRepositories = Route{
		Name: "sub-repositories",
		Path: Base.Path + "repository-paths/{name:" + reference.NameRegexp.String() + "}/repositories/list/",
		ID:   Base.Path + "repository-paths/{name}/repositories/list",
	}
	// Statistics is the API route for the sub-repositories list.
	Statistics = Route{
		Name: "statistics",
		Path: Base.Path + "statistics/",
		ID:   Base.Path + "statistics/",
	}
)

// Router returns a new *mux.Router for the Gitlab v1 API.
func Router() *mux.Router {
	return RouterWithPrefix("")
}

// RouterWithPrefix returns a new *mux.Router for the Gitlab v1 API with a configured
// prefix on all routes.
func RouterWithPrefix(prefix string) *mux.Router {
	rootRouter := mux.NewRouter()
	router := rootRouter
	if prefix != "" {
		router = router.PathPrefix(prefix).Subrouter()
	}

	router.StrictSlash(true)

	router.Path(Base.Path).Name(Base.Name)
	router.Path(RepositoryTags.Path).Name(RepositoryTags.Name)
	router.Path(RepositoryTagDetail.Path).Name(RepositoryTagDetail.Name)
	router.Path(Repositories.Path).Name(Repositories.Name)
	router.Path(SubRepositories.Path).Name(SubRepositories.Name)
	router.Path(Statistics.Path).Name(Statistics.Name)

	return rootRouter
}
