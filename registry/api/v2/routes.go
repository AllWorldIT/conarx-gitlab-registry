package v2

import "github.com/gorilla/mux"

// The following are definitions of the name under which all V2 routes are
// registered. These symbols can be used to look up a route based on the name.
const (
	RouteNameBase            = "base"
	RouteNameManifest        = "manifest"
	RouteNameTags            = "tags"
	RouteNameBlob            = "blob"
	RouteNameBlobUpload      = "blob-upload"
	RouteNameBlobUploadChunk = "blob-upload-chunk"
	RouteNameCatalog         = "catalog"

	RoutePathBase            = "/v2/"
	RoutePathManifest        = "/v2/{name}/manifests/{reference}"
	RoutePathTags            = "/v2/{name}/tags/list"
	RoutePathBlob            = "/v2/{name}/blobs/{digest}"
	RoutePathBlobUpload      = "/v2/{name}/blobs/uploads/"
	RoutePathBlobUploadChunk = "/v2/{name}/blobs/uploads/{uuid}"
	RoutePathCatalog         = "/v2/_catalog"
)

func RoutePath(routeName string) string {
	switch routeName {
	case RouteNameBase:
		return RoutePathBase
	case RouteNameManifest:
		return RoutePathManifest
	case RouteNameTags:
		return RoutePathTags
	case RouteNameBlob:
		return RoutePathBlob
	case RouteNameBlobUpload:
		return RoutePathBlobUpload
	case RouteNameBlobUploadChunk:
		return RoutePathBlobUploadChunk
	case RouteNameCatalog:
		return RoutePathCatalog
	default:
		return ""
	}
}

// Router builds a gorilla router with named routes for the various API
// methods. This can be used directly by both server implementations and
// clients.
func Router() *mux.Router {
	return RouterWithPrefix("")
}

// RouterWithPrefix builds a gorilla router with a configured prefix
// on all routes.
func RouterWithPrefix(prefix string) *mux.Router {
	rootRouter := mux.NewRouter()
	router := rootRouter
	if prefix != "" {
		router = router.PathPrefix(prefix).Subrouter()
	}

	router.StrictSlash(true)

	for _, descriptor := range routeDescriptors {
		router.Path(descriptor.Path).Name(descriptor.Name)
	}

	return rootRouter
}
