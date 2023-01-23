package urls

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/docker/distribution/reference"
	v1 "github.com/docker/distribution/registry/api/gitlab/v1"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/gorilla/mux"
)

// Instantiating a router is a costly operation and NewBuilder is called once
// per request, so we can save a lot of CPU time by reusing the same router.
var (
	defaultDistributionRouter = v2.Router()
	defaultGitLabRouter       = v1.Router()
)

// Builder creates registry API urls from a single base endpoint. It can be
// used to create urls for use in a registry client or server.
//
// All urls will be created from the given base, including the api version.
// For example, if a root of "/foo/" is provided, urls generated will be fall
// under "/foo/v2/...". Most application will only provide a schema, host and
// port, such as "https://localhost:5000/".
type Builder struct {
	root               *url.URL // url root (ie http://localhost/)
	distributionRouter *mux.Router
	gitLabRouter       *mux.Router
	relative           bool
}

// NewBuilder creates a Builder with provided root url object.
func NewBuilder(root *url.URL, relative bool) *Builder {
	return &Builder{
		root:               root,
		distributionRouter: defaultDistributionRouter,
		gitLabRouter:       defaultGitLabRouter,
		relative:           relative,
	}
}

// NewBuilderFromString workes identically to NewBuilder except it takes
// a string argument for the root, returning an error if it is not a valid
// url.
func NewBuilderFromString(root string, relative bool) (*Builder, error) {
	u, err := url.Parse(root)
	if err != nil {
		return nil, err
	}

	return NewBuilder(u, relative), nil
}

// NewBuilderFromRequest uses information from an *http.Request to
// construct the root url.
func NewBuilderFromRequest(r *http.Request, relative bool) *Builder {
	var (
		scheme = "http"
		host   = r.Host
	)

	if r.TLS != nil {
		scheme = "https"
	} else if len(r.URL.Scheme) > 0 {
		scheme = r.URL.Scheme
	}

	// Handle fowarded headers
	// Prefer "Forwarded" header as defined by rfc7239 if given
	// see https://tools.ietf.org/html/rfc7239
	if forwarded := r.Header.Get("Forwarded"); len(forwarded) > 0 {
		forwardedHeader, _, err := parseForwardedHeader(forwarded)
		if err == nil {
			if fproto := forwardedHeader["proto"]; len(fproto) > 0 {
				scheme = fproto
			}
			if fhost := forwardedHeader["host"]; len(fhost) > 0 {
				host = fhost
			}
		}
	} else {
		if forwardedProto := r.Header.Get("X-Forwarded-Proto"); len(forwardedProto) > 0 {
			scheme = forwardedProto
		}
		if forwardedHost := r.Header.Get("X-Forwarded-Host"); len(forwardedHost) > 0 {
			// According to the Apache mod_proxy docs, X-Forwarded-Host can be a
			// comma-separated list of hosts, to which each proxy appends the
			// requested host. We want to grab the first from this comma-separated
			// list.
			hosts := strings.SplitN(forwardedHost, ",", 2)
			host = strings.TrimSpace(hosts[0])
		}
	}

	basePath := v2.RouteDescriptors[v2.RouteNameBase].Path

	requestPath := r.URL.Path
	index := strings.Index(requestPath, basePath)

	u := &url.URL{
		Scheme: scheme,
		Host:   host,
	}

	if index > 0 {
		// N.B. index+1 is important because we want to include the trailing /
		u.Path = requestPath[0 : index+1]
	}

	return NewBuilder(u, relative)
}

// BuildBaseURL constructs a base url for the API, typically just "/v2/".
func (ub *Builder) BuildBaseURL() (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameBase)

	baseURL, err := route.URL()
	if err != nil {
		return "", err
	}

	return baseURL.String(), nil
}

// BuildCatalogURL constructs a url get a catalog of repositories
func (ub *Builder) BuildCatalogURL(values ...url.Values) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameCatalog)

	catalogURL, err := route.URL()
	if err != nil {
		return "", err
	}

	return appendValuesURL(catalogURL, values...).String(), nil
}

// BuildTagsURL constructs a url to list the tags in the named repository.
func (ub *Builder) BuildTagsURL(name reference.Named, values ...url.Values) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameTags)

	tagsURL, err := route.URL("name", name.Name())
	if err != nil {
		return "", err
	}

	return appendValuesURL(tagsURL, values...).String(), nil
}

// BuildTagURL constructs an url for a tag.
func (ub *Builder) BuildTagURL(ref reference.NamedTagged) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameTag)

	tagURL, err := route.URL("name", ref.Name(), "tag", ref.Tag())
	if err != nil {
		return "", err
	}

	return tagURL.String(), nil
}

// BuildManifestURL constructs a url for the manifest identified by name and
// reference. The argument reference may be either a tag or digest.
func (ub *Builder) BuildManifestURL(ref reference.Named) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameManifest)

	tagOrDigest := ""
	switch v := ref.(type) {
	case reference.Tagged:
		tagOrDigest = v.Tag()
	case reference.Digested:
		tagOrDigest = v.Digest().String()
	default:
		return "", fmt.Errorf("reference must have a tag or digest")
	}

	manifestURL, err := route.URL("name", ref.Name(), "reference", tagOrDigest)
	if err != nil {
		return "", err
	}

	return manifestURL.String(), nil
}

// BuildBlobURL constructs the url for the blob identified by name and dgst.
func (ub *Builder) BuildBlobURL(ref reference.Canonical) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameBlob)

	layerURL, err := route.URL("name", ref.Name(), "digest", ref.Digest().String())
	if err != nil {
		return "", err
	}

	return layerURL.String(), nil
}

// BuildBlobUploadURL constructs a url to begin a blob upload in the
// repository identified by name.
func (ub *Builder) BuildBlobUploadURL(name reference.Named, values ...url.Values) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameBlobUpload)

	uploadURL, err := route.URL("name", name.Name())
	if err != nil {
		return "", err
	}

	return appendValuesURL(uploadURL, values...).String(), nil
}

// BuildBlobUploadChunkURL constructs a url for the upload identified by uuid,
// including any url values. This should generally not be used by clients, as
// this url is provided by server implementations during the blob upload
// process.
func (ub *Builder) BuildBlobUploadChunkURL(name reference.Named, uuid string, values ...url.Values) (string, error) {
	route := ub.cloneDistributionRoute(v2.RouteNameBlobUploadChunk)

	uploadURL, err := route.URL("name", name.Name(), "uuid", uuid)
	if err != nil {
		return "", err
	}

	return appendValuesURL(uploadURL, values...).String(), nil
}

// BuildGitlabV1BaseURL constructs a base URL for the Gitlab v1 API.
func (ub *Builder) BuildGitlabV1BaseURL() (string, error) {
	route := ub.cloneGitLabRoute(v1.Base)

	baseURL, err := route.URL()
	if err != nil {
		return "", err
	}

	return baseURL.String(), nil
}

// BuildGitlabV1RepositoryURL constructs a URL for the Gitlab v1 API repository route by name.
func (ub *Builder) BuildGitlabV1RepositoryURL(name reference.Named, values ...url.Values) (string, error) {
	route := ub.cloneGitLabRoute(v1.Repositories)

	u, err := route.URL("name", name.Name())
	if err != nil {
		return "", err
	}

	return appendValuesURL(u, values...).String(), nil
}

// BuildGitlabV1RepositoryTagsURL constructs a URL for the Gitlab v1 API repository tags route by name.
func (ub *Builder) BuildGitlabV1RepositoryTagsURL(name reference.Named, values ...url.Values) (string, error) {
	route := ub.cloneGitLabRoute(v1.RepositoryTags)

	u, err := route.URL("name", name.Name())
	if err != nil {
		return "", err
	}

	return appendValuesURL(u, values...).String(), nil
}

// BuildGitlabV1RepositoryImportURL constructs a URL for the Gitlab v1 API
// repository import route by name.
func (ub *Builder) BuildGitlabV1RepositoryImportURL(name reference.Named, values ...url.Values) (string, error) {
	route := ub.cloneGitLabRoute(v1.RepositoryImport)

	u, err := route.URL("name", name.Name())
	if err != nil {
		return "", err
	}

	return appendValuesURL(u, values...).String(), nil
}

// BuildGitlabV1SubRepositoriesURL constructs a URL for the Gitlab v1 API sub-repositories route by name.
func (ub *Builder) BuildGitlabV1SubRepositoriesURL(name reference.Named, values ...url.Values) (string, error) {
	route := ub.cloneGitLabRoute(v1.SubRepositories)

	u, err := route.URL("name", name.Name())
	if err != nil {
		return "", err
	}

	return appendValuesURL(u, values...).String(), nil
}

// cloneDistributionRoute returns a clone of the named route from the
// distribution router. Routes must be cloned to avoid modifying them during
// url generation.
func (ub *Builder) cloneDistributionRoute(name string) clonedRoute {
	route := new(mux.Route)
	root := new(url.URL)

	*route = *ub.distributionRouter.GetRoute(name) // clone the route
	*root = *ub.root

	return clonedRoute{Route: route, root: root, relative: ub.relative}
}

// cloneGitLabRoute returns a clone of the named route from the
// distribution router. Routes must be cloned to avoid modifying them during
// url generation.
func (ub *Builder) cloneGitLabRoute(r v1.Route) clonedRoute {
	route := new(mux.Route)
	root := new(url.URL)

	*route = *ub.gitLabRouter.GetRoute(r.Name) // clone the route
	*root = *ub.root

	return clonedRoute{Route: route, root: root, relative: ub.relative}
}

type clonedRoute struct {
	*mux.Route
	root     *url.URL
	relative bool
}

func (cr clonedRoute) URL(pairs ...string) (*url.URL, error) {
	routeURL, err := cr.Route.URL(pairs...)
	if err != nil {
		return nil, err
	}

	if cr.relative {
		return routeURL, nil
	}

	if routeURL.Scheme == "" && routeURL.User == nil && routeURL.Host == "" {
		routeURL.Path = routeURL.Path[1:]
	}

	url := cr.root.ResolveReference(routeURL)
	url.Scheme = cr.root.Scheme
	return url, nil
}

// appendValuesURL appends the parameters to the url.
func appendValuesURL(u *url.URL, values ...url.Values) *url.URL {
	merged := u.Query()

	for _, v := range values {
		for k, vv := range v {
			merged[k] = append(merged[k], vv...)
		}
	}

	u.RawQuery = merged.Encode()
	return u
}

// appendValues appends the parameters to the url. Panics if the string is not
// a url.
func appendValues(u string, values ...url.Values) string {
	up, err := url.Parse(u)

	if err != nil {
		panic(err) // should never happen
	}

	return appendValuesURL(up, values...).String()
}
