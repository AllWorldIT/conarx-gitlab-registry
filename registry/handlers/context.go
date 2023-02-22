package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/docker/distribution"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/notifications"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/api/urls"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/datastore"
	"github.com/opencontainers/go-digest"
)

// Context should contain the request specific context for use in across
// handlers. Resources that don't need to be shared across handlers should not
// be on this object.
type Context struct {
	// App points to the application structure that created this context.
	*App
	context.Context

	// Repository is the repository for the current request. All requests
	// should be scoped to a single repository. This field may be nil.
	Repository distribution.Repository

	// RepositoryRemover provides method to delete a repository
	RepositoryRemover distribution.RepositoryRemover

	// Errors is a collection of errors encountered during the request to be
	// returned to the client API. If errors are added to the collection, the
	// handler *must not* start the response via http.ResponseWriter.
	Errors errcode.Errors

	urlBuilder *urls.Builder

	useDatabase     bool
	writeFSMetadata bool

	blobProvider distribution.BlobProvider

	repoCache datastore.RepositoryCache

	// TODO(stevvooe): The goal is too completely factor this context and
	// dispatching out of the web application. Ideally, we should lean on
	// context.Context for injection of these resources.

	// queueBridge is used to connect the handler with the notifications.Queue
	queueBridge *notifications.QueueBridge
}

// Value overrides context.Context.Value to ensure that calls are routed to
// correct context.
func (ctx *Context) Value(key interface{}) interface{} {
	return ctx.Context.Value(key)
}

func getName(ctx context.Context) (name string) {
	return dcontext.GetStringValue(ctx, "vars.name")
}

func getReference(ctx context.Context) (reference string) {
	return dcontext.GetStringValue(ctx, "vars.reference")
}

func getTag(ctx context.Context) (reference string) {
	return dcontext.GetStringValue(ctx, "vars.tag")
}

var errDigestNotAvailable = fmt.Errorf("digest not available in context")

func getDigest(ctx context.Context) (dgst digest.Digest, err error) {
	dgstStr := dcontext.GetStringValue(ctx, "vars.digest")

	if dgstStr == "" {
		dcontext.GetLogger(ctx).Errorf("digest not available")
		return "", errDigestNotAvailable
	}

	d, err := digest.Parse(dgstStr)
	if err != nil {
		dcontext.GetLogger(ctx).Errorf("error parsing digest=%q: %v", dgstStr, err)
		return "", err
	}

	return d, nil
}

func getUploadUUID(ctx context.Context) (uuid string) {
	return dcontext.GetStringValue(ctx, "vars.uuid")
}

// getUserName attempts to resolve a username from the context and request. If
// a username cannot be resolved, the empty string is returned.
func getUserName(ctx context.Context, r *http.Request) string {
	username := dcontext.GetStringValue(ctx, auth.UserNameKey)

	// Fallback to request user with basic auth
	if username == "" {
		var ok bool
		uname, _, ok := basicAuth(r)
		if ok {
			username = uname
		}
	}

	return username
}

// getUserType attempts to resolve a user type from the context.
func getUserType(ctx context.Context) string {
	return dcontext.GetStringValue(ctx, auth.UserTypeKey)
}
