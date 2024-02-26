package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/manifest"
	"github.com/docker/distribution/manifest/manifestlist"
	mlcompat "github.com/docker/distribution/manifest/manifestlist/compat"
	"github.com/docker/distribution/manifest/ocischema"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/manifest/schema2"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/errcode"
	v2 "github.com/docker/distribution/registry/api/v2"
	"github.com/docker/distribution/registry/auth"
	"github.com/docker/distribution/registry/datastore"
	"github.com/docker/distribution/registry/datastore/models"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/validation"
	"github.com/gorilla/handlers"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// These constants determine which architecture and OS to choose from a
// manifest list when falling back to a schema2 manifest.
const (
	defaultArch         = "amd64"
	defaultOS           = "linux"
	maxManifestBodySize = 4 << 20
	imageClass          = "image"
)

type storageType int

const (
	manifestSchema1        storageType = iota // 0
	manifestSchema2                           // 1
	manifestlistSchema                        // 2
	ociImageManifestSchema                    // 3
	ociImageIndexSchema                       // 4
)

func (t storageType) MediaType() string {
	switch t {
	case manifestSchema1:
		return schema1.MediaTypeManifest
	case manifestSchema2:
		return schema2.MediaTypeManifest
	case manifestlistSchema:
		return manifestlist.MediaTypeManifestList
	case ociImageManifestSchema:
		return v1.MediaTypeImageManifest
	case ociImageIndexSchema:
		return v1.MediaTypeImageIndex
	default:
		return ""
	}
}

// manifestDispatcher takes the request context and builds the
// appropriate handler for handling manifest requests.
func manifestDispatcher(ctx *Context, r *http.Request) http.Handler {
	manifestHandler := &manifestHandler{
		Context: ctx,
	}
	ref := getReference(ctx)
	dgst, err := digest.Parse(ref)
	if err != nil {
		// We just have a tag
		manifestHandler.Tag = ref
	} else {
		manifestHandler.Digest = dgst
	}

	mhandler := handlers.MethodHandler{
		http.MethodGet:  http.HandlerFunc(manifestHandler.GetManifest),
		http.MethodHead: http.HandlerFunc(manifestHandler.GetManifest),
	}

	if !ctx.readOnly {
		mhandler[http.MethodPut] = http.HandlerFunc(manifestHandler.PutManifest)
		mhandler[http.MethodDelete] = http.HandlerFunc(manifestHandler.DeleteManifest)
	}

	return checkOngoingRename(mhandler, ctx)
}

// manifestHandler handles http operations on image manifests.
type manifestHandler struct {
	*Context

	// One of tag or digest gets set, depending on what is present in context.
	Tag    string
	Digest digest.Digest
}

// GetManifest fetches the image manifest from the storage backend, if it exists.
func (imh *manifestHandler) GetManifest(w http.ResponseWriter, r *http.Request) {
	l := log.GetLogger(log.WithContext(imh))
	l.Debug("GetImageManifest")

	manifestGetter, err := imh.newManifestGetter(r)
	if err != nil {
		imh.Errors = append(imh.Errors, err)
		return
	}

	var (
		manifest distribution.Manifest
		getErr   error
	)

	if imh.Tag != "" {
		manifest, imh.Digest, getErr = manifestGetter.GetByTag(imh.Context, imh.Tag)
	} else {
		manifest, getErr = manifestGetter.GetByDigest(imh.Context, imh.Digest)
	}
	if getErr != nil {
		switch {
		case errors.Is(getErr, errETagMatches):
			w.WriteHeader(http.StatusNotModified)
		case errors.As(getErr, &distribution.ErrManifestUnknownRevision{}),
			errors.As(getErr, &distribution.ErrManifestUnknown{}),
			errors.Is(getErr, digest.ErrDigestInvalidFormat),
			errors.As(getErr, &distribution.ErrTagUnknown{}):
			imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnknown.WithDetail(getErr))
		case errors.Is(getErr, distribution.ErrSchemaV1Unsupported):
			imh.Errors = append(imh.Errors, v2.ErrorCodeManifestInvalid.WithMessage("Schema 1 manifest not supported"))
		default:
			imh.Errors = append(imh.Errors, errcode.FromUnknownError(getErr))
		}
		return
	}

	// determine the type of the returned manifest
	manifestType := manifestSchema1
	_, isSchema2 := manifest.(*schema2.DeserializedManifest)
	manifestList, isManifestList := manifest.(*manifestlist.DeserializedManifestList)
	if isSchema2 {
		manifestType = manifestSchema2
	} else if _, isOCImanifest := manifest.(*ocischema.DeserializedManifest); isOCImanifest {
		manifestType = ociImageManifestSchema
	} else if isManifestList {
		if manifestList.MediaType == manifestlist.MediaTypeManifestList {
			manifestType = manifestlistSchema
		} else if manifestList.MediaType == v1.MediaTypeImageIndex || manifestList.MediaType == "" {
			manifestType = ociImageIndexSchema
		}
	}

	if manifestType == manifestSchema1 {
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestInvalid.WithMessage("Schema 1 manifest not supported"))
		return
	}
	if manifestType == ociImageManifestSchema && !supports(r, ociImageManifestSchema) {
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnknown.WithMessage("OCI manifest found, but accept header does not support OCI manifests"))
		return
	}
	if manifestType == ociImageIndexSchema && !supports(r, ociImageIndexSchema) {
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnknown.WithMessage("OCI index found, but accept header does not support OCI indexes"))
		return
	}

	if isManifestList {
		logIfManifestListInvalid(imh, manifestList, http.MethodGet)
	}

	// Only rewrite manifests lists when they are being fetched by tag. If they
	// are being fetched by digest, we can't return something not matching the digest.
	if imh.Tag != "" && manifestType == manifestlistSchema && !supports(r, manifestlistSchema) {
		manifest, err = imh.rewriteManifestList(manifestList)
		if err != nil {
			switch err := err.(type) {
			case distribution.ErrManifestUnknownRevision:
				imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnknown.WithDetail(err))
			case errcode.Error:
				imh.Errors = append(imh.Errors, err)
			default:
				imh.Errors = append(imh.Errors, errcode.FromUnknownError(err))
			}
			return
		}
	}

	ct, p, err := manifest.Payload()
	if err != nil {
		imh.Errors = append(imh.Errors, errcode.FromUnknownError(err))
		return
	}

	if err := imh.queueBridge.ManifestPulled(imh.Repository.Named(), manifest, distribution.WithTagOption{Tag: imh.Tag}); err != nil {
		l.WithError(err).Error("dispatching manifest pull to queue")
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Content-Length", fmt.Sprint(len(p)))
	w.Header().Set("Docker-Content-Digest", imh.Digest.String())
	w.Header().Set("Etag", fmt.Sprintf(`"%s"`, imh.Digest))

	if r.Method == http.MethodGet {
		_, _ = w.Write(p)

		l.WithFields(log.Fields{
			"media_type":      manifestType.MediaType(),
			"size_bytes":      len(p),
			"digest":          imh.Digest,
			"tag_name":        imh.Tag,
			"reference_count": len(manifest.References()),
		}).Info("manifest downloaded")
	}
}

func supports(req *http.Request, st storageType) bool {
	// this parsing of Accept headers is not quite as full-featured as godoc.org's parser, but we don't care about "q=" values
	// https://github.com/golang/gddo/blob/e91d4165076d7474d20abda83f92d15c7ebc3e81/httputil/header/header.go#L165-L202
	for _, acceptHeader := range req.Header["Accept"] {
		// r.Header[...] is a slice in case the request contains the same header more than once
		// if the header isn't set, we'll get the zero value, which "range" will handle gracefully

		// we need to split each header value on "," to get the full list of "Accept" values (per RFC 2616)
		// https://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.1
		for _, rawMT := range strings.Split(acceptHeader, ",") {
			mediaType, _, err := mime.ParseMediaType(rawMT)
			if err != nil {
				continue
			}

			switch st {
			// Schema2 manifests are supported by default, so there's no need to
			// confirm support for them.
			case manifestSchema2:
				return true
			case manifestlistSchema:
				if mediaType == manifestlist.MediaTypeManifestList {
					return true
				}
			case ociImageManifestSchema:
				if mediaType == v1.MediaTypeImageManifest {
					return true
				}
			case ociImageIndexSchema:
				if mediaType == v1.MediaTypeImageIndex {
					return true
				}
			}
		}
	}

	return false
}

func (imh *manifestHandler) rewriteManifestList(manifestList *manifestlist.DeserializedManifestList) (distribution.Manifest, error) {
	l := log.GetLogger(log.WithContext(imh)).WithFields(log.Fields{
		"manifest_list_digest": imh.Digest.String(),
		"default_arch":         defaultArch,
		"default_os":           defaultOS})
	l.Info("client does not advertise support for manifest lists, selecting a manifest image for the default arch and os")

	// Find the image manifest corresponding to the default platform.
	var manifestDigest digest.Digest
	for _, manifestDescriptor := range manifestList.Manifests {
		if manifestDescriptor.Platform.Architecture == defaultArch && manifestDescriptor.Platform.OS == defaultOS {
			manifestDigest = manifestDescriptor.Digest
			break
		}
	}

	if manifestDigest == "" {
		return nil, v2.ErrorCodeManifestUnknown.WithDetail(
			fmt.Errorf("manifest list %s does not contain a manifest image for the platform %s/%s",
				imh.Digest, defaultOS, defaultArch))
	}

	// TODO: We're passing an empty request here to skip etag matching logic.
	// This should be handled more cleanly.
	manifestGetter, err := imh.newManifestGetter(&http.Request{})
	if err != nil {
		return nil, err
	}

	manifest, err := manifestGetter.GetByDigest(imh.Context, manifestDigest)
	if err != nil {
		return nil, err
	}

	imh.Digest = manifestDigest

	return manifest, nil
}

var errETagMatches = errors.New("etag matches")

func (imh *manifestHandler) newManifestGetter(req *http.Request) (manifestGetter, error) {
	if imh.useDatabase {
		return newDBManifestGetter(imh, req)
	}

	return newFSManifestGetter(imh, req)
}

func (imh *manifestHandler) newManifestWriter() (manifestWriter, error) {
	if !imh.useDatabase {
		return newFSManifestWriter(imh)
	}

	return &dbManifestWriter{}, nil
}

type manifestGetter interface {
	GetByTag(context.Context, string) (distribution.Manifest, digest.Digest, error)
	GetByDigest(context.Context, digest.Digest) (distribution.Manifest, error)
}

type dbManifestGetter struct {
	datastore.RepositoryStore
	repoPath string
	req      *http.Request
}

func newDBManifestGetter(imh *manifestHandler, req *http.Request) (*dbManifestGetter, error) {
	return &dbManifestGetter{
		RepositoryStore: datastore.NewRepositoryStore(imh.App.db, datastore.WithRepositoryCache(imh.repoCache)),
		repoPath:        imh.Repository.Named().Name(),
		req:             req,
	}, nil
}

func (g *dbManifestGetter) GetByTag(ctx context.Context, tagName string) (distribution.Manifest, digest.Digest, error) {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": g.repoPath, "tag_name": tagName})

	dbRepo, err := g.FindByPath(ctx, g.repoPath)
	if err != nil {
		return nil, "", err
	}

	if dbRepo == nil {
		return nil, "", distribution.ErrTagUnknown{Tag: tagName}
	}

	l.Info("getting manifest by tag from database")
	dbManifest, err := g.FindManifestByTagName(ctx, dbRepo, tagName)
	if err != nil {
		return nil, "", err
	}

	// at the DB level a tag has a FK to manifests, so a tag cannot exist unless it points to an existing manifest
	if dbManifest == nil {
		return nil, "", distribution.ErrTagUnknown{Tag: tagName}
	}

	if etagMatch(g.req, dbManifest.Digest.String()) {
		return nil, dbManifest.Digest, errETagMatches
	}

	manifest, err := dbManifestToManifest(dbManifest)
	if err != nil {
		return nil, "", err
	}

	return manifest, dbManifest.Digest, nil
}

func (g *dbManifestGetter) GetByDigest(ctx context.Context, dgst digest.Digest) (distribution.Manifest, error) {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": g.repoPath, "digest": dgst})
	l.Debug("getting manifest by digest from database")

	if etagMatch(g.req, dgst.String()) {
		return nil, errETagMatches
	}

	dbRepo, err := g.FindByPath(ctx, g.repoPath)
	if err != nil {
		return nil, err
	}

	if dbRepo == nil {
		return nil, distribution.ErrManifestUnknownRevision{
			Name:     g.repoPath,
			Revision: dgst,
		}
	}
	l.Info("getting manifest by digest from database")

	// Find manifest by its digest
	dbManifest, err := g.FindManifestByDigest(ctx, dbRepo, dgst)
	if err != nil {
		return nil, err
	}
	if dbManifest == nil {
		return nil, distribution.ErrManifestUnknownRevision{
			Name:     g.repoPath,
			Revision: dgst,
		}
	}

	return dbManifestToManifest(dbManifest)
}

type fsManifestGetter struct {
	ms  distribution.ManifestService
	ts  distribution.TagService
	req *http.Request
}

func newFSManifestGetter(imh *manifestHandler, r *http.Request) (*fsManifestGetter, error) {
	manifestService, err := imh.Repository.Manifests(imh)
	if err != nil {
		return nil, err
	}

	return &fsManifestGetter{
		ts:  imh.Repository.Tags(imh),
		ms:  manifestService,
		req: r,
	}, nil
}

func (g *fsManifestGetter) GetByTag(ctx context.Context, tagName string) (distribution.Manifest, digest.Digest, error) {
	desc, err := g.ts.Get(ctx, tagName)
	if err != nil {
		return nil, "", err
	}

	if etagMatch(g.req, desc.Digest.String()) {
		return nil, desc.Digest, errETagMatches
	}

	mfst, err := g.GetByDigest(ctx, desc.Digest)
	if err != nil {
		return nil, "", err
	}

	return mfst, desc.Digest, nil
}

func (g *fsManifestGetter) GetByDigest(ctx context.Context, dgst digest.Digest) (distribution.Manifest, error) {
	if etagMatch(g.req, dgst.String()) {
		return nil, errETagMatches
	}

	return g.ms.Get(ctx, dgst)
}

type manifestWriter interface {
	Put(*manifestHandler, distribution.Manifest) error
	Tag(*manifestHandler, distribution.Manifest, string, distribution.Descriptor) error
}

type fsManifestWriter struct {
	ms      distribution.ManifestService
	ts      distribution.TagService
	options []distribution.ManifestServiceOption
}

func newFSManifestWriter(imh *manifestHandler) (*fsManifestWriter, error) {
	manifestService, err := imh.Repository.Manifests(imh)
	if err != nil {
		return nil, err
	}

	var options []distribution.ManifestServiceOption
	if imh.Tag != "" {
		options = append(options, distribution.WithTag(imh.Tag))
	}

	return &fsManifestWriter{
		ts:      imh.Repository.Tags(imh),
		ms:      manifestService,
		options: options,
	}, nil
}

func (p *fsManifestWriter) Put(imh *manifestHandler, mfst distribution.Manifest) error {
	_, err := p.ms.Put(imh, mfst, p.options...)

	return err
}

func (p *fsManifestWriter) Tag(imh *manifestHandler, _ distribution.Manifest, tag string, desc distribution.Descriptor) error {
	return p.ts.Tag(imh, tag, desc)
}

type dbManifestWriter struct{}

func (p *dbManifestWriter) Put(imh *manifestHandler, mfst distribution.Manifest) error {
	_, payload, err := mfst.Payload()
	if err != nil {
		return err
	}

	err = dbPutManifest(imh, mfst, payload)
	var mtErr datastore.ErrUnknownMediaType
	if errors.As(err, &mtErr) {
		return v2.ErrorCodeManifestInvalid.WithDetail(mtErr.Error())
	}
	return err
}

func (p *dbManifestWriter) Tag(imh *manifestHandler, mfst distribution.Manifest, tag string, _ distribution.Descriptor) error {
	repoName := imh.Repository.Named().Name()

	// To be removed on completion of: https://gitlab.com/groups/gitlab-org/-/epics/9050
	repoCache := getRepoCache(imh)

	if err := dbTagManifest(imh, imh.db, repoCache, imh.Digest, imh.Tag, repoName); err != nil {
		if errors.Is(err, datastore.ErrManifestNotFound) {
			// If online GC was already reviewing the manifest that we want to tag, and that manifest had no
			// tags before the review start, the API is unable to stop the GC from deleting the manifest (as
			// the GC already acquired the lock on the corresponding queue row). This means that once the API
			// is unblocked and tries to create the tag, a foreign key violation error will occur (because we're
			// trying to create a tag for a manifest that no longer exists) and lead to this specific error.
			// This should be extremely rare, if it ever occurs, but if it does, we should recreate the manifest
			// and tag it, instead of returning a "manifest not found response" to clients. It's expected that
			// this route handles the creation of a manifest if it doesn't exist already.
			if err = p.Put(imh, mfst); err != nil {
				return fmt.Errorf("failed to recreate manifest in database: %w", err)
			}
			if err = dbTagManifest(imh, imh.db, repoCache, imh.Digest, imh.Tag, repoName); err != nil {
				return fmt.Errorf("failed to create tag in database after manifest recreate: %w", err)
			}
		} else {
			return fmt.Errorf("failed to create tag in database: %w", err)
		}
	}

	return nil
}

func dbManifestToManifest(dbm *models.Manifest) (distribution.Manifest, error) {
	if dbm.SchemaVersion == 1 {
		return nil, distribution.ErrSchemaV1Unsupported
	}

	if dbm.SchemaVersion != 2 {
		return nil, fmt.Errorf("unrecognized manifest schema version %d", dbm.SchemaVersion)
	}

	mediaType := dbm.MediaType
	if dbm.NonConformant {
		// parse payload and get real media type
		var versioned manifest.Versioned
		if err := json.Unmarshal(dbm.Payload, &versioned); err != nil {
			return nil, fmt.Errorf("failed to unmarshal manifest payload: %w", err)
		}
		mediaType = versioned.MediaType
	}

	// TODO: Each case here is taken directly from the respective
	// registry/storage/*manifesthandler Unmarshal method. We cannot invoke them
	// directly as they are unexported, but they are relatively simple. We should
	// determine a single place for this logic during refactoring
	// https://gitlab.com/gitlab-org/container-registry/-/issues/135

	// This can be an image manifest or a manifest list
	switch mediaType {
	case schema2.MediaTypeManifest:
		m := &schema2.DeserializedManifest{}
		if err := m.UnmarshalJSON(dbm.Payload); err != nil {
			return nil, err
		}

		return m, nil
	case v1.MediaTypeImageManifest:
		m := &ocischema.DeserializedManifest{}
		if err := m.UnmarshalJSON(dbm.Payload); err != nil {
			return nil, err
		}

		return m, nil
	case manifestlist.MediaTypeManifestList, v1.MediaTypeImageIndex:
		m := &manifestlist.DeserializedManifestList{}
		if err := m.UnmarshalJSON(dbm.Payload); err != nil {
			return nil, err
		}

		return m, nil
	case "":
		// OCI image or image index - no media type in the content

		// First see if it looks like an image index
		resIndex := &manifestlist.DeserializedManifestList{}
		if err := resIndex.UnmarshalJSON(dbm.Payload); err != nil {
			return nil, err
		}
		if resIndex.Manifests != nil {
			return resIndex, nil
		}

		// Otherwise, assume it must be an image manifest
		m := &ocischema.DeserializedManifest{}
		if err := m.UnmarshalJSON(dbm.Payload); err != nil {
			return nil, err
		}

		return m, nil
	default:
		return nil, distribution.ErrManifestVerification{fmt.Errorf("unrecognized manifest content type %s", dbm.MediaType)}
	}
}

func etagMatch(r *http.Request, etag string) bool {
	for _, headerVal := range r.Header["If-None-Match"] {
		if headerVal == etag || headerVal == fmt.Sprintf(`"%s"`, etag) { // allow quoted or unquoted
			return true
		}
	}
	return false
}

// PutManifest validates and stores a manifest in the registry.
func (imh *manifestHandler) PutManifest(w http.ResponseWriter, r *http.Request) {
	l := log.GetLogger(log.WithContext(imh))
	l.Debug("PutImageManifest")

	var jsonBuf bytes.Buffer
	if err := copyFullPayload(imh, w, r, &jsonBuf, maxManifestBodySize, "image manifest PUT"); err != nil {
		// copyFullPayload reports the error if necessary
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestInvalid.WithDetail(err.Error()))
		return
	}

	mediaType := r.Header.Get("Content-Type")
	manifest, desc, err := distribution.UnmarshalManifest(mediaType, jsonBuf.Bytes())

	if err != nil {
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestInvalid.WithDetail(err.Error()))
		return
	}

	if imh.Digest != "" {
		if desc.Digest != imh.Digest {
			l.WithFields(log.Fields{
				"payload_digest":  desc.Digest,
				"provided_digest": imh.Digest,
			}).Error("payload digest does not match provided digest")
			imh.Errors = append(imh.Errors, v2.ErrorCodeDigestInvalid)
			return
		}
	} else if imh.Tag != "" {
		imh.Digest = desc.Digest
	} else {
		imh.Errors = append(imh.Errors, v2.ErrorCodeTagInvalid.WithDetail("no tag or digest specified"))
		return
	}

	isAnOCIManifest := mediaType == v1.MediaTypeImageManifest || mediaType == v1.MediaTypeImageIndex

	if isAnOCIManifest {
		l.Debug("Putting an OCI Manifest!")
	} else {
		l.Debug("Putting a Docker Manifest!")
	}

	manifestList, isManifestList := manifest.(*manifestlist.DeserializedManifestList)

	if isManifestList {
		logIfManifestListInvalid(imh, manifestList, http.MethodPut)
	}

	if err := imh.applyResourcePolicy(manifest); err != nil {
		imh.Errors = append(imh.Errors, err)
		return
	}

	manifestWriter, err := imh.newManifestWriter()
	if err != nil {
		imh.Errors = append(imh.Errors, err)
		return
	}

	if err = manifestWriter.Put(imh, manifest); err != nil {
		imh.appendPutError(err)
		return
	}

	// Tag this manifest
	if imh.Tag != "" {
		if err = manifestWriter.Tag(imh, manifest, imh.Tag, desc); err != nil {
			imh.appendPutError(err)
			return
		}
	}

	if err := imh.queueBridge.ManifestPushed(imh.Repository.Named(), manifest, distribution.WithTagOption{Tag: imh.Tag}); err != nil {
		l.WithError(err).Error("dispatching manifest push to listener")
	}

	// Construct a canonical url for the uploaded manifest.
	ref, err := reference.WithDigest(imh.Repository.Named(), imh.Digest)
	if err != nil {
		imh.Errors = append(imh.Errors, errcode.ErrorCodeUnknown.WithDetail(err))
		return
	}

	location, err := imh.urlBuilder.BuildManifestURL(ref)
	if err != nil {
		// NOTE(stevvooe): Given the behavior above, this absurdly unlikely to
		// happen. We'll log the error here but proceed as if it worked. Worst
		// case, we set an empty location header.
		l.WithError(err).Error("error building manifest url from digest")
	}

	w.Header().Set("Location", location)
	w.Header().Set("Docker-Content-Digest", imh.Digest.String())

	w.WriteHeader(http.StatusCreated)

	l.WithFields(log.Fields{
		"media_type":      desc.MediaType,
		"size_bytes":      desc.Size,
		"digest":          desc.Digest,
		"tag_name":        imh.Tag,
		"reference_count": len(manifest.References()),
	}).Info("manifest uploaded")
}

func (imh *manifestHandler) appendPutError(err error) {
	if errors.Is(err, distribution.ErrUnsupported) {
		imh.Errors = append(imh.Errors, errcode.ErrorCodeUnsupported)
		return
	}
	if errors.Is(err, distribution.ErrAccessDenied) {
		imh.Errors = append(imh.Errors, errcode.ErrorCodeDenied)
		return
	}
	if errors.Is(err, distribution.ErrSchemaV1Unsupported) {
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestInvalid.WithDetail("manifest type unsupported"))
		return
	}
	if errors.Is(err, digest.ErrDigestInvalidFormat) {
		imh.Errors = append(imh.Errors, v2.ErrorCodeDigestInvalid.WithDetail(err))
		return
	}

	switch err := err.(type) {
	case distribution.ErrManifestVerification:
		for _, verificationError := range err {
			switch verificationError := verificationError.(type) {
			case distribution.ErrManifestBlobUnknown:
				imh.Errors = append(imh.Errors, v2.ErrorCodeManifestBlobUnknown.WithDetail(verificationError.Digest))
			case distribution.ErrManifestNameInvalid:
				imh.Errors = append(imh.Errors, v2.ErrorCodeNameInvalid.WithDetail(err))
			case distribution.ErrManifestUnverified:
				imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnverified)
			case distribution.ErrManifestReferencesExceedLimit:
				imh.Errors = append(imh.Errors, v2.ErrorCodeManifestReferenceLimit.WithDetail(err))
			case distribution.ErrManifestPayloadSizeExceedsLimit:
				imh.Errors = append(imh.Errors, v2.ErrorCodeManifestPayloadSizeLimit.WithDetail(err.Error()))
			default:
				if errors.Is(verificationError, digest.ErrDigestInvalidFormat) {
					imh.Errors = append(imh.Errors, v2.ErrorCodeDigestInvalid)
				} else {
					imh.Errors = append(imh.Errors, errcode.FromUnknownError(verificationError))
				}
			}
		}
	case errcode.Error:
		imh.Errors = append(imh.Errors, err)
	default:
		imh.Errors = append(imh.Errors, errcode.FromUnknownError(err))
	}
}

func dbPutManifest(imh *manifestHandler, manifest distribution.Manifest, payload []byte) error {
	switch reqManifest := manifest.(type) {
	case *schema2.DeserializedManifest:
		return dbPutManifestSchema2(imh, reqManifest, payload)
	case *ocischema.DeserializedManifest:
		return dbPutManifestOCI(imh, reqManifest, payload)
	case *manifestlist.DeserializedManifestList:
		return dbPutManifestList(imh, reqManifest, payload)
	default:
		return v2.ErrorCodeManifestInvalid.WithDetail("manifest type unsupported")
	}
}

const (
	manifestTagGCReviewWindow = 1 * time.Hour
	manifestTagGCLockTimeout  = 5 * time.Second
)

func dbTagManifest(ctx context.Context, db datastore.Handler, cache datastore.RepositoryCache, dgst digest.Digest, tagName, path string) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": path, "manifest_digest": dgst, "tag_name": tagName})
	l.Debug("tagging manifest")

	repositoryStore := datastore.NewRepositoryStore(db, datastore.WithRepositoryCache(cache))
	dbRepo, err := repositoryStore.FindByPath(ctx, path)
	if err != nil {
		return err
	}

	// TODO: If we return the manifest ID from the putDatabase methods, we can
	// avoid looking up the manifest by digest.
	dbManifest, err := repositoryStore.FindManifestByDigest(ctx, dbRepo, dgst)
	if err != nil {
		return err
	}
	if dbManifest == nil {
		return fmt.Errorf("manifest %s not found in database", dgst)
	}

	l.Debug("creating tag")

	// We need to find and lock a GC manifest task that is related with the manifest that we're about to tag. This
	// is needed to ensure we lock any related online GC tasks to prevent race conditions around the tag creation. See:
	// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs-gitlab/db/online-garbage-collection.md#creating-a-tag-for-an-untagged-manifest
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}
	defer tx.Rollback()

	// Prevent long running transactions by setting an upper limit of manifestTagGCLockTimeout. If the GC is holding
	// the lock of a related review record, the processing there should be fast enough to avoid this. Regardless, we
	// should not let transactions open (and clients waiting) for too long. If this sensible timeout is exceeded, abort
	// the tag creation and let the client retry. This will bubble up and lead to a 503 Service Unavailable response.
	ctx, cancel := context.WithTimeout(ctx, manifestTagGCLockTimeout)
	defer cancel()

	mts := datastore.NewGCManifestTaskStore(tx)
	if _, err := mts.FindAndLockBefore(ctx, dbRepo.NamespaceID, dbRepo.ID, dbManifest.ID, time.Now().Add(manifestTagGCReviewWindow)); err != nil {
		return err
	}

	tagStore := datastore.NewTagStore(tx)
	if err := tagStore.CreateOrUpdate(ctx, &models.Tag{
		Name:         tagName,
		NamespaceID:  dbRepo.NamespaceID,
		RepositoryID: dbRepo.ID,
		ManifestID:   dbManifest.ID,
	}); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing database transaction: %w", err)
	}
	cache.InvalidateSize(ctx, dbRepo)
	return nil
}

func dbPutManifestOCI(imh *manifestHandler, manifest *ocischema.DeserializedManifest, payload []byte) error {
	repoReader := datastore.NewRepositoryStore(imh.App.db, datastore.WithRepositoryCache(getRepoCache(imh)))
	repoPath := imh.Repository.Named().Name()

	v := validation.NewOCIValidator(
		&datastore.RepositoryManifestService{RepositoryReader: repoReader, RepositoryPath: repoPath},
		&datastore.RepositoryBlobService{RepositoryReader: repoReader, RepositoryPath: repoPath},
		imh.App.manifestRefLimit,
		imh.App.manifestPayloadSizeLimit,
		imh.App.manifestURLs,
	)

	if err := v.Validate(imh, manifest); err != nil {
		return err
	}

	return dbPutManifestV2(imh, manifest, payload, false)
}

func dbPutManifestSchema2(imh *manifestHandler, manifest *schema2.DeserializedManifest, payload []byte) error {
	repoReader := datastore.NewRepositoryStore(imh.App.db, datastore.WithRepositoryCache(getRepoCache(imh)))
	repoPath := imh.Repository.Named().Name()

	v := validation.NewSchema2Validator(
		&datastore.RepositoryManifestService{RepositoryReader: repoReader, RepositoryPath: repoPath},
		&datastore.RepositoryBlobService{RepositoryReader: repoReader, RepositoryPath: repoPath},
		imh.App.manifestRefLimit,
		imh.App.manifestPayloadSizeLimit,
		imh.App.manifestURLs,
	)

	if err := v.Validate(imh.Context, manifest); err != nil {
		return err
	}

	return dbPutManifestV2(imh, manifest, payload, false)
}

func dbPutManifestV2(imh *manifestHandler, mfst distribution.ManifestV2, payload []byte, nonConformant bool) error {
	repoPath := imh.Repository.Named().Name()

	l := log.GetLogger(log.WithContext(imh)).WithFields(log.Fields{
		"repository":      repoPath,
		"manifest_digest": imh.Digest,
		"schema_version":  mfst.Version().SchemaVersion,
	})

	// create or find target repository
	rStore := datastore.NewRepositoryStore(imh.App.db, datastore.WithRepositoryCache(getRepoCache(imh)))
	dbRepo, err := rStore.CreateOrFindByPath(imh, repoPath)
	if err != nil {
		return err
	}
	l.Info("putting manifest")

	// Find the config now to ensure that the config's blob is associated with the repository.
	dbCfgBlob, err := dbFindRepositoryBlob(imh.Context, rStore, mfst.Config(), dbRepo.Path)
	if err != nil {
		return err
	}

	dbManifest, err := rStore.FindManifestByDigest(imh.Context, dbRepo, imh.Digest)
	if err != nil {
		return err
	}
	if dbManifest == nil {
		l.Debug("manifest not found in database")

		cfg := &models.Configuration{
			MediaType: mfst.Config().MediaType,
			Digest:    dbCfgBlob.Digest,
		}

		// skip retrieval and caching of config payload if its size is over the limit
		if dbCfgBlob.Size <= datastore.ConfigSizeLimit {
			// Since filesystem writes may be optional, We cannot be sure that the
			// repository scoped filesystem blob service will have a link to the
			// configuration blob; however, since we check for repository scoped access
			// via the database above, we may retrieve the blob directly common storage.
			cfgPayload, err := imh.blobProvider.Get(imh, dbCfgBlob.Digest)
			if err != nil {
				return err
			}
			cfg.Payload = cfgPayload
		}

		m := &models.Manifest{
			NamespaceID:   dbRepo.NamespaceID,
			RepositoryID:  dbRepo.ID,
			TotalSize:     mfst.TotalSize(),
			SchemaVersion: mfst.Version().SchemaVersion,
			MediaType:     mfst.Version().MediaType,
			Digest:        imh.Digest,
			Payload:       payload,
			Configuration: cfg,
			NonConformant: nonConformant,
		}

		// if Subject present in mfst, set SubjectID in model
		ocim, ok := mfst.(distribution.ManifestOCI)
		if ok {
			subject := ocim.Subject()
			if subject.Digest.String() != "" {
				// Fetch subject_id from digest
				dbSubject, err := rStore.FindManifestByDigest(imh.Context, dbRepo, subject.Digest)
				if err != nil {
					return err
				}

				if dbSubject == nil {
					// in case something happened to the referenced manifest after validation
					return distribution.ErrManifestBlobUnknown{Digest: subject.Digest}
				}

				m.SubjectID.Int64 = dbSubject.ID
				m.SubjectID.Valid = true
			}

			if ocim.ArtifactType() != "" {
				m.ArtifactType.String = ocim.ArtifactType()
				m.ArtifactType.Valid = true
			}
		}

		// check if the manifest references non-distributable layers and mark it as such on the DB
		ll := mfst.DistributableLayers()
		m.NonDistributableLayers = len(ll) < len(mfst.Layers())

		mStore := datastore.NewManifestStore(imh.App.db)
		// Use CreateOrFind to prevent race conditions while pushing the same manifest with digest for different tags
		if err := mStore.CreateOrFind(imh, m); err != nil {
			return err
		}

		dbManifest = m

		// find and associate distributable manifest layer blobs
		for _, reqLayer := range mfst.DistributableLayers() {
			dbBlob, err := dbFindRepositoryBlob(imh.Context, rStore, reqLayer, dbRepo.Path)
			if err != nil {
				return err
			}

			// Overwrite the media type from common blob storage with the one
			// specified in the manifest json for the layer entity. The layer entity
			// has a 1-1 relationship with with the manifest, so we want to reflect
			// the manifest's description of the layer. Multiple manifest can reference
			// the same blob, so the common blob storage should remain generic.
			if ok := layerMediaTypeExists(imh, reqLayer.MediaType); ok {
				dbBlob.MediaType = reqLayer.MediaType
			}

			if err := mStore.AssociateLayerBlob(imh.Context, dbManifest, dbBlob); err != nil {
				return err
			}
		}
	}

	return nil
}

func layerMediaTypeExists(imh *manifestHandler, mt string) bool {
	l := log.GetLogger(log.WithContext(imh)).WithFields(log.Fields{"media_type": mt})
	mtStore := datastore.NewMediaTypeStore(imh.App.db)

	exists, err := mtStore.Exists(imh.Context, mt)
	if err != nil {
		l.Error("error checking for existence of media type: %v", err)
		return false
	}

	if exists {
		return true
	}

	l.Warn("unknown layer media type")

	return false
}

// dbFindRepositoryBlob finds a blob which is linked to the repository.
func dbFindRepositoryBlob(ctx context.Context, rStore datastore.RepositoryStore, desc distribution.Descriptor, repoPath string) (*models.Blob, error) {
	r, err := rStore.FindByPath(ctx, repoPath)
	if err != nil {
		return nil, err
	}
	if r == nil {
		return nil, errors.New("source repository not found in database")
	}

	b, err := rStore.FindBlob(ctx, r, desc.Digest)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, fmt.Errorf("blob not found in database")
	}

	return b, nil
}

// dbFindManifestListManifest finds a manifest which is linked to the manifest list.
func dbFindManifestListManifest(
	ctx context.Context,
	rStore datastore.RepositoryStore,
	dbRepo *models.Repository,
	dgst digest.Digest,
	repoPath string) (*models.Manifest, error) {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": repoPath, "manifest_digest": dgst})
	l.Debug("finding manifest list manifest")

	var dbManifest *models.Manifest

	dbManifest, err := rStore.FindManifestByDigest(ctx, dbRepo, dgst)
	if err != nil {
		return nil, err
	}
	if dbManifest == nil {
		return nil, fmt.Errorf("manifest %s not found", dgst)
	}

	return dbManifest, nil
}

const (
	manifestListCreateGCReviewWindow = 1 * time.Hour
	manifestListCreateGCLockTimeout  = 5 * time.Second
)

func dbPutManifestList(imh *manifestHandler, manifestList *manifestlist.DeserializedManifestList, payload []byte) error {
	if mlcompat.LikelyBuildxCache(manifestList) {
		return dbPutBuildkitIndex(imh, manifestList, payload)
	}

	repoPath := imh.Repository.Named().Name()
	l := log.GetLogger(log.WithContext(imh)).WithFields(log.Fields{
		"repository":      repoPath,
		"manifest_digest": imh.Digest,
	})
	l.Debug("putting manifest list")

	rStore := datastore.NewRepositoryStore(imh.App.db, datastore.WithRepositoryCache(getRepoCache(imh)))
	v := validation.NewManifestListValidator(
		&datastore.RepositoryManifestService{
			RepositoryReader: rStore,
			RepositoryPath:   repoPath,
		},
		&datastore.RepositoryBlobService{RepositoryReader: rStore, RepositoryPath: repoPath},
		imh.App.manifestRefLimit,
		imh.App.manifestPayloadSizeLimit,
	)

	if err := v.Validate(imh, manifestList); err != nil {
		return err
	}

	// create or find target repository
	r, err := rStore.CreateOrFindByPath(imh.Context, repoPath)
	if err != nil {
		return err
	}

	ml, err := rStore.FindManifestByDigest(imh.Context, r, imh.Digest)
	if err != nil {
		return err
	}
	if ml != nil {
		return nil
	}

	// Media type can be either Docker (`application/vnd.docker.distribution.manifest.list.v2+json`) or OCI (empty).
	// We need to make it explicit if empty, otherwise we're not able to distinguish between media types.
	mediaType := manifestList.MediaType
	if mediaType == "" {
		mediaType = v1.MediaTypeImageIndex
	}

	ml = &models.Manifest{
		NamespaceID:   r.NamespaceID,
		RepositoryID:  r.ID,
		SchemaVersion: manifestList.SchemaVersion,
		MediaType:     mediaType,
		Digest:        imh.Digest,
		Payload:       payload,
	}

	// We need to find and lock referenced manifests to ensure we lock any related online GC tasks to prevent race
	// conditions around the manifest list insert. See:
	// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs-gitlab/db/online-garbage-collection.md#creating-a-manifest-list-referencing-an-unreferenced-manifest
	mm := make([]*models.Manifest, 0, len(manifestList.Manifests))
	ids := make([]int64, 0, len(mm))
	for _, desc := range manifestList.Manifests {
		m, err := dbFindManifestListManifest(imh.Context, rStore, r, desc.Digest, r.Path)
		if err != nil {
			return err
		}
		mm = append(mm, m)
		ids = append(ids, m.ID)
	}

	tx, err := imh.db.BeginTx(imh.Context, nil)
	if err != nil {
		return fmt.Errorf("creating database transaction: %w", err)
	}
	defer tx.Rollback()

	// Prevent long running transactions by setting an upper limit of manifestListCreateGCLockTimeout. If the GC is
	// holding the lock of a related review record, the processing there should be fast enough to avoid this.
	// Regardless, we should not let transactions open (and clients waiting) for too long. If this sensible timeout
	// is exceeded, abort the request and let the client retry. This will bubble up and lead to a 503 Service
	// Unavailable response.
	ctx, cancel := context.WithTimeout(imh.Context, manifestListCreateGCLockTimeout)
	defer cancel()

	mts := datastore.NewGCManifestTaskStore(tx)
	if _, err := mts.FindAndLockNBefore(ctx, r.NamespaceID, r.ID, ids, time.Now().Add(manifestListCreateGCReviewWindow)); err != nil {
		return err
	}

	// use CreateOrFind to prevent race conditions when the same digest is used by different tags
	// and pushed at the same time https://gitlab.com/gitlab-org/container-registry/-/issues/1132
	mStore := datastore.NewManifestStore(tx)
	if err := mStore.CreateOrFind(imh, ml); err != nil {
		return err
	}

	// Associate manifests to the manifest list.
	for _, m := range mm {
		if err := mStore.AssociateManifest(imh.Context, ml, m); err != nil {
			if errors.Is(err, datastore.ErrRefManifestNotFound) {
				// This can only happen if the online GC deleted one of the referenced manifests (because they were
				// untagged/unreferenced) between the call to `FindAndLockNBefore` and `AssociateManifest`. For now
				// we need to return this error to mimic the behaviour of the corresponding filesystem validation.
				return distribution.ErrManifestVerification{
					distribution.ErrManifestBlobUnknown{Digest: m.Digest},
				}
			}
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}

	return nil
}

// applyResourcePolicy checks whether the resource class matches what has
// been authorized and allowed by the policy configuration.
func (imh *manifestHandler) applyResourcePolicy(manifest distribution.Manifest) error {
	allowedClasses := imh.App.Config.Policy.Repository.Classes
	if len(allowedClasses) == 0 {
		return nil
	}

	var class string
	switch m := manifest.(type) {
	case *schema2.DeserializedManifest:
		switch m.Config().MediaType {
		case schema2.MediaTypeImageConfig:
			class = imageClass
		case schema2.MediaTypePluginConfig:
			class = "plugin"
		default:
			return errcode.ErrorCodeDenied.WithMessage("unknown manifest class for " + m.Config().MediaType)
		}
	case *ocischema.DeserializedManifest:
		switch m.Config().MediaType {
		case v1.MediaTypeImageConfig:
			class = imageClass
		default:
			return errcode.ErrorCodeDenied.WithMessage("unknown manifest class for " + m.Config().MediaType)
		}
	}

	if class == "" {
		return nil
	}

	// Check to see if class is allowed in registry
	var allowedClass bool
	for _, c := range allowedClasses {
		if class == c {
			allowedClass = true
			break
		}
	}
	if !allowedClass {
		return errcode.ErrorCodeDenied.WithMessage(fmt.Sprintf("registry does not allow %s manifest", class))
	}

	resources := auth.AuthorizedResources(imh)
	n := imh.Repository.Named().Name()

	var foundResource bool
	for _, r := range resources {
		if r.Name == n {
			if r.Class == "" {
				r.Class = imageClass
			}
			if r.Class == class {
				return nil
			}
			foundResource = true
		}
	}

	// resource was found but no matching class was found
	if foundResource {
		return errcode.ErrorCodeDenied.WithMessage(fmt.Sprintf("repository not authorized for %s manifest", class))
	}

	return nil
}

// Workaround for https://gitlab.com/gitlab-org/container-registry/-/issues/407. This attempts to convert a Buildkit
// index to a valid OCI image manifest and validates it accordingly. The original index digest and payload are
// preserved when stored on the database.
func dbPutBuildkitIndex(imh *manifestHandler, ml *manifestlist.DeserializedManifestList, payload []byte) error {
	repoReader := datastore.NewRepositoryStore(imh.db, datastore.WithRepositoryCache(getRepoCache(imh)))
	repoPath := imh.Repository.Named().Name()

	// convert to OCI manifest and process as if it was one
	m, err := mlcompat.OCIManifestFromBuildkitIndex(ml)
	if err != nil {
		return fmt.Errorf("converting buildkit index to manifest: %w", err)
	}

	v := validation.NewOCIValidator(
		&datastore.RepositoryManifestService{RepositoryReader: repoReader, RepositoryPath: repoPath},
		&datastore.RepositoryBlobService{RepositoryReader: repoReader, RepositoryPath: repoPath},
		imh.App.manifestRefLimit,
		imh.App.manifestPayloadSizeLimit,
		imh.App.manifestURLs,
	)

	if err := v.Validate(imh, m); err != nil {
		return err
	}

	// Note that `payload` is not the deserialized manifest (`m`) payload but rather the index payload, untouched.
	// Within dbPutManifestOCIOrSchema2 we use this value for the `manifests.payload` column and source the value for
	// the `manifests.digest` column from `imh.Digest`, and not from `m`. Therefore, we keep behavioral consistency for
	// the outside world by preserving the index payload and digest while storing things internally as an OCI manifest.
	return dbPutManifestV2(imh, m, payload, true)
}

const (
	manifestDeleteGCReviewWindow = 1 * time.Hour
	manifestDeleteGCLockTimeout  = 5 * time.Second
)

// dbDeleteManifest replicates the DeleteManifest action in the metadata database. This method doesn't actually delete
// a manifest from the database (that's a task for GC, if a manifest is unreferenced), it only deletes the record that
// associates the manifest with a digest d with the repository with path repoPath. Any tags that reference the manifest
// within the repository are also deleted.
func dbDeleteManifest(ctx context.Context, db datastore.Handler, cache datastore.RepositoryCache, repoPath string, d digest.Digest) error {
	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{"repository": repoPath, "digest": d})
	l.Debug("deleting manifest from repository in database")

	rStore := datastore.NewRepositoryStore(db, datastore.WithRepositoryCache(cache))
	r, err := rStore.FindByPath(ctx, repoPath)
	if err != nil {
		return err
	}
	if r == nil {
		return fmt.Errorf("repository not found in database: %w", err)
	}

	// We need to find the manifest first and then lookup for any manifest it references (if it's a manifest list). This
	// is needed to ensure we lock any related online GC tasks to prevent race conditions around the delete. See:
	// https://gitlab.com/gitlab-org/container-registry/-/blob/master/docs-gitlab/db/online-garbage-collection.md#deleting-the-last-referencing-manifest-list
	m, err := rStore.FindManifestByDigest(ctx, r, d)
	if err != nil {
		return err
	}
	if m == nil {
		return datastore.ErrManifestNotFound
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}
	defer tx.Rollback()

	switch m.MediaType {
	case manifestlist.MediaTypeManifestList, v1.MediaTypeImageIndex:
		mStore := datastore.NewManifestStore(tx)
		mm, err := mStore.References(ctx, m)
		if err != nil {
			return err
		}

		// This should never happen, as it's not possible to delete a child manifest if it's referenced by a list, which
		// means that we'll always have at least one child manifest here. Nevertheless, log error if this ever happens.
		if len(mm) == 0 {
			l.Error("stored manifest list has no references")
			break
		}
		ids := make([]int64, 0, len(mm))
		for _, m := range mm {
			ids = append(ids, m.ID)
		}

		// Prevent long running transactions by setting an upper limit of manifestDeleteGCLockTimeout. If the GC is
		// holding the lock of a related review record, the processing there should be fast enough to avoid this.
		// Regardless, we should not let transactions open (and clients waiting) for too long. If this sensible timeout
		// is exceeded, abort the manifest delete and let the client retry. This will bubble up and lead to a 503
		// Service Unavailable response.
		ctx, cancel := context.WithTimeout(ctx, manifestDeleteGCLockTimeout)
		defer cancel()

		mts := datastore.NewGCManifestTaskStore(tx)
		if _, err := mts.FindAndLockNBefore(ctx, r.NamespaceID, r.ID, ids, time.Now().Add(manifestDeleteGCReviewWindow)); err != nil {
			return err
		}
	}

	rStore = datastore.NewRepositoryStore(tx, datastore.WithRepositoryCache(cache))
	found, err := rStore.DeleteManifest(ctx, r, d)
	if err != nil {
		return err
	}
	if !found {
		return datastore.ErrManifestNotFound
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}

	return nil
}

func (imh *manifestHandler) appendTagDeleteError(err error) {
	switch err.(type) {
	case distribution.ErrRepositoryUnknown:
		imh.Errors = append(imh.Errors, v2.ErrorCodeNameUnknown)
	case distribution.ErrTagUnknown, storagedriver.PathNotFoundError:
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnknown)
	default:
		imh.Errors = append(imh.Errors, errcode.FromUnknownError(err))
	}
}

// DeleteTag deletes a tag for a specific image name.
func (imh *manifestHandler) deleteTag() error {
	l := log.GetLogger(log.WithContext(imh))
	l.Debug("DeleteImageTag")

	if !imh.useDatabase {
		tagService := imh.Repository.Tags(imh)
		if err := tagService.Untag(imh.Context, imh.Tag); err != nil {
			return err
		}
	} else {
		// TODO: remove as part of https://gitlab.com/gitlab-org/container-registry/-/issues/1056
		repoCache := getRepoCache(imh)

		if err := dbDeleteTag(imh.Context, imh.db, repoCache, imh.Repository.Named().Name(), imh.Tag); err != nil {
			return err
		}
	}

	if err := imh.queueBridge.TagDeleted(imh.Repository.Named(), imh.Tag); err != nil {
		l.WithError(err).Error("dispatching tag delete to queue")
	}

	return nil
}

func (imh *manifestHandler) deleteManifest() error {
	l := log.GetLogger(log.WithContext(imh))
	l.Debug("DeleteImageManifest")

	if !imh.useDatabase {
		manifests, err := imh.Repository.Manifests(imh)
		if err != nil {
			return err
		}

		err = manifests.Delete(imh, imh.Digest)
		if err != nil {
			return err
		}

		tagService := imh.Repository.Tags(imh)
		referencedTags, err := tagService.Lookup(imh, distribution.Descriptor{Digest: imh.Digest})
		if err != nil {
			return err
		}

		for _, tag := range referencedTags {
			if err = tagService.Untag(imh, tag); err != nil {
				// ignore if the tag no longer exists
				if errors.As(err, &storagedriver.PathNotFoundError{}) {
					continue
				}
				return err
			}

			// we also need to send the event here since we decoupled the events from the storage drivers
			if err := imh.queueBridge.TagDeleted(imh.Repository.Named(), tag); err != nil {
				l.WithError(err).Error("queuing tag delete inside manifest delete handler")
			}
		}
	} else {
		// To be removed on completion of: https://gitlab.com/groups/gitlab-org/-/epics/9050
		repoCache := getRepoCache(imh)

		if err := dbDeleteManifest(imh.Context, imh.db, repoCache, imh.Repository.Named().String(), imh.Digest); err != nil {
			return err
		}
	}

	if err := imh.queueBridge.ManifestDeleted(imh.Repository.Named(), imh.Digest); err != nil {
		l.WithError(err).Error("queuing manifest delete")
	}

	return nil
}

// DeleteManifest removes the manifest with the given digest or the tag with the given name from the registry.
func (imh *manifestHandler) DeleteManifest(w http.ResponseWriter, _ *http.Request) {
	if !deleteEnabled(imh.App.Config) {
		imh.Errors = append(imh.Errors, errcode.ErrorCodeUnsupported)
		return
	}

	if imh.Tag != "" {
		if err := imh.deleteTag(); err != nil {
			imh.appendTagDeleteError(err)
			return
		}
	} else {
		if err := imh.deleteManifest(); err != nil {
			imh.appendManifestDeleteError(err)
			return
		}
	}

	w.WriteHeader(http.StatusAccepted)
}

func (imh *manifestHandler) appendManifestDeleteError(err error) {
	switch {
	case errors.Is(err, digest.ErrDigestUnsupported), errors.Is(err, digest.ErrDigestInvalidFormat):
		imh.Errors = append(imh.Errors, v2.ErrorCodeDigestInvalid)
	case errors.Is(err, distribution.ErrBlobUnknown), errors.Is(err, datastore.ErrManifestNotFound):
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestUnknown)
	case errors.Is(err, distribution.ErrUnsupported):
		imh.Errors = append(imh.Errors, errcode.ErrorCodeUnsupported)
	case errors.Is(err, datastore.ErrManifestReferencedInList):
		imh.Errors = append(imh.Errors, v2.ErrorCodeManifestReferencedInList)
	default:
		imh.Errors = append(imh.Errors, errcode.FromUnknownError(err))
	}
}

func logIfManifestListInvalid(ctx context.Context, ml *manifestlist.DeserializedManifestList, method string) {
	if !mlcompat.ContainsBlobs(ml) {
		return
	}

	var seenUnknownReferenceMediaTypes = make(map[string]struct{}, 0)
	var unknownReferenceMediaTypes []string

	for _, desc := range mlcompat.References(ml).Blobs {
		seenUnknownReferenceMediaTypes[desc.MediaType] = struct{}{}
	}

	for mediaType := range seenUnknownReferenceMediaTypes {
		unknownReferenceMediaTypes = append(unknownReferenceMediaTypes, mediaType)
	}

	l := log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
		"method":                        method,
		"media_type":                    ml.MediaType,
		"likely_buildx_cache":           mlcompat.LikelyBuildxCache(ml),
		"unknown_reference_media_types": strings.Join(unknownReferenceMediaTypes, ","),
	})
	l.Warn("invalid manifest list/index reference(s), please report this issue to GitLab at https://gitlab.com/gitlab-org/container-registry/-/issues/409")
}

// getRepoCache selects the cache type to be used for a repository cache. It selects between the (default)
// in-memory cache and a redis cache. The redis cache takes precedence if it is avaialible.
func getRepoCache(imh *manifestHandler) (repoCache datastore.RepositoryCache) {
	if imh.App != nil && imh.App.redisCache != nil {
		repoCache = datastore.NewCentralRepositoryCache(imh.App.redisCache)
	} else {
		repoCache = imh.repoCache
	}
	return
}
