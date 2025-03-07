package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/notifications/meta"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/urls"
	"github.com/docker/distribution/registry/client/transport"
	"github.com/docker/distribution/registry/storage/cache"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/opencontainers/go-digest"
)

// Registry provides an interface for calling Repositories, which returns a catalog of repositories.
type Registry interface {
	Repositories(ctx context.Context, repos []string, last string) (n int, err error)
}

// checkHTTPRedirect is a callback that can manipulate redirected HTTP
// requests. It is used to preserve Accept and Range headers.
func checkHTTPRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("stopped after 10 redirects")
	}

	if len(via) > 0 {
		for headerName, headerVals := range via[0].Header {
			if headerName != "Accept" && headerName != "Range" {
				continue
			}
			for _, val := range headerVals {
				// Don't add to redirected request if redirected
				// request already has a header with the same
				// name and value.
				hasValue := false
				for _, existingVal := range req.Header[headerName] {
					if existingVal == val {
						hasValue = true
						break
					}
				}
				if !hasValue {
					req.Header.Add(headerName, val)
				}
			}
		}
	}

	return nil
}

// NewRegistry creates a registry namespace which can be used to get a listing of repositories
func NewRegistry(baseURL string, httpTransport http.RoundTripper) (Registry, error) {
	ub, err := urls.NewBuilderFromString(baseURL, false)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport:     httpTransport,
		Timeout:       1 * time.Minute,
		CheckRedirect: checkHTTPRedirect,
	}

	return &registry{
		client: client,
		ub:     ub,
	}, nil
}

type registry struct {
	client *http.Client
	ub     *urls.Builder
}

// Repositories returns a lexigraphically sorted catalog given a base URL.  The 'entries' slice will be filled up to the size
// of the slice, starting at the value provided in 'last'.  The number of entries will be returned along with io.EOF if there
// are no more entries
func (r *registry) Repositories(_ context.Context, entries []string, last string) (int, error) {
	var numFilled int
	var returnErr error

	values := buildCatalogValues(len(entries), last)
	u, err := r.ub.BuildCatalogURL(values)
	if err != nil {
		return 0, err
	}

	resp, err := r.client.Get(u)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		var ctlg struct {
			Repositories []string `json:"repositories"`
		}
		decoder := json.NewDecoder(resp.Body)

		if err := decoder.Decode(&ctlg); err != nil {
			return 0, err
		}

		copy(entries, ctlg.Repositories)

		numFilled = len(ctlg.Repositories)

		link := resp.Header.Get("Link")
		if link == "" {
			returnErr = io.EOF
		}
	} else {
		return 0, HandleErrorResponse(resp)
	}

	return numFilled, returnErr
}

// NewRepository creates a new Repository for the given repository name and base URL.
func NewRepository(name reference.Named, baseURL string, httpTransport http.RoundTripper) (distribution.Repository, error) {
	ub, err := urls.NewBuilderFromString(baseURL, false)
	if err != nil {
		return nil, err
	}

	client := &http.Client{
		Transport:     httpTransport,
		CheckRedirect: checkHTTPRedirect,
		// TODO(dmcgowan): create cookie jar
	}

	return &repository{
		client: client,
		ub:     ub,
		name:   name,
	}, nil
}

type repository struct {
	client *http.Client
	ub     *urls.Builder
	name   reference.Named
}

func (r *repository) Named() reference.Named {
	return r.name
}

func (r *repository) Blobs(_ context.Context) distribution.BlobStore {
	statter := &blobStatter{
		name:   r.name,
		ub:     r.ub,
		client: r.client,
	}
	return &blobs{
		name:    r.name,
		ub:      r.ub,
		client:  r.client,
		statter: cache.NewCachedBlobStatter(memory.NewInMemoryBlobDescriptorCacheProvider(), statter),
	}
}

func (r *repository) Manifests(_ context.Context, _ ...distribution.ManifestServiceOption) (distribution.ManifestService, error) {
	// todo(richardscothern): options should be sent over the wire
	return &manifests{
		name:   r.name,
		ub:     r.ub,
		client: r.client,
		etags:  make(map[string]string),
	}, nil
}

func (r *repository) Tags(_ context.Context) distribution.TagService {
	return &tags{
		client: r.client,
		ub:     r.ub,
		name:   r.Named(),
	}
}

// tags implements remote tagging operations.
type tags struct {
	client *http.Client
	ub     *urls.Builder
	name   reference.Named
}

// All returns all tags
func (t *tags) All(_ context.Context) ([]string, error) {
	var tags []string

	listURLStr, err := t.ub.BuildTagsURL(t.name)
	if err != nil {
		return tags, err
	}

	listURL, err := url.Parse(listURLStr)
	if err != nil {
		return tags, err
	}

	for {
		resp, err := t.client.Get(listURL.String())
		if err != nil {
			return tags, err
		}
		// nolint: revive // defer
		defer resp.Body.Close()

		if !SuccessStatus(resp.StatusCode) {
			return tags, HandleErrorResponse(resp)
		}

		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return tags, err
		}

		tagsResponse := struct {
			Tags []string `json:"tags"`
		}{}
		if err := json.Unmarshal(b, &tagsResponse); err != nil {
			return tags, err
		}
		tags = append(tags, tagsResponse.Tags...)

		link := resp.Header.Get("Link")
		if link == "" {
			return tags, nil
		}

		linkURLStr := strings.Trim(strings.Split(link, ";")[0], "<>")
		linkURL, err := url.Parse(linkURLStr)
		if err != nil {
			return tags, err
		}

		listURL = listURL.ResolveReference(linkURL)
	}
}

func descriptorFromResponse(response *http.Response) (distribution.Descriptor, error) {
	desc := distribution.Descriptor{}
	headers := response.Header

	ctHeader := headers.Get("Content-Type")
	if ctHeader == "" {
		return distribution.Descriptor{}, errors.New("missing or empty Content-Type header")
	}
	desc.MediaType = ctHeader

	digestHeader := headers.Get("Docker-Content-Digest")
	if digestHeader == "" {
		responseBytes, err := io.ReadAll(response.Body)
		if err != nil {
			return distribution.Descriptor{}, err
		}
		_, desc, err := distribution.UnmarshalManifest(ctHeader, responseBytes)
		if err != nil {
			return distribution.Descriptor{}, err
		}
		return desc, nil
	}

	dgst, err := digest.Parse(digestHeader)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	desc.Digest = dgst

	lengthHeader := headers.Get("Content-Length")
	if lengthHeader == "" {
		return distribution.Descriptor{}, errors.New("missing or empty Content-Length header")
	}
	length, err := strconv.ParseInt(lengthHeader, 10, 64)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	desc.Size = length

	return desc, nil
}

// Get issues a HEAD request for a Manifest against its named endpoint in order
// to construct a descriptor for the tag.  If the registry doesn't support HEADing
// a manifest, fallback to GET.
func (t *tags) Get(_ context.Context, tag string) (distribution.Descriptor, error) {
	ref, err := reference.WithTag(t.name, tag)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	u, err := t.ub.BuildManifestURL(ref)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	newRequest := func(method string) (*http.Response, error) {
		req, err := http.NewRequest(method, u, nil)
		if err != nil {
			return nil, err
		}

		for _, t := range distribution.ManifestMediaTypes() {
			req.Header.Add("Accept", t)
		}
		resp, err := t.client.Do(req)
		return resp, err
	}

	resp, err := newRequest(http.MethodHead)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 400 && len(resp.Header.Get("Docker-Content-Digest")) > 0:
		// if the response is a success AND a Docker-Content-Digest can be retrieved from the headers
		return descriptorFromResponse(resp)
	default:
		// if the response is an error - there will be no body to decode.
		// Issue a GET request:
		//   - for data from a server that does not handle HEAD
		//   - to get error details in case of a failure
		resp, err = newRequest(http.MethodGet)
		if err != nil {
			return distribution.Descriptor{}, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 400 {
			return descriptorFromResponse(resp)
		}
		return distribution.Descriptor{}, HandleErrorResponse(resp)
	}
}

func (*tags) Lookup(_ context.Context, _ distribution.Descriptor) ([]string, error) {
	panic("not implemented")
}

func (*tags) Tag(_ context.Context, _ string, _ distribution.Descriptor) error {
	panic("not implemented")
}

func (t *tags) Untag(_ context.Context, tag string) error {
	ref, err := reference.WithTag(t.name, tag)
	if err != nil {
		return err
	}

	u, err := t.ub.BuildManifestURL(ref)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodDelete, u, nil)
	if err != nil {
		return err
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		return nil
	}
	return HandleErrorResponse(resp)
}

type manifests struct {
	name   reference.Named
	ub     *urls.Builder
	client *http.Client
	etags  map[string]string
}

func (ms *manifests) Exists(_ context.Context, dgst digest.Digest) (bool, error) {
	ref, err := reference.WithDigest(ms.name, dgst)
	if err != nil {
		return false, err
	}
	u, err := ms.ub.BuildManifestURL(ref)
	if err != nil {
		return false, err
	}

	resp, err := ms.client.Head(u)
	if err != nil {
		return false, err
	}

	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		return true, nil
	} else if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, HandleErrorResponse(resp)
}

// AddEtagToTag allows a client to supply an eTag to Get which will be
// used for a conditional HTTP request.  If the eTag matches, a nil manifest
// and ErrManifestNotModified error will be returned. etag is automatically
// quoted when added to this map.
func AddEtagToTag(tag, etag string) distribution.ManifestServiceOption {
	return etagOption{tag, etag}
}

type etagOption struct{ tag, etag string }

func (o etagOption) Apply(ms distribution.ManifestService) error {
	if ms, ok := ms.(*manifests); ok {
		ms.etags[o.tag] = fmt.Sprintf(`"%s"`, o.etag)
		return nil
	}
	return fmt.Errorf("etag options is a client-only option")
}

// ReturnContentDigest allows a client to set a the content digest on
// a successful request from the 'Docker-Content-Digest' header. This
// returned digest is represents the digest which the registry uses
// to refer to the content and can be used to delete the content.
func ReturnContentDigest(dgst *digest.Digest) distribution.ManifestServiceOption {
	return contentDigestOption{dgst}
}

type contentDigestOption struct{ digest *digest.Digest }

func (contentDigestOption) Apply(_ distribution.ManifestService) error {
	return nil
}

func (ms *manifests) Get(_ context.Context, dgst digest.Digest, options ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	var (
		digestOrTag string
		ref         reference.Named
		err         error
		contentDgst *digest.Digest
		mediaTypes  []string
	)

	for _, option := range options {
		switch opt := option.(type) {
		case distribution.WithTagOption:
			digestOrTag = opt.Tag
			ref, err = reference.WithTag(ms.name, opt.Tag)
			if err != nil {
				return nil, err
			}
		case contentDigestOption:
			contentDgst = opt.digest
		case distribution.WithManifestMediaTypesOption:
			mediaTypes = opt.MediaTypes
		default:
			err := option.Apply(ms)
			if err != nil {
				return nil, err
			}
		}
	}

	if digestOrTag == "" {
		digestOrTag = dgst.String()
		ref, err = reference.WithDigest(ms.name, dgst)
		if err != nil {
			return nil, err
		}
	}

	if len(mediaTypes) == 0 {
		mediaTypes = distribution.ManifestMediaTypes()
	}

	u, err := ms.ub.BuildManifestURL(ref)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}

	for _, t := range mediaTypes {
		req.Header.Add("Accept", t)
	}

	if _, ok := ms.etags[digestOrTag]; ok {
		req.Header.Set("If-None-Match", ms.etags[digestOrTag])
	}

	resp, err := ms.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return nil, distribution.ErrManifestNotModified
	}
	if SuccessStatus(resp.StatusCode) {
		if contentDgst != nil {
			dgst, err := digest.Parse(resp.Header.Get("Docker-Content-Digest"))
			if err == nil {
				*contentDgst = dgst
			}
		}
		mt := resp.Header.Get("Content-Type")
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		m, _, err := distribution.UnmarshalManifest(mt, body)
		if err != nil {
			return nil, err
		}
		return m, nil
	}
	return nil, HandleErrorResponse(resp)
}

// Put puts a manifest.  A tag can be specified using an options parameter which uses some shared state to hold the
// tag name in order to build the correct upload URL.
func (ms *manifests) Put(_ context.Context, m distribution.Manifest, options ...distribution.ManifestServiceOption) (digest.Digest, error) {
	ref := ms.name
	var tagged bool

	for _, option := range options {
		if opt, ok := option.(distribution.WithTagOption); ok {
			var err error
			ref, err = reference.WithTag(ref, opt.Tag)
			if err != nil {
				return "", err
			}
			tagged = true
		} else {
			err := option.Apply(ms)
			if err != nil {
				return "", err
			}
		}
	}
	mediaType, p, err := m.Payload()
	if err != nil {
		return "", err
	}

	if !tagged {
		// generate a canonical digest and Put by digest
		_, d, err := distribution.UnmarshalManifest(mediaType, p)
		if err != nil {
			return "", err
		}
		ref, err = reference.WithDigest(ref, d.Digest)
		if err != nil {
			return "", err
		}
	}

	manifestURL, err := ms.ub.BuildManifestURL(ref)
	if err != nil {
		return "", err
	}

	putRequest, err := http.NewRequest(http.MethodPut, manifestURL, bytes.NewReader(p))
	if err != nil {
		return "", err
	}

	putRequest.Header.Set("Content-Type", mediaType)

	resp, err := ms.client.Do(putRequest)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		dgstHeader := resp.Header.Get("Docker-Content-Digest")
		dgst, err := digest.Parse(dgstHeader)
		if err != nil {
			return "", err
		}

		return dgst, nil
	}

	return "", HandleErrorResponse(resp)
}

func (ms *manifests) Delete(_ context.Context, dgst digest.Digest) error {
	ref, err := reference.WithDigest(ms.name, dgst)
	if err != nil {
		return err
	}
	u, err := ms.ub.BuildManifestURL(ref)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodDelete, u, nil)
	if err != nil {
		return err
	}

	resp, err := ms.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		return nil
	}
	return HandleErrorResponse(resp)
}

// todo(richardscothern): Restore interface and implementation with merge of #1050
/*
func (ms *manifests) Enumerate(ctx context.Context, manifests []distribution.Manifest, last distribution.Manifest) (n int, err error) {
	panic("not supported")
}
*/

type blobs struct {
	name   reference.Named
	ub     *urls.Builder
	client *http.Client

	statter distribution.BlobDescriptorService
	distribution.BlobDeleter
}

func sanitizeLocation(location, base string) (string, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", err
	}

	locationURL, err := url.Parse(location)
	if err != nil {
		return "", err
	}

	return baseURL.ResolveReference(locationURL).String(), nil
}

func (bs *blobs) Stat(ctx context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	return bs.statter.Stat(ctx, dgst)
}

func (bs *blobs) Get(ctx context.Context, dgst digest.Digest) ([]byte, error) {
	reader, err := bs.Open(ctx, dgst)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

func (bs *blobs) Open(_ context.Context, dgst digest.Digest) (distribution.ReadSeekCloser, error) {
	ref, err := reference.WithDigest(bs.name, dgst)
	if err != nil {
		return nil, err
	}
	blobURL, err := bs.ub.BuildBlobURL(ref)
	if err != nil {
		return nil, err
	}

	return transport.NewHTTPReadSeeker(bs.client, blobURL,
		func(resp *http.Response) error {
			if resp.StatusCode == http.StatusNotFound {
				return distribution.ErrBlobUnknown
			}
			return HandleErrorResponse(resp)
		}), nil
}

func (*blobs) ServeBlob(_ context.Context, _ http.ResponseWriter, _ *http.Request, _ digest.Digest) (*meta.Blob, error) {
	panic("not implemented")
}

func (bs *blobs) Put(ctx context.Context, mediaType string, p []byte) (distribution.Descriptor, error) {
	writer, err := bs.Create(ctx)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	dgstr := digest.Canonical.Digester()
	n, err := io.Copy(writer, io.TeeReader(bytes.NewReader(p), dgstr.Hash()))
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if n < int64(len(p)) {
		return distribution.Descriptor{}, fmt.Errorf("short copy: wrote %d of %d", n, len(p))
	}

	desc := distribution.Descriptor{
		MediaType: mediaType,
		Size:      int64(len(p)),
		Digest:    dgstr.Digest(),
	}

	return writer.Commit(ctx, desc)
}

type optionFunc func(any) error

func (f optionFunc) Apply(v any) error {
	return f(v)
}

// WithMountFrom returns a BlobCreateOption which designates that the blob should be
// mounted from the given canonical reference.
func WithMountFrom(ref reference.Canonical) distribution.BlobCreateOption {
	return optionFunc(func(v any) error {
		opts, ok := v.(*distribution.CreateOptions)
		if !ok {
			return fmt.Errorf("unexpected options type: %T", v)
		}

		opts.Mount.ShouldMount = true
		opts.Mount.From = ref

		return nil
	})
}

func (bs *blobs) Create(ctx context.Context, options ...distribution.BlobCreateOption) (distribution.BlobWriter, error) {
	var opts distribution.CreateOptions

	for _, option := range options {
		err := option.Apply(&opts)
		if err != nil {
			return nil, err
		}
	}

	var values []url.Values

	if opts.Mount.ShouldMount {
		values = append(values, url.Values{"from": {opts.Mount.From.Name()}, "mount": {opts.Mount.From.Digest().String()}})
	}

	u, err := bs.ub.BuildBlobUploadURL(bs.name, values...)
	if err != nil {
		return nil, err
	}

	resp, err := bs.client.Post(u, "", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusCreated:
		desc, err := bs.statter.Stat(ctx, opts.Mount.From.Digest())
		if err != nil {
			return nil, err
		}
		return nil, distribution.ErrBlobMounted{From: opts.Mount.From, Descriptor: desc}
	case http.StatusAccepted:
		// TODO(dmcgowan): Check for invalid UUID
		uuid := resp.Header.Get("Docker-Upload-UUID")
		location, err := sanitizeLocation(resp.Header.Get("Location"), u)
		if err != nil {
			return nil, err
		}

		return &httpBlobUpload{
			statter:   bs.statter,
			client:    bs.client,
			uuid:      uuid,
			startedAt: time.Now(),
			location:  location,
		}, nil
	default:
		return nil, HandleErrorResponse(resp)
	}
}

func (*blobs) Resume(_ context.Context, _ string) (distribution.BlobWriter, error) {
	panic("not implemented")
}

func (bs *blobs) Delete(ctx context.Context, dgst digest.Digest) error {
	return bs.statter.Clear(ctx, dgst)
}

type blobStatter struct {
	name   reference.Named
	ub     *urls.Builder
	client *http.Client
}

func (bs *blobStatter) Stat(_ context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	ref, err := reference.WithDigest(bs.name, dgst)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	u, err := bs.ub.BuildBlobURL(ref)
	if err != nil {
		return distribution.Descriptor{}, err
	}

	resp, err := bs.client.Head(u)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		lengthHeader := resp.Header.Get("Content-Length")
		if lengthHeader == "" {
			return distribution.Descriptor{}, fmt.Errorf("missing content-length header for request: %s", u)
		}

		length, err := strconv.ParseInt(lengthHeader, 10, 64)
		if err != nil {
			return distribution.Descriptor{}, fmt.Errorf("error parsing content-length: %v", err)
		}

		return distribution.Descriptor{
			MediaType: resp.Header.Get("Content-Type"),
			Size:      length,
			Digest:    dgst,
		}, nil
	} else if resp.StatusCode == http.StatusNotFound {
		return distribution.Descriptor{}, distribution.ErrBlobUnknown
	}
	return distribution.Descriptor{}, HandleErrorResponse(resp)
}

func buildCatalogValues(maxEntries int, last string) url.Values {
	values := url.Values{}

	if maxEntries > 0 {
		values.Add("n", strconv.Itoa(maxEntries))
	}

	if last != "" {
		values.Add("last", last)
	}

	return values
}

func (bs *blobStatter) Clear(_ context.Context, dgst digest.Digest) error {
	ref, err := reference.WithDigest(bs.name, dgst)
	if err != nil {
		return err
	}
	blobURL, err := bs.ub.BuildBlobURL(ref)
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodDelete, blobURL, nil)
	if err != nil {
		return err
	}

	resp, err := bs.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if SuccessStatus(resp.StatusCode) {
		return nil
	}
	return HandleErrorResponse(resp)
}

func (*blobStatter) SetDescriptor(_ context.Context, _ digest.Digest, _ distribution.Descriptor) error {
	return nil
}
