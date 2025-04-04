package notifications

import (
	"net/http"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/notifications/meta"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/uuid"
	"github.com/opencontainers/go-digest"
)

// TODO: remove this bridge https://gitlab.com/gitlab-org/container-registry/-/issues/767
type bridge struct {
	ub                URLBuilder
	includeReferences bool
	actor             ActorRecord
	source            SourceRecord
	request           RequestRecord
	sink              Sink
}

var _ Listener = &bridge{}

// URLBuilder defines a subset of url builder to be used by the event listener.
type URLBuilder interface {
	BuildManifestURL(name reference.Named) (string, error)
	BuildBlobURL(ref reference.Canonical) (string, error)
}

// NewBridge returns a notification listener that writes records to sink,
// using the actor and source. Any urls populated in the events created by
// this bridge will be created using the URLBuilder.
func NewBridge(ub URLBuilder, source SourceRecord, actor ActorRecord, request RequestRecord, sink Sink, includeReferences bool) Listener {
	return &bridge{
		ub:                ub,
		includeReferences: includeReferences,
		actor:             actor,
		source:            source,
		request:           request,
		sink:              sink,
	}
}

// NewRequestRecord builds a RequestRecord for use in NewBridge from an
// http.Request, associating it with a request id.
func NewRequestRecord(id string, r *http.Request) RequestRecord {
	return RequestRecord{
		ID:        id,
		Addr:      context.RemoteAddr(r),
		Host:      r.Host,
		Method:    r.Method,
		UserAgent: r.UserAgent(),
	}
}

func (*bridge) ManifestPushed(_ reference.Named, _ distribution.Manifest, _ ...distribution.ManifestServiceOption) error {
	return nil
}

func (*bridge) ManifestPulled(_ reference.Named, _ distribution.Manifest, _ ...distribution.ManifestServiceOption) error {
	return nil
}

func (*bridge) ManifestDeleted(_ reference.Named, _ digest.Digest) error {
	return nil
}

func (b *bridge) BlobPushed(repo reference.Named, desc distribution.Descriptor) error {
	return b.createBlobEventAndWrite(EventActionPush, repo, desc, nil)
}

func (b *bridge) BlobPulled(repo reference.Named, desc distribution.Descriptor, eventMeta *meta.Blob) error {
	return b.createBlobEventAndWrite(EventActionPull, repo, desc, eventMeta)
}

func (b *bridge) BlobMounted(repo reference.Named, desc distribution.Descriptor, fromRepo reference.Named) error {
	event, err := b.createBlobEvent(EventActionMount, repo, desc, nil)
	if err != nil {
		return err
	}
	event.Target.FromRepository = fromRepo.Name()
	return b.sink.Write(event)
}

func (b *bridge) BlobDeleted(repo reference.Named, dgst digest.Digest) error {
	return b.createBlobDeleteEventAndWrite(EventActionDelete, repo, dgst)
}

func (*bridge) TagDeleted(_ reference.Named, _ string) error {
	return nil
}

func (b *bridge) RepoDeleted(repo reference.Named) error {
	event := b.createEvent(EventActionDelete)
	event.Target.Repository = repo.Name()

	return b.sink.Write(event)
}

func (b *bridge) createBlobDeleteEventAndWrite(action string, repo reference.Named, dgst digest.Digest) error {
	event := b.createEvent(action)
	event.Target.Digest = dgst
	event.Target.Repository = repo.Name()

	return b.sink.Write(event)
}

func (b *bridge) createBlobEventAndWrite(action string, repo reference.Named, desc distribution.Descriptor, eventMeta *meta.Blob) error {
	event, err := b.createBlobEvent(action, repo, desc, eventMeta)
	if err != nil {
		return err
	}

	return b.sink.Write(event)
}

func (b *bridge) createBlobEvent(action string, repo reference.Named, desc distribution.Descriptor, eventMeta *meta.Blob) (*Event, error) {
	event := b.createEvent(action)
	event.Target.Descriptor = desc
	event.Target.Length = desc.Size
	event.Target.Repository = repo.Name()
	if eventMeta != nil {
		event.Meta = map[string]Meta{"blob": eventMeta}
	}

	ref, err := reference.WithDigest(repo, desc.Digest)
	if err != nil {
		return nil, err
	}

	event.Target.URL, err = b.ub.BuildBlobURL(ref)
	if err != nil {
		return nil, err
	}

	return event, nil
}

// createEvent creates an event with actor and source populated.
func (b *bridge) createEvent(action string) *Event {
	event := createEvent(action)
	event.Source = b.source
	event.Actor = b.actor
	event.Request = b.request

	return event
}

// createEvent returns a new event, timestamped, with the specified action.
func createEvent(action string) *Event {
	return &Event{
		ID:        uuid.Generate().String(),
		Timestamp: time.Now(),
		Action:    action,
	}
}
