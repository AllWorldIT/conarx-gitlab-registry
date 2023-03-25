package notifications

import (
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/uuid"
	"github.com/opencontainers/go-digest"
)

// QueueBridge is a connector in between the current request and the queue.
// It holds the context that the events need per request along with the Queue
type QueueBridge struct {
	source  SourceRecord
	actor   ActorRecord
	request RequestRecord
	// TODO: replace sink with a Queue when we implement
	// https://gitlab.com/gitlab-org/container-registry/-/issues/335
	sink Sink

	ub                URLBuilder
	includeReferences bool
}

// NewQueueBridge creates an instance of the QueueBridge with the source, actor, request and the queue
func NewQueueBridge(ub URLBuilder, source SourceRecord, actor ActorRecord, request RequestRecord, sink Sink, includeReferences bool) *QueueBridge {
	return &QueueBridge{
		source:  source,
		actor:   actor,
		request: request,
		// TODO: use a queue instead of using a sink explicitly https://gitlab.com/gitlab-org/container-registry/-/issues/764
		sink:              sink,
		ub:                ub,
		includeReferences: includeReferences,
	}
}

// ManifestPushed creates a manifest event with the repository and its options. It
// queues the event to be sent.
func (qb *QueueBridge) ManifestPushed(repo reference.Named, sm distribution.Manifest, options ...distribution.ManifestServiceOption) error {
	manifestEvent, err := qb.createManifestEvent(EventActionPush, repo, sm)
	if err != nil {
		return err
	}

	for _, option := range options {
		if opt, ok := option.(distribution.WithTagOption); ok {
			manifestEvent.Target.Tag = opt.Tag
			break
		}
	}

	return qb.sink.Write(manifestEvent)
}

// ManifestPulled creates the corresponding event and queues the event to be sent.
func (qb *QueueBridge) ManifestPulled(repo reference.Named, sm distribution.Manifest, options ...distribution.ManifestServiceOption) error {
	manifestEvent, err := qb.createManifestEvent(EventActionPull, repo, sm)
	if err != nil {
		return err
	}

	for _, option := range options {
		if opt, ok := option.(distribution.WithTagOption); ok {
			manifestEvent.Target.Tag = opt.Tag
			break
		}
	}

	return qb.sink.Write(manifestEvent)
}

// ManifestDeleted creates and queues an event with the deleted manifest repository and digest.
func (qb *QueueBridge) ManifestDeleted(repo reference.Named, dgst digest.Digest) error {
	event := qb.createEvent(EventActionDelete)
	event.Target = Target{
		Descriptor: distribution.Descriptor{
			Digest: dgst,
		},
		Repository: repo.Name(),
	}

	return qb.sink.Write(event)
}

// TagDeleted creates and queues the related event with the repository tag details.
func (qb *QueueBridge) TagDeleted(repo reference.Named, tag string) error {
	event := qb.createEvent(EventActionDelete)
	event.Target.Repository = repo.Name()
	event.Target.Tag = tag

	return qb.sink.Write(event)
}

func (qb *QueueBridge) createManifestEvent(action string, repo reference.Named, sm distribution.Manifest) (*Event, error) {
	event := qb.createEvent(action)
	event.Target.Repository = repo.Name()

	mt, p, err := sm.Payload()
	if err != nil {
		return nil, err
	}

	// Ensure we have the canonical manifest descriptor here
	manifest, desc, err := distribution.UnmarshalManifest(mt, p)
	if err != nil {
		return nil, err
	}

	event.Target.MediaType = mt
	event.Target.Length = desc.Size
	event.Target.Size = desc.Size
	event.Target.Digest = desc.Digest
	if qb.includeReferences {
		event.Target.References = append(event.Target.References, manifest.References()...)
	}

	ref, err := reference.WithDigest(repo, event.Target.Digest)
	if err != nil {
		return nil, err
	}

	event.Target.URL, err = qb.ub.BuildManifestURL(ref)
	if err != nil {
		return nil, err
	}

	return event, nil
}

// createEvent creates an event with actor and source populated.
func (qb *QueueBridge) createEvent(action string) *Event {
	return &Event{
		ID:        uuid.Generate().String(),
		Timestamp: time.Now(),
		Action:    action,
		Source:    qb.source,
		Actor:     qb.actor,
		Request:   qb.request,
	}
}
