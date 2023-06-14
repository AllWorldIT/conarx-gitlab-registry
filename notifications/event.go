package notifications

import (
	"fmt"
	"time"

	"github.com/docker/distribution"
)

// EventAction constants used in action field of Event.
const (
	EventActionPull   = "pull"
	EventActionPush   = "push"
	EventActionMount  = "mount"
	EventActionDelete = "delete"
)

const (
	// EventsMediaType is the mediatype for the json event envelope. If the
	// Event, ActorRecord, SourceRecord or Envelope structs change, the version
	// number should be incremented.
	EventsMediaType = "application/vnd.docker.distribution.events.v1+json"
	// LayerMediaType is the media type for image rootfs diffs (aka "layers")
	// used by Docker. We don't expect this to change for quite a while.
	layerMediaType = "application/vnd.docker.container.image.rootfs.diff+x-gtar"
)

// Envelope defines the fields of a json event envelope message that can hold
// one or more events.
type Envelope struct {
	// Events make up the contents of the envelope. Events present in a single
	// envelope are not necessarily related.
	Events []Event `json:"events,omitempty"`
}

// TODO(stevvooe): The event type should be separate from the json format. It
// should be defined as an interface. Leaving as is for now since we don't
// need that at this time. If we make this change, the struct below would be
// called "EventRecord".

// Event provides the fields required to describe a registry event.
type Event struct {
	// ID provides a unique identifier for the event.
	ID string `json:"id,omitempty"`

	// Timestamp is the time at which the event occurred.
	Timestamp time.Time `json:"timestamp,omitempty"`

	// Action indicates what action encompasses the provided event.
	Action string `json:"action,omitempty"`

	// Target uniquely describes the target of the event.
	Target Target `json:"target,omitempty"`

	// Request covers the request that generated the event.
	Request RequestRecord `json:"request,omitempty"`

	// Actor specifies the agent that initiated the event. For most
	// situations, this could be from the authorization context of the request.
	Actor ActorRecord `json:"actor,omitempty"`

	// Source identifies the registry node that generated the event. Put
	// differently, while the actor "initiates" the event, the source
	// "generates" it.
	Source SourceRecord `json:"source,omitempty"`

	// Meta is a map of key-value pairs that are loosely associated to an event.
	// This field should be used only to propagate non-common/highly-specific details
	// of an event in cases where the detail can not be communicated in existing attributes of the `Event` struct.
	// Meta is not guaranteed to be in every event or to have simiar keys across event types.
	Meta map[string]Meta `json:"meta,omitempty"`
}

// Meta is the event meta type
type Meta interface{}

// Target uniquely describes the target of the event.
type Target struct {
	// TODO(stevvooe): Use http.DetectContentType for layers, maybe.

	distribution.Descriptor

	// Length in bytes of content. Same as Size field in Descriptor.
	// Provided for backwards compatibility.
	Length int64 `json:"length,omitempty"`

	// Repository identifies the named repository.
	Repository string `json:"repository,omitempty"`

	// FromRepository identifies the named repository which a blob was mounted
	// from if appropriate.
	FromRepository string `json:"fromRepository,omitempty"`

	// URL provides a direct link to the content.
	URL string `json:"url,omitempty"`

	// Tag provides the tag
	Tag string `json:"tag,omitempty"`

	// References provides the references descriptors.
	References []distribution.Descriptor `json:"references,omitempty"`
}

// ActorRecord specifies the agent that initiated the event. For most
// situations, this could be from the authorization context of the request.
// Data in this record can refer to both the initiating client and the
// generating request.
type ActorRecord struct {
	// Name corresponds to the subject or username associated with the
	// request context that generated the event.
	Name string `json:"name,omitempty"`

	// UserType is filled when authentication was used, and we were able to identify the user
	// successfully against the auth service.
	UserType string `json:"user_type,omitempty"`

	// TODO(stevvooe): Look into setting a session cookie to get this
	// without docker daemon.
	//    SessionID

	// TODO(stevvooe): Push the "Docker-Command" header to replace cookie and
	// get the actual command.
	//    Command
}

// RequestRecord covers the request that generated the event.
type RequestRecord struct {
	// ID uniquely identifies the request that initiated the event.
	ID string `json:"id"`

	// Addr contains the ip or hostname and possibly port of the client
	// connection that initiated the event. This is the RemoteAddr from
	// the standard http request.
	Addr string `json:"addr,omitempty"`

	// Host is the externally accessible host name of the registry instance,
	// as specified by the http host header on incoming requests.
	Host string `json:"host,omitempty"`

	// Method has the request method that generated the event.
	Method string `json:"method"`

	// UserAgent contains the user agent header of the request.
	UserAgent string `json:"useragent"`
}

// SourceRecord identifies the registry node that generated the event. Put
// differently, while the actor "initiates" the event, the source "generates"
// it.
type SourceRecord struct {
	// Addr contains the ip or hostname and the port of the registry node
	// that generated the event. Generally, this will be resolved by
	// os.Hostname() along with the running port.
	Addr string `json:"addr,omitempty"`

	// InstanceID identifies a running instance of an application. Changes
	// after each restart.
	InstanceID string `json:"instanceID,omitempty"`
}

var (
	// ErrSinkClosed is returned if a write is issued to a sink that has been
	// closed. If encountered, the error should be considered terminal and
	// retries will not be successful.
	ErrSinkClosed = fmt.Errorf("sink: closed")
)

// Sink accepts and sends events.
type Sink interface {
	// Write writes an event to the sink. If no error is returned,
	// the caller will assume that the event was committed and will not
	// try to send it again. If an error is received, the caller may retry
	// sending the event. The caller should cede the slice of memory to the
	// sink and not modify it after calling this method.
	Write(event *Event) error

	// Close the sink, possibly waiting for pending events to flush.
	Close() error
}

func (e *Event) artifact() string {
	if e.Target.Tag != "" {
		return "tag"
	}

	if e.Target.MediaType == "application/octet-stream" {
		return "blob"
	}

	// We know that the previous cases take precedence, so we can fall back to manifest. The only exception right now
	// is that there is no way to distinguish between deleted blobs and manifests.
	// TODO: support for deleted blobs https://gitlab.com/gitlab-org/container-registry/-/issues/920
	return "manifest"
}
