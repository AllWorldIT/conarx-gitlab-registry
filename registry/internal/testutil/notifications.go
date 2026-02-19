package testutil

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"sync"
	"testing"
	"time"

	"github.com/docker/distribution/notifications"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertionDelay is the polling interval when waiting for event notifications
const assertionDelay = 500 * time.Millisecond

// assertionTimeout is the maximum time to wait for event notifications
const assertionTimeout = 5 * time.Second

// NotificationServer acts as a mock server that receives event notifications as configured by the registry.
type NotificationServer struct {
	URL             string
	mu              *sync.Mutex
	receivedEvents  []notifications.Event
	databaseEnabled bool
}

// NewNotificationServer creates and starts a mock server to handle registry notifications.
func NewNotificationServer(t *testing.T, databaseEnabled bool) *NotificationServer {
	t.Helper()

	ns := &NotificationServer{
		mu:              &sync.Mutex{},
		receivedEvents:  make([]notifications.Event, 0),
		databaseEnabled: databaseEnabled,
	}

	s := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			// NOTE(prozlach): we can't use require (which internally uses
			// FailNow from testing package) in a goroutine as we may get an
			// undefined behavior
			dreq, err := httputil.DumpRequest(r, true)
			assert.NoError(t, err)
			fmt.Printf("Handler got event: \n\n%s\n\n", dreq)
			events := struct {
				Events []notifications.Event `json:"events"`
			}{}
			err = json.NewDecoder(r.Body).Decode(&events)
			assert.NoError(t, err)

			assert.Len(t, events.Events, 1)
			assert.Equal(t, notifications.EventsMediaType, r.Header.Get("Content-Type"), events.Events[0].ID)

			ns.mu.Lock()
			ns.receivedEvents = append(ns.receivedEvents, events.Events[0])
			ns.mu.Unlock()

			w.WriteHeader(http.StatusOK)
		}))

	t.Cleanup(func() {
		s.Close()
	})

	ns.URL = s.URL
	return ns
}

// AssertEventNotification polls for the expected event notification with a timeout.
// This replaces the fixed time.Sleep() with a polling-based approach to handle
// timing variability in CI environments and database load balancing scenarios.
func (ns *NotificationServer) AssertEventNotification(t *testing.T, expectedEvent notifications.Event) {
	t.Helper()

	// TODO: enable test for manifest pull when the database is enabled
	// https://gitlab.com/gitlab-org/container-registry/-/issues/777
	if expectedEvent.Action == "pull" && ns.databaseEnabled {
		return
	}

	lastSeenSize := 0

	// Poll for the event with a reasonable timeout to handle async webhook delivery
	checkF := func() bool {
		ns.mu.Lock()
		defer ns.mu.Unlock()

		if len(ns.receivedEvents) == lastSeenSize {
			t.Logf("no new events received")
			return false
		}

		// loop over the received events as we don't know the ID the notification system generated
		for _, receivedEvent := range ns.receivedEvents[lastSeenSize:] {
			if receivedEvent.Action != expectedEvent.Action {
				continue
			}

			var err error
			switch expectedEvent.Action {
			case "push":
				err = ns.validateManifestPush(t, expectedEvent, receivedEvent)
				if err != nil {
					t.Logf("manifest push event mismatch: %v", err)
					continue
				}
				return true
			case "pull":
				err = ns.validateManifestPull(t, expectedEvent, receivedEvent)
				if err != nil {
					t.Logf("manifest pulled event mismatch: %v", err)
					continue
				}
				return true
			case "delete":
				err = ns.validateManifestDelete(t, expectedEvent, receivedEvent)
				if err != nil {
					t.Logf("manifest delete event mismatch: %v", err)
					continue
				}
				return true
			case "rename":
				// validateRepositoryRename uses require internally,
				// so we need to check if it would pass without using require
				err = ns.validateRepositoryRename(t, expectedEvent, receivedEvent)
				if err != nil {
					t.Logf("manifest rename event mismatch: %v", err)
					continue
				}
				return true
			default:
				t.Errorf("unknown action: %q", expectedEvent.Action)
				return false
			}
		}
		lastSeenSize = len(ns.receivedEvents)

		return false
	}

	require.Eventually(
		t,
		checkF,
		assertionTimeout, assertionDelay,
		"expected event did not match any received events",
	)
}

func (*NotificationServer) validateManifestPush(t *testing.T, expectedEvent, receivedEvent notifications.Event) error {
	t.Helper()

	assert.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	assert.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	assert.NotEmpty(t, receivedEvent.Request, "request was empty")
	assert.NotEmpty(t, receivedEvent.Source, "source was empty")

	// we loop over a bunch of events looking for a match but we don't have a way
	// of identifying the event easily, so we can't use require.Equal or else the test would
	// immediately fail and won't let the loop continue
	if expectedEvent.Action != receivedEvent.Action {
		return fmt.Errorf("expected action: %q but got: %q", expectedEvent.Action, receivedEvent.Action)
	}

	if expectedEvent.Target.Digest != receivedEvent.Target.Digest {
		return fmt.Errorf("expected target digest: %q but got: %q", expectedEvent.Target.Digest, receivedEvent.Target.Digest)
	}

	if expectedEvent.Target.Repository != receivedEvent.Target.Repository {
		return fmt.Errorf("expected target repository: %q but got: %q", expectedEvent.Target.Repository, receivedEvent.Target.Repository)
	}

	if expectedEvent.Target.MediaType != receivedEvent.Target.MediaType {
		return fmt.Errorf("expected target media-type: %q but got: %q", expectedEvent.Target.MediaType, receivedEvent.Target.MediaType)
	}

	if expectedEvent.Target.Tag != "" && expectedEvent.Target.Tag != receivedEvent.Target.Tag {
		return fmt.Errorf("expected tag: %q but got: %q", expectedEvent.Target.Tag, receivedEvent.Target.Tag)
	}

	if expectedEvent.Target.Size != receivedEvent.Target.Size {
		return fmt.Errorf("expected target size: %d but got: %d", expectedEvent.Target.Size, receivedEvent.Target.Size)
	}

	return nil
}

// validateManifestDelete only action, repository and tag are part of the received event
func (*NotificationServer) validateManifestDelete(t *testing.T, expectedEvent, receivedEvent notifications.Event) error {
	t.Helper()

	assert.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	assert.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	assert.NotEmpty(t, receivedEvent.Request, "request was empty")
	assert.NotEmpty(t, receivedEvent.Source, "source was empty")

	if expectedEvent.Action != receivedEvent.Action {
		return fmt.Errorf("expected action: %q but got: %q", expectedEvent.Action, receivedEvent.Action)
	}

	if expectedEvent.Target.Digest != receivedEvent.Target.Digest {
		return fmt.Errorf("expected target digest: %q but got: %q", expectedEvent.Target.Digest, receivedEvent.Target.Digest)
	}

	if expectedEvent.Target.Repository != receivedEvent.Target.Repository {
		return fmt.Errorf("expected target repository: %q but got: %q", expectedEvent.Target.Repository, receivedEvent.Target.Repository)
	}

	// delete manifest sends two events, one with digest and one with tag so we need to validate
	// according to the expected event's tag
	if expectedEvent.Target.Tag != "" && expectedEvent.Target.Tag != receivedEvent.Target.Tag {
		return fmt.Errorf("expected tag: %q but got: %q", expectedEvent.Target.Tag, receivedEvent.Target.Tag)
	} else if expectedEvent.Target.Tag == "" && receivedEvent.Target.Tag != "" {
		return fmt.Errorf("expected tag to be empty but but got: %q", receivedEvent.Target.Tag)
	}

	if expectedEvent.Actor != receivedEvent.Actor {
		return fmt.Errorf("expected actor: %q but got: %q", expectedEvent.Actor, receivedEvent.Actor)
	}
	return nil
}

func (*NotificationServer) validateManifestPull(t *testing.T, expectedEvent, receivedEvent notifications.Event) error {
	t.Helper()

	assert.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	assert.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	assert.NotEmpty(t, receivedEvent.Request, "request was empty")
	assert.NotEmpty(t, receivedEvent.Source, "source was empty")

	if expectedEvent.Action != receivedEvent.Action {
		return fmt.Errorf("expected action: %q but got: %q", expectedEvent.Action, receivedEvent.Action)
	}

	if expectedEvent.Target.MediaType != receivedEvent.Target.MediaType {
		return fmt.Errorf("expected media type: %q but got: %q", expectedEvent.Target.MediaType, receivedEvent.Target.MediaType)
	}

	if expectedEvent.Target.Digest != receivedEvent.Target.Digest {
		return fmt.Errorf("expected target digest: %q but got: %q", expectedEvent.Target.Digest, receivedEvent.Target.Digest)
	}

	if expectedEvent.Target.Repository != receivedEvent.Target.Repository {
		return fmt.Errorf("expected target repository: %q but got: %q", expectedEvent.Target.Repository, receivedEvent.Target.Repository)
	}

	if expectedEvent.Target.Size != receivedEvent.Target.Size {
		return fmt.Errorf("expected target size: %d but got: %d", expectedEvent.Target.Size, receivedEvent.Target.Size)
	}

	return nil
}

// validateRepositoryRename validates that a rename event contains the necessary fields.
func (*NotificationServer) validateRepositoryRename(t *testing.T, expectedEvent, receivedEvent notifications.Event) error {
	t.Helper()

	assert.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	assert.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	assert.NotEmpty(t, receivedEvent.Request, "request was empty")
	assert.NotEmpty(t, receivedEvent.Source, "source was empty")

	if expectedEvent.Action != receivedEvent.Action {
		return fmt.Errorf("expected action: %q but got: %q", expectedEvent.Action, receivedEvent.Action)
	}

	if expectedEvent.Target.Repository != receivedEvent.Target.Repository {
		return fmt.Errorf("expected target repository: %q but got: %q", expectedEvent.Target.Repository, receivedEvent.Target.Repository)
	}

	if expectedEvent.Target.Rename.Type != receivedEvent.Target.Rename.Type {
		return fmt.Errorf("expected rename type: %q but got: %q", expectedEvent.Target.Rename.Type, receivedEvent.Target.Rename.Type)
	}

	if expectedEvent.Target.Rename.From != receivedEvent.Target.Rename.From {
		return fmt.Errorf("expected rename from path: %q but got: %q", expectedEvent.Target.Rename.From, receivedEvent.Target.Rename.From)
	}

	if expectedEvent.Target.Rename.To != receivedEvent.Target.Rename.To {
		return fmt.Errorf("expected rename to path: %q but got: %q", expectedEvent.Target.Rename.To, receivedEvent.Target.Rename.To)
	}

	return nil
}
