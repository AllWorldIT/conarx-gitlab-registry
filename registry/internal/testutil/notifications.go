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

// assertionDelay used when the event notifications mock server is used, to account for the time
// for the mock server to receive the expected notification.
const assertionDelay = 50 * time.Millisecond

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
			// t.FailNow()) in a goroutine as we may get an undefined behavior
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

func (ns *NotificationServer) AssertEventNotification(t *testing.T, expectedEvent notifications.Event) {
	t.Helper()

	// TODO: enable test for manifest pull when the database is enabled
	// https://gitlab.com/gitlab-org/container-registry/-/issues/777
	if expectedEvent.Action == "pull" && ns.databaseEnabled {
		return
	}

	// allow some time for the mock server to handle the notification
	time.Sleep(assertionDelay)

	ns.mu.Lock()
	defer ns.mu.Unlock()

	// loop over the received events as we don't know the ID the notification system generated
	for _, receivedEvent := range ns.receivedEvents {
		if receivedEvent.Action != expectedEvent.Action {
			continue
		}

		var err error
		switch expectedEvent.Action {
		case "push":
			// TODO: handle different push validations
			err = ns.validateManifestPush(t, expectedEvent, receivedEvent)
			if err != nil {
				t.Logf("manifest push event mismatch: %v", err)
				continue
			}
			// found a match!
			return
		case "pull":
			err := ns.validateManifestPull(t, expectedEvent, receivedEvent)
			if err != nil {
				t.Logf("manifest pulled event mismatch: %v", err)
				continue
			}

			return
		case "delete":
			err := ns.validateManifestDelete(t, expectedEvent, receivedEvent)
			if err != nil {
				t.Logf("manifest delete event mismatch: %v", err)
				continue
			}

			return
		case "rename":
			validateRepositoryRename(t, expectedEvent, receivedEvent)

			return
		default:
			t.Errorf("unknown action: %q", expectedEvent.Action)
		}
	}

	t.Errorf("expected event did not match any received events")
}

func (*NotificationServer) validateManifestPush(t *testing.T, expectedEvent, receivedEvent notifications.Event) error {
	t.Helper()

	require.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	require.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	require.NotEmpty(t, receivedEvent.Request, "request was empty")
	require.NotEmpty(t, receivedEvent.Source, "source was empty")

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

	require.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	require.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	require.NotEmpty(t, receivedEvent.Request, "request was empty")
	require.NotEmpty(t, receivedEvent.Source, "source was empty")

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

	require.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	require.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	require.NotEmpty(t, receivedEvent.Request, "request was empty")
	require.NotEmpty(t, receivedEvent.Source, "source was empty")

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
func validateRepositoryRename(t *testing.T, expectedEvent, receivedEvent notifications.Event) {
	t.Helper()

	require.NotEmpty(t, receivedEvent.ID, "event ID was empty")
	require.NotEmpty(t, receivedEvent.Timestamp, "timestamp was empty")
	require.NotEmpty(t, receivedEvent.Request, "request was empty")
	require.NotEmpty(t, receivedEvent.Source, "source was empty")
	require.Equal(t, expectedEvent.Action, receivedEvent.Action)
	require.Equal(t, expectedEvent.Target.Repository, receivedEvent.Target.Repository)
	require.Equal(t, expectedEvent.Target.Rename, receivedEvent.Target.Rename)
}
