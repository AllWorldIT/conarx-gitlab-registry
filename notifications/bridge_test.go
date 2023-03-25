package notifications

import (
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/manifest/schema1"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/api/urls"
	"github.com/docker/distribution/uuid"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
)

var (
	// common environment for expected manifest events.

	repo   = "test/repo"
	source = SourceRecord{
		Addr:       "remote.test",
		InstanceID: uuid.Generate().String(),
	}
	ub = mustUB(urls.NewBuilderFromString("http://test.example.com/", false))

	actor = ActorRecord{
		Name: "test",
	}
	request = RequestRecord{}
	layers  = []schema1.FSLayer{
		{
			BlobSum: "asdf",
		},
		{
			BlobSum: "qwer",
		},
	}
	m = schema1.Manifest{
		Name:     repo,
		Tag:      "latest",
		FSLayers: layers,
	}

	sm      *schema1.SignedManifest
	payload []byte
	dgst    digest.Digest
)

func TestEventBridgeManifestPulled(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPull, event)

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.ManifestPulled(repoRef, sm); err != nil {
		t.Fatalf("unexpected error notifying manifest pull: %v", err)
	}
}

func TestEventBridgeManifestPushed(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPush, event)

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.ManifestPushed(repoRef, sm); err != nil {
		t.Fatalf("unexpected error notifying manifest pull: %v", err)
	}
}

func TestEventBridgeManifestPushedWithTag(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPush, event)
		if event.Target.Tag != "latest" {
			t.Fatalf("missing or unexpected tag: %#v", event.Target)
		}

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.ManifestPushed(repoRef, sm, distribution.WithTag(m.Tag)); err != nil {
		t.Fatalf("unexpected error notifying manifest pull: %v", err)
	}
}

func TestEventBridgeManifestPulledWithTag(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPull, event)
		if event.Target.Tag != "latest" {
			t.Fatalf("missing or unexpected tag: %#v", event.Target)
		}

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.ManifestPulled(repoRef, sm, distribution.WithTag(m.Tag)); err != nil {
		t.Fatalf("unexpected error notifying manifest pull: %v", err)
	}
}

func TestEventBridgeManifestDeleted(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkDeleted(t, EventActionDelete, event)
		if event.Target.Digest != dgst {
			t.Fatalf("unexpected digest on event target: %q != %q", event.Target.Digest, dgst)
		}
		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.ManifestDeleted(repoRef, dgst); err != nil {
		t.Fatalf("unexpected error notifying manifest pull: %v", err)
	}
}

func TestEventBridgeTagDeleted(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkDeleted(t, EventActionDelete, event)
		if event.Target.Tag != m.Tag {
			t.Fatalf("unexpected tag on event target: %q != %q", event.Target.Tag, m.Tag)
		}
		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.TagDeleted(repoRef, m.Tag); err != nil {
		t.Fatalf("unexpected error notifying tag deletion: %v", err)
	}
}

func TestEventBridgeRepoDeleted(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkDeleted(t, EventActionDelete, event)
		return nil
	})

	repoRef, _ := reference.WithName(repo)
	if err := l.RepoDeleted(repoRef); err != nil {
		t.Fatalf("unexpected error notifying repo deletion: %v", err)
	}
}

func createTestEnv(t *testing.T, fn testSinkFn) Listener {
	pk, err := libtrust.GenerateECP256PrivateKey()
	if err != nil {
		t.Fatalf("error generating private key: %v", err)
	}

	sm, err = schema1.Sign(&m, pk)
	if err != nil {
		t.Fatalf("error signing manifest: %v", err)
	}

	payload = sm.Canonical
	dgst = digest.FromBytes(payload)

	return NewBridge(ub, source, actor, request, fn, true)
}

func checkDeleted(t *testing.T, action string, event *Event) {
	if event == nil {
		t.Fatal("event is nil")
	}

	if event.Source != source {
		t.Fatalf("source not equal: %#v != %#v", event.Source, source)
	}

	if event.Request != request {
		t.Fatalf("request not equal: %#v != %#v", event.Request, request)
	}

	if event.Actor != actor {
		t.Fatalf("request not equal: %#v != %#v", event.Actor, actor)
	}

	if event.Target.Repository != repo {
		t.Fatalf("unexpected repository: %q != %q", event.Target.Repository, repo)
	}
}

func checkCommonManifest(t *testing.T, action string, event *Event) {
	checkCommon(t, event)

	if event.Action != action {
		t.Fatalf("unexpected event action: %q != %q", event.Action, action)
	}

	repoRef, _ := reference.WithName(repo)
	ref, _ := reference.WithDigest(repoRef, dgst)
	u, err := ub.BuildManifestURL(ref)
	if err != nil {
		t.Fatalf("error building expected url: %v", err)
	}

	if event.Target.URL != u {
		t.Fatalf("incorrect url passed: \n%q != \n%q", event.Target.URL, u)
	}

	if len(event.Target.References) != len(layers) {
		t.Fatalf("unexpected number of references %v != %v", len(event.Target.References), len(layers))
	}
	for i, targetReference := range event.Target.References {
		if targetReference.Digest != layers[i].BlobSum {
			t.Fatalf("unexpected reference: %q != %q", targetReference.Digest, layers[i].BlobSum)
		}
	}
}

func checkCommon(t *testing.T, event *Event) {
	if event == nil {
		t.Fatal("event is nil")
	}

	if event.Source != source {
		t.Fatalf("source not equal: %#v != %#v", event.Source, source)
	}

	if event.Request != request {
		t.Fatalf("request not equal: %#v != %#v", event.Request, request)
	}

	if event.Actor != actor {
		t.Fatalf("request not equal: %#v != %#v", event.Actor, actor)
	}

	if event.Target.Digest != dgst {
		t.Fatalf("unexpected digest on event target: %q != %q", event.Target.Digest, dgst)
	}

	if event.Target.Length != int64(len(payload)) {
		t.Fatalf("unexpected target length: %v != %v", event.Target.Length, len(payload))
	}

	if event.Target.Repository != repo {
		t.Fatalf("unexpected repository: %q != %q", event.Target.Repository, repo)
	}
}

type testSinkFn func(events *Event) error

func (tsf testSinkFn) Write(events *Event) error {
	return tsf(events)
}

func (tsf testSinkFn) Close() error { return nil }

func mustUB(ub *urls.Builder, err error) *urls.Builder {
	if err != nil {
		panic(err)
	}

	return ub
}
