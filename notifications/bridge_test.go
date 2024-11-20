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
	"github.com/stretchr/testify/require"
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
	l := createQueueBridgeTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPull, event)

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.ManifestPulled(repoRef, sm), "unexpected error notifying manifest pull")
}

func TestEventBridgeManifestPushed(t *testing.T) {
	l := createQueueBridgeTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPush, event)

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.ManifestPushed(repoRef, sm), "unexpected error notifying manifest pull")
}

func TestEventBridgeManifestPushedWithTag(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPush, event)
		require.Equal(t, "latest", event.Target.Tag, "missing or unexpected tag: %#v", event.Target)

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.ManifestPushed(repoRef, sm, distribution.WithTag(m.Tag)), "unexpected error notifying manifest pull")
}

func TestEventBridgeManifestPulledWithTag(t *testing.T) {
	l := createQueueBridgeTestEnv(t, func(event *Event) error {
		checkCommonManifest(t, EventActionPull, event)
		require.Equal(t, "latest", event.Target.Tag, "missing or unexpected tag: %#v", event.Target)

		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.ManifestPulled(repoRef, sm, distribution.WithTag(m.Tag)), "unexpected error notifying manifest pull")
}

func TestEventBridgeManifestDeleted(t *testing.T) {
	l := createQueueBridgeTestEnv(t, func(event *Event) error {
		checkDeleted(t, EventActionDelete, event)
		require.Equal(t, event.Target.Digest, dgst, "unexpected digest on event target")
		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.ManifestDeleted(repoRef, dgst), "unexpected error notifying manifest pull")
}

func TestEventBridgeTagDeleted(t *testing.T) {
	l := createQueueBridgeTestEnv(t, func(event *Event) error {
		checkDeleted(t, EventActionDelete, event)
		require.Equal(t, event.Target.Tag, m.Tag, "unexpected tag on event target")
		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.TagDeleted(repoRef, m.Tag), "unexpected error notifying tag deletion")
}

func TestEventBridgeRepoRenamed(t *testing.T) {
	rename := Rename{
		Type: NameRename,
		To:   "foo/bar",
		From: repo,
	}

	l := createQueueBridgeTestEnv(t, func(event *Event) (err error) {
		checkRenamed(t, EventActionRename, rename, event)
		return
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.RepoRenamed(repoRef, rename), "unexpected error notifying repo rename")
}

func TestEventBridgeRepoDeleted(t *testing.T) {
	l := createTestEnv(t, func(event *Event) error {
		checkDeleted(t, EventActionDelete, event)
		return nil
	})

	repoRef, _ := reference.WithName(repo)
	require.NoError(t, l.RepoDeleted(repoRef), "unexpected error notifying repo deletion")
}

func createTestEnv(t *testing.T, fn testSinkFn) Listener {
	pk, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "error generating private key")

	sm, err = schema1.Sign(&m, pk)
	require.NoError(t, err, "error signing manifest")

	payload = sm.Canonical
	dgst = digest.FromBytes(payload)

	return NewBridge(ub, source, actor, request, fn, true)
}

func createQueueBridgeTestEnv(t *testing.T, fn testSinkFn) *QueueBridge {
	pk, err := libtrust.GenerateECP256PrivateKey()
	require.NoError(t, err, "error generating private key")

	sm, err = schema1.Sign(&m, pk)
	require.NoError(t, err, "error signing manifest")

	payload = sm.Canonical
	dgst = digest.FromBytes(payload)

	return NewQueueBridge(ub, source, actor, request, fn, true)
}

func checkDeleted(t *testing.T, action string, event *Event) {
	t.Helper()

	require.NotNil(t, event, "event is nil")
	require.Equal(t, event.Source, source, "source not equal")
	require.Equal(t, event.Request, request, "request not equal")
	require.Equal(t, event.Actor, actor, "actor not equal")
	require.Equal(t, event.Action, action, "action not equal")
	require.Equal(t, event.Target.Repository, repo, "unexpected repository")
}

func checkRenamed(t *testing.T, action string, rename Rename, event *Event) {
	require.NotNil(t, event)
	require.Equal(t, event.Action, action)
	require.Equal(t, event.Source, source)
	require.Equal(t, event.Request, request)
	require.Equal(t, event.Actor, actor)
	require.Equal(t, event.Target.Repository, repo)
	require.EqualValues(t, event.Target.Rename, &rename)
}

func checkCommonManifest(t *testing.T, action string, event *Event) {
	checkCommon(t, event)
	require.Equal(t, event.Action, action, "unexpected event action")

	repoRef, _ := reference.WithName(repo)
	ref, _ := reference.WithDigest(repoRef, dgst)
	u, err := ub.BuildManifestURL(ref)
	require.NoError(t, err, "error building expected url")
	require.Equal(t, event.Target.URL, u, "incorrect url passed")

	require.Equal(t, len(event.Target.References), len(layers), "unexpected number of references")
	for i, targetReference := range event.Target.References {
		require.Equal(t, targetReference.Digest, layers[i].BlobSum, "unexpected reference")
	}
}

func checkCommon(t *testing.T, event *Event) {
	require.NotNil(t, event, "event is nil")
	require.Equal(t, event.Source, source, "source not equal")
	require.Equal(t, event.Request, request, "request not equal")
	require.Equal(t, event.Actor, actor, "actor not equal")
	require.Equal(t, event.Target.Digest, dgst, "unexpected digest on event target")
	require.Equal(t, event.Target.Length, int64(len(payload)), "unexpected target length")
	require.Equal(t, event.Target.Repository, repo, "unexpected repository")
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
