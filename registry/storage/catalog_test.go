package storage

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/docker/distribution/registry/storage/cache/memory"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
	"github.com/docker/distribution/testutil"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type setupEnv struct {
	ctx      context.Context
	driver   driver.StorageDriver
	expected []string
	registry distribution.Namespace
}

func setupFS(t *testing.T) *setupEnv {
	d := inmemory.New()
	ctx := context.Background()
	registry, err := NewRegistry(ctx, d, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableRedirect, EnableSchema1)
	require.NoError(t, err, "error creating registry")

	repos := []string{
		"foo/a",
		"foo/b",
		"foo-bar/a",
		"bar/c",
		"bar/d",
		"bar/e",
		"foo/d/in",
		"foo-bar/b",
		"test",
	}

	for _, repo := range repos {
		makeRepo(ctx, t, repo, registry)
	}

	expected := []string{
		"bar/c",
		"bar/d",
		"bar/e",
		"foo/a",
		"foo/b",
		"foo/d/in",
		"foo-bar/a",
		"foo-bar/b",
		"test",
	}

	return &setupEnv{
		ctx:      ctx,
		driver:   d,
		expected: expected,
		registry: registry,
	}
}

func makeRepo(ctx context.Context, t *testing.T, name string, reg distribution.Namespace) {
	named, err := reference.WithName(name)
	require.NoError(t, err)

	repo, _ := reg.Repository(ctx, named)
	manifests, _ := repo.Manifests(ctx)

	layers, err := testutil.CreateRandomLayers(1)
	require.NoError(t, err)

	err = testutil.UploadBlobs(repo, layers)
	require.NoError(t, err, "failed to upload layers")

	getKeys := func(digests map[digest.Digest]io.ReadSeeker) []digest.Digest {
		dgst := make([]digest.Digest, 0, len(digests))
		for d := range digests {
			dgst = append(dgst, d)
		}
		return dgst
	}

	manifest, err := testutil.MakeSchema1Manifest(getKeys(layers))
	require.NoError(t, err)

	_, err = manifests.Put(ctx, manifest)
	require.NoError(t, err, "manifest upload failed")
}

func TestCatalog(t *testing.T) {
	env := setupFS(t)

	p := make([]string, 50)

	numFilled, err := env.registry.Repositories(env.ctx, p, "")
	assert.Equal(t, len(env.expected), numFilled, "missing items in catalog")
	assert.True(t, testEq(p, env.expected, len(env.expected)), "expected catalog repos err")
	assert.ErrorIs(t, err, io.EOF, "catalog has more values which we aren't expecting")
}

func TestCatalogInParts(t *testing.T) {
	env := setupFS(t)

	chunkLen := 3
	p := make([]string, chunkLen)

	numFilled, err := env.registry.Repositories(env.ctx, p, "")
	// nolint: testifylint // require-error
	assert.NotErrorIs(t, err, io.EOF, "expected more values in catalog")
	assert.Equal(t, len(p), numFilled, "expected more values in catalog")
	assert.True(t, testEq(p, env.expected[0:chunkLen], numFilled), "expected catalog first chunk err")

	lastRepo := p[len(p)-1]
	numFilled, err = env.registry.Repositories(env.ctx, p, lastRepo)
	// nolint: testifylint // require-error
	assert.NotEqual(t, err, io.EOF, "expected more values in catalog")
	assert.Equal(t, len(p), numFilled, "expected more values in catalog")
	assert.True(t, testEq(p, env.expected[chunkLen:chunkLen*2], numFilled), "expected catalog second chunk err")

	lastRepo = p[len(p)-1]
	numFilled, err = env.registry.Repositories(env.ctx, p, lastRepo)
	// nolint: testifylint // require-error
	assert.ErrorIs(t, err, io.EOF, "expected end of catalog")
	assert.Equal(t, len(p), numFilled, "expected end of catalog")
	assert.True(t, testEq(p, env.expected[chunkLen*2:chunkLen*3], numFilled), "expected catalog third chunk err")

	lastRepo = p[len(p)-1]
	numFilled, err = env.registry.Repositories(env.ctx, p, lastRepo)
	// nolint: testifylint // require-error
	assert.ErrorIs(t, err, io.EOF, "catalog has more values which we aren't expecting")
	assert.Equal(t, 0, numFilled, "expected catalog fourth chunk err")
}

func TestCatalogEnumerate(t *testing.T) {
	env := setupFS(t)

	var repos []string
	reposChan := make(chan string)

	// Consume found repos in a separate Goroutine to prevent blocking on foundReposChan.
	done := make(chan struct{})
	go func() {
		for r := range reposChan {
			repos = append(repos, r)
		}
		done <- struct{}{}
	}()

	repositoryEnumerator := env.registry.(distribution.RepositoryEnumerator)
	err := repositoryEnumerator.Enumerate(env.ctx, func(repoName string) error {
		reposChan <- repoName
		return nil
	})
	require.NoError(t, err, "unexpected catalog enumerate err")

	close(reposChan)
	<-done

	assert.Equal(t, len(env.expected), len(repos), "expected catalog enumerate doesn't have correct number of values")

	sort.Slice(repos, func(i, j int) bool {
		return lessPath(repos[i], repos[j])
	})

	assert.True(t, testEq(repos, env.expected, len(env.expected)), "expected catalog to enumerate over all values:\nexpected:\n%v\ngot:\n%v",
		env.expected, repos)
}

func testEq(a, b []string, size int) bool {
	for cnt := 0; cnt < size-1; cnt++ {
		if a[cnt] != b[cnt] {
			return false
		}
	}
	return true
}

func setupBadWalkEnv(t *testing.T) *setupEnv {
	d := newBadListDriver()
	ctx := context.Background()
	registry, err := NewRegistry(ctx, d, BlobDescriptorCacheProvider(memory.NewInMemoryBlobDescriptorCacheProvider()), EnableRedirect, EnableSchema1)
	require.NoError(t, err, "error creating registry")

	return &setupEnv{
		ctx:      ctx,
		driver:   d,
		registry: registry,
	}
}

type badListDriver struct {
	driver.StorageDriver
}

var _ driver.StorageDriver = &badListDriver{}

func newBadListDriver() *badListDriver {
	return &badListDriver{StorageDriver: inmemory.New()}
}

func (*badListDriver) List(_ context.Context, _ string) ([]string, error) {
	return nil, fmt.Errorf("List error")
}

func TestCatalogWalkError(t *testing.T) {
	env := setupBadWalkEnv(t)
	p := make([]string, 1)

	_, err := env.registry.Repositories(env.ctx, p, "")
	assert.NotErrorIs(t, err, io.EOF, "expected catalog driver list error")
}

func BenchmarkPathCompareEqual(b *testing.B) {
	b.StopTimer()
	pp := randomPath()
	ppb := make([]byte, len(pp))
	copy(ppb, pp)
	x, y := pp, string(ppb)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		lessPath(x, y)
	}
}

func BenchmarkPathCompareNotEqual(b *testing.B) {
	b.StopTimer()
	x, y := randomPath(), randomPath()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		lessPath(x, y)
	}
}

func BenchmarkPathCompareNative(b *testing.B) {
	b.StopTimer()
	x, y := randomPath(), randomPath()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		c := x < y
		// nolint: revive // bool-literal-in-expr
		_ = c && false
	}
}

func BenchmarkPathCompareNativeEqual(b *testing.B) {
	b.StopTimer()
	pp := randomPath()
	x, y := pp, pp
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		c := x < y
		// nolint: revive // bool-literal-in-expr
		_ = c && false
	}
}

var (
	filenameChars  = []byte("abcdefghijklmnopqrstuvwxyz0123456789")
	separatorChars = []byte("._-")
)

func randomPath() string {
	path := "/"
	length := int64(100)
	for int64(len(path)) < length {
		chunkLength := rand.Int64N(length-int64(len(path))) + 1
		chunk := randomFilename(chunkLength)
		path += chunk
		remaining := length - int64(len(path))
		if remaining == 1 {
			path += randomFilename(1)
		} else if remaining > 1 {
			path += "/"
		}
	}
	return path
}

func randomFilename(length int64) string {
	b := make([]byte, length)
	wasSeparator := true
	for i := range b {
		if !wasSeparator && i < len(b)-1 && rand.IntN(4) == 0 {
			b[i] = separatorChars[rand.IntN(len(separatorChars))]
			wasSeparator = true
		} else {
			b[i] = filenameChars[rand.IntN(len(filenameChars))]
			wasSeparator = false
		}
	}
	return string(b)
}
