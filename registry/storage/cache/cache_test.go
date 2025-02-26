package cache

import (
	"context"
	"errors"
	"testing"

	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func TestCacheSet(t *testing.T) {
	cache := newTestStatter()
	backend := newTestStatter()
	st := NewCachedBlobStatter(cache, backend)
	ctx := context.Background()

	dgst := digest.Digest("dontvalidate")
	_, err := st.Stat(ctx, dgst)
	require.ErrorIs(t, err, distribution.ErrBlobUnknown)

	desc := distribution.Descriptor{
		Digest: dgst,
	}
	require.NoError(t, backend.SetDescriptor(ctx, dgst, desc))

	actual, err := st.Stat(ctx, dgst)
	require.NoError(t, err)
	require.Equal(t, desc.Digest, actual.Digest, "unexpected descriptor")

	require.Len(t, cache.sets, 1)
	require.NotEmpty(t, cache.sets[dgst])
	require.Equal(t, desc.Digest, cache.sets[dgst][0].Digest, "unexpected descriptor")

	desc2 := distribution.Descriptor{
		Digest: digest.Digest("dontvalidate 2"),
	}
	cache.sets[dgst] = append(cache.sets[dgst], desc2)

	actual, err = st.Stat(ctx, dgst)
	require.NoError(t, err)
	require.Equal(t, desc2.Digest, actual.Digest, "unexpected descriptor")
}

func TestCacheError(t *testing.T) {
	cache := newErrTestStatter(errors.New("cache error"))
	backend := newTestStatter()
	st := NewCachedBlobStatter(cache, backend)
	ctx := context.Background()

	dgst := digest.Digest("dontvalidate")
	_, err := st.Stat(ctx, dgst)
	require.Equal(t, err, distribution.ErrBlobUnknown)

	desc := distribution.Descriptor{
		Digest: dgst,
	}
	require.NoError(t, backend.SetDescriptor(ctx, dgst, desc))

	actual, err := st.Stat(ctx, dgst)
	require.NoError(t, err)
	require.Equal(t, desc.Digest, actual.Digest, "unexpected descriptor")

	require.Empty(t, cache.sets, "set should not be called after stat error")
}

func newTestStatter() *testStatter {
	return &testStatter{
		stats: make([]digest.Digest, 0),
		sets:  make(map[digest.Digest][]distribution.Descriptor),
	}
}

func newErrTestStatter(err error) *testStatter {
	return &testStatter{
		sets: make(map[digest.Digest][]distribution.Descriptor),
		err:  err,
	}
}

type testStatter struct {
	stats []digest.Digest
	sets  map[digest.Digest][]distribution.Descriptor
	err   error
}

func (s *testStatter) Stat(_ context.Context, dgst digest.Digest) (distribution.Descriptor, error) {
	if s.err != nil {
		return distribution.Descriptor{}, s.err
	}

	if set := s.sets[dgst]; len(set) > 0 {
		return set[len(set)-1], nil
	}

	return distribution.Descriptor{}, distribution.ErrBlobUnknown
}

func (s *testStatter) SetDescriptor(_ context.Context, dgst digest.Digest, desc distribution.Descriptor) error {
	s.sets[dgst] = append(s.sets[dgst], desc)
	return s.err
}

func (s *testStatter) Clear(_ context.Context, _ digest.Digest) error {
	return s.err
}
