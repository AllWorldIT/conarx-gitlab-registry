package testutil

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// RandomBlob is used when we explicitly do not want to re-use pseudo-random
// sequence (i.e. we need two different blobs), or using BlobFactory would
// bring too much boilerplate.
func RandomBlob(t testing.TB, size int) []byte {
	t.Helper()

	res := make([]byte, size)

	seed := SeedFromUnixNano(time.Now().UnixNano())
	t.Logf("using rng seed %v for random blob", seed)

	rng := rand.NewChaCha8(seed)
	// NOTE(prozlach): On my laptop, generating 2GiB of rng data takes around
	// 1s. rng always returns requested data and never returns an error
	_, _ = rng.Read(res)

	return res
}

type BlobberFactory struct {
	// NOTE(prozlach): even though it is slice, it is meant to be immutable
	// after creation so that we avoid reallocations. See also the panic() call
	// in the GetBlober() func.
	// NOTE(prozlach): in theory we could skip caching and simply reset rng
	// every time with the same seed to get the same pseudo-random sequence,
	// but we have tests (e.g. TestWriteReadLargeStreams) in storage drivers
	// testsuite that rely on pushing large slices of data to backend in order
	// to test chunking. So we would need to allocate a big slice/chunk of
	// memory anyway and we would need extra CPU cycles to generate blobs in
	// other places not to mention the complexity. The current approach seemed
	// simpler.
	rngCache []byte
}

func NewBlobberFactory(initialRngCacheSize int64, seed SeedT) *BlobberFactory {
	res := &BlobberFactory{
		rngCache: make([]byte, initialRngCacheSize),
	}

	rng := rand.NewChaCha8([32]byte(seed))
	// NOTE(prozlach): On my laptop, generating 2GiB of rng data takes around
	// 1s. rng always returns requested data and never returns an error
	_, _ = rng.Read(res.rngCache)

	return res
}

func (bf *BlobberFactory) GetBlobber(rngCacheSize int64) *Blobber {
	if rngCacheSize > int64(len(bf.rngCache)) {
		panic(fmt.Sprintf(
			"Blober of size %d was requested, while the factory was initialized "+
				"with cache size equal to %d. In order to avoid unnecessary memory "+
				"reallocations, please adjust the caller of NewBloberFactory to match "+
				"the requested size", rngCacheSize, len(bf.rngCache),
		))
	}
	return NewBlober(bf.rngCache[:rngCacheSize])
}

func NewBlober(rngCache []byte) *Blobber {
	return &Blobber{
		rngCache: rngCache,
	}
}

type Blobber struct {
	rngCache []byte
}

func (b *Blobber) GetAllBytes() []byte {
	return b.rngCache
}

func (b *Blobber) Size() int {
	return len(b.rngCache)
}

func (b *Blobber) GetReader() *bytes.Reader {
	return bytes.NewReader(b.rngCache)
}

// AssertStreamEqual reads data from the provided reader and compares it to the cached rngCache starting from the specified offset.
func (b *Blobber) AssertStreamEqual(t testing.TB, r io.Reader, offset int64, streamID string) bool {
	// Check if offset is valid
	if !assert.GreaterOrEqualf(t, offset, int64(0), "offset must not be negative, streamID: %s", streamID) {
		return false
	}
	if !assert.LessOrEqualf(t, offset, int64(len(b.rngCache)), "offset exceeds rng cache length, streamID: %s", streamID) {
		return false
	}

	// NOTE(prozlach): The size of the readBuffer is dictated by how much a
	// human can eyeball in one go in case when there are differences in the
	// data received and the internal state. Beyond that - the higher value
	// could boost performance but it is not a priority.
	readBuffer := make([]byte, 512)

	// NOTE(prozlach): `assert.*` calls do a lot in the background, and this is
	// a tight loop that is executed many, many times when comparing large
	// streams, hence we optimize using ifs and abort early in case when
	// assertion fails.
	for chunkNumber, currentOffset := 0, offset; currentOffset < int64(len(b.rngCache)); {
		// Calculate how many bytes we can actually compare
		remainingBytes := int64(len(b.rngCache)) - currentOffset

		// Read from the provided reader
		bytesRead, err := r.Read(readBuffer)
		if int64(bytesRead) > remainingBytes {
			return assert.LessOrEqualf(t, bytesRead, remainingBytes, "input stream is longer than cached data, streamID: %s", streamID)
		}
		if bytesRead > 0 && !bytes.Equal(b.rngCache[currentOffset:currentOffset+int64(bytesRead)], readBuffer[:bytesRead]) {
			return assert.Equalf(
				t,
				b.rngCache[currentOffset:currentOffset+int64(bytesRead)], readBuffer[:bytesRead],
				"difference between streams found. Chunk number %d, current offset: %d, starting offset %d, streamID: %s", chunkNumber, currentOffset, offset, streamID,
			)
		}

		currentOffset += int64(bytesRead)
		chunkNumber++

		if err != nil {
			if err == io.EOF {
				// nolint: testifylint // we are comparing offsets here
				return assert.EqualValues(t, currentOffset, len(b.rngCache), "input stream is longer than cached data, streamID: %s", streamID)
			}
			return assert.NoError(t, err, "reading input stream failed, streamID: %s", streamID)
		}
	}

	return true
}

func (b *Blobber) RequireStreamEqual(t testing.TB, r io.Reader, offset int64, streamID string) {
	if !b.AssertStreamEqual(t, r, offset, streamID) {
		t.FailNow()
	}
}
