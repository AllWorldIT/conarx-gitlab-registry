package testutil

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/opencontainers/go-digest"
)

// CreateRandomTarFile creates a random tarfile, returning it as an
// io.ReadSeeker along with its digest. An error is returned if there is a
// problem generating valid content.
// nolint: gosec //  G404
func CreateRandomTarFile(seed SeedT) (rs io.ReadSeeker, dgst digest.Digest, err error) {
	// NOTE(prozlach): Pre-allocate resulting BytesBuffer. For that we need to
	// know in advance file sizes though.
	nFiles := rand.IntN(10) + 10
	fileSizes := make([]int64, nFiles)
	fileSizesTotal := int64(0)
	for i := 0; i < nFiles; i++ {
		fileSizes[i] = rand.Int64N(1<<20) + 1<<20
		// Tar header is 512bytes per file:
		fileSizesTotal += fileSizes[i] + 512
	}
	target := bytes.NewBuffer(make([]byte, 0, fileSizesTotal))
	wr := tar.NewWriter(target)

	rng := rand.NewChaCha8([32]byte(seed))

	// Perturb this on each iteration of the loop below.
	header := &tar.Header{
		Mode:       0o644,
		ModTime:    time.Now(),
		Typeflag:   tar.TypeReg,
		Uname:      "randocalrissian",
		Gname:      "cloudcity",
		AccessTime: time.Now(),
		ChangeTime: time.Now(),
	}

	for fileNumber := 0; fileNumber < nFiles; fileNumber++ {
		header.Name = fmt.Sprint(fileNumber)
		header.Size = fileSizes[fileNumber]

		if err := wr.WriteHeader(header); err != nil {
			return nil, "", err
		}

		nn, err := io.Copy(wr, io.LimitReader(rng, fileSizes[fileNumber]))
		if nn != fileSizes[fileNumber] {
			return nil, "", fmt.Errorf("short copy writing random file to tar")
		}

		if err != nil {
			return nil, "", err
		}

		if err := wr.Flush(); err != nil {
			return nil, "", err
		}
	}

	if err := wr.Close(); err != nil {
		return nil, "", err
	}

	dgst = digest.FromBytes(target.Bytes())

	return bytes.NewReader(target.Bytes()), dgst, nil
}

// CreateRandomLayers returns a map of n digests. We don't particularly care
// about the order of said digests (since they're all random anyway).
func CreateRandomLayers(n int) (map[digest.Digest]io.ReadSeeker, error) {
	digestMap := make(map[digest.Digest]io.ReadSeeker)
	for i := 0; i < n; i++ {
		seed, err := ChaChaSeed()
		if err != nil {
			return nil, fmt.Errorf("creating rng seed: %w", err)
		}

		rs, ds, err := CreateRandomTarFile(seed)
		if err != nil {
			return nil, fmt.Errorf("unexpected error generating test layer file: %v", err)
		}

		digestMap[ds] = rs
	}
	return digestMap, nil
}

// UploadBlobs lets you upload blobs to a repository
func UploadBlobs(repository distribution.Repository, layers map[digest.Digest]io.ReadSeeker) error {
	ctx := context.Background()
	for digest, rs := range layers {
		wr, err := repository.Blobs(ctx).Create(ctx)
		if err != nil {
			return fmt.Errorf("unexpected error creating upload: %v", err)
		}

		if _, err := io.Copy(wr, rs); err != nil {
			return fmt.Errorf("unexpected error copying to upload: %v", err)
		}

		if _, err := wr.Commit(ctx, distribution.Descriptor{Digest: digest}); err != nil {
			return fmt.Errorf("unexpected error committinng upload: %v", err)
		}
	}
	return nil
}
