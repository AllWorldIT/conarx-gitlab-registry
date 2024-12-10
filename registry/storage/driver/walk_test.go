package driver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type changingFileSystem struct {
	StorageDriver
	fileset   []string
	keptFiles map[string]bool
}

func (cfs *changingFileSystem) List(_ context.Context, _ string) ([]string, error) {
	return cfs.fileset, nil
}

func (cfs *changingFileSystem) Stat(_ context.Context, path string) (FileInfo, error) {
	kept, ok := cfs.keptFiles[path]
	if ok && kept {
		return &FileInfoInternal{
			FileInfoFields: FileInfoFields{
				Path: path,
			},
		}, nil
	}
	return nil, PathNotFoundError{}
}

func TestWalkFileRemoved(t *testing.T) {
	d := &changingFileSystem{
		fileset: []string{"zoidberg", "bender"},
		keptFiles: map[string]bool{
			"zoidberg": true,
		},
	}
	infos := make([]FileInfo, 0)
	err := WalkFallback(context.Background(), d, "", func(fileInfo FileInfo) error {
		infos = append(infos, fileInfo)
		return nil
	})
	assert.Len(t, infos, 1)
	assert.Equal(t, "zoidberg", infos[0].Path(), "unexpected path set during walk")
	require.NoError(t, err)
}

type errorFileSystem struct {
	StorageDriver
	fileSet    []string
	errorFiles map[string]error
}

var (
	errTopLevelDir      = errors.New("test error: this directory is bad")
	errDeeplyNestedFile = errors.New("test error: this file is bad")
)

func (efs *errorFileSystem) List(_ context.Context, _ string) ([]string, error) {
	return efs.fileSet, nil
}

func (efs *errorFileSystem) Stat(_ context.Context, path string) (FileInfo, error) {
	err, ok := efs.errorFiles[path]
	if ok {
		return nil, err
	}
	return &FileInfoInternal{
		FileInfoFields: FileInfoFields{
			Path: path,
		},
	}, nil
}

func TestWalkParallelError(t *testing.T) {
	d := &errorFileSystem{
		fileSet: []string{
			"apple/banana",
			"apple/orange",
			"apple/orange/blossom/ring",
			"apple/orange/pumpkin/latte",
			"apple/orange/pumpkin/shake",
			"foo/bar",
			"foo/baz",
			"mongoose",
			"zebra",
		},
		errorFiles: map[string]error{
			"apple/orange":               errTopLevelDir,
			"apple/orange/pumpkin/latte": errDeeplyNestedFile,
		},
	}

	infos := make([]FileInfo, 0)
	err := WalkFallback(context.Background(), d, "", func(fileInfo FileInfo) error {
		infos = append(infos, fileInfo)
		return nil
	})

	require.ErrorIs(t, err, errTopLevelDir, "expected to report a top level directory error, but reported an error from a deeply nested file")
	assert.Less(t, len(infos), len(d.fileSet), "expected walk to terminate early")
}
