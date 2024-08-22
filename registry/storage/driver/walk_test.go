package driver

import (
	"context"
	"errors"
	"testing"
)

type changingFileSystem struct {
	StorageDriver
	fileset   []string
	keptFiles map[string]bool
}

func (cfs *changingFileSystem) List(ctx context.Context, path string) ([]string, error) {
	return cfs.fileset, nil
}

func (cfs *changingFileSystem) Stat(ctx context.Context, path string) (FileInfo, error) {
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
	infos := []FileInfo{}
	err := WalkFallback(context.Background(), d, "", func(fileInfo FileInfo) error {
		infos = append(infos, fileInfo)
		return nil
	})
	if len(infos) != 1 || infos[0].Path() != "zoidberg" {
		t.Errorf("unexpected path set during walk: %s", infos)
	}
	if err != nil {
		t.Fatalf(err.Error())
	}
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

func (efs *errorFileSystem) List(ctx context.Context, path string) ([]string, error) {
	return efs.fileSet, nil
}

func (efs *errorFileSystem) Stat(ctx context.Context, path string) (FileInfo, error) {
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

	infos := []FileInfo{}
	err := WalkFallback(context.Background(), d, "", func(fileInfo FileInfo) error {
		infos = append(infos, fileInfo)
		return nil
	})

	if err != errTopLevelDir {
		t.Error("Expected to report a top level directory error, but reported an error from a deeply nested file")
	}

	if !(len(infos) < len(d.fileSet)) {
		t.Errorf("Expected walk to terminate early, encountered %d of %d files",
			len(infos), len(d.fileSet))
	}
}
