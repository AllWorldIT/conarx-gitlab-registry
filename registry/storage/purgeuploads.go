package storage

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/uuid"
	"github.com/sirupsen/logrus"
)

// uploadData stored the location of temporary files created during a layer upload
// along with the date the upload was started
type uploadData struct {
	containingDir string
	startedAt     time.Time
}

func newUploadData() uploadData {
	return uploadData{
		containingDir: "",
		// default to far in future to protect against missing startedat
		startedAt: time.Now().Add(10000 * time.Hour),
	}
}

// syncUploadData provides thread-safe operations on a map of uploadData.
type syncUploadData struct {
	sync.Mutex
	members map[string]uploadData
}

// set the passed uuid's uploadData to data
func (s *syncUploadData) set(u string, data uploadData) {
	s.Lock()
	defer s.Unlock()

	s.members[u] = data
}

// get uploadData by uuid
func (s *syncUploadData) get(u string) (uploadData, bool) {
	s.Lock()
	defer s.Unlock()

	up, ok := s.members[u]

	return up, ok
}

// PurgeUploads deletes files from the upload directory
// created before olderThan.  The list of files deleted and errors
// encountered are returned
func PurgeUploads(ctx context.Context, sdriver driver.StorageDriver, olderThan time.Time, actuallyDelete bool) ([]string, []error) {
	logrus.Infof("PurgeUploads starting: olderThan=%s, actuallyDelete=%t", olderThan, actuallyDelete)
	uploadData, errors := getOutstandingUploads(ctx, sdriver)
	var deleted []string
	for _, uploadData := range uploadData {
		if uploadData.startedAt.Before(olderThan) {
			var err error
			logrus.Infof("Upload files in %s have older date (%s) than purge date (%s).  Removing upload directory.",
				uploadData.containingDir, uploadData.startedAt, olderThan)
			if actuallyDelete {
				err = sdriver.Delete(ctx, uploadData.containingDir)
			}
			if err == nil {
				deleted = append(deleted, uploadData.containingDir)
			} else {
				errors = append(errors, err)
			}
		}
	}

	logrus.Infof("Purge uploads finished.  Num deleted=%d, num errors=%d", len(deleted), len(errors))
	return deleted, errors
}

// getOutstandingUploads walks the upload directory, collecting files
// which could be eligible for deletion.  The only reliable way to
// classify the age of a file is with the date stored in the startedAt
// file, so gather files by UUID with a date from startedAt.
func getOutstandingUploads(ctx context.Context, sdriver driver.StorageDriver) (map[string]uploadData, []error) {
	var errors []error
	uploads := syncUploadData{sync.Mutex{}, make(map[string]uploadData, 0)}

	root, err := pathFor(repositoriesRootPathSpec{})
	if err != nil {
		return uploads.members, append(errors, err)
	}

	errCh := make(chan error)
	errDone := make(chan struct{})
	go func() {
		for err := range errCh {
			errors = append(errors, err)
		}
		errDone <- struct{}{}
	}()

	err = sdriver.WalkParallel(ctx, root, func(fileInfo driver.FileInfo) error {
		filePath := fileInfo.Path()
		_, file := path.Split(filePath)
		if (strings.HasPrefix(file, "_") || strings.HasPrefix(file, "hashstates")) && fileInfo.IsDir() && file != "_uploads" {
			// Reserved directory
			return driver.ErrSkipDir
		}

		uuid, isContainingDir := uuidFromPath(filePath)
		if uuid == "" {
			// Cannot reliably delete
			return nil
		}
		ud, ok := uploads.get(uuid)
		if !ok {
			ud = newUploadData()
		}
		if isContainingDir {
			ud.containingDir = filePath
		}
		if file == "startedat" {
			t, err := readStartedAtFile(sdriver, filePath)
			if err != nil {
				errCh <- fmt.Errorf("%s: %s", filePath, err)
				return nil
			}
			ud.startedAt = t
		}

		uploads.set(uuid, ud)
		return nil
	})
	if err != nil {
		errCh <- fmt.Errorf("%s: %s", root, err)
	}

	close(errCh)
	<-errDone

	return uploads.members, errors
}

// uuidFromPath extracts the upload UUID from a given path
// If the UUID is the last path component, this is the containing
// directory for all upload files
func uuidFromPath(p string) (string, bool) {
	components := strings.Split(p, "/")
	for i := len(components) - 1; i >= 0; i-- {
		if u, err := uuid.Parse(components[i]); err == nil {
			return u.String(), i == len(components)-1
		}
	}
	return "", false
}

// readStartedAtFile reads the date from an upload's startedAtFile
func readStartedAtFile(sdriver driver.StorageDriver, p string) (time.Time, error) {
	// todo:(richardscothern) - pass in a context
	startedAtBytes, err := sdriver.GetContent(context.Background(), p)
	if err != nil {
		return time.Now(), err
	}
	startedAt, err := time.Parse(time.RFC3339, string(startedAtBytes))
	if err != nil {
		return time.Now(), err
	}
	return startedAt, nil
}
