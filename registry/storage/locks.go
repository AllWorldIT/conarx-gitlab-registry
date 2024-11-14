package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/docker/distribution"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/storage/driver"
)

// Locker provides access to information about storage locks.
type Locker interface {
	// IsLocked returns true if the locker is locked.
	IsLocked(ctx context.Context) (bool, error)

	// Lock applies the lock.
	Lock(ctx context.Context) error

	// Unlock removes a lock.
	Unlock(ctx context.Context) error
}

// lockers hold a database and filesystem lockers
// and implements the distribution.Lockers interface
type lockers struct {
	DB Locker
	FS Locker
}

// versionedLock exists to fill the lock file with content to avoid an empty
// file which could have possible storage driver implementation differences.
// Using a version allows us the option to fill the lock file with data in the
// future, while maintaining compatibility for historic locks.
type versionedLock struct {
	// Version is the lock version.
	Version int `json:"version"`
}

// DatabaseInUseLocker is a locker that signals that this object storage is
// managed by the metadata database.
type DatabaseInUseLocker struct {
	Driver driver.StorageDriver
}

// IsLocked returns true if the lock file is present.
func (l *DatabaseInUseLocker) IsLocked(ctx context.Context) (bool, error) {
	path, err := l.path()
	if err != nil {
		return false, err
	}

	fi, err := l.Driver.Stat(ctx, path)
	if err != nil {
		if errors.As(err, &driver.PathNotFoundError{}) {
			return false, nil
		}
		return false, err
	}

	if fi.IsDir() {
		log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
			"path": path,
			"lock": "databaseInUse",
		}).Warn("lock path should not be a directory")
		return false, err
	}

	return true, nil
}

// Lock applies the lock by writing a lock file.
func (l *DatabaseInUseLocker) Lock(ctx context.Context) error {
	locked, err := l.IsLocked(ctx)
	if err != nil {
		return err
	}
	if locked {
		return nil
	}

	path, err := l.path()
	if err != nil {
		return err
	}

	vl := versionedLock{Version: 1}
	b, err := json.Marshal(&vl)
	if err != nil {
		return fmt.Errorf("marshaling lockfile json")
	}

	return l.Driver.PutContent(ctx, path, b)
}

// Unlock removes a lock by removing a lock file. This method is idempotent.
func (l *DatabaseInUseLocker) Unlock(ctx context.Context) error {
	locked, err := l.IsLocked(ctx)
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}

	path, err := l.path()
	if err != nil {
		return err
	}

	return l.Driver.Delete(ctx, path)
}

func (l *DatabaseInUseLocker) path() (string, error) {
	return pathFor(lockFilePathSpec{name: "database-in-use"})
}

// FilesystemInUseLocker is a locker that signals that this object storage is
// managed by the filesystem metadata.
type FilesystemInUseLocker struct {
	Driver driver.StorageDriver
}

// IsLocked returns true if the lock file is present.
func (l *FilesystemInUseLocker) IsLocked(ctx context.Context) (bool, error) {
	path, err := l.path()
	if err != nil {
		return false, err
	}

	fi, err := l.Driver.Stat(ctx, path)
	if err != nil {
		if errors.As(err, &driver.PathNotFoundError{}) {
			return false, nil
		}
		return false, err
	}

	if fi.IsDir() {
		log.GetLogger(log.WithContext(ctx)).WithFields(log.Fields{
			"path": path,
			"lock": "filesystemInUse",
		}).Warn("lock path should not be a directory")
		return false, err
	}

	return true, nil
}

// Lock applies the lock by writing a lock file.
func (l *FilesystemInUseLocker) Lock(ctx context.Context) error {
	locked, err := l.IsLocked(ctx)
	if err != nil {
		return err
	}
	if locked {
		return nil
	}

	path, err := l.path()
	if err != nil {
		return err
	}

	vl := versionedLock{Version: 1}
	b, err := json.Marshal(&vl)
	if err != nil {
		return fmt.Errorf("marshaling lockfile json")
	}

	return l.Driver.PutContent(ctx, path, b)
}

// Unlock removes a lock by removing a lock file. This method is idempotent.
func (l *FilesystemInUseLocker) Unlock(ctx context.Context) error {
	locked, err := l.IsLocked(ctx)
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}

	path, err := l.path()
	if err != nil {
		return err
	}

	return l.Driver.Delete(ctx, path)
}

func (l *FilesystemInUseLocker) path() (string, error) {
	return pathFor(lockFilePathSpec{name: "filesystem-in-use"})
}

var _ distribution.Lockers = &lockers{}

func (l *lockers) DBLock(ctx context.Context) error {
	return l.DB.Lock(ctx)
}

func (l *lockers) DBUnlock(ctx context.Context) error {
	return l.DB.Unlock(ctx)
}

func (l *lockers) DBIsLocked(ctx context.Context) (bool, error) {
	return l.DB.IsLocked(ctx)
}

func (l *lockers) FSLock(ctx context.Context) error {
	return l.FS.Lock(ctx)
}

func (l *lockers) FSUnlock(ctx context.Context) error {
	return l.FS.Unlock(ctx)
}
