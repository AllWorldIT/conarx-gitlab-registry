package testdriver

import (
	"context"
	"fmt"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/docker/distribution/registry/storage/driver/inmemory"
)

const driverName = "testdriver"

func init() {
	factory.Register(driverName, &testDriverFactory{})
}

// testDriverFactory implements the factory.StorageDriverFactory interface.
type testDriverFactory struct{}

func (*testDriverFactory) Create(_ map[string]any) (storagedriver.StorageDriver, error) {
	return New(), nil
}

// TestDriver is a StorageDriver for testing purposes. The Writer returned by this driver
// simulates the case where Write operations are buffered. This causes the value returned by Size to lag
// behind until Close (or Commit, or Cancel) is called.
type TestDriver struct {
	storagedriver.StorageDriver
}

type testFileWriter struct {
	storagedriver.FileWriter
	prevchunk []byte
}

var _ storagedriver.StorageDriver = &TestDriver{}

// New constructs a new StorageDriver for testing purposes. The Writer returned by this driver
// simulates the case where Write operations are buffered. This causes the value returned by Size to lag
// behind until Close (or Commit, or Cancel) is called.
func New() *TestDriver {
	return &TestDriver{StorageDriver: inmemory.New()}
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (td *TestDriver) Writer(ctx context.Context, path string, doAppend bool) (storagedriver.FileWriter, error) {
	fw, err := td.StorageDriver.Writer(ctx, path, doAppend)
	return &testFileWriter{FileWriter: fw}, err
}

func (tfw *testFileWriter) Write(p []byte) (int, error) {
	_, err := tfw.FileWriter.Write(tfw.prevchunk)
	tfw.prevchunk = make([]byte, len(p))
	copy(tfw.prevchunk, p)
	return len(p), err
}

func (tfw *testFileWriter) Close() error {
	_, err := tfw.Write(nil)
	if err != nil {
		return fmt.Errorf("writing test file: %w", err)
	}
	return tfw.FileWriter.Close()
}

func (tfw *testFileWriter) Cancel() error {
	_, err := tfw.Write(nil)
	if err != nil {
		return fmt.Errorf("writing test file: %w", err)
	}
	return tfw.FileWriter.Cancel()
}

func (tfw *testFileWriter) Commit() error {
	_, err := tfw.Write(nil)
	if err != nil {
		return fmt.Errorf("writing test file: %w", err)
	}
	return tfw.FileWriter.Commit()
}
