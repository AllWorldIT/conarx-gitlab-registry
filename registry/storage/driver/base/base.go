// Package base provides a base implementation of the storage driver that can
// be used to implement common checks. The goal is to increase the amount of
// code sharing.
//
// The canonical approach to use this class is to embed in the exported driver
// struct such that calls are proxied through this implementation. First,
// declare the internal driver, as follows:
//
//	type driver struct { ... internal ...}
//
// The resulting type should implement StorageDriver such that it can be the
// target of a Base struct. The exported type can then be declared as follows:
//
//	type Driver struct {
//		Base
//	}
//
// Because Driver embeds Base, it effectively implements Base. If the driver
// needs to intercept a call, before going to base, Driver should implement
// that method. Effectively, Driver can intercept calls before coming in and
// driver implements the actual logic.
//
// To further shield the embed from other packages, it is recommended to
// employ a private embed struct:
//
//	type baseEmbed struct {
//		base.Base
//	}
//
// Then, declare driver to embed baseEmbed, rather than Base directly:
//
//	type Driver struct {
//		baseEmbed
//	}
//
// The type now implements StorageDriver, proxying through Base, without
// exporting an unnecessary field.
package base

import (
	"context"
	"errors"
	"io"
	"time"

	dcontext "github.com/docker/distribution/context"
	prometheus "github.com/docker/distribution/metrics"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/go-metrics"
)

// storageAction is the metrics of blob related operations
var storageAction = prometheus.StorageNamespace.NewLabeledTimer("action", "The number of seconds that the storage action takes", "driver", "action")

func init() {
	metrics.Register(prometheus.StorageNamespace)
}

// Base provides a wrapper around a storagedriver implementation that provides
// common path and bounds checking.
type Base struct {
	storagedriver.StorageDriver
}

// Format errors received from the storage driver
func (base *Base) setDriverName(e error) error {
	if e == nil {
		return nil
	}

	switch {
	// NOTE(prozlach): These types have field set in the code already
	case errors.As(e, new(storagedriver.ErrUnsupportedMethod)):
		return e
	case errors.As(e, new(storagedriver.PathNotFoundError)):
		return e
	case errors.As(e, new(storagedriver.InvalidPathError)):
		return e
	case errors.As(e, new(storagedriver.InvalidOffsetError)):
		return e
	// NOTE(prozlach): Anything else needs to be wrapped
	default:
		storageError := storagedriver.Error{
			DriverName: base.StorageDriver.Name(),
			Enclosed:   e,
		}

		return storageError
	}
}

// GetContent wraps GetContent of underlying storage driver.
func (base *Base) GetContent(ctx context.Context, path string) ([]byte, error) {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.GetContent(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) {
		return nil, storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	b, e := base.StorageDriver.GetContent(ctx, path)
	storageAction.WithValues(base.Name(), "GetContent").UpdateSince(start)
	return b, base.setDriverName(e)
}

// PutContent wraps PutContent of underlying storage driver.
func (base *Base) PutContent(ctx context.Context, path string, content []byte) error {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.PutContent(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) {
		return storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	err := base.setDriverName(base.StorageDriver.PutContent(ctx, path, content))
	storageAction.WithValues(base.Name(), "PutContent").UpdateSince(start)
	return err
}

// Reader wraps Reader of underlying storage driver.
func (base *Base) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.Reader(%q, %d)", base.Name(), path, offset)

	if offset < 0 {
		return nil, storagedriver.InvalidOffsetError{Path: path, Offset: offset, DriverName: base.StorageDriver.Name()}
	}

	if !storagedriver.PathRegexp.MatchString(path) {
		return nil, storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	rc, e := base.StorageDriver.Reader(ctx, path, offset)
	return rc, base.setDriverName(e)
}

// Writer wraps Writer of underlying storage driver.
func (base *Base) Writer(ctx context.Context, path string, doAppend bool) (storagedriver.FileWriter, error) {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.Writer(%q, %v)", base.Name(), path, doAppend)

	if !storagedriver.PathRegexp.MatchString(path) {
		return nil, storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	writer, e := base.StorageDriver.Writer(ctx, path, doAppend)
	return writer, base.setDriverName(e)
}

// Stat wraps Stat of underlying storage driver.
func (base *Base) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.Stat(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) && path != "/" {
		return nil, storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	fi, e := base.StorageDriver.Stat(ctx, path)
	storageAction.WithValues(base.Name(), "Stat").UpdateSince(start)
	return fi, base.setDriverName(e)
}

// List wraps List of underlying storage driver.
func (base *Base) List(ctx context.Context, path string) ([]string, error) {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.List(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) && path != "/" {
		return nil, storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	str, e := base.StorageDriver.List(ctx, path)
	storageAction.WithValues(base.Name(), "List").UpdateSince(start)
	return str, base.setDriverName(e)
}

// Move wraps Move of underlying storage driver.
func (base *Base) Move(ctx context.Context, sourcePath, destPath string) error {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.Move(%q, %q", base.Name(), sourcePath, destPath)

	if !storagedriver.PathRegexp.MatchString(sourcePath) {
		return storagedriver.InvalidPathError{Path: sourcePath, DriverName: base.StorageDriver.Name()}
	} else if !storagedriver.PathRegexp.MatchString(destPath) {
		return storagedriver.InvalidPathError{Path: destPath, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	err := base.setDriverName(base.StorageDriver.Move(ctx, sourcePath, destPath))
	storageAction.WithValues(base.Name(), "Move").UpdateSince(start)
	return err
}

// Delete wraps Delete of underlying storage driver.
func (base *Base) Delete(ctx context.Context, path string) error {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.Delete(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) {
		return storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	err := base.setDriverName(base.StorageDriver.Delete(ctx, path))
	storageAction.WithValues(base.Name(), "Delete").UpdateSince(start)
	return err
}

// URLFor wraps URLFor of underlying storage driver.
func (base *Base) URLFor(ctx context.Context, path string, options map[string]any) (string, error) {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.URLFor(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) {
		return "", storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	start := time.Now()
	str, e := base.StorageDriver.URLFor(ctx, path, options)
	storageAction.WithValues(base.Name(), "URLFor").UpdateSince(start)
	return str, base.setDriverName(e)
}

// Walk wraps Walk of underlying storage driver.
func (base *Base) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	ctx, done := dcontext.WithTrace(ctx)
	defer done("%s.Walk(%q)", base.Name(), path)

	if !storagedriver.PathRegexp.MatchString(path) && path != "/" {
		return storagedriver.InvalidPathError{Path: path, DriverName: base.StorageDriver.Name()}
	}

	return base.setDriverName(base.StorageDriver.Walk(ctx, path, f))
}
