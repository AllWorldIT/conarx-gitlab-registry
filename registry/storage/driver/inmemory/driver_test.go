package inmemory

import (
	"context"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"
	"github.com/stretchr/testify/suite"
)

func TestInMemoryDriverSuite(t *testing.T) {
	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return New(), nil
		},
		func() (storagedriver.StorageDriver, error) {
			return New(), nil
		},
		nil,
	)
	suite.Run(t, ts)
}

func BenchmarkInMemoryDriverSuite(b *testing.B) {
	ts := testsuites.NewDriverSuite(
		context.Background(),
		func() (storagedriver.StorageDriver, error) {
			return New(), nil
		},
		func() (storagedriver.StorageDriver, error) {
			return New(), nil
		},
		nil,
	)

	ts.SetupSuiteWithB(b)
	b.Cleanup(func() { ts.TearDownSuiteWithB(b) })

	// NOTE(prozlach): This is a method of embedded function, we need to pass
	// the reference to "outer" struct directly
	benchmarks := ts.EnumerateBenchmarks()

	for _, benchmark := range benchmarks {
		b.Run(benchmark.Name, benchmark.Func)
	}
}
