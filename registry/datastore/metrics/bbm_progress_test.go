package metrics

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	dmetrics "github.com/docker/distribution/metrics"
	"github.com/docker/distribution/registry/datastore/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// helper to build a redis client backed by miniredis
func newMiniRedisClient(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func TestNewBBMProgressCollector_DefaultsAndOptions(t *testing.T) {
	mr, client := newMiniRedisClient(t)
	defer mr.Close()

	t.Run("with defaults", func(t *testing.T) {
		// dummy executor not used here
		exec := func(_ context.Context) (models.BackgroundMigrationsWithProgress, error) { return nil, nil }
		c, err := NewBBMProgressCollector(exec, client)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, defaultInterval, c.interval)
		require.Equal(t, defaultLeaseDuration, c.leaseDuration)
	})

	t.Run("with custom options", func(t *testing.T) {
		exec := func(_ context.Context) (models.BackgroundMigrationsWithProgress, error) { return nil, nil }
		customInterval := 5 * time.Second
		customLease := 12 * time.Second
		c, err := NewBBMProgressCollector(exec, client, WithProgressInterval(customInterval), WithProgressLeaseDuration(customLease))
		require.NoError(t, err)
		require.Equal(t, customInterval, c.interval)
		require.Equal(t, customLease, c.leaseDuration)
	})

	t.Run("validation: lease <= interval", func(t *testing.T) {
		exec := func(_ context.Context) (models.BackgroundMigrationsWithProgress, error) { return nil, nil }
		_, err := NewBBMProgressCollector(exec, client, WithProgressInterval(10*time.Second), WithProgressLeaseDuration(10*time.Second))
		require.EqualError(t, err, "bbm metrics lease duration (10s) must be longer than interval (10s)")

		_, err = NewBBMProgressCollector(exec, client, WithProgressInterval(10*time.Second), WithProgressLeaseDuration(5*time.Second))
		require.EqualError(t, err, "bbm metrics lease duration (5s) must be longer than interval (10s)")
	})
}

func TestBBMProgressCollector_Collect_ComputesProgress(t *testing.T) {
	resetQueryCollector(t)
	// fresh registry to isolate metric
	reg := prometheus.NewRegistry()
	reg.MustRegister(bbmProgressGauge)
	bbmProgressGauge.Reset()

	// Create mock progress data
	progressData := models.BackgroundMigrationsWithProgress{
		&models.BackgroundMigrationWithProgress{
			BackgroundMigration: models.BackgroundMigration{
				ID:              1,
				Name:            "mig1",
				BatchSize:       10,
				Status:          models.BackgroundMigrationRunning,
				TotalTupleCount: sql.NullInt64{Int64: 100, Valid: true},
			},
			BackgroundMigrationProgress: models.BackgroundMigrationProgress{
				FinishedJobs: 3,
				Progress:     30.0,
				Capped:       false,
			},
		},
		&models.BackgroundMigrationWithProgress{
			BackgroundMigration: models.BackgroundMigration{
				ID:              2,
				Name:            "mig2",
				BatchSize:       10,
				Status:          models.BackgroundMigrationFinished,
				TotalTupleCount: sql.NullInt64{Int64: 0, Valid: false},
			},
			BackgroundMigrationProgress: models.BackgroundMigrationProgress{
				FinishedJobs: 7,
				Progress:     100.0,
				Capped:       false,
			},
		},
		&models.BackgroundMigrationWithProgress{
			BackgroundMigration: models.BackgroundMigration{
				ID:              3,
				Name:            "mig3",
				BatchSize:       10,
				Status:          models.BackgroundMigrationRunning,
				TotalTupleCount: sql.NullInt64{Int64: 100, Valid: true},
			},
			BackgroundMigrationProgress: models.BackgroundMigrationProgress{
				FinishedJobs: 10,
				Progress:     99.9,
				Capped:       true,
			},
		},
	}

	exec := func(_ context.Context) (models.BackgroundMigrationsWithProgress, error) {
		return progressData, nil
	}

	// redis not used here beyond construction
	mr, client := newMiniRedisClient(t)
	defer mr.Close()

	c, err := NewBBMProgressCollector(exec, client)
	require.NoError(t, err)

	// call collect directly
	ctx := context.Background()
	c.collect(ctx)

	// build expected exposition
	var expected bytes.Buffer
	_, _ = expected.WriteString(fmt.Sprintf(`
# HELP %s_%s_%s Background migration progress percentage (0-100).
# TYPE %s_%s_%s gauge
%s_%s_%s{migration_id="1",migration_name="mig1",status="running"} 30
%s_%s_%s{migration_id="2",migration_name="mig2",status="finished"} 100
%s_%s_%s{migration_id="3",migration_name="mig3",status="running"} 99.9
`,
		dmetrics.NamespacePrefix, subsystem, "bbm_progress_percent",
		dmetrics.NamespacePrefix, subsystem, "bbm_progress_percent",
		dmetrics.NamespacePrefix, subsystem, "bbm_progress_percent",
		dmetrics.NamespacePrefix, subsystem, "bbm_progress_percent",
		dmetrics.NamespacePrefix, subsystem, "bbm_progress_percent"))

	fullName := fmt.Sprintf("%s_%s_%s", dmetrics.NamespacePrefix, subsystem, "bbm_progress_percent")
	err = testutil.GatherAndCompare(reg, &expected, fullName)
	require.NoError(t, err)
}

func TestBBMProgressCollector_StartStop_WithLock(t *testing.T) {
	resetQueryCollector(t)
	mr, client := newMiniRedisClient(t)
	defer mr.Close()

	var calls int
	exec := func(_ context.Context) (models.BackgroundMigrationsWithProgress, error) {
		calls++
		return make(models.BackgroundMigrationsWithProgress, 0), nil
	}

	// Use short timings for test
	c, err := NewBBMProgressCollector(exec, client, WithProgressInterval(30*time.Millisecond), WithProgressLeaseDuration(80*time.Millisecond))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.Start(ctx)

	// initial collect + a couple of intervals
	require.Eventually(t, func() bool { return calls >= 2 }, 300*time.Millisecond, 10*time.Millisecond)

	c.Stop()
}

func TestBBMProgressCollector_LeaderExclusivity(t *testing.T) {
	resetQueryCollector(t)
	mr, client := newMiniRedisClient(t)
	defer mr.Close()

	// build executor that supports arbitrary calls
	mkExec := func(counter *int) BBMProgressExecutor {
		return func(_ context.Context) (models.BackgroundMigrationsWithProgress, error) {
			*counter++
			return make(models.BackgroundMigrationsWithProgress, 0), nil
		}
	}

	var calls1, calls2 int
	c1, err := NewBBMProgressCollector(mkExec(&calls1), client, WithProgressInterval(40*time.Millisecond), WithProgressLeaseDuration(120*time.Millisecond))
	require.NoError(t, err)
	c2, err := NewBBMProgressCollector(mkExec(&calls2), client, WithProgressInterval(40*time.Millisecond), WithProgressLeaseDuration(120*time.Millisecond))
	require.NoError(t, err)

	ctx := context.Background()
	c1.Start(ctx)
	// Wait for first to take the lock and start collecting
	require.Eventually(t, func() bool { return calls1 >= 1 }, 200*time.Millisecond, 10*time.Millisecond)

	c2.Start(ctx)

	// Give the second collector time; it should not collect while lock is held
	time.Sleep(150 * time.Millisecond)

	c1.Stop()
	c2.Stop()

	require.GreaterOrEqual(t, calls1, 1)
	require.Equal(t, 0, calls2)
}

// resetQueryCollector cleans up query metrics after test to prevent pollution.
func resetQueryCollector(t *testing.T) {
	t.Cleanup(func() {
		queryTotal.Reset()
		queryDurationHist.Reset()
	})
}
