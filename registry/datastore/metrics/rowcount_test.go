package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// mockRowCountExecutor is a mock implementation of RowCountExecutor for testing
type mockRowCountExecutor struct {
	count int64
	err   error
	calls []mockCall
}

type mockCall struct {
	query string
	args  []any
}

func (m *mockRowCountExecutor) Execute(_ context.Context, query string, args ...any) (int64, error) {
	m.calls = append(m.calls, mockCall{query: query, args: args})
	return m.count, m.err
}

func TestNewRowCountCollector(t *testing.T) {
	// Setup mock Redis
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	executor := &mockRowCountExecutor{count: 100}

	t.Run("with defaults", func(t *testing.T) {
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)
		require.NotNil(t, collector)
		require.Equal(t, defaultInterval, collector.interval)
		require.Equal(t, defaultLeaseDuration, collector.leaseDuration)
		require.Len(t, collector.queries, 1) // Default gc_blob_review_queue query
	})

	t.Run("with custom interval", func(t *testing.T) {
		customInterval := 5 * time.Second
		collector, err := NewRowCountCollector(executor.Execute, redisClient, WithInterval(customInterval))
		require.NoError(t, err)
		require.NotNil(t, collector)
		require.Equal(t, customInterval, collector.interval)
		require.Equal(t, defaultLeaseDuration, collector.leaseDuration)
	})

	t.Run("with custom lease duration", func(t *testing.T) {
		customLease := 60 * time.Second
		collector, err := NewRowCountCollector(executor.Execute, redisClient, WithLeaseDuration(customLease))
		require.NoError(t, err)
		require.NotNil(t, collector)
		require.Equal(t, defaultInterval, collector.interval)
		require.Equal(t, customLease, collector.leaseDuration)
	})

	t.Run("with both options", func(t *testing.T) {
		customInterval := 5 * time.Second
		customLease := 60 * time.Second
		collector, err := NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(customInterval),
			WithLeaseDuration(customLease),
		)
		require.NoError(t, err)
		require.NotNil(t, collector)
		require.Equal(t, customInterval, collector.interval)
		require.Equal(t, customLease, collector.leaseDuration)
	})

	t.Run("validation error when lease duration <= interval", func(t *testing.T) {
		// Test case where lease duration equals interval
		collector, err := NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(30*time.Second),
			WithLeaseDuration(30*time.Second),
		)
		require.Error(t, err)
		require.Nil(t, collector)
		require.EqualError(t, err, "database metrics lease duration (30s) must be longer than interval (30s)")

		// Test case where lease duration is less than interval
		collector, err = NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(30*time.Second),
			WithLeaseDuration(20*time.Second),
		)
		require.Error(t, err)
		require.Nil(t, collector)
		require.EqualError(t, err, "database metrics lease duration (20s) must be longer than interval (30s)")
	})
}

func TestRowCountCollector_RegisterQuery(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	executor := &mockRowCountExecutor{count: 100}
	collector, err := NewRowCountCollector(executor.Execute, redisClient)
	require.NoError(t, err)

	// Should start with 1 default query
	require.Len(t, collector.queries, 1)

	// Register a new query
	newQuery := RowCountQuery{
		Name:        "repositories",
		Description: "Number of repositories",
		Query:       "SELECT COUNT(*) FROM repositories",
		Args:        nil,
	}
	collector.RegisterQuery(newQuery)

	// Should now have 2 queries
	require.Len(t, collector.queries, 2)
	require.Equal(t, newQuery, collector.queries[1])
}

func TestRowCountCollector_collectMetrics(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	t.Run("successful collection", func(t *testing.T) {
		executor := &mockRowCountExecutor{count: 42}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		// Register an additional query
		collector.RegisterQuery(RowCountQuery{
			Name:        "test_table",
			Description: "Test table",
			Query:       "SELECT COUNT(*) FROM test_table",
			Args:        nil,
		})

		// Collect metrics
		ctx := context.Background()
		collector.collectMetrics(ctx)

		// Verify both queries were executed
		require.Len(t, executor.calls, 2)
		require.Equal(t, "SELECT COUNT(*) FROM gc_blob_review_queue", executor.calls[0].query)
		require.Equal(t, "SELECT COUNT(*) FROM test_table", executor.calls[1].query)
	})

	t.Run("with query error", func(t *testing.T) {
		executor := &mockRowCountExecutor{err: errors.New("database error")}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		// This should not panic, just log the error
		ctx := context.Background()
		collector.collectMetrics(ctx)

		// Verify query was attempted
		require.Len(t, executor.calls, 1)
	})

	t.Run("with query arguments", func(t *testing.T) {
		executor := &mockRowCountExecutor{count: 123}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		// Register a query with arguments
		collector.RegisterQuery(RowCountQuery{
			Name:        "filtered_query",
			Description: "Filtered query",
			Query:       "SELECT COUNT(*) FROM table WHERE status = $1",
			Args:        []any{"active"},
		})

		ctx := context.Background()
		collector.collectMetrics(ctx)

		// Verify the query with args was executed
		require.Len(t, executor.calls, 2) // default + new query
		require.Equal(t, "SELECT COUNT(*) FROM table WHERE status = $1", executor.calls[1].query)
		require.Equal(t, []any{"active"}, executor.calls[1].args)
	})
}

func TestRowCountCollector_run(t *testing.T) {
	t.Run("acquires lock and collects metrics", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		// Use very short intervals for testing
		collector, err := NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(50*time.Millisecond),
			WithLeaseDuration(100*time.Millisecond),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start the collector
		collector.Start(ctx)

		// Wait for at least 2 collections (initial + 2 intervals)
		require.Eventually(t, func() bool {
			return len(executor.calls) >= 3
		}, 200*time.Millisecond, 10*time.Millisecond, "expected at least 3 metric collections")

		// Stop the collector
		collector.Stop()
	})

	t.Run("fails to acquire lock when already held", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor1 := &mockRowCountExecutor{count: 100}
		executor2 := &mockRowCountExecutor{count: 200}

		collector1, err := NewRowCountCollector(executor1.Execute, redisClient)
		require.NoError(t, err)
		collector2, err := NewRowCountCollector(executor2.Execute, redisClient)
		require.NoError(t, err)

		ctx := context.Background()

		// Start first collector
		collector1.Start(ctx)

		// Wait for first collector to acquire lock and start collecting
		require.Eventually(t, func() bool {
			return len(executor1.calls) > 0
		}, 100*time.Millisecond, 10*time.Millisecond, "first collector should start collecting")

		// Try to start second collector - should fail to acquire lock
		collector2.Start(ctx)

		// Give second collector time to try acquiring lock
		// Use Never to ensure it doesn't collect any metrics
		require.Never(t, func() bool {
			return len(executor2.calls) > 0
		}, 100*time.Millisecond, 10*time.Millisecond, "second collector should not collect any metrics")

		// Stop both
		collector1.Stop()
		collector2.Stop()

		// Final verification
		require.NotEmpty(t, executor1.calls)
		require.Empty(t, executor2.calls)
	})

	t.Run("stops on context cancellation", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(50*time.Millisecond),
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		// Start the collector
		collector.Start(ctx)

		// Wait for initial collection
		require.Eventually(t, func() bool {
			return len(executor.calls) >= 1
		}, 100*time.Millisecond, 10*time.Millisecond, "should collect at least once")

		// Cancel context
		cancel()

		// Wait for stop
		collector.Stop()
	})
}

func TestRowCountCollector_WithInterval(t *testing.T) {
	opt := WithInterval(5 * time.Second)
	collector := &RowCountCollector{}
	opt(collector)
	require.Equal(t, 5*time.Second, collector.interval)
}

func TestRowCountCollector_WithLeaseDuration(t *testing.T) {
	opt := WithLeaseDuration(60 * time.Second)
	collector := &RowCountCollector{}
	opt(collector)
	require.Equal(t, 60*time.Second, collector.leaseDuration)
}

func TestRowCountCollector_runLockExtension(t *testing.T) {
	t.Run("extends lock to maintain ownership", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(30*time.Millisecond),
			WithLeaseDuration(50*time.Millisecond), // Short lease for testing
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Manually acquire lock to test extension behavior
		lock, err := collector.locker.Obtain(ctx, rowCountLockKey, collector.leaseDuration, nil)
		require.NoError(t, err)
		defer lock.Release(ctx)

		// Start lock extension in background
		done := make(chan struct{})
		go func() {
			defer close(done)
			collector.runLockExtension(ctx, lock)
		}()

		// Test that lock remains held due to extensions (observable behavior)
		// Without extensions, lock would expire after 50ms
		require.Eventually(t, func() bool {
			// Try to acquire the same lock - should fail if still held due to extension
			testLock, err := collector.locker.Obtain(ctx, rowCountLockKey, time.Millisecond, nil)
			if err == nil {
				testLock.Release(ctx) // Clean up if we got it
				return false          // Lock was not held (extensions not working)
			}
			// Lock is held = extension is working
			return errors.Is(err, redislock.ErrNotObtained)
		}, 100*time.Millisecond, 10*time.Millisecond, "lock should remain held due to extensions")

		// Cancel context to stop extension
		cancel()

		// Wait for goroutine to finish
		select {
		case <-done:
			// Extension goroutine stopped as expected
		case <-time.After(100 * time.Millisecond):
			t.Fatal("lock extension goroutine did not stop within timeout")
		}
	})

	t.Run("stops immediately on collector stop", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(
			executor.Execute,
			redisClient,
			WithInterval(50*time.Millisecond),
			WithLeaseDuration(100*time.Millisecond),
		)
		require.NoError(t, err)

		ctx := context.Background()

		// Manually acquire lock
		lock, err := collector.locker.Obtain(ctx, rowCountLockKey, collector.leaseDuration, nil)
		require.NoError(t, err)
		defer lock.Release(ctx)

		// Start lock extension in background
		done := make(chan struct{})
		go func() {
			defer close(done)
			collector.runLockExtension(ctx, lock)
		}()

		// Stop the collector (closes stopCh)
		collector.Stop()

		// Extension goroutine should stop quickly when stopCh is closed
		select {
		case <-done:
			// Extension goroutine stopped as expected
		case <-time.After(50 * time.Millisecond):
			t.Fatal("lock extension goroutine did not stop promptly when collector was stopped")
		}
	})
}

func TestRowCountRegistrar(t *testing.T) {
	t.Run("register and unregister", func(t *testing.T) {
		registrar := NewRowCountRegistrar()

		// Initial state should be unregistered
		registrar.SetRowCount("test", 100) // Should not panic even when unregistered

		// Register
		err := registrar.Register()
		require.NoError(t, err)

		// Second register should be idempotent
		err = registrar.Register()
		require.NoError(t, err)

		// Set some values
		registrar.SetRowCount("test", 200)

		// Unregister
		registrar.Unregister()

		// Second unregister should be idempotent
		registrar.Unregister()

		// Setting after unregister should not panic
		registrar.SetRowCount("test", 300)
	})

	t.Run("handles already registered error", func(t *testing.T) {
		// Create two registrars that will try to register the same metric
		registrar1 := NewRowCountRegistrar()
		registrar2 := NewRowCountRegistrar()

		// First registration should succeed
		err := registrar1.Register()
		require.NoError(t, err)
		defer registrar1.Unregister()

		// Second registration should handle AlreadyRegisteredError gracefully
		err = registrar2.Register()
		require.NoError(t, err) // Should not return error due to handling AlreadyRegisteredError
	})
}
