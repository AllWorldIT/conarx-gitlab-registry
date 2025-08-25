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
		time.Sleep(100 * time.Millisecond)

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

func TestRowCountCollector_runLockRefresh(t *testing.T) {
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
		defer func() {
			// Context is canceled in this test, so we expect either success or context.Canceled
			if err := lock.Release(ctx); err != nil {
				require.ErrorIs(t, err, context.Canceled, "unexpected error releasing lock")
			}
		}()

		// Start lock refresh in background
		done := make(chan struct{})
		go func() {
			defer close(done)
			collector.runLockRefresh(ctx, lock)
		}()

		// Test that lock remains held due to extensions (observable behavior)
		// Without extensions, lock would expire after 50ms
		time.Sleep(75 * time.Millisecond) // Wait beyond original lease

		// Try to acquire the same lock - should fail if still held due to extension
		testLock, err := collector.locker.Obtain(ctx, rowCountLockKey, time.Millisecond, nil)
		if err == nil {
			// Context might be canceled, so handle that case
			if err := testLock.Release(ctx); err != nil {
				require.ErrorIs(t, err, context.Canceled, "unexpected error releasing test lock")
			}
			t.Fatal("lock extension did not work - lock was not held after lease duration")
		}
		// Lock is held = extension is working
		require.ErrorIs(t, err, redislock.ErrNotObtained)

		// Cancel context to stop extension
		cancel()

		// Wait for goroutine to finish
		select {
		case <-done:
			// Refresh goroutine stopped as expected
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
		defer func() {
			// Using background context, so should not error
			require.NoError(t, lock.Release(ctx))
		}()

		// Start lock refresh in background
		done := make(chan struct{})
		go func() {
			defer close(done)
			collector.runLockRefresh(ctx, lock)
		}()

		// Stop the collector (closes stopCh)
		collector.Stop()

		// Refresh goroutine should stop quickly when stopCh is closed
		select {
		case <-done:
			// Refresh goroutine stopped as expected
		case <-time.After(50 * time.Millisecond):
			t.Fatal("lock refresh goroutine did not stop promptly when collector was stopped")
		}
	})

	t.Run("gives up leadership after consecutive refresh failures", func(t *testing.T) {
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

		// Acquire lock
		lock, err := collector.locker.Obtain(ctx, rowCountLockKey, collector.leaseDuration, nil)
		require.NoError(t, err)

		// Start lock refresh in background
		done := make(chan struct{})
		go func() {
			defer close(done)
			collector.runLockRefresh(ctx, lock)
		}()

		// Wait a moment for the refresh ticker to start
		time.Sleep(60 * time.Millisecond)

		// Delete the lock key to simulate losing the lock (will cause refresh failures)
		mr.Del(rowCountLockKey)

		// The refresh should fail 3 times (at 50ms intervals) and then give up
		// Refresh interval is leaseDuration/2 = 50ms
		// So failures at ~50ms, ~100ms, ~150ms, then return
		select {
		case <-done:
			// Expected: goroutine stopped after max retries
		case <-time.After(250 * time.Millisecond):
			t.Fatal("lock refresh goroutine did not stop after max consecutive failures")
		}
	})
}

func TestRowCountCollector_acquireLock_retry(t *testing.T) {
	t.Run("successfully acquires lock on first attempt", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		ctx := context.Background()

		// Lock is available, should acquire immediately
		lock, attemptCount := collector.acquireLock(ctx)

		require.NotNil(t, lock, "should acquire lock")
		require.Equal(t, 1, attemptCount, "should succeed on first attempt")

		// Clean up
		require.NoError(t, lock.Release(ctx))
	})

	t.Run("retries when lock is held by another instance", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		ctx := context.Background()

		// First, acquire the lock with another locker to simulate another instance holding it
		otherLocker := redislock.New(redisClient)
		otherLock, err := otherLocker.Obtain(ctx, rowCountLockKey, 200*time.Millisecond, nil)
		require.NoError(t, err)

		// Try to acquire lock - should fail initially but retry
		var lock *redislock.Lock
		var attemptCount int

		go func() {
			lock, attemptCount = collector.acquireLock(ctx)
		}()

		// Wait for first attempt to happen
		time.Sleep(100 * time.Millisecond)
		// Release lock to allow retry to succeed
		require.NoError(t, otherLock.Release(ctx))

		// Wait for lock acquisition after retry
		require.Eventually(t, func() bool {
			return lock != nil && attemptCount > 1
		}, 20*time.Second, 100*time.Millisecond, "should have acquired lock after retry")

		// Clean up
		require.NoError(t, lock.Release(ctx))
	})

	t.Run("stops retrying on context cancellation", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())

		// Hold the lock with another instance
		otherLocker := redislock.New(redisClient)
		otherLock, err := otherLocker.Obtain(ctx, rowCountLockKey, 60*time.Second, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, otherLock.Release(context.Background()))
		}()

		// Try to acquire lock in background
		var lock *redislock.Lock
		var attemptCount int
		stopped := make(chan bool)

		go func() {
			lock, attemptCount = collector.acquireLock(ctx)
			stopped <- true
		}()

		// Wait for at least one attempt and then cancel
		time.Sleep(100 * time.Millisecond)
		cancel()

		// Should stop quickly after cancellation
		require.Eventually(t, func() bool {
			select {
			case <-stopped:
				return true
			default:
				return false
			}
		}, 1*time.Second, 10*time.Millisecond, "acquireLock should stop promptly on context cancellation")

		require.Nil(t, lock, "should not have acquired lock after context cancel")
		require.GreaterOrEqual(t, attemptCount, 1, "should have attempted at least once")
	})

	t.Run("stops retrying on collector stop", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		defer mr.Close()

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		ctx := context.Background()

		// Hold the lock with another instance
		otherLocker := redislock.New(redisClient)
		otherLock, err := otherLocker.Obtain(ctx, rowCountLockKey, 60*time.Second, nil)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, otherLock.Release(ctx))
		}()

		// Try to acquire lock in background
		var lock *redislock.Lock
		stopped := make(chan bool)

		go func() {
			lock, _ = collector.acquireLock(ctx)
			stopped <- true
		}()

		// Wait a moment then stop collector
		time.Sleep(200 * time.Millisecond)
		collector.Stop()

		// Should stop quickly after stop signal
		require.Eventually(t, func() bool {
			select {
			case <-stopped:
				return true
			default:
				return false
			}
		}, 1*time.Second, 10*time.Millisecond, "acquireLock should stop promptly on collector stop")

		require.Nil(t, lock, "should not have acquired lock after collector stop")
	})

	t.Run("handles Redis errors with retry", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)

		redisClient := redis.NewClient(&redis.Options{
			Addr: mr.Addr(),
		})

		executor := &mockRowCountExecutor{count: 100}
		collector, err := NewRowCountCollector(executor.Execute, redisClient)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Close Redis to simulate connection error
		mr.Close()

		// Should retry but fail due to Redis being down
		lock, attemptCount := collector.acquireLock(ctx)

		require.Nil(t, lock, "should not acquire lock when Redis is down")
		require.GreaterOrEqual(t, attemptCount, 1, "should have attempted at least once")
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
