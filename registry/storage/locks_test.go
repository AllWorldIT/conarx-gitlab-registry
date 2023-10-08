package storage

import (
	"context"
	"testing"

	"github.com/docker/distribution/registry/storage/driver/testdriver"
	"github.com/stretchr/testify/require"
)

func TestDatabaseInUseLockerLock(t *testing.T) {
	ctx := context.Background()
	driver := testdriver.New()

	dbLocker := DatabaseInUseLocker{Driver: driver}

	// Check that IsLocked returns false.
	locked, err := dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.False(t, locked)

	// Engage the lock.
	require.NoError(t, dbLocker.Lock(ctx))

	// IsLocked now returns true.
	locked, err = dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.True(t, locked)

	// Locking is idempotent.
	require.NoError(t, dbLocker.Lock(ctx))

	// IsLocked still returns true.
	locked, err = dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.True(t, locked)
}

func TestDatabaseInUseLockerUnLock(t *testing.T) {
	ctx := context.Background()
	driver := testdriver.New()

	dbLocker := DatabaseInUseLocker{Driver: driver}

	// Check that the lock is not locked.
	locked, err := dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.False(t, locked)

	// Unlocking succeeds even if no lock is present.
	require.NoError(t, dbLocker.Unlock(ctx))

	// Engage the lock.
	require.NoError(t, dbLocker.Lock(ctx))

	// IsLocked now returns true
	locked, err = dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.True(t, locked)

	// Unlock.
	require.NoError(t, dbLocker.Unlock(ctx))

	// IsLocked now returns false.
	locked, err = dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.False(t, locked)

	// Unlock is idempotent.
	require.NoError(t, dbLocker.Unlock(ctx))

	// IsLocked still returns false.
	locked, err = dbLocker.IsLocked(ctx)
	require.NoError(t, err)
	require.False(t, locked)
}
