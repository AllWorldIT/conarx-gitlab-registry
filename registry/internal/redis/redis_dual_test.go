package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNewDualCache(t *testing.T) {
	primaryDB, _ := redismock.NewClientMock()
	standByDB, _ := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	t.Run("create dual cache with both caches", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)
		require.NotNil(t, dualCache)
		require.True(t, dualCache.IsPrimaryCacheEnabled())
	})

	t.Run("create dual cache with options", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache,
			iredis.WithReadFromStandBy(),
			iredis.WithEnableDualWrite())
		require.NotNil(t, dualCache)
		require.True(t, dualCache.IsPrimaryCacheEnabled())
	})

	t.Run("create dual cache with nil primary", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)
		require.NotNil(t, dualCache)
		require.False(t, dualCache.IsPrimaryCacheEnabled())
	})

	t.Run("create dual cache with nil standby", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil)
		require.NotNil(t, dualCache)
		require.True(t, dualCache.IsPrimaryCacheEnabled())
	})

	t.Run("create dual cache with both nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, nil)
		require.NotNil(t, dualCache)
		require.False(t, dualCache.IsPrimaryCacheEnabled())
	})
}

func TestDualCache_UseStandByAsPrimary(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	originalPrimary := iredis.NewCache(primaryDB)
	originalStandBy := iredis.NewCache(standByDB)

	key := "testKey"
	value := "testValue"

	t.Run("swap primary and standby caches", func(t *testing.T) {
		// Expect reads to come from the original standby (now primary)
		standByMock.ExpectGet(key).SetVal(value)

		dualCache := iredis.NewDualCache(originalPrimary, originalStandBy)

		// Verify initial state
		require.True(t, dualCache.IsPrimaryCacheEnabled())

		// Swap caches
		dualCache.UseStandByAsPrimary()

		// After swap, what was standby should now be primary
		// Now read from the new primary (original standby)
		result, err := dualCache.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_Get(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	key := "testKey"
	value := "testValue"

	t.Run("read from standby when enabled - primary not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())

		standByMock.ExpectGet(key).SetVal(value)
		// No expectations set for standByMock - it should not be called

		result, err := dualCache.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("read from primary when standby read disabled - standby not accessed for read", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectGet(key).SetVal(value)
		// No expectations set for standByMock - it should not be called for read

		result, err := dualCache.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when reading if primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		result, err := dualCache.Get(context.Background(), key)
		require.Error(t, err)
		require.Empty(t, result)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil even with read from standby enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache, iredis.WithReadFromStandBy())

		result, err := dualCache.Get(context.Background(), key)
		require.Error(t, err)
		require.Empty(t, result)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)

		require.NoError(t, standByMock.ExpectationsWereMet())
		require.NoError(t, primaryMock.ExpectationsWereMet())
	})

	t.Run("error when standby is nil and not reading from primary", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil, iredis.WithReadFromStandBy())

		result, err := dualCache.Get(context.Background(), key)
		require.ErrorIs(t, err, iredis.ErrorNoStandByCacheConfigured)
		require.Empty(t, result)

		require.NoError(t, standByMock.ExpectationsWereMet())
		require.NoError(t, primaryMock.ExpectationsWereMet())
	})
}

func TestDualCache_GetWithTTL(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	key := "testKey"
	value := "testValue"
	ttl := 60 * time.Second

	t.Run("read from primary - standby not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectGet(key).SetVal(value)
		primaryMock.ExpectTTL(key).SetVal(ttl)

		result, ttlResult, err := dualCache.GetWithTTL(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, result)
		require.Equal(t, ttl, ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("read from standby when primary read disabled - primary not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())

		standByMock.ExpectGet(key).SetVal(value)
		standByMock.ExpectTTL(key).SetVal(ttl)

		result, ttlResult, err := dualCache.GetWithTTL(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, value, result)
		require.Equal(t, ttl, ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		result, ttlResult, err := dualCache.GetWithTTL(context.Background(), key)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)
		require.Empty(t, result)
		require.Equal(t, time.Duration(0), ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when standby is nil - read from standby enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil, iredis.WithReadFromStandBy())

		result, ttlResult, err := dualCache.GetWithTTL(context.Background(), key)
		require.ErrorIs(t, err, iredis.ErrorNoStandByCacheConfigured)
		require.Empty(t, result)
		require.Equal(t, time.Duration(0), ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("primary error with correct return values", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)
		pError := errors.New("primary error")
		primaryMock.ExpectGet(key).SetErr(pError)

		result, ttlResult, err := dualCache.GetWithTTL(context.Background(), key)
		require.ErrorIs(t, err, pError)
		require.Empty(t, result)
		require.Equal(t, time.Duration(0), ttlResult)
		require.Contains(t, err.Error(), "primary cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("standby error with correct return values", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())
		sErr := errors.New("standby error")
		standByMock.ExpectGet(key).SetErr(sErr)

		result, ttlResult, err := dualCache.GetWithTTL(context.Background(), key)
		require.ErrorIs(t, err, sErr)
		require.Empty(t, result)
		require.Equal(t, time.Duration(0), ttlResult)
		require.Contains(t, err.Error(), "standby cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_Set(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	defaultTTL := 5 * time.Minute
	primaryCache := iredis.NewCache(primaryDB, iredis.WithDefaultTTL(defaultTTL))
	standByCache := iredis.NewCache(standByDB, iredis.WithDefaultTTL(defaultTTL))

	key := "testKey"
	value := "testValue"

	t.Run("write to primary only when dual write disabled - standby not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectSet(key, value, defaultTTL).SetVal("OK")

		err := dualCache.Set(context.Background(), key, value)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("write to both when dual write enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, value, defaultTTL).SetVal("OK")
		standByMock.ExpectSet(key, value, defaultTTL).SetVal("OK")

		err := dualCache.Set(context.Background(), key, value)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("write with custom TTL", func(t *testing.T) {
		customTTL := 30 * time.Second
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, value, customTTL).SetVal("OK")
		standByMock.ExpectSet(key, value, customTTL).SetVal("OK")

		err := dualCache.Set(context.Background(), key, value, iredis.WithTTL(customTTL))
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("primary failure causes operation failure", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())
		pError := errors.New("primary error")
		primaryMock.ExpectSet(key, value, defaultTTL).SetErr(pError)
		// standBy should not be called when primary fails

		err := dualCache.Set(context.Background(), key, value)
		require.ErrorIs(t, err, pError)
		require.Contains(t, err.Error(), "primary cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("standby failure allows operation to succeed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, value, defaultTTL).SetVal("OK")
		standByMock.ExpectSet(key, value, defaultTTL).SetErr(errors.New("standby error"))

		err := dualCache.Set(context.Background(), key, value)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache, iredis.WithEnableDualWrite())

		err := dualCache.Set(context.Background(), key, value)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("write only to primary when no standby cache", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, value, defaultTTL).SetVal("OK")

		err := dualCache.Set(context.Background(), key, value)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_Delete(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	key := "testKey"

	t.Run("delete from primary only when dual write disabled - standby not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectDel(key).SetVal(1)

		err := dualCache.Delete(context.Background(), key)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("delete from both when dual write enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectDel(key).SetVal(1)
		standByMock.ExpectDel(key).SetVal(1)

		err := dualCache.Delete(context.Background(), key)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("primary failure causes operation failure", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		pErr := errors.New("delete error")
		primaryMock.ExpectDel(key).SetErr(pErr)

		err := dualCache.Delete(context.Background(), key)
		require.ErrorIs(t, err, pErr)
		require.Contains(t, err.Error(), "primary cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("standby failure allows operation to succeed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		sErr := errors.New("standby delete error")
		primaryMock.ExpectDel(key).SetVal(1)
		standByMock.ExpectDel(key).SetErr(sErr)

		err := dualCache.Delete(context.Background(), key)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		err := dualCache.Delete(context.Background(), key)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_UnmarshalGet(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	key := "testKey"
	testObj := TestObject{Name: "foo"}
	data, _ := msgpack.Marshal(testObj)

	t.Run("read from primary when enabled - standby not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectGet(key).SetVal(string(data))

		var resultObj TestObject
		err := dualCache.UnmarshalGet(context.Background(), key, &resultObj)
		require.NoError(t, err)
		require.Equal(t, testObj, resultObj)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("read from standby when primary read disabled - primary not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())

		standByMock.ExpectGet(key).SetVal(string(data))

		var resultObj TestObject
		err := dualCache.UnmarshalGet(context.Background(), key, &resultObj)
		require.NoError(t, err)
		require.Equal(t, testObj, resultObj)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		var resultObj TestObject
		err := dualCache.UnmarshalGet(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when standby is nil and read from standby enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil, iredis.WithReadFromStandBy())

		var resultObj TestObject
		err := dualCache.UnmarshalGet(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, iredis.ErrorNoStandByCacheConfigured)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error propagation from primary with correct return", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		unmarshalErr := errors.New("unmarshal error")
		primaryMock.ExpectGet(key).SetErr(unmarshalErr)

		var resultObj TestObject
		err := dualCache.UnmarshalGet(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, unmarshalErr)
		require.Contains(t, err.Error(), "primary cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error from standby causes operation failure when read from standby", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())

		unmarshalErr := errors.New("unmarshal error")
		standByMock.ExpectGet(key).SetErr(unmarshalErr)

		var resultObj TestObject
		err := dualCache.UnmarshalGet(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, unmarshalErr)
		require.Contains(t, err.Error(), "standby cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_UnmarshalGetWithTTL(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	key := "testKey"
	testObj := TestObject{Name: "foo"}
	data, _ := msgpack.Marshal(testObj)
	ttl := 60 * time.Second

	t.Run("read from primary when enabled - standby not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectGet(key).SetVal(string(data))
		primaryMock.ExpectTTL(key).SetVal(ttl)

		var resultObj TestObject
		ttlResult, err := dualCache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
		require.NoError(t, err)
		require.Equal(t, testObj, resultObj)
		require.Equal(t, ttl, ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("read from standby when primary read disabled - primary not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())

		standByMock.ExpectGet(key).SetVal(string(data))
		standByMock.ExpectTTL(key).SetVal(ttl)

		var resultObj TestObject
		ttlResult, err := dualCache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
		require.NoError(t, err)
		require.Equal(t, testObj, resultObj)
		require.Equal(t, ttl, ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		var resultObj TestObject
		ttlResult, err := dualCache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)
		require.Equal(t, time.Duration(0), ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when standby is nil and not reading from primary", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil, iredis.WithReadFromStandBy())

		var resultObj TestObject
		ttlResult, err := dualCache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, iredis.ErrorNoStandByCacheConfigured)
		require.Equal(t, time.Duration(0), ttlResult)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("primary error with wrong return values", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		pError := errors.New("primary error")
		primaryMock.ExpectGet(key).SetErr(pError)

		var resultObj TestObject
		ttlResult, err := dualCache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, pError)
		require.Equal(t, time.Duration(0), ttlResult)
		require.Contains(t, err.Error(), "primary cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("secondary error with wrong return values", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithReadFromStandBy())

		sErr := errors.New("secondary error")
		standByMock.ExpectGet(key).SetErr(sErr)

		var resultObj TestObject
		ttlResult, err := dualCache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
		require.ErrorIs(t, err, sErr)
		require.Equal(t, time.Duration(0), ttlResult)
		require.Contains(t, err.Error(), "standby cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_MarshalSet(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	defaultTTL := 5 * time.Minute
	primaryCache := iredis.NewCache(primaryDB, iredis.WithDefaultTTL(defaultTTL))
	standByCache := iredis.NewCache(standByDB, iredis.WithDefaultTTL(defaultTTL))

	key := "testKey"
	testObj := TestObject{Name: "foo"}
	data, _ := msgpack.Marshal(testObj)

	t.Run("write to primary only when dual write disabled - standby not accessed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectSet(key, data, defaultTTL).SetVal("OK")

		err := dualCache.MarshalSet(context.Background(), key, testObj)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("write to both when dual write enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, data, defaultTTL).SetVal("OK")
		standByMock.ExpectSet(key, data, defaultTTL).SetVal("OK")

		err := dualCache.MarshalSet(context.Background(), key, testObj)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("write with custom TTL", func(t *testing.T) {
		customTTL := 30 * time.Second
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, data, customTTL).SetVal("OK")
		standByMock.ExpectSet(key, data, customTTL).SetVal("OK")

		err := dualCache.MarshalSet(context.Background(), key, testObj, iredis.WithTTL(customTTL))
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("primary failure causes operation failure", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		mErr := errors.New("marshal set error")
		primaryMock.ExpectSet(key, data, defaultTTL).SetErr(mErr)
		// standBy should not be called when primary fails

		err := dualCache.MarshalSet(context.Background(), key, testObj)
		require.ErrorIs(t, err, mErr)
		require.Contains(t, err.Error(), "primary cache:")

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("standby failure allows operation to succeed", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectSet(key, data, defaultTTL).SetVal("OK")
		standByMock.ExpectSet(key, data, defaultTTL).SetErr(errors.New("standby marshal error"))

		err := dualCache.MarshalSet(context.Background(), key, testObj)
		require.NoError(t, err)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		err := dualCache.MarshalSet(context.Background(), key, testObj)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)

		require.NoError(t, standByMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_RunScript(t *testing.T) {
	primaryDB, primaryMock := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	script := redis.NewScript(`
		-- A simple Lua script that returns the sum of two arguments
		return ARGV[1] + ARGV[2]
	`)
	keys := []string{"test-key"}
	args := []any{1, 2}

	t.Run("successful execution on primary only when dual write disabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache)

		primaryMock.ExpectEvalSha(script.Hash(), keys, args...).SetVal(3)
		// standBy should not be called when dual write is disabled

		result, err := dualCache.RunScript(context.Background(), script, keys, args...)
		require.NoError(t, err)
		require.Equal(t, 3, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("successful execution on both caches when dual write enabled", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectEvalSha(script.Hash(), keys, args...).SetVal(3)
		standByMock.ExpectEvalSha(script.Hash(), keys, args...).SetVal(3)

		result, err := dualCache.RunScript(context.Background(), script, keys, args...)
		require.NoError(t, err)
		require.Equal(t, 3, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("primary failure causes operation failure", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		rsError := errors.New("primary script error")
		primaryMock.ExpectEvalSha(script.Hash(), keys, args...).SetErr(rsError)
		// standBy should not be called when primary fails

		result, err := dualCache.RunScript(context.Background(), script, keys, args...)
		require.ErrorIs(t, err, rsError)
		require.Contains(t, err.Error(), "primary cache:")
		require.Empty(t, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("standby failure logged but operation succeeds", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, standByCache, iredis.WithEnableDualWrite())

		primaryMock.ExpectEvalSha(script.Hash(), keys, args...).SetVal(3)
		standByMock.ExpectEvalSha(script.Hash(), keys, args...).SetErr(errors.New("standby script error"))

		result, err := dualCache.RunScript(context.Background(), script, keys, args...)
		require.NoError(t, err)
		require.Equal(t, 3, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})

	t.Run("error when primary is nil", func(t *testing.T) {
		dualCache := iredis.NewDualCache(nil, standByCache)

		result, err := dualCache.RunScript(context.Background(), script, keys, args...)
		require.ErrorIs(t, err, iredis.ErrorNoPrimaryCacheConfigured)
		require.Nil(t, result)

		require.NoError(t, primaryMock.ExpectationsWereMet())
		require.NoError(t, standByMock.ExpectationsWereMet())
	})
}

func TestDualCache_InjectStandByCache(t *testing.T) {
	primaryDB, _ := redismock.NewClientMock()
	standByDB, standByMock := redismock.NewClientMock()

	primaryCache := iredis.NewCache(primaryDB)
	standByCache := iredis.NewCache(standByDB)

	key := "testKey"
	value := "testValue"

	t.Run("inject standby cache directly replaces instance", func(t *testing.T) {
		dualCache := iredis.NewDualCache(primaryCache, nil, iredis.WithReadFromStandBy())

		// try to read from the nil standby cache, which should fail
		result, err := dualCache.Get(context.Background(), key)
		require.Empty(t, result)
		require.ErrorIs(t, err, iredis.ErrorNoStandByCacheConfigured)

		// Inject a valid standby and expect reads to work with the injected standby
		standByMock.ExpectGet(key).SetVal(value)
		dualCache.InjectStandByCache(standByCache)

		result, err = dualCache.Get(context.Background(), key)
		require.Equal(t, value, result)
		require.NoError(t, err)
	})
}
