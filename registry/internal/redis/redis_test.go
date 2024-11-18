package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/go-redis/redismock/v9"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCache_Get(t *testing.T) {
	db, mock := redismock.NewClientMock()
	cache := iredis.NewCache(db)

	key := "testKey"
	value := "testValue"
	mock.ExpectGet(key).SetVal(value)

	result, err := cache.Get(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, value, result)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCache_GetWithTTL(t *testing.T) {
	db, mock := redismock.NewClientMock()
	cache := iredis.NewCache(db)

	key := "testKey"
	value := "testValue"
	ttl := 60 * time.Second

	mock.ExpectGet(key).SetVal(value)
	mock.ExpectTTL(key).SetVal(ttl)

	result, ttlResult, err := cache.GetWithTTL(context.Background(), key)
	require.NoError(t, err)
	assert.Equal(t, value, result)
	assert.Equal(t, ttl, ttlResult)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCache_Set(t *testing.T) {
	db, mock := redismock.NewClientMock()
	defaultTTL := 5 * time.Minute
	cache := iredis.NewCache(db, iredis.WithDefaultTTL(defaultTTL))

	tests := []struct {
		name  string
		key   string
		value string
		ttl   time.Duration
	}{
		{
			name:  "with default TTL",
			key:   "testKey1",
			value: "testValue1",
			ttl:   defaultTTL,
		},
		{
			name:  "with custom TTL",
			key:   "testKey2",
			value: "testValue2",
			ttl:   60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock.ExpectSet(tt.key, tt.value, tt.ttl).SetVal("OK")

			if tt.ttl == defaultTTL {
				err := cache.Set(context.Background(), tt.key, tt.value)
				require.NoError(t, err)
			} else {
				err := cache.Set(context.Background(), tt.key, tt.value, iredis.WithTTL(tt.ttl))
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

type TestObject struct {
	Name string
}

func TestCache_MarshalGet(t *testing.T) {
	db, mock := redismock.NewClientMock()
	cache := iredis.NewCache(db)

	key := "testKey"

	testObj := TestObject{
		Name: "foo",
	}
	data, _ := msgpack.Marshal(testObj)

	mock.ExpectGet(key).SetVal(string(data))

	var resultObj TestObject
	err := cache.UnmarshalGet(context.Background(), key, &resultObj)
	require.NoError(t, err)
	assert.Equal(t, testObj, resultObj)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCache_MarshalGetWithTTL(t *testing.T) {
	db, mock := redismock.NewClientMock()
	cache := iredis.NewCache(db)

	key := "testKey"
	testObj := TestObject{
		Name: "foo",
	}
	data, _ := msgpack.Marshal(testObj)
	ttl := time.Second * 60

	mock.ExpectGet(key).SetVal(string(data))
	mock.ExpectTTL(key).SetVal(ttl)

	var resultObj TestObject
	ttlResult, err := cache.UnmarshalGetWithTTL(context.Background(), key, &resultObj)
	require.NoError(t, err)
	assert.Equal(t, testObj, resultObj)
	assert.Equal(t, ttl, ttlResult)

	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCache_MarshalSet(t *testing.T) {
	db, mock := redismock.NewClientMock()
	defaultTTL := 5 * time.Minute
	cache := iredis.NewCache(db, iredis.WithDefaultTTL(defaultTTL))

	testObj := TestObject{
		Name: "foo",
	}
	data, _ := msgpack.Marshal(testObj)

	tests := []struct {
		name string
		key  string
		ttl  time.Duration
	}{
		{
			name: "with default TTL",
			key:  "testKey1",
			ttl:  defaultTTL,
		},
		{
			name: "with custom TTL",
			key:  "testKey2",
			ttl:  60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock.ExpectSet(tt.key, data, tt.ttl).SetVal("OK")

			if tt.ttl == defaultTTL {
				err := cache.MarshalSet(context.Background(), tt.key, testObj)
				require.NoError(t, err)
			} else {
				err := cache.MarshalSet(context.Background(), tt.key, testObj, iredis.WithTTL(tt.ttl))
				require.NoError(t, err)
			}

			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestCache_RunScript(t *testing.T) {
	db, mock := redismock.NewClientMock()
	cache := iredis.NewCache(db)

	script := redis.NewScript(`
		-- A simple Lua script that returns the sum of two arguments
		return ARGV[1] + ARGV[2]
	`)
	keys := []string{"test-key"}
	args := []any{1, 2}

	t.Run("successful execution", func(t *testing.T) {
		mock.ExpectEvalSha(script.Hash(), keys, args...).SetVal(3)

		result, err := cache.RunScript(context.Background(), script, keys, args...)
		require.NoError(t, err)
		assert.Equal(t, 3, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("failed execution", func(t *testing.T) {
		expectedErr := "foo"
		mock.ExpectEvalSha(script.Hash(), keys, args...).SetErr(errors.New(expectedErr))

		result, err := cache.RunScript(context.Background(), script, keys, args...)
		require.Error(t, err)
		require.EqualError(t, err, expectedErr)
		assert.Nil(t, result)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
