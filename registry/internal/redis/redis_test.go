package redis_test

import (
	"context"
	"testing"
	"time"

	iredis "github.com/docker/distribution/registry/internal/redis"
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCache_Get(t *testing.T) {
	db, mock := redismock.NewClientMock()
	cache := iredis.NewCache(db)

	key := "testKey"
	value := "testValue"
	mock.ExpectGet(key).SetVal(value)

	result, err := cache.Get(context.Background(), key)
	assert.NoError(t, err)
	assert.Equal(t, value, result)

	assert.NoError(t, mock.ExpectationsWereMet())
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
	assert.NoError(t, err)
	assert.Equal(t, value, result)
	assert.Equal(t, ttl, ttlResult)

	assert.NoError(t, mock.ExpectationsWereMet())
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
				assert.NoError(t, err)
			} else {
				err := cache.Set(context.Background(), tt.key, tt.value, iredis.WithTTL(tt.ttl))
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
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
	assert.NoError(t, err)
	assert.Equal(t, testObj, resultObj)

	assert.NoError(t, mock.ExpectationsWereMet())
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
	assert.NoError(t, err)
	assert.Equal(t, testObj, resultObj)
	assert.Equal(t, ttl, ttlResult)

	assert.NoError(t, mock.ExpectationsWereMet())
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
				assert.NoError(t, err)
			} else {
				err := cache.MarshalSet(context.Background(), tt.key, testObj, iredis.WithTTL(tt.ttl))
				assert.NoError(t, err)
			}

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
