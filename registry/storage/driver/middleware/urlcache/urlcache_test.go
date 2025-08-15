package urlcache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/docker/distribution/log"
	"github.com/docker/distribution/registry/internal/testutil"
	"github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/mocks"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

func TestNewURLCacheStorageMiddleware(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mocks.NewMockStorageDriver(ctrl)

	testCases := []struct {
		name        string
		options     map[string]any
		wantErr     bool
		errContains string
		validate    func(t *testing.T, m *urlCacheStorageMiddleware)
	}{
		{
			name:        "missing redis cache",
			options:     make(map[string]any),
			wantErr:     true,
			errContains: "`_redisCache` key has not been passed to urlcache middleware",
		},
		{
			name: "default configuration with redis",
			options: map[string]any{
				"_redisCache": testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, defaultMinURLValidity, m.minURLValidity)
				assert.Equal(t, driver.DefaultSignedURLExpiry, m.defaultURLValidity)
				assert.NotNil(t, m.cache)
				assert.False(t, m.dryRun)
			},
		},
		{
			name: "custom min_url_validity as duration",
			options: map[string]any{
				"min_url_validity": 5 * time.Minute,
				"_redisCache":      testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 5*time.Minute, m.minURLValidity)
			},
		},
		{
			name: "custom min_url_validity as string",
			options: map[string]any{
				"min_url_validity": "15m",
				"_redisCache":      testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 15*time.Minute, m.minURLValidity)
			},
		},
		{
			name: "custom min_url_validity as int seconds",
			options: map[string]any{
				"min_url_validity": 300,
				"_redisCache":      testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 5*time.Minute, m.minURLValidity)
			},
		},
		{
			name: "custom min_url_validity as int64 seconds",
			options: map[string]any{
				"min_url_validity": int64(600),
				"_redisCache":      testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 10*time.Minute, m.minURLValidity)
			},
		},
		{
			name: "invalid min_url_validity type",
			options: map[string]any{
				"min_url_validity": []string{"invalid"},
				"_redisCache":      testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "cannot parse",
		},
		{
			name: "invalid min_url_validity string",
			options: map[string]any{
				"min_url_validity": "invalid",
				"_redisCache":      testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "invalid min_url_validity",
		},
		{
			name: "custom default_url_validity as duration",
			options: map[string]any{
				"default_url_validity": 2 * time.Hour,
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 2*time.Hour, m.defaultURLValidity)
			},
		},
		{
			name: "custom default_url_validity as string",
			options: map[string]any{
				"default_url_validity": "1h30m",
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 90*time.Minute, m.defaultURLValidity)
			},
		},
		{
			name: "custom default_url_validity as int seconds",
			options: map[string]any{
				"default_url_validity": 7200,
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 2*time.Hour, m.defaultURLValidity)
			},
		},
		{
			name: "custom default_url_validity as int64 seconds",
			options: map[string]any{
				"default_url_validity": int64(7200),
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.Equal(t, 2*time.Hour, m.defaultURLValidity)
			},
		},
		{
			name: "invalid default_url_validity type",
			options: map[string]any{
				"default_url_validity": true,
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "cannot parse",
		},
		{
			name: "min_url_validity >= default_url_validity",
			options: map[string]any{
				"min_url_validity":     2 * time.Hour,
				"default_url_validity": 1 * time.Hour,
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "min_url_validity (2h0m0s) must be less than defaulturlvalidity (1h0m0s)",
		},
		{
			name: "unrecognized configuration key",
			options: map[string]any{
				"unknownkey":  "value",
				"_redisCache": testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "unrecognized configuration key: unknownkey",
		},
		{
			name: "invalid default_url_validity string format",
			options: map[string]any{
				"default_url_validity": "not-a-duration",
				"_redisCache":          testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "invalid default_url_validity",
		},
		{
			name: "dry_run as bool true",
			options: map[string]any{
				"dry_run":     true,
				"_redisCache": testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.True(t, m.dryRun)
			},
		},
		{
			name: "dry_run as string true",
			options: map[string]any{
				"dry_run":     "true",
				"_redisCache": testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.True(t, m.dryRun)
			},
		},
		{
			name: "dry_run as string false",
			options: map[string]any{
				"dry_run":     "false",
				"_redisCache": testutil.RedisCache(t, 0),
			},
			validate: func(t *testing.T, m *urlCacheStorageMiddleware) {
				assert.False(t, m.dryRun)
			},
		},
		{
			name: "invalid dry_run value",
			options: map[string]any{
				"dry_run":     123,
				"_redisCache": testutil.RedisCache(t, 0),
			},
			wantErr:     true,
			errContains: "dry_run option must be a boolean",
		},
		{
			name: "invalid redis cache type",
			options: map[string]any{
				"_redisCache": "not a redis cache",
			},
			wantErr:     true,
			errContains: "redis cache passed to the middleware cannot be used as the type is wrong",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, tc.options)
			if cleanup != nil {
				defer cleanup()
			}

			if tc.wantErr {
				require.Error(tt, err)
				assert.Contains(tt, err.Error(), tc.errContains)
				return
			}

			require.NoError(tt, err)
			require.NotNil(tt, middleware)

			if tc.validate != nil {
				urlCache := middleware.(*urlCacheStorageMiddleware)
				tc.validate(tt, urlCache)
			}
		})
	}
}

func TestEntryKey(t *testing.T) {
	uc := &urlCacheStorageMiddleware{}

	testCases := []struct {
		name     string
		path     string
		options1 map[string]any
		options2 map[string]any
		same     bool
	}{
		{
			name:     "same path and options",
			path:     "/test/path",
			options1: map[string]any{"method": "GET"},
			options2: map[string]any{"method": "GET"},
			same:     true,
		},
		{
			name:     "different paths",
			path:     "/test/path1",
			options1: nil,
			options2: nil,
			same:     false,
		},
		{
			name:     "different options",
			path:     "/test/path",
			options1: map[string]any{"method": "GET"},
			options2: map[string]any{"method": "POST"},
			same:     false,
		},
		{
			name:     "options order doesn't matter",
			path:     "/test/path",
			options1: map[string]any{"a": "1", "b": "2"},
			options2: map[string]any{"b": "2", "a": "1"},
			same:     true,
		},
		{
			name:     "expiry option is ignored",
			path:     "/test/path",
			options1: map[string]any{"method": "GET", "expiry": time.Now()},
			options2: map[string]any{"method": "GET", "expiry": time.Now().Add(1 * time.Hour)},
			same:     true,
		},
		{
			name:     "nil vs empty options",
			path:     "/test/path",
			options1: nil,
			options2: make(map[string]any),
			same:     true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			cacheKey1, statsKey1 := uc.entryKey(tc.path, tc.options1)
			var cacheKey2 string
			var statsKey2 uint64
			if tc.name == "different paths" {
				cacheKey2, statsKey2 = uc.entryKey("/test/path2", tc.options2)
			} else {
				cacheKey2, statsKey2 = uc.entryKey(tc.path, tc.options2)
			}

			if tc.same {
				assert.Equal(tt, cacheKey1, cacheKey2)
				assert.Equal(tt, statsKey1, statsKey2)
			} else {
				assert.NotEqual(tt, cacheKey1, cacheKey2)
				assert.NotEqual(tt, statsKey1, statsKey2)
			}

			// Verify key format
			assert.Contains(tt, cacheKey1, "registry:storage:urlcache:")
		})
	}
}

func TestGetExpiresAt(t *testing.T) {
	uc := &urlCacheStorageMiddleware{}
	mockClock := clock.NewMock()
	systemClock = mockClock

	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	mockClock.Set(now)

	testCases := []struct {
		name        string
		options     map[string]any
		expected    time.Time
		errContains string
	}{
		{
			name:     "nil options",
			options:  nil,
			expected: now.Add(driver.DefaultSignedURLExpiry),
		},
		{
			name:     "empty options",
			options:  make(map[string]any),
			expected: now.Add(driver.DefaultSignedURLExpiry),
		},
		{
			name: "expiry option present",
			options: map[string]any{
				"expiry": now.Add(2 * time.Hour),
			},
			expected: now.Add(2 * time.Hour),
		},
		{
			name: "invalid expiry type",
			options: map[string]any{
				"expiry": "invalid",
			},
			errContains: "must be of type time.Time",
		},
		{
			name: "other options present",
			options: map[string]any{
				"method": "GET",
				"other":  "value",
			},
			expected: now.Add(driver.DefaultSignedURLExpiry),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			result, err := uc.getExpiresAt(tc.options)
			if tc.errContains != "" {
				require.ErrorContains(tt, err, tc.errContains)
			} else {
				require.NoError(tt, err)
				assert.Equal(tt, tc.expected, result)
			}
		})
	}
}

func TestCacheEntrySize(t *testing.T) {
	testCases := []struct {
		name         string
		entry        CacheEntry
		expectedSize int64
	}{
		{
			name: "empty url",
			entry: CacheEntry{
				URL: "",
			},
			expectedSize: 40, // struct overhead only
		},
		{
			name: "short url",
			entry: CacheEntry{
				URL: "https://example.com",
			},
			expectedSize: 40 + 19, // struct overhead + len("https://example.com")
		},
		{
			name: "long url",
			entry: CacheEntry{
				URL: "https://very-long-domain-name.example.com/path/to/resource/with/many/segments/and/parameters?foo=bar&baz=qux",
			},
			expectedSize: 40 + 108, // struct overhead + url length
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			size := tc.entry.size()
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

// URLForTestSuite is a test suite for URLFor functionality
type URLForTestSuite struct {
	suite.Suite
	ctrl      *gomock.Controller
	mockClock *clock.Mock
	now       time.Time

	ctx context.Context
}

func (s *URLForTestSuite) SetupSuite() {
	s.mockClock = clock.NewMock()
	systemClock = s.mockClock
	s.now = time.Date(2025, 6, 15, 7, 0, 0, 0, time.UTC)
	s.mockClock.Set(s.now)
}

func (*URLForTestSuite) TearDownSuite() {
	systemClock = clock.New()
}

func (s *URLForTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	// Reset time to baseline for each test
	s.mockClock.Set(s.now)

	s.ctx = log.WithLogger(context.Background(), log.GetLogger(log.WithTestingTB(s.T())))
}

func (s *URLForTestSuite) TestCacheMissEntryNotSeenBefore() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		Return("https://example.com/test", nil).
		Times(1)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Call URLFor
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)

	// Verify cache state in Redis
	key, _ := uc.entryKey("/test/path", map[string]any{"expiry": s.now.Add(driver.DefaultSignedURLExpiry)})
	entry := new(CacheEntry)
	err = redisCache.UnmarshalGet(s.ctx, key, entry)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", entry.URL)
}

func (s *URLForTestSuite) TestCacheHitWithSufficientValidity() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	// No calls to URLFor expected for cache hit
	mockDriver.EXPECT().URLFor(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Setup cache
	key, _ := uc.entryKey("/test/path", nil)
	entry := CacheEntry{
		URL: "https://cached.com/test",
	}
	err = redisCache.MarshalSet(s.ctx, key, entry)
	require.NoError(s.T(), err)

	// Call URLFor
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://cached.com/test", url)
}

func (s *URLForTestSuite) TestErrorFromUnderlyingDriverNotCached() {
	resErr := errors.New("storage error")
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		Return("", resErr).
		Times(1)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Call URLFor
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.Error(s.T(), err)
	require.ErrorIs(s.T(), err, resErr)
	assert.Empty(s.T(), url)

	// Verify nothing is cached
	key, _ := uc.entryKey("/test/path", nil)
	var entry CacheEntry
	err = redisCache.UnmarshalGet(s.ctx, key, &entry)
	require.ErrorIs(s.T(), err, redis.Nil)
}

func (s *URLForTestSuite) TestDryRunMode() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	// In dry run mode, driver should be called every time
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		Return("https://example.com/test", nil).
		Times(2)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
		"dry_run":     true,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// First call
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)

	// Second call - should still call driver despite cache
	url, err = uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)
}

func (s *URLForTestSuite) TestMinURLValidityCheck() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		Return("https://example.com/test", nil).
		Times(1)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache":      redisCache,
		"min_url_validity": 10 * time.Minute,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Call URLFor with short expiry
	options := map[string]any{
		"expiry": s.now.Add(5 * time.Minute), // Less than minURLValidity
	}
	url, err := uc.URLFor(s.ctx, "/test/path", options)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)

	// Verify nothing was cached due to short validity
	key, _ := uc.entryKey("/test/path", options)
	var entry CacheEntry
	err = redisCache.UnmarshalGet(s.ctx, key, &entry)
	require.ErrorIs(s.T(), err, redis.Nil)
}

func (s *URLForTestSuite) TestOptionsNilVsSetSameCacheBehavior() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, opts map[string]any) (string, error) {
			// Verify that expiry was set to default
			require.NotNil(s.T(), opts)
			assert.Contains(s.T(), opts, "expiry")
			return "https://example.com/test", nil
		}).
		Times(1)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Call URLFor with nil optionsrequire
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)

	// Verify cached
	key, _ := uc.entryKey("/test/path", map[string]any{"expiry": s.now.Add(driver.DefaultSignedURLExpiry)})
	var entry CacheEntry
	err = redisCache.UnmarshalGet(s.ctx, key, &entry)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", entry.URL)
}

func (s *URLForTestSuite) TestCacheMissWithExpiryParameterSet() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)

	// Expect call to underlying driver with the expiry parameter
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, opts map[string]any) (string, error) {
			// Verify expiry parameter is passed through
			require.NotNil(s.T(), opts)
			assert.Contains(s.T(), opts, "expiry")
			expiry, ok := opts["expiry"].(time.Time)
			assert.True(s.T(), ok)
			assert.Equal(s.T(), s.now.Add(3*time.Hour), expiry)
			return "https://example.com/test-with-expiry", nil
		}).
		Times(1)

	redisCache := testutil.RedisCache(s.T(), 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Set expiry time
	expiryTime := s.now.Add(3 * time.Hour)
	options := map[string]any{
		"expiry": expiryTime,
		"method": "GET",
	}

	// First call - cache miss
	url, err := uc.URLFor(s.ctx, "/test/path", options)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test-with-expiry", url)

	// Verify entry is cached with correct TTL
	key, _ := uc.entryKey("/test/path", options)
	var cached CacheEntry
	value, ttl, err := redisCache.GetWithTTL(s.ctx, key)
	require.NoError(s.T(), err)
	assert.NotEmpty(s.T(), value)
	// TTL should be approximately 2 hours 50 minutes plus/minus a small delta for processing time
	assert.Greater(s.T(), ttl, 2*time.Hour+48*time.Minute)
	assert.LessOrEqual(s.T(), ttl, 2*time.Hour+52*time.Minute)

	// Unmarshal to verify content
	err = redisCache.UnmarshalGet(s.ctx, key, &cached)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test-with-expiry", cached.URL)
}

func (s *URLForTestSuite) TestRedisSetError() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		Return("https://example.com/test", nil).
		Times(1)

	redisCache, redisMock := testutil.RedisCacheMock(s.T(), 0)

	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	key, _ := uc.entryKey("/test/path", map[string]any{"expiry": s.now.Add(driver.DefaultSignedURLExpiry)})
	redisMock.ExpectGet(key).RedisNil()
	redisMock.CustomMatch(func(_, actual []any) error {
		if actual[1] != key {
			return errors.New("key does not match")
		}
		return nil
	}).ExpectSetArgs(
		key,
		"",
		redis.SetArgs{TTL: 20 * time.Minute},
	).SetErr(fmt.Errorf("foo error"))

	// Call URLFor - should still return URL even if caching fails
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)

	require.NoError(s.T(), redisMock.ExpectationsWereMet())
}

func (s *URLForTestSuite) TestRedisGetError() {
	mockDriver := mocks.NewMockStorageDriver(s.ctrl)
	mockDriver.EXPECT().URLFor(gomock.Any(), "/test/path", gomock.Any()).
		Return("https://example.com/test", nil).
		Times(1)

	redisCache, redisMock := testutil.RedisCacheMock(s.T(), 0)

	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(s.T(), err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	key, _ := uc.entryKey("/test/path", map[string]any{"expiry": s.now.Add(driver.DefaultSignedURLExpiry)})
	redisMock.ExpectGet(key).SetErr(fmt.Errorf("foo error"))
	redisMock.CustomMatch(func(_, actual []any) error {
		if actual[1] != key {
			return errors.New("key does not match")
		}
		return nil
	}).ExpectSetArgs(
		key,
		"",
		redis.SetArgs{TTL: 20 * time.Minute},
	).SetVal("OK")

	// Call URLFor - should fall back to driver due to Redis error
	url, err := uc.URLFor(s.ctx, "/test/path", nil)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), "https://example.com/test", url)
}

func TestURLForSuite(t *testing.T) {
	suite.Run(t, new(URLForTestSuite))
}

func TestConcurrentAccess(t *testing.T) {
	const concurrencyFactor = 100

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDriver := mocks.NewMockStorageDriver(ctrl)
	for i := 0; i < 5; i++ {
		mockDriver.EXPECT().URLFor(gomock.Any(), fmt.Sprintf("/test/path/%d", i), gomock.Any()).
			Return(fmt.Sprintf("https://example.com/test/%d", i), nil).
			MinTimes(1)
	}

	redisCache := testutil.RedisCache(t, 0)
	middleware, cleanup, err := newURLCacheStorageMiddleware(mockDriver, map[string]any{
		"_redisCache": redisCache,
	})
	require.NoError(t, err)
	defer cleanup()
	uc := middleware.(*urlCacheStorageMiddleware)

	// Synchronization primitives
	wgDone := new(sync.WaitGroup)
	startSignal := make(chan struct{})
	readySignal := make(chan struct{}, concurrencyFactor)

	ctx := log.WithLogger(context.Background(), log.GetLogger(log.WithTestingTB(t)))

	// Launch goroutines
	for i := 0; i < concurrencyFactor; i++ {
		wgDone.Add(1)
		go func(idx int) {
			defer wgDone.Done()

			readySignal <- struct{}{}
			<-startSignal

			// Execute the actual work
			path := fmt.Sprintf("/test/path/%d", idx%5)
			cachedPath, err := uc.URLFor(ctx, path, nil)
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("https://example.com/test/%d", idx%5), cachedPath)
		}(i)
	}

	for i := 0; i < concurrencyFactor; i++ {
		<-readySignal
	}
	close(readySignal)
	close(startSignal)

	// Wait for all goroutines to complete
	wgDone.Wait()
}

func TestCacheKeyUniqueness(t *testing.T) {
	uc := &urlCacheStorageMiddleware{}

	// Generate many keys and ensure no collisions for different inputs
	keys := make(map[string]string)
	statsKeys := make(map[uint64]string)

	paths := []string{"/a", "/b", "/test/path", "/test/path2", "/very/long/path/to/resource"}
	optionSets := []map[string]any{
		nil,
		{},
		{"method": "GET"},
		{"method": "POST"},
		{"method": "GET", "param": "value"},
		{"param": "value", "method": "GET"}, // Same as above but different order
	}

	for _, path := range paths {
		for i, opts := range optionSets {
			key, statsKey := uc.entryKey(path, opts)

			// Check for uniqueness unless it's the reordered options case
			if i == 5 && path == paths[0] { // Skip the intentionally same key
				continue
			}

			identifier := fmt.Sprintf("%s-%v", path, opts)
			if existing, exists := keys[key]; exists {
				if i == 5 { // This is expected for reordered options
					continue
				}
				require.Equalf(t, existing, identifier, "Cache key collision: %s and %s produced same key %s", existing, identifier, key)
			}
			keys[key] = identifier

			// Stats keys should also be unique (except for reordered options)
			if existing, exists := statsKeys[statsKey]; exists {
				if i == 5 { // This is expected for reordered options
					continue
				}
				require.Equalf(t, existing, identifier, "Stats key collision: %s and %s produced same stats key %d", existing, identifier, statsKey)
			}
			statsKeys[statsKey] = identifier
		}
	}
}
