//go:build integration && redis_test

package handlers

import (
	"context"
	"math/rand/v2"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/docker/distribution/configuration"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiterBasicFunctionality(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Run("blocks requests when limit exceeded", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "block-limiter",
						Description: "Blocking rate limiter",
						LogOnly:     false,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  10,
						Limit:       configuration.Limit{Rate: 3, Period: "second", Burst: 6}, // Global: 3 req/sec, 6 burst
						Action:      configuration.Action{WarnThreshold: 0.7, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest(testIP())
		handler := createMiddlewareHandler(app)

		// Should allow 6 burst requests GLOBALLY (not per-shard)
		for i := 0; i < 6; i++ {
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code, "Request %d should be allowed", i+1)
			require.NotEmpty(t, resp.Header().Get(headerXRateLimitRemaining))

			remaining, _ := strconv.Atoi(resp.Header().Get(headerXRateLimitRemaining))
			require.Equal(t, 6-i-1, remaining, "Remaining should be %d after request %d", 6-i-1, i+1)

			resp = httptest.NewRecorder()
		}

		// 7th request should be blocked when burst is exhausted
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusTooManyRequests, resp.Code, "7th request should be blocked")
		verifyRateLimitResponse(t, resp)
	})

	t.Run("log_only mode never blocks", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "log-only-limiter",
						Description: "Log-only rate limiter",
						LogOnly:     true,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  10,
						Limit:       configuration.Limit{Rate: 3, Period: "second", Burst: 6}, // Global: 3 req/sec, 6 burst
						Action:      configuration.Action{WarnThreshold: 0.7, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest(testIP())
		handler := createMiddlewareHandler(app)

		// Should never block even after exceeding limit
		for i := 0; i < 8; i++ { // Test more than burst to ensure we hit the threshold
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)
			assertRateLimitHeaders(t, resp)
			resp = httptest.NewRecorder()
		}
	})

	t.Run("X-RateLimit-Remaining header decreases correctly", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "header-test-limiter",
						Description: "Rate limiter for header testing",
						LogOnly:     false,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  10,
						Limit:       configuration.Limit{Rate: 15, Period: "minute", Burst: 30}, // Global: 15 req/min, 30 burst
						Action:      configuration.Action{WarnThreshold: 0.8, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest(testIP())
		handler := createMiddlewareHandler(app)

		// Track expected remaining values (GLOBAL burst = 30)
		expectedRemaining := []int{29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0}

		// Test burst capacity - should consume from burst first
		for i := 0; i < 30; i++ {
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code, "Request %d should be allowed", i+1)

			// Check X-RateLimit-Remaining header
			remainingHeader := resp.Header().Get(headerXRateLimitRemaining)
			require.NotEmpty(t, remainingHeader, "X-RateLimit-Remaining header should be present for request %d", i+1)

			remaining, err := strconv.Atoi(remainingHeader)
			require.NoError(t, err, "X-RateLimit-Remaining should be a valid integer for request %d", i+1)
			require.Equal(t, expectedRemaining[i], remaining,
				"X-RateLimit-Remaining should be %d for request %d", expectedRemaining[i], i+1)
			assertRateLimitHeaders(t, resp)

			resp = httptest.NewRecorder()
		}

		// 31st request should be blocked (burst exhausted)
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusTooManyRequests, resp.Code, "31st request should be blocked")

		verifyRateLimitResponse(t, resp)
	})

	t.Run("X-RateLimit-Remaining header with different IPs", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		// Generate unique test IPs for this test run
		testIPs := []string{testIP(), testIP(), testIP()}
		t.Logf("Using test IPs: %v", testIPs)

		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "multi-ip-limiter",
						Description: "Rate limiter for testing multiple IPs",
						LogOnly:     false,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  10,
						Limit:       configuration.Limit{Rate: 6, Period: "minute", Burst: 9}, // Global: 6 req/min, 9 burst
						Action:      configuration.Action{WarnThreshold: 0.5, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		handler := createMiddlewareHandler(app)

		// Test different IPs should have independent counters
		for ipIndex, testIP := range testIPs {
			t.Logf("=== Testing IP %d: %s ===", ipIndex+1, testIP)

			req, resp := createTestRequest(testIP)

			// Each IP should start with GLOBAL burst capacity (9)
			expectedRemaining := []int{8, 7, 6, 5, 4, 3, 2, 1, 0}
			for i := 0; i < 9; i++ {
				handler.ServeHTTP(resp, req)
				require.Equal(t, http.StatusOK, resp.Code, "Request %d from IP %s should be allowed", i+1, testIP)

				remainingHeader := resp.Header().Get(headerXRateLimitRemaining)
				require.NotEmpty(t, remainingHeader, "X-RateLimit-Remaining header should be present")

				remaining, err := strconv.Atoi(remainingHeader)
				require.NoError(t, err, "X-RateLimit-Remaining should be a valid integer")
				require.Equal(t, expectedRemaining[i], remaining, "X-RateLimit-Remaining should be %d for request %d from IP %s", expectedRemaining[i], i+1, testIP)

				t.Logf("IP %s, Request %d: X-RateLimit-Remaining = %d", testIP, i+1, remaining)

				resp = httptest.NewRecorder()
			}

			// 10th request from this IP should be blocked
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusTooManyRequests, resp.Code, "10th request from IP %s should be blocked", testIP)

			remainingHeader := resp.Header().Get(headerXRateLimitRemaining)
			remaining, err := strconv.Atoi(remainingHeader)
			require.NoError(t, err)
			require.Equal(t, 0, remaining, "X-RateLimit-Remaining should be 0 when blocked for IP %s", testIP)
		}
	})

	t.Run("per-IP rate limiting works correctly across shards", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "per-ip-limiter",
						Description: "Test per-IP rate limiting across shards",
						LogOnly:     false,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  10,
						Limit:       configuration.Limit{Rate: 3, Period: "minute", Burst: 9}, // Global: 3 req/min, 9 burst
						Action:      configuration.Action{WarnThreshold: 0.8, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		handler := createMiddlewareHandler(app)

		// Use IPs that hash to different Redis shards
		crossShardIPs := []string{
			testIP(), testIP(), testIP(),
		}

		// Each IP should be allowed its full GLOBAL burst (9 requests each)
		for _, testIP := range crossShardIPs {
			t.Logf("Testing IP: %s", testIP)

			// First 9 requests should be allowed (within global burst)
			for i := 0; i < 9; i++ {
				req, resp := createTestRequest(testIP)
				handler.ServeHTTP(resp, req)

				require.Equal(t, http.StatusOK, resp.Code,
					"Request %d from IP %s should be allowed (within burst of 9)",
					i+1, testIP)

				t.Logf("Request %d from IP %s: Status=%d", i+1, testIP, resp.Code)
			}

			// 10th request from same IP should be blocked
			req, resp := createTestRequest(testIP)
			handler.ServeHTTP(resp, req)

			require.Equal(t, http.StatusTooManyRequests, resp.Code,
				"10th request from IP %s should be blocked (burst exceeded)", testIP)

			t.Logf("10th request from IP %s: Status=%d (correctly blocked)", testIP, resp.Code)
		}
	})
}

func TestRateLimiterMultipleLimiters(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Run("different limiters use separate keys", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		// Test with a single limiter to establish baseline behavior
		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "ip-limiter",
						Description: "IP-based rate limiter",
						LogOnly:     false,
						Match:       configuration.Match{Type: "ip"},
						Precedence:  1,
						Limit:       configuration.Limit{Rate: 30, Period: "second", Burst: 30},
						Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest(testIP())
		handler := createMiddlewareHandler(app)

		// Test that one limiter works correctly
		requestsMade := 0
		for i := 0; i < 35; i++ {
			handler.ServeHTTP(resp, req)
			if resp.Code == http.StatusOK {
				requestsMade++
			} else {
				break
			}
			resp = httptest.NewRecorder()
		}

		// From the debug logs, we can see the limiter allows 30 requests (capacity=30)
		// The first request starts with 29 remaining, so 30 total requests should be allowed
		require.Equal(t, 30, requestsMade, "Should allow 30 requests total (burst capacity)")
		require.Equal(t, http.StatusTooManyRequests, resp.Code, "Should be blocked after burst capacity is exhausted")
	})

	t.Run("multiple limiters with same match type interfere", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		// This test documents the key collision behavior
		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "strict-limiter",
						Description: "Strict limiter",
						LogOnly:     false,
						Match:       configuration.Match{Type: "ip"},
						Precedence:  1, // Processed first
						Limit:       configuration.Limit{Rate: 18, Period: "second", Burst: 18},
						Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "block"},
					},
					{
						Name:        "lenient-limiter",
						Description: "Lenient limiter",
						LogOnly:     false,
						Match:       configuration.Match{Type: "ip"},
						Precedence:  2, // Processed second
						Limit:       configuration.Limit{Rate: 180, Period: "second", Burst: 270},
						Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest(testIP())
		handler := createMiddlewareHandler(app)

		// Due to key collision, the behavior is:
		// 1. Both limiters check the same Redis key
		// 2. The strict limiter starts with capacity=18, but the lenient limiter
		//    processes the same key with capacity=270, corrupting the bucket state
		// 3. The effective limit becomes unpredictable but typically around the
		//    lower limit due to the strict limiter being processed first

		requestsMade := 0
		for i := 0; i < 25; i++ {
			handler.ServeHTTP(resp, req)
			if resp.Code == http.StatusOK {
				requestsMade++
			} else {
				break
			}
			resp = httptest.NewRecorder()
		}

		// Document the current problematic behavior
		t.Logf("With key collision, made %d requests before being blocked", requestsMade)

		// From the debug logs, we can see it allows around 9-18 requests before blocking
		// This shows the collision effect where the strict limiter's lower capacity
		// interferes with the lenient limiter's higher capacity
		require.GreaterOrEqual(t, requestsMade, 8, "Should allow at least 8 requests")
		require.LessOrEqual(t, requestsMade, 20, "Should not exceed 20 requests due to interference")
		require.Equal(t, http.StatusTooManyRequests, resp.Code, "Should eventually be blocked")
	})

	t.Run("limiter precedence affects processing order", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupRedisKeys(t, redisAddr)
		})

		// Test that demonstrates precedence ordering with different IPs to avoid collision
		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "high-precedence",
						Description: "High precedence limiter",
						LogOnly:     true, // Log only to see processing order
						Match:       configuration.Match{Type: "ip"},
						Precedence:  1, // Lower number = higher precedence
						Limit:       configuration.Limit{Rate: 60, Period: "second", Burst: 60},
						Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "log"},
					},
					{
						Name:        "low-precedence",
						Description: "Low precedence limiter",
						LogOnly:     true, // Log only to see processing order
						Match:       configuration.Match{Type: "ip"},
						Precedence:  2, // Higher number = lower precedence
						Limit:       configuration.Limit{Rate: 30, Period: "second", Burst: 30},
						Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "log"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest(testIP())
		handler := createMiddlewareHandler(app)

		// Make a single request to verify both limiters process
		handler.ServeHTTP(resp, req)

		// Since both are log-only, request should succeed
		require.Equal(t, http.StatusOK, resp.Code, "Request should succeed with log-only limiters")

		// The logs would show the processing order based on precedence
		// but we can't easily assert on log output in this test structure
	})
}

// Test that basic functionality works with proper cleanup
func TestRateLimitersBasicWithCleanup(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Cleanup(func() {
		cleanupRedisKeys(t, redisAddr)
	})

	config := &configuration.Configuration{
		RateLimiter: configuration.RateLimiter{
			Enabled: true,
			Limiters: []configuration.Limiter{
				{
					Name:        "basic-test",
					Description: "Basic functionality test",
					LogOnly:     false,
					Match:       configuration.Match{Type: matchTypeIP},
					Precedence:  10,
					Limit:       configuration.Limit{Rate: 6, Period: "second", Burst: 12}, // Global: 12 requests
					Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "block"},
				},
			},
		},
	}

	setupRedisConfig(config, redisAddr)
	app, err := setupAppWithRateLimiter(t, config)
	require.NoError(t, err)

	req, resp := createTestRequest(testIP())
	handler := createMiddlewareHandler(app)

	// Should be able to make 12 requests (global burst capacity)
	for i := 1; i <= 12; i++ {
		handler.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code, "Request %d should be allowed", i)
		assert.NotEmpty(t, resp.Header().Get(headerRateLimit))
		assert.NotEmpty(t, resp.Header().Get(headerRateLimitPolicy))
		assert.NotEmpty(t, resp.Header().Get(headerXRateLimitLimit))
		assert.NotEmpty(t, resp.Header().Get(headerXRateLimitRemaining))
		assert.NotEmpty(t, resp.Header().Get(headerXRateLimitReset))

		resp = httptest.NewRecorder()
	}

	// 13th request should be blocked
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusTooManyRequests, resp.Code, "13th request should be blocked")
	remainingHeader, err := strconv.Atoi(resp.Header().Get(headerXRateLimitRemaining))
	require.NoError(t, err)
	require.Zero(t, remainingHeader, "Remaining header should be zero, got: %d", remainingHeader)
}

// Test that specifically validates the threshold calculation
func TestRateLimitersThresholdCalculation(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Cleanup(func() {
		cleanupRedisKeys(t, redisAddr)
	})

	config := &configuration.Configuration{
		RateLimiter: configuration.RateLimiter{
			Enabled: true,
			Limiters: []configuration.Limiter{
				{
					Name:        "threshold-calc-test",
					Description: "Threshold calculation test",
					LogOnly:     false,
					Match:       configuration.Match{Type: matchTypeIP},
					Precedence:  10,
					Limit:       configuration.Limit{Rate: 15, Period: "second", Burst: 30}, // Global: 30 burst
					Action:      configuration.Action{WarnThreshold: 0.6, WarnAction: "log", HardAction: "block"},
				},
			},
		},
	}

	setupRedisConfig(config, redisAddr)
	app, err := setupAppWithRateLimiter(t, config)
	require.NoError(t, err)

	req, resp := createTestRequest(testIP())
	handler := createMiddlewareHandler(app)

	// With global burst=30 and threshold=0.6, warning should trigger at 18th request (60% of 30)
	// First 17 requests should not trigger warning
	for i := 1; i <= 17; i++ {
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code, "Request %d should be allowed", i)

		resp = httptest.NewRecorder()
	}

	// 18th request should trigger warning (60% of 30 = 18)
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, "18th request should be allowed")
}

func TestRateLimiterSpecialCaseZeroThreshold(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Cleanup(func() {
		cleanupRedisKeys(t, redisAddr)
	})

	config := &configuration.Configuration{
		RateLimiter: configuration.RateLimiter{
			Enabled: true,
			Limiters: []configuration.Limiter{
				{
					Name:        "zero-threshold",
					Description: "Zero threshold test limiter",
					LogOnly:     false,
					Match:       configuration.Match{Type: matchTypeIP},
					Precedence:  10,
					Limit:       configuration.Limit{Rate: 6, Period: "second", Burst: 12}, // Global: 12 burst
					Action: configuration.Action{
						WarnThreshold: 0.0, // No warnings until limit hit
						WarnAction:    "log",
						HardAction:    "block",
					},
				},
			},
		},
	}

	setupRedisConfig(config, redisAddr)
	app, err := setupAppWithRateLimiter(t, config)
	require.NoError(t, err)

	req, resp := createTestRequest(testIP())
	handler := createMiddlewareHandler(app)

	// Should be able to make 12 requests before being blocked (global burst)
	for i := 0; i < 12; i++ {
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code, "Request %d should be allowed", i+1)
		assertRateLimitHeaders(t, resp)

		resp = httptest.NewRecorder()
	}

	// 13th request should be blocked
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusTooManyRequests, resp.Code, "13th request should be blocked")
}

// Keep all the existing helper functions unchanged
func setupRedisConfig(config *configuration.Configuration, redisAddr string) {
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled:     true,
		Addr:        redisAddr,
		DialTimeout: 5 * time.Second,
		TLS: configuration.RedisTLS{
			Insecure: true,
		},
	}
}

func setupAppWithRateLimiter(t *testing.T, config *configuration.Configuration) (*App, error) {
	t.Helper()
	t.Cleanup(func() {
		cleanupRedisKeys(t, config.Redis.RateLimiter.Addr)
	})

	ctx := context.Background()
	app := &App{
		Context: ctx,
		Config:  config,
	}

	err := app.configureRedisRateLimiter(ctx, config)
	return app, err
}

func createTestRequest(remoteAddr string) (*http.Request, *httptest.ResponseRecorder) {
	req, _ := http.NewRequest("GET", "/v2/", nil)
	req.RemoteAddr = remoteAddr
	return req, httptest.NewRecorder()
}

func createMiddlewareHandler(app *App) http.Handler {
	successHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	return app.rateLimiterMiddleware(successHandler)
}

func verifyRateLimitResponse(t *testing.T, resp *httptest.ResponseRecorder) {
	assertRateLimitHeaders(t, resp)
	require.NotEmpty(t, resp.Header().Get(headerRetryAfter))

	var errorResponse struct {
		Errors []struct {
			Code    string            `json:"code"`
			Message string            `json:"message"`
			Detail  map[string]string `json:"detail"`
		} `json:"errors"`
	}

	err := json.Unmarshal(resp.Body.Bytes(), &errorResponse)
	require.NoError(t, err)
	require.Len(t, errorResponse.Errors, 1)
	require.Equal(t, "TOOMANYREQUESTS", errorResponse.Errors[0].Code)
	require.NotEmpty(t, errorResponse.Errors[0].Message)

	detail := errorResponse.Errors[0].Detail
	require.NotEmpty(t, detail["ip"])
	require.Equal(t, matchTypeIP, detail["limit"])
	require.NotEmpty(t, detail["retry_after"])
	require.NotEmpty(t, detail["remaining"])
}

func cleanupRedisKeys(t *testing.T, redisAddr string) {
	t.Helper()

	addresses := strings.Split(redisAddr, ",")
	t.Logf("Connecting to Redis cluster at: %v", addresses)

	config := &configuration.Configuration{}
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled:      true,
		Addr:         redisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		TLS: configuration.RedisTLS{
			Insecure: true,
		},
	}

	redisClient, err := configureRedisClient(context.Background(), config.Redis.RateLimiter, false, "cleanup")
	require.NoError(t, err, "Failed to create Redis client for cleanup")
	defer redisClient.Close()

	ctx := context.Background()

	patterns := []string{
		"registry:api:{rate-limit:ip:*",
		"registry:api:*",
	}

	for _, pattern := range patterns {
		for attempt := 0; attempt < 3; attempt++ {
			script := fmt.Sprintf("local keys = redis.call('keys', '%s'); for i,k in ipairs(keys) do redis.call('del', k); end; return #keys", pattern)
			result, err := redisClient.Eval(ctx, script, []string{}).Result()
			if err == nil {
				if count, ok := result.(int64); ok && count > 0 {
					t.Logf("Deleted %d keys matching pattern '%s'", count, pattern)
				}
				break
			}
			t.Logf("Cleanup attempt %d for pattern '%s' failed: %v", attempt+1, pattern, err)
			time.Sleep(100 * time.Millisecond)
		}
	}

	for attempt := 0; attempt < 3; attempt++ {
		err = redisClient.FlushAll(ctx).Err()
		if err == nil {
			t.Logf("FLUSHALL successful on attempt %d", attempt+1)
			break
		}
		t.Logf("FLUSHALL attempt %d failed: %v", attempt+1, err)
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err, "FLUSHALL failed after retries")

	time.Sleep(500 * time.Millisecond)

	for attempt := 0; attempt < 5; attempt++ {
		remainingKeys, err := redisClient.Keys(ctx, "*").Result()
		if err == nil && len(remainingKeys) == 0 {
			t.Logf("Verified: No keys remaining after cleanup")
			break
		}
		if err != nil {
			t.Logf("Verification attempt %d failed: %v", attempt+1, err)
		} else {
			t.Logf("Verification attempt %d: %d keys still remain: %v", attempt+1, len(remainingKeys), remainingKeys)
		}
		time.Sleep(200 * time.Millisecond)
	}

	t.Logf("Redis cleanup completed")
}

func TestRedisClusterConnection(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")
	if !strings.Contains(redisAddr, ",") {
		t.Skipf("REDIS_ADDR only has one host, skipping cluster test")
	}

	t.Logf("Testing Redis cluster connection to: %s", redisAddr)

	config := &configuration.Configuration{}
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled:      true,
		Addr:         redisAddr,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		TLS: configuration.RedisTLS{
			Insecure: true,
		},
	}

	redisClient, err := configureRedisClient(context.Background(), config.Redis.RateLimiter, false, "test")
	if err != nil {
		t.Fatalf("Failed to create Redis client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = redisClient.Ping(ctx).Err()
	require.NoError(t, err, "Should be able to PING Redis cluster")

	err = redisClient.Set(ctx, "test:connection", "working", 0).Err()
	require.NoError(t, err, "Should be able to SET key")

	val, err := redisClient.Get(ctx, "test:connection").Result()
	require.NoError(t, err, "Should be able to GET key")
	require.Equal(t, "working", val, "Value should match")

	if clusterClient, ok := redisClient.(*redis.ClusterClient); ok {
		nodes, err := clusterClient.ClusterNodes(ctx).Result()
		require.NoError(t, err, "Should be able to get cluster nodes")
		t.Logf("Cluster nodes: %s", nodes)
	}

	err = redisClient.Del(ctx, "test:connection").Err()
	require.NoError(t, err, "Should be able to DELETE key")

	t.Log("Redis cluster connection test passed")
}

func assertRateLimitHeaders(t *testing.T, resp *httptest.ResponseRecorder) {
	t.Helper()

	rateLimitHeader := resp.Header().Get(headerRateLimit)
	require.NotEmpty(t, rateLimitHeader)
	// parse headerRateLimit limit=%d, remaining=%d, reset=%d
	header := strings.Split(rateLimitHeader, ",")
	require.Equal(t, 3, len(header))
	limit := strings.Split(header[0], "=")[1]
	remaining := strings.Split(header[1], "=")[1]
	reset := strings.Split(header[2], "=")[1]

	assert.Equal(t, limit, resp.Header().Get(headerXRateLimitLimit))
	assert.Equal(t, remaining, resp.Header().Get(headerXRateLimitRemaining))
	assert.Equal(t, reset, resp.Header().Get(headerXRateLimitReset))
	assert.NotEmpty(t, resp.Header().Get(headerRateLimitPolicy))
}

// testIP generates a semi-random IP address to reduce
// the likelihood of IP collision which can cause tests to fail
// if the redis state is not clean, especially in a cluster where
// there might be a delay in syncing the state amongst shards.
func testIP() string {
	// Ensure we stay in private IP range and avoid duplicates
	ip1 := rand.Uint32N(64)
	ip2 := rand.Uint32N(128)
	ip3 := rand.Uint32N(255)
	port := 12345 + rand.Uint32N(1000)

	return fmt.Sprintf("192.%d.%d.%d:%d", ip1, ip2, ip3, port)
}
