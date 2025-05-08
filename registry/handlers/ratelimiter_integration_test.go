//go:build integration && redis_test

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/docker/distribution/configuration"

	"github.com/stretchr/testify/require"
)

func TestRateLimiterBasicFunctionality(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Run("blocks requests when limit exceeded", func(t *testing.T) {
		cleanupRedisKeys(t, redisAddr)

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
						Limit:       configuration.Limit{Rate: 1, Period: "second", Burst: 2},
						Action:      configuration.Action{WarnThreshold: 0.7, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest("192.168.1.1:12345")
		handler := createMiddlewareHandler(app)

		// Should allow burst requests
		for i := 0; i < 2; i++ {
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)
			require.NotEmpty(t, resp.Header().Get(headerXRateLimitRemaining))
			resp = httptest.NewRecorder()
		}

		// Should block when limit exceeded
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusTooManyRequests, resp.Code)
		verifyRateLimitResponse(t, resp)
	})

	t.Run("log_only mode never blocks", func(t *testing.T) {
		cleanupRedisKeys(t, redisAddr)

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
						Limit:       configuration.Limit{Rate: 1, Period: "second", Burst: 2},
						Action:      configuration.Action{WarnThreshold: 0.7, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp := createTestRequest("192.168.1.2:12345")
		handler := createMiddlewareHandler(app)

		// Should never block even after exceeding limit
		for i := 0; i < 5; i++ {
			handler.ServeHTTP(resp, req)
			require.Equal(t, http.StatusOK, resp.Code)

			if i >= 2 {
				require.NotEmpty(t, resp.Header().Get(headerXRateLimitWarning),
					"Warning header should be set after threshold")
				require.NotEmpty(t, resp.Header().Get(headerXRateLimitPercentage),
					"Percentage header should be set after threshold")
			}

			resp = httptest.NewRecorder()
		}
	})
}

func TestMultipleLimiters(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	t.Run("limiter precedence", func(t *testing.T) {
		cleanupRedisKeys(t, redisAddr)

		config := &configuration.Configuration{
			RateLimiter: configuration.RateLimiter{
				Enabled: true,
				Limiters: []configuration.Limiter{
					{
						Name:        "high-precedence-strict",
						Description: "High precedence strict limiter",
						LogOnly:     false,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  10,
						Limit:       configuration.Limit{Rate: 1, Period: "second", Burst: 2},
						Action:      configuration.Action{WarnThreshold: 0.5, WarnAction: "log", HardAction: "block"},
					},
					{
						Name:        "low-precedence-lenient",
						Description: "Low precedence lenient limiter",
						LogOnly:     false,
						Match:       configuration.Match{Type: matchTypeIP},
						Precedence:  20,
						Limit:       configuration.Limit{Rate: 10, Period: "second", Burst: 15},
						Action:      configuration.Action{WarnThreshold: 0.8, WarnAction: "log", HardAction: "block"},
					},
				},
			},
		}

		setupRedisConfig(config, redisAddr)
		app, err := setupAppWithRateLimiter(t, config)
		require.NoError(t, err)
		require.Len(t, app.rateLimiters, 2)

		req, resp := createTestRequest("192.168.200.1:12345")
		handler := createMiddlewareHandler(app)

		// Make 5 requests - should be blocked by strict limiter before reaching lenient limiter
		var requestCount int
		for i := 0; i < 5; i++ {
			handler.ServeHTTP(resp, req)
			requestCount++

			if resp.Code == http.StatusTooManyRequests {
				break
			}

			resp = httptest.NewRecorder()
		}

		// Should be limited by first limiter (max requests = burst + 1)
		require.LessOrEqual(t, requestCount, 3, "Should be limited by strict limiter")
		require.Equal(t, http.StatusTooManyRequests, resp.Code)

		// Now test with log_only setting
		cleanupRedisKeys(t, redisAddr)

		// Set first limiter to log_only
		config.RateLimiter.Limiters[0].LogOnly = true

		setupRedisConfig(config, redisAddr)
		app, err = setupAppWithRateLimiter(t, config)
		require.NoError(t, err)

		req, resp = createTestRequest("192.168.200.2:12345")
		handler = createMiddlewareHandler(app)

		// With log_only=true for first limiter, we should hit limits of second limiter
		requestCount = 0
		for i := 0; i < 20; i++ {
			handler.ServeHTTP(resp, req)
			requestCount++

			if resp.Code == http.StatusTooManyRequests {
				break
			}

			resp = httptest.NewRecorder()
		}

		// Should make more requests before being limited by second limiter
		require.GreaterOrEqual(t, requestCount, 3,
			"Should have made more requests before being limited by second limiter")
		require.Equal(t, http.StatusTooManyRequests, resp.Code)
	})
}

func TestWarningThresholds(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	cleanupRedisKeys(t, redisAddr)

	config := &configuration.Configuration{
		RateLimiter: configuration.RateLimiter{
			Enabled: true,
			Limiters: []configuration.Limiter{
				{
					Name:        "warning-test",
					Description: "Warning threshold test limiter",
					LogOnly:     false,
					Match:       configuration.Match{Type: matchTypeIP},
					Precedence:  10,
					Limit:       configuration.Limit{Rate: 2, Period: "second", Burst: 4},
					Action:      configuration.Action{WarnThreshold: 0.5, WarnAction: "log", HardAction: "block"},
				},
			},
		},
	}

	setupRedisConfig(config, redisAddr)
	app, err := setupAppWithRateLimiter(t, config)
	require.NoError(t, err)

	req, resp := createTestRequest("192.168.10.1:12345")
	handler := createMiddlewareHandler(app)

	// First request should not trigger warning
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	require.Empty(t, resp.Header().Get(headerXRateLimitWarning))
	require.Empty(t, resp.Header().Get(headerXRateLimitPercentage))

	// Second request should trigger warning
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code)
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitWarning),
		"Warning header should be set when threshold reached")
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitPercentage),
		"Percentage header should be set when threshold reached")

	// Keep making requests until rate limited
	for i := 0; i < 5; i++ {
		resp = httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code == http.StatusTooManyRequests {
			break
		}
	}

	// Verify rate limited response
	require.Equal(t, http.StatusTooManyRequests, resp.Code)
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitRemaining))
	require.NotEmpty(t, resp.Header().Get(headerRetryAfter))
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitWarning))
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitPercentage))
}

func TestSpecialCaseZeroThreshold(t *testing.T) {
	redisAddr := os.Getenv("REDIS_ADDR")
	require.NotEmpty(t, redisAddr, "REDIS_ADDR environment variable must be set")

	cleanupRedisKeys(t, redisAddr)

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
					Limit:       configuration.Limit{Rate: 2, Period: "second", Burst: 3},
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

	req, resp := createTestRequest("192.168.20.1:12345")
	handler := createMiddlewareHandler(app)

	// No requests should trigger warning before limit
	for i := 0; i < 3; i++ {
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code)
		require.Empty(t, resp.Header().Get(headerXRateLimitWarning),
			"Warning header should not be set with threshold=0.0")
		require.Empty(t, resp.Header().Get(headerXRateLimitPercentage),
			"Percentage header should not be set with threshold=0.0")
		resp = httptest.NewRecorder()
	}

	// Once limited, should set warning
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusTooManyRequests, resp.Code)
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitWarning),
		"Warning header should be set when rate limited even with threshold=0.0")
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitPercentage),
		"Percentage header should be set when rate limited even with threshold=0.0")
}

func setupRedisConfig(config *configuration.Configuration, redisAddr string) {
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled: true,
		Addr:    redisAddr,
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

	// Configure Redis client for rate limiter
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
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitRemaining))
	require.NotEmpty(t, resp.Header().Get(headerRetryAfter))
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitWarning),
		"X-RateLimit-Warning should be set when rate limited")
	require.NotEmpty(t, resp.Header().Get(headerXRateLimitPercentage),
		"X-RateLimit-Percentage should be set when rate limited")

	// Verify the error response contains the expected fields
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

	// Verify the detail field contains the expected information
	detail := errorResponse.Errors[0].Detail
	require.NotEmpty(t, detail["ip"])
	require.Equal(t, matchTypeIP, detail["limit"])
	require.NotEmpty(t, detail["retry_after"])
	require.NotEmpty(t, detail["remaining"])
}

func cleanupRedisKeys(t *testing.T, redisAddr string) {
	t.Helper()

	// Setup redis client directly
	config := &configuration.Configuration{}
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled: true,
		Addr:    redisAddr,
		TLS: configuration.RedisTLS{
			Insecure: true,
		},
	}

	redisClient, err := configureRedisClient(context.Background(), config.Redis.RateLimiter, false, "test")
	require.NoError(t, err)

	// Use pattern to delete all rate limit keys
	err = redisClient.Eval(context.Background(),
		"local keys = redis.call('keys', 'registry:api:{rate-limit:*'); for i,k in ipairs(keys) do redis.call('del', k); end; return #keys",
		[]string{}).Err()
	require.NoError(t, err)
}
