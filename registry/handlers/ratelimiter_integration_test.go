//go:build integration && redis_test

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
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
	"github.com/stretchr/testify/suite"
)

// RateLimiterTestSuite provides a common test suite for all rate limiter tests
type RateLimiterTestSuite struct {
	suite.Suite
	redisAddr   string
	redisClient redis.UniversalClient
	app         *App
	config      *configuration.Configuration
}

// SetupSuite runs once before all tests in the suite
func (s *RateLimiterTestSuite) SetupSuite() {
	s.redisAddr = os.Getenv("REDIS_ADDR")
	s.Require().NotEmpty(s.redisAddr, "REDIS_ADDR environment variable must be set")

	s.T().Logf("Setting up test suite with Redis at: %s", s.redisAddr)
}

// SetupTest runs before each individual test
func (s *RateLimiterTestSuite) SetupTest() {
	s.T().Logf("Setting up test: %s", s.T().Name())
	s.cleanupRedisKeys()
}

// TearDownTest runs after each individual test
func (s *RateLimiterTestSuite) TearDownTest() {
	s.T().Logf("Tearing down test: %s", s.T().Name())

	if s.redisClient != nil {
		s.redisClient.Close()
		s.redisClient = nil
	}
}

// TearDownSuite runs once after all tests in the suite
func (s *RateLimiterTestSuite) TearDownSuite() {
	s.T().Logf("Tearing down test suite")
	s.cleanupRedisKeys()
}

// setupConfig creates a configuration with the given limiters
func (s *RateLimiterTestSuite) setupConfig(limiters []configuration.Limiter) *configuration.Configuration {
	config := &configuration.Configuration{
		RateLimiter: configuration.RateLimiter{
			Enabled:  true,
			Limiters: limiters,
		},
		Redis: configuration.Redis{
			RateLimiter: configuration.RedisCommon{
				Enabled:     true,
				Addr:        s.redisAddr,
				DialTimeout: 2 * time.Second,
			},
		},
	}
	return config
}

// setupApp creates an App instance with the given configuration
func (s *RateLimiterTestSuite) setupApp(config *configuration.Configuration) *App {
	ctx := context.Background()
	app := &App{
		Context: ctx,
		Config:  config,
	}

	err := app.configureRedisRateLimiter(ctx, config)
	s.Require().NoError(err)

	s.app = app
	s.config = config

	s.redisClient, err = configureRedisClient(ctx, config.Redis.RateLimiter, false, "test-suite")
	s.Require().NoError(err, "Failed to create test Redis client")

	return app
}

// createTestRequest creates a test HTTP request with the given remote address
func (s *RateLimiterTestSuite) createTestRequest(remoteAddr string) (*http.Request, *httptest.ResponseRecorder) {
	req, err := http.NewRequest("GET", "/v2/", nil)
	s.Require().NoError(err)
	req.RemoteAddr = remoteAddr
	return req, httptest.NewRecorder()
}

// createMiddlewareHandler creates a test handler with rate limiting middleware
func (s *RateLimiterTestSuite) createMiddlewareHandler(app *App) http.Handler {
	successHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		msg := []byte("success")
		n, err := w.Write(msg)
		s.NoError(err)
		s.Equal(len(msg), n)
	})
	return app.rateLimiterMiddleware(successHandler)
}

// testIP generates a semi-random IP address to reduce collision likelihood
func (*RateLimiterTestSuite) testIP() string {
	ip1 := rand.Uint32N(64)
	ip2 := rand.Uint32N(128)
	ip3 := rand.Uint32N(255)
	port := 12345 + rand.Uint32N(1000)
	return fmt.Sprintf("192.%d.%d.%d:%d", ip1, ip2, ip3, port)
}

// assertRateLimitHeaders verifies that all expected rate limit headers are present
func (s *RateLimiterTestSuite) assertRateLimitHeaders(resp *httptest.ResponseRecorder) {
	rateLimitHeader := resp.Header().Get(headerRateLimit)
	s.Require().NotEmpty(rateLimitHeader)

	// Parse headerRateLimit: limit=%d, remaining=%d, reset=%d
	header := strings.Split(rateLimitHeader, ",")
	s.Require().Len(header, 3)

	limit := strings.Split(header[0], "=")[1]
	remaining := strings.Split(header[1], "=")[1]
	reset := strings.Split(header[2], "=")[1]

	s.Equal(limit, resp.Header().Get(headerXRateLimitLimit))
	s.Equal(remaining, resp.Header().Get(headerXRateLimitRemaining))
	s.Equal(reset, resp.Header().Get(headerXRateLimitReset))
	s.NotEmpty(resp.Header().Get(headerRateLimitPolicy))
}

// verifyRateLimitResponse checks the response when rate limit is exceeded
func (s *RateLimiterTestSuite) verifyRateLimitResponse(resp *httptest.ResponseRecorder) {
	s.assertRateLimitHeaders(resp)
	s.NotEmpty(resp.Header().Get(headerRetryAfter))

	var errorResponse struct {
		Errors []struct {
			Code    string            `json:"code"`
			Message string            `json:"message"`
			Detail  map[string]string `json:"detail"`
		} `json:"errors"`
	}

	err := json.Unmarshal(resp.Body.Bytes(), &errorResponse)
	s.Require().NoError(err)
	s.Require().Len(errorResponse.Errors, 1)
	s.Equal("TOOMANYREQUESTS", errorResponse.Errors[0].Code)
	s.NotEmpty(errorResponse.Errors[0].Message)

	detail := errorResponse.Errors[0].Detail
	s.NotEmpty(detail["ip"])
	s.Equal(matchTypeIP, detail["limit"])
	s.NotEmpty(detail["retry_after"])
	s.NotEmpty(detail["remaining"])
}

// cleanupRedisKeys removes all test keys from Redis
func (s *RateLimiterTestSuite) cleanupRedisKeys() {
	addresses := strings.Split(s.redisAddr, ",")
	s.T().Logf("Cleaning up Redis keys at: %v", addresses)

	config := &configuration.Configuration{}
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled:      true,
		Addr:         s.redisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		TLS: configuration.RedisTLS{
			Insecure: true,
		},
	}

	redisClient, err := configureRedisClient(context.Background(), config.Redis.RateLimiter, false, "cleanup")
	s.Require().NoError(err, "Failed to create Redis client for cleanup")
	defer redisClient.Close()

	ctx := context.Background()
	s.Require().EventuallyWithT(
		func(tt *assert.CollectT) {
			err := redisClient.FlushAll(ctx).Err()
			assert.NoError(tt, err)
		},
		5*time.Second,
		500*time.Millisecond,
		"flushing Redis databases has failed")

	remainingKeys, err := redisClient.Keys(ctx, "*").Result()
	s.Require().NoError(err)
	s.Require().Empty(remainingKeys)

	s.T().Logf("Redis cleanup completed")
}

func (s *RateLimiterTestSuite) TestBlocksRequestsWhenLimitExceeded() {
	limiter := configuration.Limiter{
		Name:        "block-limiter",
		Description: "Blocking rate limiter",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 3, Period: "second", Burst: 6},
		Action:      configuration.Action{WarnThreshold: 0.7, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// Should allow 6 burst requests
	for i := 0; i < 6; i++ {
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusOK, resp.Code, "Request %d should be allowed", i+1)
		s.NotEmpty(resp.Header().Get(headerXRateLimitRemaining))

		remaining, _ := strconv.Atoi(resp.Header().Get(headerXRateLimitRemaining))
		s.Equal(6-i-1, remaining, "Remaining should be %d after request %d", 6-i-1, i+1)

		resp = httptest.NewRecorder()
	}

	// 7th request should be blocked
	handler.ServeHTTP(resp, req)
	s.Equal(http.StatusTooManyRequests, resp.Code, "7th request should be blocked")
	s.verifyRateLimitResponse(resp)
}

func (s *RateLimiterTestSuite) TestLogOnlyModeNeverBlocks() {
	limiter := configuration.Limiter{
		Name:        "log-only-limiter",
		Description: "Log-only rate limiter",
		LogOnly:     true,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 3, Period: "second", Burst: 6},
		Action:      configuration.Action{WarnThreshold: 0.7, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// Should never block even after exceeding limit
	for i := 0; i < 8; i++ {
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusOK, resp.Code)
		s.assertRateLimitHeaders(resp)
		resp = httptest.NewRecorder()
	}
}

func (s *RateLimiterTestSuite) TestRateLimitRemainingHeaderDecreasesCorrectly() {
	limiter := configuration.Limiter{
		Name:        "header-test-limiter",
		Description: "Rate limiter for header testing",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 15, Period: "minute", Burst: 30},
		Action:      configuration.Action{WarnThreshold: 0.8, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// Track expected remaining values (global burst = 30)
	expectedRemaining := make([]int, 30)
	for i := 0; i < 30; i++ {
		expectedRemaining[i] = 29 - i
	}

	// Test burst capacity - should consume from burst first
	for i := 0; i < 30; i++ {
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusOK, resp.Code, "Request %d should be allowed", i+1)

		remainingHeader := resp.Header().Get(headerXRateLimitRemaining)
		s.NotEmpty(remainingHeader, "X-RateLimit-Remaining header should be present for request %d", i+1)

		remaining, err := strconv.Atoi(remainingHeader)
		s.Require().NoError(err, "X-RateLimit-Remaining should be a valid integer for request %d", i+1)
		s.Equal(expectedRemaining[i], remaining,
			"X-RateLimit-Remaining should be %d for request %d", expectedRemaining[i], i+1)
		s.assertRateLimitHeaders(resp)

		resp = httptest.NewRecorder()
	}

	// 31st request should be blocked (burst exhausted)
	handler.ServeHTTP(resp, req)
	s.Equal(http.StatusTooManyRequests, resp.Code, "31st request should be blocked")
	s.verifyRateLimitResponse(resp)
}

func (s *RateLimiterTestSuite) TestDifferentIPsHaveIndependentCounters() {
	testIPs := []string{s.testIP(), s.testIP(), s.testIP()}
	s.T().Logf("Using test IPs: %v", testIPs)

	limiter := configuration.Limiter{
		Name:        "multi-ip-limiter",
		Description: "Rate limiter for testing multiple IPs",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 6, Period: "minute", Burst: 9},
		Action:      configuration.Action{WarnThreshold: 0.5, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	// Test different IPs should have independent counters
	for ipIndex, testIP := range testIPs {
		s.T().Logf("=== Testing IP %d: %s ===", ipIndex+1, testIP)

		req, resp := s.createTestRequest(testIP)

		// Each IP should start with global burst capacity (9)
		expectedRemaining := []int{8, 7, 6, 5, 4, 3, 2, 1, 0}
		for i := 0; i < 9; i++ {
			handler.ServeHTTP(resp, req)
			s.Equal(http.StatusOK, resp.Code, "Request %d from IP %s should be allowed", i+1, testIP)

			remainingHeader := resp.Header().Get(headerXRateLimitRemaining)
			s.NotEmpty(remainingHeader, "X-RateLimit-Remaining header should be present")

			remaining, err := strconv.Atoi(remainingHeader)
			s.Require().NoError(err, "X-RateLimit-Remaining should be a valid integer")
			s.Equal(expectedRemaining[i], remaining, "X-RateLimit-Remaining should be %d for request %d from IP %s", expectedRemaining[i], i+1, testIP)

			s.T().Logf("IP %s, Request %d: X-RateLimit-Remaining = %d", testIP, i+1, remaining)

			resp = httptest.NewRecorder()
		}

		// 10th request from this IP should be blocked
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusTooManyRequests, resp.Code, "10th request from IP %s should be blocked", testIP)

		remainingHeader := resp.Header().Get(headerXRateLimitRemaining)
		remaining, err := strconv.Atoi(remainingHeader)
		s.Require().NoError(err)
		s.Zero(remaining, "X-RateLimit-Remaining should be 0 when blocked for IP %s", testIP)
	}
}

func (s *RateLimiterTestSuite) TestPerIPRateLimitingAcrossShards() {
	limiter := configuration.Limiter{
		Name:        "per-ip-limiter",
		Description: "Test per-IP rate limiting across shards",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 3, Period: "minute", Burst: 9},
		Action:      configuration.Action{WarnThreshold: 0.8, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	// Use IPs that hash to different Redis shards
	crossShardIPs := []string{s.testIP(), s.testIP(), s.testIP()}

	// Each IP should be allowed its full global burst (9 requests each)
	for _, testIP := range crossShardIPs {
		s.T().Logf("Testing IP: %s", testIP)

		// First 9 requests should be allowed (within global burst)
		for i := 0; i < 9; i++ {
			req, resp := s.createTestRequest(testIP)
			handler.ServeHTTP(resp, req)

			s.Equal(http.StatusOK, resp.Code,
				"Request %d from IP %s should be allowed (within burst of 9)",
				i+1, testIP)

			s.T().Logf("Request %d from IP %s: Status=%d", i+1, testIP, resp.Code)
		}

		// 10th request from same IP should be blocked
		req, resp := s.createTestRequest(testIP)
		handler.ServeHTTP(resp, req)

		s.Equal(http.StatusTooManyRequests, resp.Code,
			"10th request from IP %s should be blocked (burst exceeded)", testIP)

		s.T().Logf("10th request from IP %s: Status=%d (correctly blocked)", testIP, resp.Code)
	}
}

// Test suite for multiple limiters
type MultipleLimitersTestSuite struct {
	RateLimiterTestSuite
}

func (s *MultipleLimitersTestSuite) TestLimiterPrecedenceAffectsProcessingOrder() {
	limiters := []configuration.Limiter{
		{
			Name:        "high-precedence",
			Description: "High precedence limiter",
			LogOnly:     true,
			Match:       configuration.Match{Type: "ip"},
			Precedence:  1,
			Limit:       configuration.Limit{Rate: 60, Period: "second", Burst: 60},
			Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "log"},
		},
		{
			Name:        "low-precedence",
			Description: "Low precedence limiter",
			LogOnly:     true,
			Match:       configuration.Match{Type: "ip"},
			Precedence:  2,
			Limit:       configuration.Limit{Rate: 30, Period: "second", Burst: 30},
			Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "log"},
		},
	}

	config := s.setupConfig(limiters)
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// Make a single request to verify both limiters process
	handler.ServeHTTP(resp, req)

	// Since both are log-only, request should succeed
	s.Equal(http.StatusOK, resp.Code, "Request should succeed with log-only limiters")
}

// Additional test suites for specific functionality

type BasicFunctionalityTestSuite struct {
	RateLimiterTestSuite
}

func (s *BasicFunctionalityTestSuite) TestBasicFunctionality() {
	limiter := configuration.Limiter{
		Name:        "basic-test",
		Description: "Basic functionality test",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 6, Period: "second", Burst: 12},
		Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// Should be able to make 12 requests (global burst capacity)
	for i := 1; i <= 12; i++ {
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusOK, resp.Code, "Request %d should be allowed", i)
		s.NotEmpty(resp.Header().Get(headerRateLimit))
		s.NotEmpty(resp.Header().Get(headerRateLimitPolicy))
		s.NotEmpty(resp.Header().Get(headerXRateLimitLimit))
		s.NotEmpty(resp.Header().Get(headerXRateLimitRemaining))
		s.NotEmpty(resp.Header().Get(headerXRateLimitReset))

		resp = httptest.NewRecorder()
	}

	// 13th request should be blocked
	handler.ServeHTTP(resp, req)
	s.Equal(http.StatusTooManyRequests, resp.Code, "13th request should be blocked")

	remainingHeader, err := strconv.Atoi(resp.Header().Get(headerXRateLimitRemaining))
	s.Require().NoError(err)
	s.Zero(remainingHeader, "Remaining header should be zero, got: %d", remainingHeader)
}

type ThresholdTestSuite struct {
	RateLimiterTestSuite
}

func (s *ThresholdTestSuite) TestThresholdCalculation() {
	limiter := configuration.Limiter{
		Name:        "threshold-calc-test",
		Description: "Threshold calculation test",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 15, Period: "second", Burst: 30},
		Action:      configuration.Action{WarnThreshold: 0.6, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// With global burst=30 and threshold=0.6, warning should trigger at 18th request
	for i := 1; i <= 17; i++ {
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusOK, resp.Code, "Request %d should be allowed", i)
		resp = httptest.NewRecorder()
	}

	// 18th request should trigger warning (60% of 30 = 18)
	handler.ServeHTTP(resp, req)
	s.Equal(http.StatusOK, resp.Code, "18th request should be allowed")
}

func (s *ThresholdTestSuite) TestZeroThreshold() {
	limiter := configuration.Limiter{
		Name:        "zero-threshold",
		Description: "Zero threshold test limiter",
		LogOnly:     false,
		Match:       configuration.Match{Type: matchTypeIP},
		Precedence:  10,
		Limit:       configuration.Limit{Rate: 6, Period: "second", Burst: 12},
		Action:      configuration.Action{WarnThreshold: 0.0, WarnAction: "log", HardAction: "block"},
	}

	config := s.setupConfig([]configuration.Limiter{limiter})
	app := s.setupApp(config)
	handler := s.createMiddlewareHandler(app)

	req, resp := s.createTestRequest(s.testIP())

	// Should be able to make 12 requests before being blocked
	for i := 0; i < 12; i++ {
		handler.ServeHTTP(resp, req)
		s.Equal(http.StatusOK, resp.Code, "Request %d should be allowed", i+1)
		s.assertRateLimitHeaders(resp)
		resp = httptest.NewRecorder()
	}

	// 13th request should be blocked
	handler.ServeHTTP(resp, req)
	s.Equal(http.StatusTooManyRequests, resp.Code, "13th request should be blocked")
}

// Redis connection test suite
type RedisConnectionTestSuite struct {
	RateLimiterTestSuite
}

func (s *RedisConnectionTestSuite) TestRedisClusterConnection() {
	if !strings.Contains(s.redisAddr, ",") {
		s.T().Skipf("REDIS_ADDR only has one host, skipping cluster test")
	}

	s.T().Logf("Testing Redis cluster connection to: %s", s.redisAddr)

	config := &configuration.Configuration{}
	config.Redis.RateLimiter = configuration.RedisCommon{
		Enabled:      true,
		Addr:         s.redisAddr,
		DialTimeout:  10 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		TLS: configuration.RedisTLS{
			Insecure: true,
		},
	}

	redisClient, err := configureRedisClient(context.Background(), config.Redis.RateLimiter, false, "test")
	s.Require().NoError(err, "Failed to create Redis client")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = redisClient.Ping(ctx).Err()
	s.Require().NoError(err, "Should be able to PING Redis cluster")

	err = redisClient.Set(ctx, "test:connection", "working", 0).Err()
	s.Require().NoError(err, "Should be able to SET key")

	val, err := redisClient.Get(ctx, "test:connection").Result()
	s.Require().NoError(err, "Should be able to GET key")
	s.Equal("working", val, "Value should match")

	if clusterClient, ok := redisClient.(*redis.ClusterClient); ok {
		nodes, err := clusterClient.ClusterNodes(ctx).Result()
		s.Require().NoError(err, "Should be able to get cluster nodes")
		s.T().Logf("Cluster nodes: %s", nodes)
	}

	err = redisClient.Del(ctx, "test:connection").Err()
	s.Require().NoError(err, "Should be able to DELETE key")

	s.T().Log("Redis cluster connection test passed")
}

// Test runner functions
func TestRateLimiterBasicFunctionality(t *testing.T) {
	suite.Run(t, new(RateLimiterTestSuite))
}

func TestRateLimiterMultipleLimiters(t *testing.T) {
	suite.Run(t, new(MultipleLimitersTestSuite))
}

func TestRateLimitersBasicWithCleanup(t *testing.T) {
	suite.Run(t, new(BasicFunctionalityTestSuite))
}

func TestRateLimitersThresholdCalculation(t *testing.T) {
	suite.Run(t, new(ThresholdTestSuite))
}

func TestRedisClusterConnection(t *testing.T) {
	suite.Run(t, new(RedisConnectionTestSuite))
}
