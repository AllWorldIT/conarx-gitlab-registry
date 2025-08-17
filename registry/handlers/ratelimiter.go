package handlers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/docker/distribution/configuration"
	dcontext "github.com/docker/distribution/context"
	"github.com/docker/distribution/registry/api/errcode"
	"github.com/docker/distribution/registry/ratelimiter"
	"github.com/hashicorp/go-multierror"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

const (
	componentKey  = "component"
	componentName = "registry.rate_limiter"

	matchTypeIP    = "ip"
	matchTypeIPKey = `registry:api:{rate-limit:ip:%s}`

	// Draft IETF header definitions (draft-ietf-httpapi-ratelimit-headers-07)
	// https://www.ietf.org/archive/id/draft-ietf-httpapi-ratelimit-headers-07.html
	headerRateLimit       = "RateLimit"
	headerRateLimitFormat = "limit=%d, remaining=%d, reset=%d"
	headerRateLimitPolicy = "RateLimit-Policy"
	// Policy format example: 100;w=60 means 100 requests in a 60 seconds window
	headerRateLimitPolicyFormat = "%d;w=%d"
	headerRetryAfter            = "Retry-After"
	// Legacy headers X-RateLimit-* headers
	headerXRateLimitLimit     = "X-RateLimit-Limit"
	headerXRateLimitRemaining = "X-RateLimit-Remaining"
	headerXRateLimitReset     = "X-RateLimit-Reset"

	IPV6PrefixLength = 64
)

var (
	validMatchTypes  = []string{matchTypeIP}
	validWarnActions = []string{"none", "log"}
	validHardActions = []string{"none", "log", "block"}
	validPeriods     = []string{"second", "minute", "hour"}
)

func (app *App) configureRateLimiters(redisClient redis.UniversalClient, config *configuration.RateLimiter) error {
	l := dcontext.GetLogger(app.Context)

	orderedLimiters, err := parseLimitersConfig(config)
	if err != nil {
		return err
	}

	if len(orderedLimiters) == 0 {
		return nil
	}

	app.rateLimiters = make([]RateLimiter, 0, len(orderedLimiters))
	for name, orderedLimiter := range orderedLimiters {
		cfg := orderedLimiter.Limiter
		limiter := ratelimiter.New(redisClient, cfg)
		l.WithFields(logrus.Fields{
			"name":           name,
			"description":    cfg.Description,
			"log_only":       cfg.LogOnly,
			"match_type":     cfg.Match.Type,
			"rate":           cfg.Limit.Rate,
			"burst":          cfg.Limit.Burst,
			"period":         cfg.Limit.Period,
			"warn_threshold": cfg.Action.WarnThreshold,
			"warn_action":    cfg.Action.WarnAction,
			"hard_action":    cfg.Action.HardAction,
		}).Info("Configured rate limiter")

		app.rateLimiters = append(app.rateLimiters, limiter)
	}
	return nil
}

// OrderedLimiter is a helper struct to sort limiters by precedence
type OrderedLimiter struct {
	Name    string
	Limiter *configuration.Limiter
}

func parseLimitersConfig(rateLimiterCfg *configuration.RateLimiter) ([]OrderedLimiter, error) {
	if !rateLimiterCfg.Enabled {
		return nil, nil
	}

	limiters := make(map[string]*configuration.Limiter)
	keys := make([]string, 0, len(rateLimiterCfg.Limiters))
	mError := new(multierror.Error)
	for _, limiterConfig := range rateLimiterCfg.Limiters {
		err := validateLimiter(&limiterConfig)
		if err != nil {
			mError = multierror.Append(mError, err)
			continue
		}
		keys = append(keys, limiterConfig.Name)
		limiters[limiterConfig.Name] = &limiterConfig
	}

	if len(mError.Errors) > 0 {
		return nil, mError.ErrorOrNil()
	}

	// sort limiters by precedence in ascending order
	sort.Slice(keys, func(i, j int) bool { return limiters[keys[i]].Precedence < limiters[keys[j]].Precedence })

	// Create a slice of OrderedLimiter
	orderedLimiters := make([]OrderedLimiter, 0, len(keys))
	for _, key := range keys {
		orderedLimiters = append(orderedLimiters, OrderedLimiter{
			Name:    key,
			Limiter: limiters[key],
		})
	}

	return orderedLimiters, nil
}

func validateLimiter(c *configuration.Limiter) error {
	mError := new(multierror.Error)
	if c.Name == "" {
		mError = multierror.Append(mError, fmt.Errorf("limiter name cannot be empty"))
	}
	if c.Precedence <= 0 {
		mError = multierror.Append(mError, fmt.Errorf("limiter precedence must be a positive integer"))
	}

	c.Match.Type = strings.TrimSpace(strings.ToLower(c.Match.Type))
	if !slices.Contains(validMatchTypes, c.Match.Type) {
		mError = multierror.Append(mError, fmt.Errorf("match.type must be one of: %+v", validMatchTypes))
	}

	if c.Limit.Rate <= 0 {
		mError = multierror.Append(mError, fmt.Errorf("rate must be a positive integer"))
	}
	if c.Limit.Burst <= 0 {
		mError = multierror.Append(mError, fmt.Errorf("burst must be a positive integer"))
	}
	if !slices.Contains(validPeriods, c.Limit.Period) {
		mError = multierror.Append(mError, fmt.Errorf("period must be one of: %+v", validPeriods))
	} else {
		switch c.Limit.Period {
		case "second":
			c.Limit.PeriodDuration = time.Second
		case "minute":
			c.Limit.PeriodDuration = time.Minute
		case "hour":
			c.Limit.PeriodDuration = time.Hour
		default:
			c.Limit.PeriodDuration = time.Second
		}
	}

	if !slices.Contains(validWarnActions, c.Action.WarnAction) {
		mError = multierror.Append(mError, fmt.Errorf("action.warn_action must be one of: %+v", validWarnActions))
	}

	if c.Action.WarnThreshold < 0.0 || c.Action.WarnThreshold > 1.0 {
		mError = multierror.Append(mError, fmt.Errorf("action.warn_threshold must be between 0.0 and 1.0"))
	}

	if !slices.Contains(validHardActions, c.Action.HardAction) {
		mError = multierror.Append(mError, fmt.Errorf("action.hard_action must be one of: %+v", validHardActions))
	}

	return mError.ErrorOrNil()
}

// RateLimiter represents a rate limiter that can be used to control the rate of requests.
type RateLimiter interface {
	// Allowed checks if a request is allowed based on the given key and limit.
	// Returns true if the request is allowed, false otherwise.
	Allowed(ctx context.Context, key string) (*ratelimiter.Result, error)
	Config() *configuration.Limiter
}

func (app *App) rateLimiterMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(app.rateLimiters) == 0 {
			next.ServeHTTP(w, r)
			return
		}

		l := dcontext.GetLogger(r.Context())
		ctx := app.context(w, r)

		// Process each limiter in order of precedence
		for _, limiter := range app.rateLimiters {
			blocked := processLimiter(ctx, w, r, limiter, l)
			if blocked {
				return // Request was blocked, don't continue to next limiter or handler
			}
		}

		next.ServeHTTP(w, r)
	})
}

// processLimiter handles a single rate limiter for a request
// Returns true if the request was blocked
func processLimiter(ctx *Context, w http.ResponseWriter, r *http.Request, limiter RateLimiter, l dcontext.Logger) bool {
	cfg := limiter.Config()

	key, ok := getRateLimitKey(r, cfg.Match.Type, l)
	if !ok {
		return false
	}

	result, err := limiter.Allowed(r.Context(), key)
	if err != nil {
		serveErrorJSON(w, err, ctx, l)
		return true // Block the request on error
	}

	checkWarningThreshold(result, cfg, l)
	writeRateLimiterHeaders(w, result, limiter)

	// Check if rate limit exceeded
	if result.Allowed <= 0 {
		logRateLimitedRequest(ctx, r, "request blocked: rate limit exceeded", cfg, result)

		if !cfg.LogOnly && cfg.Action.HardAction == "block" {
			blockRateLimitedRequest(w, r, result, cfg.Match.Type, ctx, l)
			return true
		}
	}

	return false
}

// writeRateLimiterHeaders writes the appropriate headers based on the result of the rate limiter
// it writes to the headerRateLimit, headerRateLimitPolicy and headerXRateLimit* headers
func writeRateLimiterHeaders(w http.ResponseWriter, result *ratelimiter.Result, rateLimiter RateLimiter) {
	cfg := rateLimiter.Config()

	// Set legacy X-RateLimit-* headers for backward compatibility
	w.Header().Set(headerXRateLimitLimit, fmt.Sprintf("%d", cfg.Limit.Rate))
	w.Header().Set(headerXRateLimitRemaining, fmt.Sprintf("%d", result.Remaining))
	w.Header().Set(headerXRateLimitReset, fmt.Sprintf("%d", result.Reset))

	// Set draft IETF RateLimit header (draft-ietf-httpapi-ratelimit-headers-07)
	// Format: limit=<limit>, remaining=<remaining>, reset=<reset>
	w.Header().Set(headerRateLimit, fmt.Sprintf(headerRateLimitFormat,
		cfg.Limit.Rate, result.Remaining, result.Reset))

	// Set RateLimit-Policy header
	// Format: <limit>;w=<window_in_seconds>
	windowSeconds := int(cfg.Limit.PeriodDuration.Seconds())
	w.Header().Set(headerRateLimitPolicy, fmt.Sprintf(headerRateLimitPolicyFormat,
		cfg.Limit.Rate, windowSeconds))

	// Set Retry-After header if rate limit exceeded
	if result.Allowed <= 0 && result.RetryAfter > 0 {
		w.Header().Set(headerRetryAfter, fmt.Sprintf("%.0f", result.RetryAfter.Seconds()))
	}
}

// getRateLimitKey determines the key to use for rate limiting based on match type
func getRateLimitKey(r *http.Request, matchType string, l dcontext.Logger) (string, bool) {
	switch matchType {
	case matchTypeIP:
		return fmt.Sprintf(matchTypeIPKey, encodeIPBase64(GetIPV4orIPV6Prefix(r.RemoteAddr))), true
	default:
		l.Warnf("rate_limiter unsupported match type: %s, skipping", matchType)
		return "", false
	}
}

// checkWarningThreshold checks if warning threshold is reached and logs appropriately
func checkWarningThreshold(result *ratelimiter.Result, cfg *configuration.Limiter, l dcontext.Logger) {
	warnThreshold := cfg.Action.WarnThreshold
	// Special case for threshold 0.0 - no warnings needed
	if warnThreshold <= 0 {
		// Don't log warnings when threshold is 0, but still return true
		// for rate-limited responses so headers get set
		return
	}

	// Calculate usage percentage based on the GCRA algorithm behavior
	var usagePercentage float64
	switch {
	case result.Allowed <= 0:
		// Already over limit
		usagePercentage = 1.0
	case result.Allowed > 0 && result.Remaining <= 0:
		// When Remaining is 0 but we're still allowed (using burst)
		// Calculate how much of the burst we've used based on allowed tokens
		burstCapacity := float64(cfg.Limit.Burst)
		regularCapacity := float64(cfg.Limit.Rate)
		totalCapacity := regularCapacity + burstCapacity

		// At this point regularCapacity is fully used, and we're using some of burstCapacity
		// The amount of burst we're using is represented by result.Allowed
		burstUsed := burstCapacity - float64(result.Allowed)
		totalUsed := regularCapacity + burstUsed

		usagePercentage = totalUsed / totalCapacity
	default:
		// Normal case - remaining > 0
		// Higher values mean more usage
		usagePercentage = 1.0 - (float64(result.Remaining) / float64(cfg.Limit.Burst))
	}

	if usagePercentage >= warnThreshold {
		logger := l.WithFields(logrus.Fields{
			"name":           cfg.Name,
			"description":    cfg.Description,
			"warn_threshold": warnThreshold,
			"usage":          usagePercentage,
		})

		switch cfg.Action.WarnAction {
		case "log":
			logger.Warn("rate_limiter reached threshold")
		case "none":
			fallthrough
		default:
			logger.Debug("rate_limiter reached threshold but no action will be taken")
		}
	}
}

// blockRateLimitedRequest handles blocking a request that exceeded rate limits
func blockRateLimitedRequest(w http.ResponseWriter, r *http.Request, result *ratelimiter.Result, matchType string, ctx *Context, l dcontext.Logger) {
	w.Header().Set(headerXRateLimitRemaining, fmt.Sprintf("%d", result.Remaining))
	w.Header().Set(headerRetryAfter, fmt.Sprintf("%f", result.RetryAfter.Seconds()))

	detail := map[string]string{
		"ip":          GetIPV4orIPV6Prefix(r.RemoteAddr),
		"limit":       matchType,
		"retry_after": result.RetryAfter.String(),
		"remaining":   fmt.Sprintf("%d", result.Remaining),
	}

	serveErrorJSON(w, errcode.ErrorCodeTooManyRequests.WithDetail(detail), ctx, l)
}

// serveErrorJSON handles serving an error response as JSON
func serveErrorJSON(w http.ResponseWriter, err error, ctx *Context, l dcontext.Logger) {
	var errorToServe errcode.Error
	if !errors.As(err, &errorToServe) {
		errorToServe = errcode.FromUnknownError(err)
	}

	if err := errcode.ServeJSON(w, errorToServe); err != nil {
		l.Errorf("error serving error json: %v (from %v)", err, ctx.Errors)
	}
}

func logRateLimitedRequest(ctx *Context, r *http.Request, msg string, limiterConfig *configuration.Limiter, result *ratelimiter.Result) {
	dcontext.GetLogger(ctx).WithFields(logrus.Fields{
		componentKey:  componentName,
		"name":        limiterConfig.Name,
		"log_only":    limiterConfig.LogOnly,
		"method":      r.Method,
		"path":        r.URL.Path, // Using path instead of full URL to reduce log size
		"source_ip":   GetIPV4orIPV6Prefix(r.RemoteAddr),
		"retry_after": result.RetryAfter,               // Essential for understanding when rate limit resets
		"action":      limiterConfig.Action.HardAction, // Important to know if blocking or just logging
	}).Info(msg)
}

// GetIPV4orIPV6Prefix returns either the full IPv4 address or the /64 prefix
// of the IPv6 address from the provided remote address, without the port.
// For IPv4 it returns the full address. For IPv6 it returns the /64 prefix.
func GetIPV4orIPV6Prefix(remoteAddr string) string {
	remoteIP, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		remoteIP = remoteAddr
	}

	addr, err := netip.ParseAddr(remoteIP)
	if err != nil {
		return remoteIP
	}

	if addr.Is4() {
		return remoteIP
	} else if addr.Is6() {
		ipv6Prefix, err := addr.Prefix(IPV6PrefixLength)
		if err != nil {
			return remoteIP
		}
		return ipv6Prefix.String()
	}

	return remoteIP
}

func encodeIPBase64(ip string) string {
	return base64.StdEncoding.EncodeToString([]byte(ip))
}
