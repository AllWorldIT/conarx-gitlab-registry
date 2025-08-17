package ratelimiter

import (
	"fmt"
	"testing"

	"github.com/docker/distribution/configuration"
	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/require"
)

var validLimiterCfg = configuration.Limiter{
	Name:        "test rate limiter",
	Description: "description",
	LogOnly:     true,
	Match: configuration.Match{
		Type: "IP",
	},
	Precedence: 10,
	Limit: configuration.Limit{
		Rate:   100,
		Period: "minute",
		Burst:  200,
	},
	Action: configuration.Action{
		WarnThreshold: 0.7,
		WarnAction:    "log",
		HardAction:    "block",
	},
}

func TestRateLimiter_parseLimitersConfig_Precedence(t *testing.T) {
	t.Run("multiple limiters with different precedence", func(t *testing.T) {
		rateLimiterCfg := []configuration.Limiter{
			func(cfg configuration.Limiter) configuration.Limiter {
				cfg.Name = "third"
				cfg.Precedence = 30
				return cfg
			}(validLimiterCfg),
			func(cfg configuration.Limiter) configuration.Limiter {
				cfg.Name = "first"
				cfg.Precedence = 10
				return cfg
			}(validLimiterCfg),
			func(cfg configuration.Limiter) configuration.Limiter {
				cfg.Name = "second"
				cfg.Precedence = 20
				return cfg
			}(validLimiterCfg),
		}

		gotCfg, err := parseLimitersConfig(rateLimiterCfg)
		require.NoError(t, err)
		require.Len(t, gotCfg, 3)

		// gotCfg is a map[string]*configuration.Limiter, so we cannot easily
		// test for the order without an index
		i := 0
		for _, l := range gotCfg {
			switch i {
			case 0:
				require.Equal(t, "first", l.Name)
				require.Equal(t, int64(10), l.Limiter.Precedence)
			case 1:
				require.Equal(t, "second", l.Name)
				require.Equal(t, int64(20), l.Limiter.Precedence)
			case 2:
				require.Equal(t, "third", l.Name)
				require.Equal(t, int64(30), l.Limiter.Precedence)
			}
			i++
		}
	})

	t.Run("multiple limiters with same precedence keeps the order", func(t *testing.T) {
		rateLimiterCfg := []configuration.Limiter{
			func(cfg configuration.Limiter) configuration.Limiter {
				cfg.Name = "third"
				cfg.Precedence = 30
				return cfg
			}(validLimiterCfg),
			func(cfg configuration.Limiter) configuration.Limiter {
				cfg.Name = "first"
				cfg.Precedence = 30
				return cfg
			}(validLimiterCfg),
		}
		gotCfg, err := parseLimitersConfig(rateLimiterCfg)
		require.NoError(t, err)
		require.Len(t, gotCfg, 2)

		i := 0
		for _, l := range gotCfg {
			switch i {
			case 0:
				require.Equal(t, "third", l.Name)
				require.Equal(t, int64(30), l.Limiter.Precedence)
			case 1:
				require.Equal(t, "first", l.Name)
				require.Equal(t, int64(30), l.Limiter.Precedence)
			}
			i++
		}
	})
}

func TestRateLimiter_validateLimiter(t *testing.T) {
	tcs := map[string]struct {
		cfg         *configuration.Limiter
		expectedErr error
	}{
		"valid": {
			cfg:         &validLimiterCfg,
			expectedErr: nil,
		},
		"invalid config multierror": {
			cfg: func(cfg configuration.Limiter) *configuration.Limiter {
				cfg.Name = ""
				cfg.Precedence = -1
				cfg.Match.Type = ""
				cfg.Limit.Rate = -1
				cfg.Limit.Burst = -1
				cfg.Limit.Period = "unknown"
				cfg.Action.WarnAction = "unknown"
				cfg.Action.WarnThreshold = -1.0
				cfg.Action.HardAction = "unknown"
				return &cfg
			}(validLimiterCfg),
			expectedErr: (&multierror.Error{
				Errors: []error{
					fmt.Errorf("limiter name cannot be empty"),
					fmt.Errorf("limiter precedence must be a positive integer"),
					fmt.Errorf("match.type must be one of: %+v", validMatchTypes),
					fmt.Errorf("rate must be a positive integer"),
					fmt.Errorf("burst must be a positive integer"),
					fmt.Errorf("period must be one of: %+v", validPeriods),
					fmt.Errorf("action.warn_action must be one of: %+v", validWarnActions),
					fmt.Errorf("action.warn_threshold must be between 0.0 and 1.0"),
					fmt.Errorf("action.hard_action must be one of: %+v", validHardActions),
				},
			}).ErrorOrNil(),
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			got := validateLimiter(tc.cfg)
			if tc.expectedErr == nil {
				require.NoError(t, got)
				return
			}

			require.EqualError(t, got, tc.expectedErr.Error())
		})
	}
}
