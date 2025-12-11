package redis

import (
	"bytes"
	"fmt"
	"testing"
	"text/template"

	"github.com/docker/distribution/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type statsMock redis.PoolStats

func (m statsMock) PoolStats() *redis.PoolStats {
	return &redis.PoolStats{
		Hits:       m.Hits,
		Misses:     m.Misses,
		Timeouts:   m.Timeouts,
		TotalConns: m.TotalConns,
		IdleConns:  m.IdleConns,
		StaleConns: m.StaleConns,
	}
}

func TestNewPoolStatsCollector(t *testing.T) {
	mock := &statsMock{
		Hits:       132,
		Misses:     4,
		Timeouts:   1,
		TotalConns: 10,
		IdleConns:  5,
		StaleConns: 5,
	}

	testCases := []struct {
		name             string
		opts             []Option
		expectedLabels   prometheus.Labels
		expectedMaxConns int
	}{
		{
			name: "default",
			expectedLabels: prometheus.Labels{
				"instance": defaultInstanceName,
			},
		},
		{
			name: "with instance name",
			opts: []Option{
				WithInstanceName("bar"),
			},
			expectedLabels: prometheus.Labels{
				"instance": "bar",
			},
		},
		{
			name: "with max conns",
			opts: []Option{
				WithMaxConns(5),
			},
			expectedLabels: prometheus.Labels{
				"instance": defaultInstanceName,
			},
			expectedMaxConns: 5,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(tt *testing.T) {
			c := NewPoolStatsCollector(mock, tc.opts...)

			validateGauge(tt, c, hitsName, hitsDesc, float64(mock.Hits), tc.expectedLabels)
			validateGauge(tt, c, missesName, missesDesc, float64(mock.Misses), tc.expectedLabels)
			validateGauge(tt, c, timeoutsName, timeoutsDesc, float64(mock.Timeouts), tc.expectedLabels)
			validateGauge(tt, c, totalConnsName, totalConnsDesc, float64(mock.TotalConns), tc.expectedLabels)
			validateGauge(tt, c, idleConnsName, idleConnsDesc, float64(mock.IdleConns), tc.expectedLabels)
			validateGauge(tt, c, staleConnsName, staleConnsDesc, float64(mock.StaleConns), tc.expectedLabels)
			validateGauge(tt, c, maxConnsName, maxConnsDesc, float64(tc.expectedMaxConns), tc.expectedLabels)
		})
	}
}

type labelsIter struct {
	Dict    prometheus.Labels
	Counter int
}

func (l *labelsIter) HasMore() bool {
	l.Counter++
	return l.Counter < len(l.Dict)
}

func validateGauge(t *testing.T, collector prometheus.Collector, name, desc string, value float64, labels prometheus.Labels) {
	tmpl := template.New("")
	tmpl.Delims("[[", "]]")
	txt := `
# HELP [[.Name]] [[.Desc]]
# TYPE [[.Name]] [[.Type]]
[[.Name]]{[[range $k, $v := .Labels.Dict]][[$k]]="[[$v]]"[[if $.Labels.HasMore]],[[end]][[end]]} [[.Value]]
`
	_, err := tmpl.Parse(txt)
	require.NoError(t, err)

	var expected bytes.Buffer
	fullName := fmt.Sprintf("%s_%s_%s", metrics.NamespacePrefix, subSystem, name)

	err = tmpl.Execute(&expected, struct {
		Name   string
		Desc   string
		Type   string
		Value  float64
		Labels *labelsIter
	}{
		Name:   fullName,
		Desc:   desc,
		Labels: &labelsIter{Dict: labels},
		Value:  value,
		Type:   "gauge",
	})
	require.NoError(t, err)

	err = testutil.CollectAndCompare(collector, &expected, fullName)
	require.NoError(t, err)
}
