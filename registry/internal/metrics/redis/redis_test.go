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

	tests := []struct {
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
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewPoolStatsCollector(mock, tt.opts...)

			validateGauge(t, c, hitsName, hitsDesc, float64(mock.Hits), tt.expectedLabels)
			validateGauge(t, c, missesName, missesDesc, float64(mock.Misses), tt.expectedLabels)
			validateGauge(t, c, timeoutsName, timeoutsDesc, float64(mock.Timeouts), tt.expectedLabels)
			validateGauge(t, c, totalConnsName, totalConnsDesc, float64(mock.TotalConns), tt.expectedLabels)
			validateGauge(t, c, idleConnsName, idleConnsDesc, float64(mock.IdleConns), tt.expectedLabels)
			validateGauge(t, c, staleConnsName, staleConnsDesc, float64(mock.StaleConns), tt.expectedLabels)
			validateGauge(t, c, maxConnsName, maxConnsDesc, float64(tt.expectedMaxConns), tt.expectedLabels)
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
