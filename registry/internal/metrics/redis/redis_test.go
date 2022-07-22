package redis

import (
	"bytes"
	"fmt"
	"testing"
	"text/template"

	"github.com/docker/distribution/metrics"

	redis "github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
		name           string
		opts           []Option
		expectedLabels prometheus.Labels
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewPoolStatsCollector(mock, tt.opts...)

			validateMetric(t, c, hitsName, hitsDesc, "gauge", float64(mock.Hits), tt.expectedLabels)
			validateMetric(t, c, missesName, missesDesc, "gauge", float64(mock.Misses), tt.expectedLabels)
			validateMetric(t, c, timeoutsName, timeoutsDesc, "gauge", float64(mock.Timeouts), tt.expectedLabels)
			validateMetric(t, c, totalConnsName, totalConnsDesc, "gauge", float64(mock.TotalConns), tt.expectedLabels)
			validateMetric(t, c, idleConnsName, idleConnsDesc, "gauge", float64(mock.IdleConns), tt.expectedLabels)
			validateMetric(t, c, staleConnsName, staleConnsDesc, "gauge", float64(mock.StaleConns), tt.expectedLabels)
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

func validateMetric(t *testing.T, collector prometheus.Collector, name string, desc string, valueType string, value float64, labels prometheus.Labels) {
	t.Helper()

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
		Type:   valueType,
	})
	require.NoError(t, err)

	err = testutil.CollectAndCompare(collector, &expected, fullName)
	require.NoError(t, err)
}
