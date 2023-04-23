package redis

import (
	"github.com/docker/distribution/metrics"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	redisprom "github.com/trim21/go-redis-prometheus"
)

const (
	// Names for the recorded metrics.
	hitsName       = "pool_stats_hits"
	missesName     = "pool_stats_misses"
	timeoutsName   = "pool_stats_timeouts"
	totalConnsName = "pool_stats_total_conns"
	idleConnsName  = "pool_stats_idle_conns"
	staleConnsName = "pool_stats_stale_conns"
	maxConnsName   = "pool_stats_max_conns"

	// Descriptions for the recorded metrics.
	hitsDesc       = "The number of times a free connection was found in the pool."
	missesDesc     = "The number of times a free connection was not found in the pool."
	timeoutsDesc   = "The number of times a wait timeout occurred."
	totalConnsDesc = "The total number of connections in the pool."
	idleConnsDesc  = "The number of idle connections in the pool."
	staleConnsDesc = "The number of stale connections removed from the pool."
	maxConnsDesc   = "The maximum number of connections in the pool."

	subSystem           = "redis"
	defaultInstanceName = "unnamed"
)

// PoolStatsGetter describes a getter for *redis.PoolStats.
type PoolStatsGetter interface {
	PoolStats() *redis.PoolStats
}

var _ PoolStatsGetter = (*redis.Client)(nil)

// Options represents options to customize the exported metrics.
type Options struct {
	InstanceName string
	MaxConns     int
}

// Option is a functional option to customize defaults.
type Option func(*Options)

// defaultOptions returns the default options.
func defaultOptions() *Options {
	return &Options{
		InstanceName: defaultInstanceName,
	}
}

func (options *Options) merge(opts ...Option) {
	for _, opt := range opts {
		opt(options)
	}
}

// WithInstanceName sets the name of the Redis instance.
func WithInstanceName(name string) Option {
	return func(options *Options) {
		options.InstanceName = name
	}
}

// WithMaxConns enables a gauge metric to report the size of the Redis connection pool. This cannot be automatically
// detected as all other pool metrics because redis.PoolStats does not expose such attribute. Use this if you need to
// monitor the pool saturation.
func WithMaxConns(n int) Option {
	return func(options *Options) {
		options.MaxConns = n
	}
}

// poolStatsCollector is a Prometheus collector for Redis connection pool statuses.
type poolStatsCollector struct {
	client  PoolStatsGetter
	options *Options

	hitsDesc       *prometheus.Desc
	missesDesc     *prometheus.Desc
	timeoutsDesc   *prometheus.Desc
	totalConnsDesc *prometheus.Desc
	idleConnsDesc  *prometheus.Desc
	staleConnsDesc *prometheus.Desc
	maxConnsDesc   *prometheus.Desc
}

// Describe implements prometheus.Collector.
func (c *poolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hitsDesc
	ch <- c.missesDesc
	ch <- c.timeoutsDesc
	ch <- c.totalConnsDesc
	ch <- c.idleConnsDesc
	ch <- c.staleConnsDesc
	ch <- c.maxConnsDesc
}

// Collect implements prometheus.Collector.
func (c *poolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.client.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.hitsDesc, prometheus.GaugeValue, float64(stats.Hits))
	ch <- prometheus.MustNewConstMetric(c.missesDesc, prometheus.GaugeValue, float64(stats.Misses))
	ch <- prometheus.MustNewConstMetric(c.timeoutsDesc, prometheus.GaugeValue, float64(stats.Timeouts))
	ch <- prometheus.MustNewConstMetric(c.totalConnsDesc, prometheus.GaugeValue, float64(stats.TotalConns))
	ch <- prometheus.MustNewConstMetric(c.idleConnsDesc, prometheus.GaugeValue, float64(stats.IdleConns))
	ch <- prometheus.MustNewConstMetric(c.staleConnsDesc, prometheus.GaugeValue, float64(stats.StaleConns))
	ch <- prometheus.MustNewConstMetric(c.maxConnsDesc, prometheus.GaugeValue, float64(c.options.MaxConns))
}

var _ prometheus.Collector = (*poolStatsCollector)(nil)

// NewPoolStatsCollector returns a new Redis pool stats collector that implements prometheus.Collector.
func NewPoolStatsCollector(client PoolStatsGetter, opts ...Option) prometheus.Collector {
	options := defaultOptions()
	options.merge(opts...)

	constLabels := prometheus.Labels{"instance": options.InstanceName}

	return &poolStatsCollector{
		options: options,
		client:  client,
		hitsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, hitsName),
			hitsDesc,
			nil,
			constLabels,
		),
		missesDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, missesName),
			missesDesc,
			nil,
			constLabels,
		),
		timeoutsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, timeoutsName),
			timeoutsDesc,
			nil,
			constLabels,
		),
		totalConnsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, totalConnsName),
			totalConnsDesc,
			nil,
			constLabels,
		),
		idleConnsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, idleConnsName),
			idleConnsDesc,
			nil,
			constLabels,
		),
		staleConnsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, staleConnsName),
			staleConnsDesc,
			nil,
			constLabels,
		),
		maxConnsDesc: prometheus.NewDesc(
			prometheus.BuildFQName(metrics.NamespacePrefix, subSystem, maxConnsName),
			maxConnsDesc,
			nil,
			constLabels,
		),
	}
}

var buckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1}

// InstrumentClient instruments a Redis client with Prometheus metrics for operations and connection pool stats.
func InstrumentClient(client redis.UniversalClient, opts ...Option) {
	options := &Options{}
	options.merge(opts...)

	// command metrics, through https://github.com/globocom/go-redis-prometheus
	hook := redisprom.NewHook(
		redisprom.WithNamespace(metrics.NamespacePrefix),
		redisprom.WithInstanceName(options.InstanceName),
		redisprom.WithDurationBuckets(buckets),
	)
	client.AddHook(hook)

	// connection pool metrics
	c := NewPoolStatsCollector(client, opts...)

	prometheus.MustRegister(c)
}
