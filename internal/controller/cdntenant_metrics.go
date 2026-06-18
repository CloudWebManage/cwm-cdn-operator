package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	secondarySyncAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cwm_cdn_secondary_sync_attempts_total",
			Help: "Total number of CDN tenant secondary sync attempts by secondary and status.",
		},
		[]string{"secondary", "status"},
	)
	secondarySyncLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "cwm_cdn_secondary_sync_latency_seconds",
			Help:    "Latency of CDN tenant secondary sync attempts by secondary.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"secondary"},
	)
	secondarySyncStale = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cwm_cdn_secondary_sync_stale_seconds",
			Help: "Seconds since the last successful CDN tenant secondary sync by secondary.",
		},
		[]string{"secondary"},
	)
)

func init() {
	metrics.Registry.MustRegister(secondarySyncAttempts, secondarySyncLatency, secondarySyncStale)
}
