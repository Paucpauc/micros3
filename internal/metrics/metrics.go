package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "micros3"

var (
	RequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "requests_total",
		Help:      "Total number of S3 API requests",
	}, []string{"method", "bucket", "code"})

	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "request_duration_seconds",
		Help:      "S3 API request duration in seconds",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method", "bucket"})

	BytesWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bytes_written_total",
		Help:      "Total bytes written (PUT/POST) per bucket",
	}, []string{"method", "bucket"})

	BytesRead = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "bytes_read_total",
		Help:      "Total bytes read (GET) per bucket",
	}, []string{"method", "bucket"})

	ObjectsTotal = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "objects_total",
		Help:      "Total number of S3 objects per bucket",
	}, []string{"bucket"})

	StorageUsedBytes = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "storage_used_bytes",
		Help:      "Total storage used in bytes per bucket",
	}, []string{"bucket"})

	ClusterRole = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "cluster_role",
		Help:      "Current node role: 1=leader, 0=follower",
	})

	ClusterStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "cluster_status",
		Help:      "Current node status: 1 for the active status, 0 for others",
	}, []string{"status"})

	WritesBlocked = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "writes_blocked",
		Help:      "Whether writes are currently blocked by sync lease (1=blocked, 0=unblocked)",
	})

	ActiveWrites = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "active_writes",
		Help:      "Number of currently active write transactions",
	})

	ReplicationPrepareTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "replication_prepare_total",
		Help:      "Total number of 2PC prepare attempts",
	}, []string{"result"})

	ReplicationCommitTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "replication_commit_total",
		Help:      "Total number of 2PC commit attempts",
	}, []string{"result"})

	ReplicationAbortTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "replication_abort_total",
		Help:      "Total number of 2PC aborts",
	}, []string{"result"})

	SyncLeaseActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "sync_lease_active",
		Help:      "Whether a sync lease is currently active (1=active, 0=inactive)",
	})

	BucketsTotal = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "buckets_total",
		Help:      "Total number of S3 buckets",
	})

	ProxyRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "proxy_requests_total",
		Help:      "Total number of requests proxied to leader",
	}, []string{"method"})

	MultipartUploadsActive = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "multipart_uploads_active",
		Help:      "Number of active multipart uploads",
	})

	DedupLinksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "dedup_links_total",
		Help:      "Total number of hardlinks created by deduplication",
	})

	DedupRunsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "dedup_runs_total",
		Help:      "Total number of deduplication runs",
	})
)

func SetClusterStatus(status string) {
	allStatuses := []string{"OFFLINE", "SYNCING", "READY", "ERROR"}
	for _, s := range allStatuses {
		val := 0.0
		if s == status {
			val = 1.0
		}
		ClusterStatus.WithLabelValues(s).Set(val)
	}
}
