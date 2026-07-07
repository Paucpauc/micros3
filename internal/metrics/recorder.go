package metrics

import "github.com/paucpauc/micros3/internal/application/s3app"

// Compile-time check that PrometheusRecorder implements MetricsRecorder.
var _ s3app.MetricsRecorder = (*PrometheusRecorder)(nil)

// PrometheusRecorder implements s3app.MetricsRecorder using Prometheus counters/gauges.
type PrometheusRecorder struct{}

// NewPrometheusRecorder returns a MetricsRecorder backed by the Prometheus
// metrics registered in this package.
func NewPrometheusRecorder() *PrometheusRecorder {
	return &PrometheusRecorder{}
}

func (r *PrometheusRecorder) SetBucketsTotal(count int) {
	BucketsTotal.Set(float64(count))
}

func (r *PrometheusRecorder) SetObjectsTotal(bucket string, count int64) {
	ObjectsTotal.WithLabelValues(bucket).Set(float64(count))
}

func (r *PrometheusRecorder) SetStorageUsedBytes(bucket string, bytes int64) {
	StorageUsedBytes.WithLabelValues(bucket).Set(float64(bytes))
}

func (r *PrometheusRecorder) SetClusterRole(isLeader bool) {
	if isLeader {
		ClusterRole.Set(1)
	} else {
		ClusterRole.Set(0)
	}
}

func (r *PrometheusRecorder) SetClusterStatus(status string) {
	SetClusterStatus(status)
}

func (r *PrometheusRecorder) SetSyncLeaseActive(active bool) {
	if active {
		SyncLeaseActive.Set(1)
	} else {
		SyncLeaseActive.Set(0)
	}
}

func (r *PrometheusRecorder) SetWritesBlocked(blocked bool) {
	if blocked {
		WritesBlocked.Set(1)
	} else {
		WritesBlocked.Set(0)
	}
}

func (r *PrometheusRecorder) SetActiveWrites(count int) {
	ActiveWrites.Set(float64(count))
}

func (r *PrometheusRecorder) IncReplicationPrepare(result string) {
	ReplicationPrepareTotal.WithLabelValues(result).Inc()
}

func (r *PrometheusRecorder) IncReplicationCommit(result string) {
	ReplicationCommitTotal.WithLabelValues(result).Inc()
}

func (r *PrometheusRecorder) IncReplicationAbort(reason string) {
	ReplicationAbortTotal.WithLabelValues(reason).Inc()
}
