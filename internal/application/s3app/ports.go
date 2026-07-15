package s3app

import (
	"context"
	"io"
	"time"

	"github.com/paucpauc/micros3/internal/domain/s3"
)

// StorageRepository defines the storage interface for local file/meta management
type StorageRepository interface {
	// Bucket operations
	CreateBucket(bucket string) error
	DeleteBucket(bucket string) error
	HasBucket(bucket string) (bool, error)
	ListBuckets() ([]string, error)

	// Transaction (2PC) operations
	StageObject(txID string, r io.Reader, size int64, meta s3.ObjectMeta, tx s3.Transaction) (s3.ObjectMeta, error)
	CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error)
	AbortTransaction(txID string) error
	GetTransaction(txID string) (s3.Transaction, error)
	GetStagedObjectReader(txID string) (io.ReadCloser, error)

	// Object operations
	GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error)
	GetObjectMeta(bucket, key string) (s3.ObjectMeta, error)
	DeleteObject(bucket, key string) error
	ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error)

	// EC shard operations. These are used when an object has been converted
	// from replica to erasure-coded storage. Each node stores exactly one
	// shard (indexed by ECChunkIndex in the metadata) plus the full metadata.
	PutECShard(bucket, key string, shardIndex int, r io.Reader, size int64, meta s3.ObjectMeta) error
	GetECShard(bucket, key string, shardIndex int) (io.ReadCloser, error)
	HasECShard(bucket, key string, shardIndex int) (bool, error)
	DeleteECShard(bucket, key string, shardIndex int) error
	// UpdateObjectMeta overwrites the metadata file for an existing object
	// (used when converting replica -> EC and vice versa).
	UpdateObjectMeta(bucket, key string, meta s3.ObjectMeta) error
	// RemoveReplicaData deletes only the full replica data file for an
	// object, leaving the metadata and any EC shards intact. It is used
	// after a successful replica -> EC conversion to reclaim the space
	// occupied by the original full copy. If the data file does not exist
	// (e.g. the object was already converted), it is a no-op.
	RemoveReplicaData(bucket, key string) error

	// Multipart Upload operations
	CreateMultipartUpload(bucket, key string) (string, error)
	SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error)
	GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error)
	DeleteMultipartPart(bucket, uploadID string, partNum int) error
	GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error)
	AbortMultipartUpload(bucket, uploadID string) error
	GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error)
	ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error)
}

// MaintenanceRepository defines optional storage maintenance operations.
// Implementations may support a subset of these methods; callers should use
// a type assertion to check availability. This keeps storage-specific cleanup
// logic out of the application layer and the composition root.
type MaintenanceRepository interface {
	// CleanupExpiredTransactions aborts prepared transactions older than maxAge.
	CleanupExpiredTransactions(maxAge time.Duration) ([]s3.Transaction, error)
	// CleanupOrphanedObjects removes object data that has no corresponding metadata
	// and is older than minAge.
	CleanupOrphanedObjects(minAge time.Duration) (int, error)
	// CleanupExpiredMultipartUploads aborts multipart uploads older than maxAge.
	CleanupExpiredMultipartUploads(maxAge time.Duration) ([]s3.MultipartUpload, error)
}

// MetricsRecorder abstracts the emission of operational metrics so that the
// application layer does not depend on a specific metrics library (e.g. Prometheus).
type MetricsRecorder interface {
	SetBucketsTotal(count int)
	SetObjectsTotal(bucket string, count int64)
	SetStorageUsedBytes(bucket string, bytes int64)
	SetClusterRole(isLeader bool)
	SetClusterStatus(status string)
	SetSyncLeaseActive(active bool)
	SetWritesBlocked(blocked bool)
	SetActiveWrites(count int)
	IncReplicationPrepare(result string)
	IncReplicationCommit(result string)
	IncReplicationAbort(reason string)
}

// Replicator defines the interface for replicating transactions to other nodes
type Replicator interface {
	PrepareAll(ctx context.Context, tx s3.Transaction, meta s3.ObjectMeta) map[string]error
	CommitAll(ctx context.Context, txID, bucket, key string) map[string]error
	AbortAll(ctx context.Context, txID string) map[string]error
}

// SyncCoordinator defines the interface for leader-driven synchronization.
// When a follower requests sync, the leader uses this coordinator to drive
// the entire process: query the follower's keys, push missing/updated
// objects, and delete extraneous ones.
type SyncCoordinator interface {
	SyncFollower(ctx context.Context, nodeID, followerAddr string) error
}

// ECReader reconstructs object data from erasure-coded shards. The service
// uses it transparently in GetObject when the object's StorageMode is EC.
type ECReader interface {
	ReadECObject(ctx context.Context, bucket, key string) (io.ReadCloser, s3.ObjectMeta, error)
}

// ClusterManager defines the interface for cluster membership and roles
type ClusterManager interface {
	NodeID() string
	IsLeader() bool
	LeaderInternalAddress() string
	AliveFollowers() []string
	Mode() string
	MarkDead(nodeID string)
	MarkAlive(nodeID, internalAddr string)
	Status() string
	SetLocalStatus(status string)
}
