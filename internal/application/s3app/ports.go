package s3app

import (
	"context"
	"io"

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

	// Multipart Upload operations
	CreateMultipartUpload(bucket, key string) (string, error)
	SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error)
	GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error)
	GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error)
	AbortMultipartUpload(bucket, uploadID string) error
	GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error)
	ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error)
}

// Replicator defines the interface for replicating transactions to other nodes
type Replicator interface {
	PrepareAll(ctx context.Context, tx s3.Transaction, meta s3.ObjectMeta) map[string]error
	CommitAll(ctx context.Context, txID, bucket, key string) map[string]error
	AbortAll(ctx context.Context, txID string) map[string]error
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

