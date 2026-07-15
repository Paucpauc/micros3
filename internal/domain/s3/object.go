package s3

import (
	"context"
	"errors"
	"time"
)

type ContextKey string

const RequestIDKey ContextKey = "request_id"

type valueOnlyContext struct {
	context.Context
}

func (valueOnlyContext) Deadline() (deadline time.Time, ok bool) { return }
func (valueOnlyContext) Done() <-chan struct{}                   { return nil }
func (valueOnlyContext) Err() error                              { return nil }

func WithoutCancel(ctx context.Context) context.Context {
	return valueOnlyContext{ctx}
}

func GetRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if val := ctx.Value(RequestIDKey); val != nil {
		if s, ok := val.(string); ok {
			return s
		}
	}
	return ""
}

// Transaction states
const (
	TxPrepared  = "PREPARED"
	TxCommitted = "COMMITTED"
	TxAborted   = "ABORTED"
)

// Transaction operations
const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
)

// StorageMode describes how an object's data is stored across the cluster.
//
//   - StorageModeReplica: the full object data is replicated to every node
//     (the original behaviour before EC support).
//   - StorageModeEC: the object is split into k data shards and m parity
//     shards (Reed-Solomon). Each node stores exactly one shard plus the
//     full metadata. The shard index held by the local node is recorded in
//     ECChunkIndex.
type StorageMode string

const (
	StorageModeReplica StorageMode = "REPLICA"
	StorageModeEC      StorageMode = "EC"
)

// ECParams describes the erasure-coding scheme used for an object.
type ECParams struct {
	// K is the number of data shards.
	K int `json:"k"`
	// M is the number of parity shards.
	M int `json:"m"`
	// ShardSize is the size of a single (padded) shard in bytes.
	ShardSize int64 `json:"shard_size"`
}

// ObjectMeta represents metadata for a stored object
type ObjectMeta struct {
	ContentType   string            `json:"content_type"`
	ContentLength int64             `json:"content_length"`
	ETag          string            `json:"etag"`
	CRC32         uint32            `json:"crc32"`
	CreatedAt     time.Time         `json:"created_at"`
	ModifiedAt    time.Time         `json:"modified_at"`
	UserMetadata  map[string]string `json:"user_metadata,omitempty"`

	// StorageMode indicates whether the object is stored as a full replica
	// or as an erasure-coded shard. Defaults to REPLICA when empty for
	// backward compatibility with existing metadata files.
	StorageMode StorageMode `json:"storage_mode,omitempty"`

	// ECParams is populated when StorageMode == StorageModeEC. It records
	// the (k+m) scheme and the shard size so that any node can reason about
	// the object layout.
	ECParams ECParams `json:"ec_params,omitempty"`

	// ECChunkIndex is the zero-based shard index stored on the local node
	// when StorageMode == StorageModeEC. For REPLICA objects it is ignored.
	ECChunkIndex int `json:"ec_chunk_index,omitempty"`
}

// IsEC returns true when the object is stored in erasure-coded form.
func (m ObjectMeta) IsEC() bool {
	return m.StorageMode == StorageModeEC
}

// Transaction represents a 2PC transaction log
type Transaction struct {
	ID        string    `json:"id"`
	Operation string    `json:"operation"` // "PUT" or "DELETE"
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	State     string    `json:"state"` // "PREPARED", "COMMITTED", "ABORTED"
	CreatedAt time.Time `json:"created_at"`
}

// MultipartUpload represents an initiated multipart upload session
type MultipartUpload struct {
	UploadID  string    `json:"upload_id"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	Initiated time.Time `json:"initiated"`
}

// UploadPart represents a single part of a multipart upload
type UploadPart struct {
	PartNumber int       `json:"part_number"`
	Size       int64     `json:"size"`
	ETag       string    `json:"etag"`
	CRC32      uint32    `json:"crc32"`
	ModifiedAt time.Time `json:"modified_at"`
}

// Validate transition checks if the state machine transition is allowed
func (tx *Transaction) TransitionTo(newState string) error {
	switch tx.State {
	case TxPrepared:
		if newState == TxCommitted || newState == TxAborted {
			tx.State = newState
			return nil
		}
	case TxCommitted, TxAborted:
		if tx.State == newState {
			return nil // idempotent
		}
		return errors.New("cannot change state from " + tx.State + " to " + newState)
	}
	return errors.New("invalid transition from " + tx.State + " to " + newState)
}

// ObjectInfo contains basic S3 object information for listing
type ObjectInfo struct {
	Key          string    `json:"key"`
	LastModified time.Time `json:"last_modified"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	StorageClass string    `json:"storage_class"`
}

// ListObjectsResult represents S3 ListObjectsV2 output
type ListObjectsResult struct {
	Name                  string       `json:"name"`
	Prefix                string       `json:"prefix"`
	KeyCount              int          `json:"key_count"`
	MaxKeys               int          `json:"max_keys"`
	IsTruncated           bool         `json:"is_truncated"`
	NextContinuationToken string       `json:"next_continuation_token,omitempty"`
	Contents              []ObjectInfo `json:"contents,omitempty"`
	CommonPrefixes        []string     `json:"common_prefixes,omitempty"`
}

// CompletePart represents a part number and ETag for completing a multipart upload
type CompletePart struct {
	PartNumber int    `json:"part_number"`
	ETag       string `json:"etag"`
}
