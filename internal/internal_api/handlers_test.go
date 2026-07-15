package internal_api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"go.uber.org/zap"
)

// Mocks for internal handlers tests

type mockCluster struct {
	status string
}

func (m *mockCluster) NodeID() string                               { return "node-1" }
func (m *mockCluster) IsLeader() bool                               { return true }
func (m *mockCluster) LeaderInternalAddress() string                { return "" }
func (m *mockCluster) AliveFollowers() []string                     { return nil }
func (m *mockCluster) KnownFollowers() []string                     { return nil }
func (m *mockCluster) Mode() string                                 { return "static" }
func (m *mockCluster) MarkDead(nodeID string)                       {}
func (m *mockCluster) MarkAlive(nodeID, internalAddr string)        {}
func (m *mockCluster) RegisterFollower(nodeID, internalAddr string) {}
func (m *mockCluster) Status() string                               { return m.status }
func (m *mockCluster) SetLocalStatus(status string)                 { m.status = status }

type mockMetricsRecorder struct{}

func (m *mockMetricsRecorder) SetBucketsTotal(count int)                      {}
func (m *mockMetricsRecorder) SetObjectsTotal(bucket string, count int64)     {}
func (m *mockMetricsRecorder) SetStorageUsedBytes(bucket string, bytes int64) {}
func (m *mockMetricsRecorder) SetClusterRole(isLeader bool)                   {}
func (m *mockMetricsRecorder) SetClusterStatus(status string)                 {}
func (m *mockMetricsRecorder) SetSyncLeaseActive(active bool)                 {}
func (m *mockMetricsRecorder) SetWritesBlocked(blocked bool)                  {}
func (m *mockMetricsRecorder) SetActiveWrites(count int)                      {}
func (m *mockMetricsRecorder) IncReplicationPrepare(result string)            {}
func (m *mockMetricsRecorder) IncReplicationCommit(result string)             {}
func (m *mockMetricsRecorder) IncReplicationAbort(reason string)              {}

type mockReplicator struct{}

func (m *mockReplicator) PrepareAll(ctx context.Context, tx s3.Transaction, meta s3.ObjectMeta) map[string]error {
	return nil
}
func (m *mockReplicator) CommitAll(ctx context.Context, txID, bucket, key string) map[string]error {
	return nil
}
func (m *mockReplicator) AbortAll(ctx context.Context, txID string) map[string]error { return nil }

type mockSyncCoordinator struct {
	called       bool
	nodeID       string
	followerAddr string
	err          error
}

func (m *mockSyncCoordinator) SyncFollower(ctx context.Context, nodeID, followerAddr string) error {
	m.called = true
	m.nodeID = nodeID
	m.followerAddr = followerAddr
	return m.err
}

type mockStorage struct {
	buckets       map[string]bool
	stagedObjects map[string][]byte
	committedKeys map[string]bool
	stagedMetas   map[string]s3.ObjectMeta
	txs           map[string]s3.Transaction
}

func (m *mockStorage) CreateBucket(bucket string) error {
	m.buckets[bucket] = true
	return nil
}
func (m *mockStorage) DeleteBucket(bucket string) error {
	delete(m.buckets, bucket)
	return nil
}
func (m *mockStorage) HasBucket(bucket string) (bool, error) {
	return m.buckets[bucket], nil
}
func (m *mockStorage) ListBuckets() ([]string, error) {
	var l []string
	for b := range m.buckets {
		l = append(l, b)
	}
	return l, nil
}
func (m *mockStorage) StageObject(txID string, r io.Reader, size int64, meta s3.ObjectMeta, tx s3.Transaction) (s3.ObjectMeta, error) {
	m.txs[txID] = tx
	if tx.Operation == s3.OpPut {
		data, _ := io.ReadAll(r)
		m.stagedObjects[txID] = data
		meta.ContentLength = int64(len(data))
		m.stagedMetas[txID] = meta
	}
	return meta, nil
}
func (m *mockStorage) CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error) {
	m.committedKeys[bucket+"/"+key] = true
	return s3.ObjectMeta{}, nil
}
func (m *mockStorage) AbortTransaction(txID string) error {
	delete(m.stagedObjects, txID)
	return nil
}
func (m *mockStorage) GetTransaction(txID string) (s3.Transaction, error) {
	tx, ok := m.txs[txID]
	if !ok {
		return s3.Transaction{}, errors.New("not found")
	}
	return tx, nil
}
func (m *mockStorage) GetStagedObjectReader(txID string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockStorage) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	return io.NopCloser(bytes.NewReader([]byte("object data"))), s3.ObjectMeta{ContentType: "text/plain"}, nil
}
func (m *mockStorage) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	return s3.ObjectMeta{ContentType: "text/plain"}, nil
}
func (m *mockStorage) DeleteObject(bucket, key string) error {
	delete(m.committedKeys, bucket+"/"+key)
	return nil
}
func (m *mockStorage) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	return s3.ListObjectsResult{
		Name: bucket,
		Contents: []s3.ObjectInfo{
			{Key: "file.txt", Size: 11, LastModified: time.Now()},
		},
	}, nil
}
func (m *mockStorage) CreateMultipartUpload(bucket, key string) (string, error) { return "", nil }
func (m *mockStorage) SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error) {
	return s3.UploadPart{}, nil
}
func (m *mockStorage) GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockStorage) DeleteMultipartPart(bucket, uploadID string, partNum int) error {
	return nil
}
func (m *mockStorage) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	return nil, nil
}
func (m *mockStorage) AbortMultipartUpload(bucket, uploadID string) error { return nil }
func (m *mockStorage) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	return s3.MultipartUpload{}, nil
}
func (m *mockStorage) ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error) {
	return nil, nil
}

// EC shard no-op stubs (EC is not exercised in unit tests).
func (m *mockStorage) PutECShard(bucket, key string, shardIndex int, r io.Reader, size int64, meta s3.ObjectMeta) error {
	return nil
}
func (m *mockStorage) GetECShard(bucket, key string, shardIndex int) (io.ReadCloser, error) {
	return nil, errors.New("no EC shard")
}
func (m *mockStorage) HasECShard(bucket, key string, shardIndex int) (bool, error) {
	return false, nil
}
func (m *mockStorage) DeleteECShard(bucket, key string, shardIndex int) error        { return nil }
func (m *mockStorage) UpdateObjectMeta(bucket, key string, meta s3.ObjectMeta) error { return nil }
func (m *mockStorage) RemoveReplicaData(bucket, key string) error                    { return nil }

func TestInternalHandlersAuth(t *testing.T) {
	store := &mockStorage{buckets: make(map[string]bool)}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	h := NewHandler(store, svc, &mockCluster{status: "READY"}, nil, "secret-token", zap.NewNop())

	// Request without token should fail
	req := httptest.NewRequest(http.MethodGet, "/internal/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}

	// Request with correct token should succeed
	req = httptest.NewRequest(http.MethodGet, "/internal/health", nil)
	req.Header.Set("X-MicroS3-Token", "secret-token")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

func TestInternalHealthStats(t *testing.T) {
	store := &mockStorage{
		buckets:       map[string]bool{"mybucket": true},
		stagedObjects: make(map[string][]byte),
		committedKeys: make(map[string]bool),
		stagedMetas:   make(map[string]s3.ObjectMeta),
		txs:           make(map[string]s3.Transaction),
	}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	h := NewHandler(store, svc, &mockCluster{status: "READY"}, nil, "secret-token", zap.NewNop())

	req := httptest.NewRequest(http.MethodGet, "/internal/health", nil)
	req.Header.Set("X-MicroS3-Token", "secret-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp HealthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if resp.NodeID != "node-1" || resp.State != "READY" || resp.Role != "leader" {
		t.Errorf("unexpected health response values: %+v", resp)
	}
	// We expect 1 object (mocked in ListObjectsV2) with size 11
	if resp.ObjectsCount != 1 || resp.StorageUsedBytes != 11 {
		t.Errorf("unexpected counts: objects=%d, bytes=%d", resp.ObjectsCount, resp.StorageUsedBytes)
	}
}

func TestInternal2PCReplicationHandlers(t *testing.T) {
	store := &mockStorage{
		buckets:       map[string]bool{"mybucket": true},
		stagedObjects: make(map[string][]byte),
		committedKeys: make(map[string]bool),
		stagedMetas:   make(map[string]s3.ObjectMeta),
		txs:           make(map[string]s3.Transaction),
	}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	h := NewHandler(store, svc, &mockCluster{status: "READY"}, nil, "secret-token", zap.NewNop())

	txID := "tx-567"
	payload := []byte("2pc payload replicated")

	// 1. Prepare
	req := httptest.NewRequest(http.MethodPost, "/internal/prepare", bytes.NewReader(payload))
	req.Header.Set("X-MicroS3-Token", "secret-token")
	req.Header.Set("X-MicroS3-TxID", txID)
	req.Header.Set("X-MicroS3-Operation", s3.OpPut)
	req.Header.Set("X-MicroS3-Bucket", "mybucket")
	req.Header.Set("X-MicroS3-Key", "obj.txt")
	req.Header.Set("X-MicroS3-CRC32", "99")

	meta := s3.ObjectMeta{ContentType: "text/plain"}
	mBytes, _ := json.Marshal(meta)
	req.Header.Set("X-MicroS3-Meta", base64.StdEncoding.EncodeToString(mBytes))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("prepare failed: %d - %s", rec.Code, rec.Body.String())
	}

	// Verify staged locally
	if !bytes.Equal(store.stagedObjects[txID], payload) {
		t.Errorf("staging mismatch: got %q", store.stagedObjects[txID])
	}
	if store.stagedMetas[txID].CRC32 != 99 {
		t.Errorf("staging meta CRC mismatch")
	}

	// 2. Commit
	commitBody, _ := json.Marshal(map[string]string{
		"tx_id":  txID,
		"bucket": "mybucket",
		"key":    "obj.txt",
	})
	req = httptest.NewRequest(http.MethodPost, "/internal/commit", bytes.NewReader(commitBody))
	req.Header.Set("X-MicroS3-Token", "secret-token")
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("commit failed: %d - %s", rec.Code, rec.Body.String())
	}

	// Verify committed
	if !store.committedKeys["mybucket/obj.txt"] {
		t.Errorf("expected key to be committed")
	}
}

func TestInternalS3ProxyForwarding(t *testing.T) {
	store := &mockStorage{buckets: map[string]bool{"mybucket": true}}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())

	// Fake S3 API handler that counts requests
	calledS3 := false
	fakeS3Handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calledS3 = true
		if r.Method != http.MethodGet || r.URL.Path != "/mybucket/file.txt" || r.URL.RawQuery != "list-type=2" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("s3 response"))
	})

	h := NewHandler(store, svc, &mockCluster{status: "READY"}, fakeS3Handler, "secret-token", zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/internal/s3-proxy", nil)
	req.Header.Set("X-MicroS3-Token", "secret-token")
	req.Header.Set("X-MicroS3-Original-Method", http.MethodGet)
	req.Header.Set("X-MicroS3-Original-Path", "/mybucket/file.txt")
	req.Header.Set("X-MicroS3-Original-RawQuery", "list-type=2")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("proxy request failed: %d - %s", rec.Code, rec.Body.String())
	}

	if !calledS3 {
		t.Errorf("s3 handler was not called by proxy")
	}
	if rec.Body.String() != "s3 response" {
		t.Errorf("unexpected proxy response body: %q", rec.Body.String())
	}
}

func TestInternalSyncRequestHandler(t *testing.T) {
	store := &mockStorage{
		buckets:       map[string]bool{"mybucket": true},
		stagedObjects: make(map[string][]byte),
		committedKeys: make(map[string]bool),
		stagedMetas:   make(map[string]s3.ObjectMeta),
		txs:           make(map[string]s3.Transaction),
	}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	coord := &mockSyncCoordinator{}
	svc.SetSyncCoordinator(coord)
	h := NewHandler(store, svc, &mockCluster{status: "READY"}, nil, "secret-token", zap.NewNop())

	req := httptest.NewRequest(http.MethodPost, "/internal/sync-request?node_id=follower-1&internal_addr=http://10.0.0.5:9001", nil)
	req.Header.Set("X-MicroS3-Token", "secret-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	if !coord.called {
		t.Error("expected SyncCoordinator.SyncFollower to be called")
	}
	if coord.nodeID != "follower-1" {
		t.Errorf("expected node_id=follower-1, got %s", coord.nodeID)
	}
	if coord.followerAddr != "http://10.0.0.5:9001" {
		t.Errorf("expected follower_addr=http://10.0.0.5:9001, got %s", coord.followerAddr)
	}
}

func TestInternalSyncRequestMissingParams(t *testing.T) {
	store := &mockStorage{buckets: make(map[string]bool)}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	h := NewHandler(store, svc, &mockCluster{status: "READY"}, nil, "secret-token", zap.NewNop())

	// Missing internal_addr
	req := httptest.NewRequest(http.MethodPost, "/internal/sync-request?node_id=follower-1", nil)
	req.Header.Set("X-MicroS3-Token", "secret-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestInternalSyncDeleteHandler(t *testing.T) {
	store := &mockStorage{
		buckets:       map[string]bool{"mybucket": true},
		stagedObjects: make(map[string][]byte),
		committedKeys: map[string]bool{"mybucket/keep.txt": true, "mybucket/delete.txt": true},
		stagedMetas:   make(map[string]s3.ObjectMeta),
		txs:           make(map[string]s3.Transaction),
	}
	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	h := NewHandler(store, svc, &mockCluster{status: "READY"}, nil, "secret-token", zap.NewNop())

	deleteBody, _ := json.Marshal(struct {
		Keys []KeyInfo `json:"keys"`
	}{
		Keys: []KeyInfo{
			{Bucket: "mybucket", Key: "delete.txt"},
			{Bucket: "mybucket", Key: "extra.txt"},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/sync-delete", bytes.NewReader(deleteBody))
	req.Header.Set("X-MicroS3-Token", "secret-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify keys were deleted from committedKeys
	if store.committedKeys["mybucket/delete.txt"] {
		t.Error("expected delete.txt to be deleted")
	}
	if store.committedKeys["mybucket/extra.txt"] {
		t.Error("expected extra.txt to be deleted")
	}
	if !store.committedKeys["mybucket/keep.txt"] {
		t.Error("expected keep.txt to remain")
	}
}
