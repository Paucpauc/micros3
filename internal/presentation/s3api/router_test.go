package s3api

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"go.uber.org/zap"
)

// Reusable mock implementations

type mockCluster struct{}

func (m *mockCluster) NodeID() string                               { return "node-1" }
func (m *mockCluster) IsLeader() bool                               { return true }
func (m *mockCluster) LeaderInternalAddress() string                { return "" }
func (m *mockCluster) AliveFollowers() []string                     { return nil }
func (m *mockCluster) KnownFollowers() []string                     { return nil }
func (m *mockCluster) Mode() string                                 { return "single" }
func (m *mockCluster) MarkDead(nodeID string)                       {}
func (m *mockCluster) MarkAlive(nodeID, internalAddr string)        {}
func (m *mockCluster) RegisterFollower(nodeID, internalAddr string) {}
func (m *mockCluster) Status() string                               { return "READY" }
func (m *mockCluster) SetLocalStatus(status string)                 {}

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

type mockStorage struct {
	buckets map[string]bool
	objects map[string][]byte
	metas   map[string]s3.ObjectMeta
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
	data, _ := io.ReadAll(r)
	meta.ContentLength = int64(len(data))
	meta.ETag = "\"mocked-etag\""
	m.objects[txID] = data
	m.metas[txID] = meta
	return meta, nil
}
func (m *mockStorage) CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error) {
	path := bucket + "/" + key
	m.objects[path] = m.objects[txID]
	m.metas[path] = m.metas[txID]
	return m.metas[path], nil
}
func (m *mockStorage) AbortTransaction(txID string) error {
	return nil
}
func (m *mockStorage) GetTransaction(txID string) (s3.Transaction, error) {
	return s3.Transaction{}, nil
}
func (m *mockStorage) GetStagedObjectReader(txID string) (io.ReadCloser, error) {
	data, exists := m.objects[txID]
	if !exists {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (m *mockStorage) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	path := bucket + "/" + key
	data := m.objects[path]
	meta := m.metas[path]
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}
func (m *mockStorage) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	path := bucket + "/" + key
	return m.metas[path], nil
}
func (m *mockStorage) DeleteObject(bucket, key string) error {
	path := bucket + "/" + key
	delete(m.objects, path)
	delete(m.metas, path)
	return nil
}
func (m *mockStorage) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	var contents []s3.ObjectInfo
	for path, data := range m.objects {
		if strings.HasPrefix(path, bucket+"/") {
			key := strings.TrimPrefix(path, bucket+"/")
			contents = append(contents, s3.ObjectInfo{
				Key:          key,
				Size:         int64(len(data)),
				ETag:         "\"mocked-etag\"",
				LastModified: time.Now(),
			})
		}
	}
	return s3.ListObjectsResult{
		Name:     bucket,
		Contents: contents,
	}, nil
}
func (m *mockStorage) CreateMultipartUpload(bucket, key string) (string, error) { return "up-123", nil }
func (m *mockStorage) SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error) {
	return s3.UploadPart{PartNumber: partNum, ETag: "\"part-etag\"", Size: 10}, nil
}
func (m *mockStorage) GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader([]byte("part-data"))), nil
}
func (m *mockStorage) DeleteMultipartPart(bucket, uploadID string, partNum int) error {
	return nil
}
func (m *mockStorage) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	return []s3.UploadPart{{PartNumber: 1, ETag: "\"part-etag\"", Size: 10}}, nil
}
func (m *mockStorage) AbortMultipartUpload(bucket, uploadID string) error { return nil }
func (m *mockStorage) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	return s3.MultipartUpload{Bucket: bucket, Key: "mykey"}, nil
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

func TestS3APIRouting(t *testing.T) {
	store := &mockStorage{
		buckets: make(map[string]bool),
		objects: make(map[string][]byte),
		metas:   make(map[string]s3.ObjectMeta),
	}
	_ = store.CreateBucket("mybucket")

	svc := s3app.NewService(store, &mockReplicator{}, &mockCluster{}, &mockMetricsRecorder{}, zap.NewNop())
	handler := NewHandler(svc, nil, &mockCluster{}, "token", false, zap.NewNop())

	// Test 1: Put Object
	body := []byte("hello s3 api")
	req := httptest.NewRequest(http.MethodPut, "/mybucket/file.txt", bytes.NewReader(body))
	req.Header.Set("Content-Length", "12")
	req.Header.Set("Content-Type", "text/plain")

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d. Body: %s", rec.Code, rec.Body.String())
	}

	// Test 2: Get Object
	req = httptest.NewRequest(http.MethodGet, "/mybucket/file.txt", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
	if rec.Body.String() != "hello s3 api" {
		t.Errorf("expected 'hello s3 api', got %q", rec.Body.String())
	}
	if rec.Header().Get("Content-Type") != "text/plain" {
		t.Errorf("expected text/plain, got %q", rec.Header().Get("Content-Type"))
	}

	// Test 3: List Objects XML
	req = httptest.NewRequest(http.MethodGet, "/mybucket?list-type=2", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	var listResult ListBucketResult
	err := xml.NewDecoder(rec.Body).Decode(&listResult)
	if err != nil {
		t.Fatalf("failed to decode list objects xml: %v", err)
	}

	if listResult.Name != "mybucket" {
		t.Errorf("expected name 'mybucket', got %s", listResult.Name)
	}
	if len(listResult.Contents) != 1 || listResult.Contents[0].Key != "file.txt" {
		t.Errorf("unexpected contents: %+v", listResult.Contents)
	}

	// Test 4: Health Check
	req = httptest.NewRequest(http.MethodGet, "/health", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 health check, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"status":"OK"`) {
		t.Errorf("unexpected health body: %q", rec.Body.String())
	}

	// Test 5: Metrics Endpoint
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 metrics, got %d", rec.Code)
	}
	metricsStr := rec.Body.String()
	if !strings.Contains(metricsStr, "micros3_requests_total") {
		t.Errorf("expected micros3_requests_total in metrics, got:\n%s", metricsStr)
	}
	if !strings.Contains(metricsStr, "micros3_cluster_role") {
		t.Errorf("expected micros3_cluster_role in metrics, got:\n%s", metricsStr)
	}
}

func TestRouterAllowLocalReads(t *testing.T) {
	store := &mockStorage{
		buckets: make(map[string]bool),
		objects: make(map[string][]byte),
		metas:   make(map[string]s3.ObjectMeta),
	}
	_ = store.CreateBucket("mybucket")
	_, _ = store.StageObject("tx-1", bytes.NewReader([]byte("data")), 4, s3.ObjectMeta{}, s3.Transaction{})
	_, _ = store.CommitTransaction("tx-1", "mybucket", "file.txt")

	// Mock follower cluster manager
	followerCluster := &mockClusterManager{
		isLeader: false,
		status:   "READY",
	}
	svc := s3app.NewService(store, &mockReplicator{}, followerCluster, &mockMetricsRecorder{}, zap.NewNop())

	// 1. With allowLocalReads = false: GET request should be proxied
	handlerWithoutLocalReads := NewHandler(svc, nil, followerCluster, "token", false, zap.NewNop())
	req := httptest.NewRequest(http.MethodGet, "/mybucket/file.txt", nil)
	rec := httptest.NewRecorder()
	handlerWithoutLocalReads.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable when proxying with no leader address, got %d", rec.Code)
	}

	// 2. With allowLocalReads = true: GET request should be handled locally
	handlerWithLocalReads := NewHandler(svc, nil, followerCluster, "token", true, zap.NewNop())
	req = httptest.NewRequest(http.MethodGet, "/mybucket/file.txt", nil)
	rec = httptest.NewRecorder()
	handlerWithLocalReads.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK when handling GET locally, got %d", rec.Code)
	}

	// 3. With allowLocalReads = true but status not READY: GET request should still be proxied
	notReadyCluster := &mockClusterManager{
		isLeader: false,
		status:   "SYNCING",
	}
	svcNotReady := s3app.NewService(store, &mockReplicator{}, notReadyCluster, &mockMetricsRecorder{}, zap.NewNop())
	handlerNotReady := NewHandler(svcNotReady, nil, notReadyCluster, "token", true, zap.NewNop())
	req = httptest.NewRequest(http.MethodGet, "/mybucket/file.txt", nil)
	rec = httptest.NewRecorder()
	handlerNotReady.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable when status is not READY, got %d", rec.Code)
	}

	// 4. PUT request should always be proxied
	req = httptest.NewRequest(http.MethodPut, "/mybucket/file.txt", bytes.NewReader([]byte("new data")))
	rec = httptest.NewRecorder()
	handlerWithLocalReads.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503 Service Unavailable for PUT on follower, got %d", rec.Code)
	}
}

type mockClusterManager struct {
	isLeader bool
	status   string
}

func (m *mockClusterManager) NodeID() string                               { return "node" }
func (m *mockClusterManager) IsLeader() bool                               { return m.isLeader }
func (m *mockClusterManager) LeaderInternalAddress() string                { return "" }
func (m *mockClusterManager) AliveFollowers() []string                     { return nil }
func (m *mockClusterManager) KnownFollowers() []string                     { return nil }
func (m *mockClusterManager) Mode() string                                 { return "static" }
func (m *mockClusterManager) MarkDead(nodeID string)                       {}
func (m *mockClusterManager) MarkAlive(nodeID, internalAddr string)        {}
func (m *mockClusterManager) RegisterFollower(nodeID, internalAddr string) {}
func (m *mockClusterManager) Status() string                               { return m.status }
func (m *mockClusterManager) SetLocalStatus(status string)                 {}
