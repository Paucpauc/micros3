package s3app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/domain/s3"
	"go.uber.org/zap"
)

// --- Mocks ---

type mockStorage struct {
	buckets       map[string]bool
	objects       map[string][]byte
	objectMetas   map[string]s3.ObjectMeta
	transactions  map[string]s3.Transaction
	stagedObjects map[string][]byte
	stagedMetas   map[string]s3.ObjectMeta

	multipartUploads map[string]s3.MultipartUpload
	multipartParts   map[string]map[int]s3.UploadPart
	multipartReaders map[string]map[int][]byte
	deletedParts     []int
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		buckets:          make(map[string]bool),
		objects:          make(map[string][]byte),
		objectMetas:      make(map[string]s3.ObjectMeta),
		transactions:     make(map[string]s3.Transaction),
		stagedObjects:    make(map[string][]byte),
		stagedMetas:      make(map[string]s3.ObjectMeta),
		multipartUploads: make(map[string]s3.MultipartUpload),
		multipartParts:   make(map[string]map[int]s3.UploadPart),
		multipartReaders: make(map[string]map[int][]byte),
	}
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
	var list []string
	for b := range m.buckets {
		list = append(list, b)
	}
	return list, nil
}

func (m *mockStorage) StageObject(txID string, r io.Reader, size int64, meta s3.ObjectMeta, tx s3.Transaction) (s3.ObjectMeta, error) {
	m.transactions[txID] = tx
	if tx.Operation == s3.OpPut {
		data, err := io.ReadAll(r)
		if err != nil {
			return s3.ObjectMeta{}, err
		}
		meta.ContentLength = int64(len(data))
		if meta.ETag == "" {
			meta.ETag = "\"mock-etag\""
		}
		meta.CRC32 = 12345
		m.stagedObjects[txID] = data
		m.stagedMetas[txID] = meta
	}
	return m.stagedMetas[txID], nil
}

func (m *mockStorage) CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error) {
	tx, exists := m.transactions[txID]
	if !exists {
		return s3.ObjectMeta{}, errors.New("tx not found")
	}
	tx.State = s3.TxCommitted
	m.transactions[txID] = tx

	path := bucket + "/" + key
	if tx.Operation == s3.OpPut {
		m.objects[path] = m.stagedObjects[txID]
		m.objectMetas[path] = m.stagedMetas[txID]
		return m.stagedMetas[txID], nil
	} else if tx.Operation == s3.OpDelete {
		delete(m.objects, path)
		delete(m.objectMetas, path)
	}
	return s3.ObjectMeta{}, nil
}

func (m *mockStorage) AbortTransaction(txID string) error {
	tx, exists := m.transactions[txID]
	if exists {
		tx.State = s3.TxAborted
		m.transactions[txID] = tx
	}
	delete(m.stagedObjects, txID)
	delete(m.stagedMetas, txID)
	return nil
}

func (m *mockStorage) GetTransaction(txID string) (s3.Transaction, error) {
	return m.transactions[txID], nil
}

func (m *mockStorage) GetStagedObjectReader(txID string) (io.ReadCloser, error) {
	data, exists := m.stagedObjects[txID]
	if !exists {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockStorage) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	path := bucket + "/" + key
	data, exists := m.objects[path]
	if !exists {
		return nil, s3.ObjectMeta{}, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), m.objectMetas[path], nil
}

func (m *mockStorage) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	path := bucket + "/" + key
	meta, exists := m.objectMetas[path]
	if !exists {
		return s3.ObjectMeta{}, errors.New("not found")
	}
	return meta, nil
}

func (m *mockStorage) DeleteObject(bucket, key string) error {
	path := bucket + "/" + key
	delete(m.objects, path)
	delete(m.objectMetas, path)
	return nil
}

func (m *mockStorage) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	return s3.ListObjectsResult{}, nil
}

func (m *mockStorage) CreateMultipartUpload(bucket, key string) (string, error) {
	id := "upload-xyz"
	m.multipartUploads[id] = s3.MultipartUpload{
		UploadID:  id,
		Bucket:    bucket,
		Key:       key,
		Initiated: time.Now(),
	}
	m.multipartParts[id] = make(map[int]s3.UploadPart)
	m.multipartReaders[id] = make(map[int][]byte)
	return id, nil
}

func (m *mockStorage) SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error) {
	data, _ := io.ReadAll(r)
	m.multipartReaders[uploadID][partNum] = data
	part := s3.UploadPart{
		PartNumber: partNum,
		Size:       int64(len(data)),
		ETag:       "\"0123456789abcdef0123456789abcdef\"",
		CRC32:      999,
		ModifiedAt: time.Now(),
	}
	m.multipartParts[uploadID][partNum] = part
	return part, nil
}

func (m *mockStorage) GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error) {
	data := m.multipartReaders[uploadID][partNum]
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockStorage) DeleteMultipartPart(bucket, uploadID string, partNum int) error {
	m.deletedParts = append(m.deletedParts, partNum)
	if m.multipartParts[uploadID] != nil {
		delete(m.multipartParts[uploadID], partNum)
	}
	if m.multipartReaders[uploadID] != nil {
		delete(m.multipartReaders[uploadID], partNum)
	}
	return nil
}

func (m *mockStorage) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	var list []s3.UploadPart
	for _, p := range m.multipartParts[uploadID] {
		list = append(list, p)
	}
	return list, nil
}

func (m *mockStorage) AbortMultipartUpload(bucket, uploadID string) error {
	delete(m.multipartUploads, uploadID)
	delete(m.multipartParts, uploadID)
	delete(m.multipartReaders, uploadID)
	return nil
}

func (m *mockStorage) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	return m.multipartUploads[uploadID], nil
}

func (m *mockStorage) ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error) {
	return nil, nil
}

// EC shard no-op stubs (EC is not exercised in unit tests).
func (m *mockStorage) PutECShard(bucket, key string, shardIndex int, r io.Reader, size int64, meta s3.ObjectMeta) error {
	return nil
}
func (m *mockStorage) GetECShard(bucket, key string, shardIndex int) (io.ReadCloser, error) {
	return nil, fmt.Errorf("no EC shard")
}
func (m *mockStorage) HasECShard(bucket, key string, shardIndex int) (bool, error) {
	return false, nil
}
func (m *mockStorage) DeleteECShard(bucket, key string, shardIndex int) error        { return nil }
func (m *mockStorage) UpdateObjectMeta(bucket, key string, meta s3.ObjectMeta) error { return nil }
func (m *mockStorage) RemoveReplicaData(bucket, key string) error                    { return nil }

// Replicator Mock

type mockReplicator struct {
	prepareErr error
	commitErr  error
	aborted    []string
	prepared   []s3.Transaction
	committed  []string
}

func (m *mockReplicator) PrepareAll(ctx context.Context, tx s3.Transaction, meta s3.ObjectMeta) map[string]error {
	m.prepared = append(m.prepared, tx)
	errs := make(map[string]error)
	if m.prepareErr != nil {
		errs["node-2"] = m.prepareErr
	}
	return errs
}

func (m *mockReplicator) CommitAll(ctx context.Context, txID, bucket, key string) map[string]error {
	m.committed = append(m.committed, txID)
	errs := make(map[string]error)
	if m.commitErr != nil {
		errs["node-2"] = m.commitErr
	}
	return errs
}

func (m *mockReplicator) AbortAll(ctx context.Context, txID string) map[string]error {
	m.aborted = append(m.aborted, txID)
	return nil
}

// ClusterManager Mock

type mockClusterManager struct {
	followers  []string
	deadNodes  []string
	aliveNodes map[string]string // nodeID -> internalAddr
}

func (m *mockClusterManager) NodeID() string                { return "node-1" }
func (m *mockClusterManager) IsLeader() bool                { return true }
func (m *mockClusterManager) LeaderInternalAddress() string { return "localhost:9001" }
func (m *mockClusterManager) AliveFollowers() []string      { return m.followers }
func (m *mockClusterManager) KnownFollowers() []string      { return m.followers }
func (m *mockClusterManager) Mode() string                  { return "static" }
func (m *mockClusterManager) MarkDead(nodeID string) {
	m.deadNodes = append(m.deadNodes, nodeID)
}
func (m *mockClusterManager) MarkAlive(nodeID, internalAddr string) {
	if m.aliveNodes == nil {
		m.aliveNodes = make(map[string]string)
	}
	m.aliveNodes[nodeID] = internalAddr
}
func (m *mockClusterManager) RegisterFollower(nodeID, internalAddr string) {}
func (m *mockClusterManager) RefreshFollowers(_ context.Context)           {}
func (m *mockClusterManager) Status() string                               { return "READY" }
func (m *mockClusterManager) SetLocalStatus(status string)                 {}

// SyncCoordinator Mock

type mockSyncCoordinator struct {
	syncCalled bool
	syncNodeID string
	syncAddr   string
	syncErr    error
}

func (m *mockSyncCoordinator) SyncFollower(ctx context.Context, nodeID, followerAddr string) error {
	m.syncCalled = true
	m.syncNodeID = nodeID
	m.syncAddr = followerAddr
	return m.syncErr
}

// MetricsRecorder Mock (no-op)

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

// --- Tests ---
func TestPutObjectStandaloneSuccess(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	_ = service.CreateBucket("test-bucket")
	content := []byte("object content")

	meta, err := service.PutObject(context.Background(), "test-bucket", "obj.txt", bytes.NewReader(content), int64(len(content)), s3.ObjectMeta{})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if meta.ContentLength != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), meta.ContentLength)
	}

	// Verify locally committed
	readCloser, _, err := service.GetObject("test-bucket", "obj.txt")
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	readContent, _ := io.ReadAll(readCloser)
	if !bytes.Equal(readContent, content) {
		t.Errorf("read mismatch: got %q", readContent)
	}
}

func TestPutObjectReplicationSuccess(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{followers: []string{"node-2"}}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	content := []byte("replicated content")
	_, err := service.PutObject(context.Background(), "test-bucket", "obj.txt", bytes.NewReader(content), int64(len(content)), s3.ObjectMeta{})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Verify replicator received prepare and commit calls
	if len(replicator.prepared) != 1 {
		t.Errorf("expected 1 prepare call, got %d", len(replicator.prepared))
	}
	if len(replicator.committed) != 1 {
		t.Errorf("expected 1 commit call, got %d", len(replicator.committed))
	}
}

func TestPutObjectReplicationPrepareFailure(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{prepareErr: errors.New("network timeout")}
	cluster := &mockClusterManager{followers: []string{"node-2"}}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	content := []byte("failed replication content")
	_, err := service.PutObject(context.Background(), "test-bucket", "obj.txt", bytes.NewReader(content), int64(len(content)), s3.ObjectMeta{})
	if err == nil {
		t.Fatalf("expected error from failed replication prepare")
	}

	// Verify replicator abort was called
	if len(replicator.aborted) != 1 {
		t.Errorf("expected abort to be called once, got %d", len(replicator.aborted))
	}

	// Verify cluster marked node-2 as dead
	if len(cluster.deadNodes) != 1 || cluster.deadNodes[0] != "node-2" {
		t.Errorf("expected node-2 to be marked dead, got %v", cluster.deadNodes)
	}

	// Local transaction state should be aborted/cleaned
	if len(storage.objects) != 0 {
		t.Errorf("object should not be stored locally after abort")
	}
}

func TestPutObjectWaitBehavior(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	_ = service.CreateBucket("test-bucket")
	content := []byte("waiting content")

	// 1. Block writes by starting a sync lease
	service.StartSyncLease("node-2")
	service.SetWriteBlockBehavior("wait")

	errChan := make(chan error, 1)
	go func() {
		_, err := service.PutObject(context.Background(), "test-bucket", "obj.txt", bytes.NewReader(content), int64(len(content)), s3.ObjectMeta{})
		errChan <- err
	}()

	// 2. Sleep briefly to ensure the goroutine is blocked
	select {
	case err := <-errChan:
		t.Fatalf("expected write to block, but it returned immediately with err: %v", err)
	case <-time.After(100 * time.Millisecond):
		// Success: it is blocked as expected
	}

	// 3. Unblock writes
	service.EndSyncLease("node-2")

	// 4. Verify the write completes successfully
	select {
	case err := <-errChan:
		if err != nil {
			t.Fatalf("PutObject failed after unblocking: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for PutObject to complete after unblocking")
	}
}

func TestCompleteMultipartUploadDeletesPartsOnTheFly(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	bucket := "test-bucket"
	key := "large-file.bin"
	_ = service.CreateBucket(bucket)

	uploadID, err := service.CreateMultipartUpload(bucket, key)
	if err != nil {
		t.Fatalf("failed to create multipart upload: %v", err)
	}

	// Save 3 parts
	part1 := []byte("part1-data-")
	part2 := []byte("part2-data-")
	part3 := []byte("part3-data")

	p1, err := service.SaveMultipartPart(bucket, uploadID, 1, bytes.NewReader(part1))
	if err != nil {
		t.Fatalf("failed to save part 1: %v", err)
	}
	p2, err := service.SaveMultipartPart(bucket, uploadID, 2, bytes.NewReader(part2))
	if err != nil {
		t.Fatalf("failed to save part 2: %v", err)
	}
	p3, err := service.SaveMultipartPart(bucket, uploadID, 3, bytes.NewReader(part3))
	if err != nil {
		t.Fatalf("failed to save part 3: %v", err)
	}

	requestedParts := []s3.CompletePart{
		{PartNumber: 1, ETag: p1.ETag},
		{PartNumber: 2, ETag: p2.ETag},
		{PartNumber: 3, ETag: p3.ETag},
	}

	meta, err := service.CompleteMultipartUpload(context.Background(), bucket, key, uploadID, requestedParts)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	// Verify the final concatenated content length
	expectedLen := int64(len(part1) + len(part2) + len(part3))
	if meta.ContentLength != expectedLen {
		t.Errorf("expected ContentLength %d, got %d", expectedLen, meta.ContentLength)
	}

	// Verify that DeleteMultipartPart was called for parts 1, 2, and 3
	expectedDeleted := []int{1, 2, 3}
	if len(storage.deletedParts) != 3 {
		t.Fatalf("expected 3 deleted parts, got %d: %v", len(storage.deletedParts), storage.deletedParts)
	}
	for i, v := range expectedDeleted {
		if storage.deletedParts[i] != v {
			t.Errorf("expected deleted part at index %d to be %d, got %d", i, v, storage.deletedParts[i])
		}
	}
}

func TestHandleSyncRequestSuccess(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	coord := &mockSyncCoordinator{}
	service.SetSyncCoordinator(coord)

	err := service.HandleSyncRequest(context.Background(), "follower-1", "http://10.0.0.5:9001")
	if err != nil {
		t.Fatalf("HandleSyncRequest failed: %v", err)
	}

	if !coord.syncCalled {
		t.Error("expected SyncCoordinator.SyncFollower to be called")
	}
	if coord.syncNodeID != "follower-1" {
		t.Errorf("expected node_id=follower-1, got %s", coord.syncNodeID)
	}
	if coord.syncAddr != "http://10.0.0.5:9001" {
		t.Errorf("expected follower_addr=http://10.0.0.5:9001, got %s", coord.syncAddr)
	}

	// Verify follower was marked alive
	if cluster.aliveNodes["follower-1"] != "http://10.0.0.5:9001" {
		t.Error("expected follower-1 to be marked alive with correct address")
	}

	// Verify writes are no longer blocked after sync
	if service.IsWritesBlocked() {
		t.Error("expected writes to be unblocked after sync completion")
	}
}

func TestHandleSyncRequestNoCoordinator(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	// No coordinator set
	err := service.HandleSyncRequest(context.Background(), "follower-1", "http://10.0.0.5:9001")
	if err == nil {
		t.Fatal("expected error when sync coordinator is not configured")
	}
}

func TestHandleSyncRequestCoordinatorError(t *testing.T) {
	storage := newMockStorage()
	replicator := &mockReplicator{}
	cluster := &mockClusterManager{}
	service := NewService(storage, replicator, cluster, &mockMetricsRecorder{}, zap.NewNop())

	coord := &mockSyncCoordinator{syncErr: errors.New("network failure")}
	service.SetSyncCoordinator(coord)

	err := service.HandleSyncRequest(context.Background(), "follower-1", "http://10.0.0.5:9001")
	if err == nil {
		t.Fatal("expected error from coordinator failure")
	}

	// Verify follower was NOT marked alive
	if _, exists := cluster.aliveNodes["follower-1"]; exists {
		t.Error("expected follower to NOT be marked alive on sync failure")
	}

	// Verify writes are unblocked even on failure (lease ended)
	if service.IsWritesBlocked() {
		t.Error("expected writes to be unblocked after sync failure (lease should end)")
	}
}
