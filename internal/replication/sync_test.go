package replication

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

// --- Mocks ---

type mockSyncCluster struct {
	status string
}

func (m *mockSyncCluster) NodeID() string                        { return "follower-1" }
func (m *mockSyncCluster) IsLeader() bool                        { return false }
func (m *mockSyncCluster) LeaderInternalAddress() string         { return "" }
func (m *mockSyncCluster) AliveFollowers() []string              { return nil }
func (m *mockSyncCluster) Mode() string                          { return "static" }
func (m *mockSyncCluster) MarkDead(nodeID string)                {}
func (m *mockSyncCluster) MarkAlive(nodeID, internalAddr string) {}
func (m *mockSyncCluster) Status() string                        { return m.status }
func (m *mockSyncCluster) SetLocalStatus(status string)          { m.status = status }

type mockSyncStorage struct {
	mu            sync.Mutex
	buckets       map[string]bool
	objects       map[string][]byte
	metas         map[string]s3.ObjectMeta
	stagedObjects map[string][]byte
	stagedMetas   map[string]s3.ObjectMeta
	txs           map[string]s3.Transaction
}

func (m *mockSyncStorage) CreateBucket(bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.buckets[bucket] = true
	return nil
}
func (m *mockSyncStorage) DeleteBucket(bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.buckets, bucket)
	return nil
}
func (m *mockSyncStorage) HasBucket(bucket string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.buckets[bucket], nil
}
func (m *mockSyncStorage) ListBuckets() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var l []string
	for b := range m.buckets {
		l = append(l, b)
	}
	return l, nil
}
func (m *mockSyncStorage) StageObject(txID string, r io.Reader, size int64, meta s3.ObjectMeta, tx s3.Transaction) (s3.ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.txs[txID] = tx
	if tx.Operation == s3.OpPut {
		data, _ := io.ReadAll(r)
		m.stagedObjects[txID] = data
		meta.ContentLength = int64(len(data))
		m.stagedMetas[txID] = meta
	}
	return meta, nil
}
func (m *mockSyncStorage) CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := bucket + "/" + key
	m.objects[path] = m.stagedObjects[txID]
	m.metas[path] = m.stagedMetas[txID]
	return m.metas[path], nil
}
func (m *mockSyncStorage) AbortTransaction(txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.stagedObjects, txID)
	return nil
}
func (m *mockSyncStorage) GetTransaction(txID string) (s3.Transaction, error) {
	return s3.Transaction{}, nil
}
func (m *mockSyncStorage) GetStagedObjectReader(txID string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockSyncStorage) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := bucket + "/" + key
	data := m.objects[path]
	meta := m.metas[path]
	return io.NopCloser(bytes.NewReader(data)), meta, nil
}
func (m *mockSyncStorage) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := bucket + "/" + key
	meta := m.metas[path]
	return meta, nil
}
func (m *mockSyncStorage) DeleteObject(bucket, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	path := bucket + "/" + key
	delete(m.objects, path)
	delete(m.metas, path)
	return nil
}
func (m *mockSyncStorage) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var contents []s3.ObjectInfo
	for path, data := range m.objects {
		if strings.HasPrefix(path, bucket+"/") {
			key := strings.TrimPrefix(path, bucket+"/")
			contents = append(contents, s3.ObjectInfo{
				Key:          key,
				Size:         int64(len(data)),
				LastModified: time.Now(),
			})
		}
	}
	return s3.ListObjectsResult{
		Name:     bucket,
		Contents: contents,
	}, nil
}
func (m *mockSyncStorage) CreateMultipartUpload(bucket, key string) (string, error) { return "", nil }
func (m *mockSyncStorage) SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error) {
	return s3.UploadPart{}, nil
}
func (m *mockSyncStorage) GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockSyncStorage) DeleteMultipartPart(bucket, uploadID string, partNum int) error {
	return nil
}
func (m *mockSyncStorage) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	return nil, nil
}
func (m *mockSyncStorage) AbortMultipartUpload(bucket, uploadID string) error { return nil }
func (m *mockSyncStorage) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	return s3.MultipartUpload{}, nil
}
func (m *mockSyncStorage) ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error) {
	return nil, nil
}

// EC shard no-op stubs (EC is not exercised in unit tests).
func (m *mockSyncStorage) PutECShard(bucket, key string, shardIndex int, r io.Reader, size int64, meta s3.ObjectMeta) error {
	return nil
}
func (m *mockSyncStorage) GetECShard(bucket, key string, shardIndex int) (io.ReadCloser, error) {
	return nil, io.ErrUnexpectedEOF
}
func (m *mockSyncStorage) HasECShard(bucket, key string, shardIndex int) (bool, error) {
	return false, nil
}
func (m *mockSyncStorage) DeleteECShard(bucket, key string, shardIndex int) error        { return nil }
func (m *mockSyncStorage) UpdateObjectMeta(bucket, key string, meta s3.ObjectMeta) error { return nil }
func (m *mockSyncStorage) RemoveReplicaData(bucket, key string) error                    { return nil }

// --- Leader-driven SyncCoordinator test ---

func TestSyncCoordinatorSyncFollower(t *testing.T) {
	var mu sync.Mutex
	getKeysCalled := false
	prepareCalled := false
	commitCalled := false
	syncDeleteCalled := false
	var deletedKeys []internal_api.KeyInfo

	// Follower mock HTTP server — the leader will call these endpoints
	followerSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.URL.Path == "/internal/keys" {
			getKeysCalled = true
			// Follower reports its current keys
			resp := internal_api.KeysResponse{
				Keys: []internal_api.KeyInfo{
					// File that matches leader exactly — should NOT be pushed
					{Bucket: "mybucket", Key: "match.txt", CRC32: 111, Size: 5},
					// File that differs from leader — should be pushed (updated)
					{Bucket: "mybucket", Key: "update.txt", CRC32: 999, Size: 7},
					// Extraneous file — should be deleted
					{Bucket: "mybucket", Key: "delete.txt", CRC32: 888, Size: 6},
				},
				TotalCount: 3,
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/internal/prepare" {
			prepareCalled = true
			// Read the body (object data pushed by leader)
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "PREPARED"})
		} else if r.URL.Path == "/internal/commit" {
			commitCalled = true
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "COMMITTED"})
		} else if r.URL.Path == "/internal/sync-delete" {
			syncDeleteCalled = true
			var body struct {
				Keys []internal_api.KeyInfo `json:"keys"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			deletedKeys = body.Keys
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "DELETED"})
		}
	}))
	defer followerSrv.Close()

	// Leader storage — contains the authoritative state
	leaderStore := &mockSyncStorage{
		buckets: map[string]bool{"mybucket": true},
		objects: map[string][]byte{
			"mybucket/match.txt":  []byte("match"),
			"mybucket/update.txt": []byte("update"),
			"mybucket/new.txt":    []byte("newfile"),
		},
		metas: map[string]s3.ObjectMeta{
			"mybucket/match.txt":  {CRC32: 111},
			"mybucket/update.txt": {CRC32: 222},
			"mybucket/new.txt":    {CRC32: 333},
		},
		stagedObjects: make(map[string][]byte),
		stagedMetas:   make(map[string]s3.ObjectMeta),
		txs:           make(map[string]s3.Transaction),
	}

	client := internal_api.NewClient("token", 2*time.Second)
	coord := NewSyncCoordinator(client, leaderStore, zap.NewNop())

	err := coord.SyncFollower(context.Background(), "follower-1", followerSrv.URL)
	if err != nil {
		t.Fatalf("SyncFollower failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if !getKeysCalled {
		t.Error("Expected leader to call /internal/keys on follower")
	}
	if !prepareCalled {
		t.Error("Expected leader to call /internal/prepare on follower")
	}
	if !commitCalled {
		t.Error("Expected leader to call /internal/commit on follower")
	}
	if !syncDeleteCalled {
		t.Error("Expected leader to call /internal/sync-delete on follower")
	}

	// Verify that the extraneous "delete.txt" was requested for deletion
	foundDelete := false
	for _, k := range deletedKeys {
		if k.Bucket == "mybucket" && k.Key == "delete.txt" {
			foundDelete = true
		}
	}
	if !foundDelete {
		t.Error("Expected delete.txt to be in sync-delete request")
	}
}

// --- Follower SyncWorker test ---

func TestSyncWorkerSynchronize(t *testing.T) {
	var mu sync.Mutex
	syncRequestCalled := false
	var receivedNodeID string
	var receivedInternalAddr string

	// Leader mock HTTP server — receives sync-request from follower
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.URL.Path == "/internal/sync-request" {
			syncRequestCalled = true
			receivedNodeID = r.URL.Query().Get("node_id")
			receivedInternalAddr = r.URL.Query().Get("internal_addr")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer leaderSrv.Close()

	client := internal_api.NewClient("token", 2*time.Second)
	clusterMock := &mockSyncCluster{status: "SYNCING"}

	worker := NewSyncWorker(client, clusterMock, zap.NewNop(), 9001)

	// Inject leader address via wrapper
	wrappedCluster := &testWrappedCluster{
		ClusterManager: clusterMock,
		leaderAddr:     leaderSrv.URL,
	}
	worker.cluster = wrappedCluster

	// Set POD_IP so selfInternalAddr is populated
	t.Setenv("POD_IP", "10.0.0.5")
	// Rebuild worker with POD_IP set
	worker = NewSyncWorker(client, wrappedCluster, zap.NewNop(), 9001)

	err := worker.Synchronize(context.Background())
	if err != nil {
		t.Fatalf("Synchronize failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if !syncRequestCalled {
		t.Error("Expected follower to call /internal/sync-request on leader")
	}
	if receivedNodeID != "follower-1" {
		t.Errorf("Expected node_id=follower-1, got %s", receivedNodeID)
	}
	if receivedInternalAddr != "http://10.0.0.5:9001" {
		t.Errorf("Expected internal_addr=http://10.0.0.5:9001, got %s", receivedInternalAddr)
	}
}

type testWrappedCluster struct {
	s3app.ClusterManager
	leaderAddr string
}

func (t *testWrappedCluster) LeaderInternalAddress() string {
	return t.leaderAddr
}
