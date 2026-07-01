package replication

import (
	"bytes"
	"context"
	"encoding/base64"
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

// Reusing mock implementations for Sync tests

type mockSyncCluster struct {
	status string
}

func (m *mockSyncCluster) NodeID() string                 { return "follower-1" }
func (m *mockSyncCluster) IsLeader() bool                 { return false }
func (m *mockSyncCluster) LeaderInternalAddress() string { return "" }
func (m *mockSyncCluster) AliveFollowers() []string       { return nil }
func (m *mockSyncCluster) Mode() string                   { return "static" }
func (m *mockSyncCluster) MarkDead(nodeID string)         {}
func (m *mockSyncCluster) MarkAlive(nodeID, internalAddr string)         {}
func (m *mockSyncCluster) Status() string                 { return m.status }
func (m *mockSyncCluster) SetLocalStatus(status string)   { m.status = status }

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
func (m *mockSyncStorage) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	return nil, nil
}
func (m *mockSyncStorage) AbortMultipartUpload(bucket, uploadID string) error { return nil }
func (m *mockSyncStorage) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	return s3.MultipartUpload{}, nil
}
func (m *mockSyncStorage) ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error) { return nil, nil }

func TestSyncWorkerSynchronize(t *testing.T) {
	var mu sync.Mutex
	syncStarted := false
	syncDone := false
	getKeysCalled := false

	// 1. Leader mock HTTP server
	leaderSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.URL.Path == "/internal/sync-start" {
			syncStarted = true
			w.WriteHeader(http.StatusOK)
		} else if r.URL.Path == "/internal/sync-done" {
			syncDone = true
			w.WriteHeader(http.StatusOK)
		} else if r.URL.Path == "/internal/sync-heartbeat" {
			w.WriteHeader(http.StatusOK)
		} else if r.URL.Path == "/internal/keys" {
			getKeysCalled = true
			resp := internal_api.KeysResponse{
				Keys: []internal_api.KeyInfo{
					// File that matches local exactly
					{Bucket: "mybucket", Key: "match.txt", CRC32: 111, Size: 5},
					// File that differs locally (out of sync)
					{Bucket: "mybucket", Key: "update.txt", CRC32: 222, Size: 6},
					// New file completely
					{Bucket: "mybucket", Key: "new.txt", CRC32: 333, Size: 7},
				},
				TotalCount: 3,
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
		} else if r.URL.Path == "/internal/object" {
			key := r.URL.Query().Get("key")

			var data []byte
			var meta s3.ObjectMeta

			if key == "update.txt" {
				data = []byte("update")
				meta = s3.ObjectMeta{ContentType: "text/plain", CRC32: 222}
			} else if key == "new.txt" {
				data = []byte("newfile")
				meta = s3.ObjectMeta{ContentType: "text/plain", CRC32: 333}
			}

			metaBytes, _ := json.Marshal(meta)
			w.Header().Set("X-MicroS3-Meta", base64.StdEncoding.EncodeToString(metaBytes))
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		}
	}))
	defer leaderSrv.Close()

	// 2. Setup follower storage
	localStore := &mockSyncStorage{
		buckets: map[string]bool{"mybucket": true},
		objects: map[string][]byte{
			// Match file (same)
			"mybucket/match.txt": []byte("match"),
			// Out of sync file (differs in CRC/size)
			"mybucket/update.txt": []byte("oldval"),
			// Extraneous file (should be deleted)
			"mybucket/delete.txt": []byte("delete"),
		},
		metas: map[string]s3.ObjectMeta{
			"mybucket/match.txt":  {CRC32: 111},
			"mybucket/update.txt": {CRC32: 999}, // different
			"mybucket/delete.txt": {CRC32: 888},
		},
		stagedObjects: make(map[string][]byte),
		stagedMetas:   make(map[string]s3.ObjectMeta),
		txs:           make(map[string]s3.Transaction),
	}

	client := internal_api.NewClient("token", 2*time.Second)
	clusterMock := &mockSyncCluster{status: "SYNCING"}
	// Override leader address returned by cluster
	clusterMockLeAddr := leaderSrv.URL
	
	worker := NewSyncWorker(client, clusterMock, localStore, zap.NewNop(), 9001)

	// Create a wrapper of ClusterManager to inject leader address
	wrappedCluster := &testWrappedCluster{
		ClusterManager: clusterMock,
		leaderAddr:     clusterMockLeAddr,
	}
	worker.cluster = wrappedCluster

	// Run Synchronization
	err := worker.Synchronize(context.Background())
	if err != nil {
		t.Fatalf("Synchronize failed: %v", err)
	}

	// Verify states
	mu.Lock()
	defer mu.Unlock()

	if !syncStarted || !syncDone || !getKeysCalled {
		t.Errorf("Expected sync endpoints to be called on leader: started=%t, keys=%t, done=%t", syncStarted, getKeysCalled, syncDone)
	}

	localStore.mu.Lock()
	defer localStore.mu.Unlock()

	// Verify match.txt is untouched
	if string(localStore.objects["mybucket/match.txt"]) != "match" {
		t.Errorf("match.txt was modified unexpectedly")
	}

	// Verify update.txt is updated to new value
	if string(localStore.objects["mybucket/update.txt"]) != "update" {
		t.Errorf("update.txt was not updated: %s", string(localStore.objects["mybucket/update.txt"]))
	}
	if localStore.metas["mybucket/update.txt"].CRC32 != 222 {
		t.Errorf("update.txt metadata CRC was not updated")
	}

	// Verify new.txt is downloaded
	if string(localStore.objects["mybucket/new.txt"]) != "newfile" {
		t.Errorf("new.txt was not downloaded")
	}

	// Verify delete.txt is removed
	if _, exists := localStore.objects["mybucket/delete.txt"]; exists {
		t.Errorf("delete.txt was not removed")
	}
}

type testWrappedCluster struct {
	s3app.ClusterManager
	leaderAddr string
}

func (t *testWrappedCluster) LeaderInternalAddress() string {
	return t.leaderAddr
}
