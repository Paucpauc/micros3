package replication

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

// Mocks for tests

type mockCluster struct {
	followers []string
}

func (m *mockCluster) NodeID() string                 { return "leader" }
func (m *mockCluster) IsLeader() bool                 { return true }
func (m *mockCluster) LeaderInternalAddress() string { return "" }
func (m *mockCluster) AliveFollowers() []string       { return m.followers }
func (m *mockCluster) Mode() string                   { return "static" }
func (m *mockCluster) MarkDead(nodeID string)         {}
func (m *mockCluster) MarkAlive(nodeID, internalAddr string)         {}
func (m *mockCluster) Status() string                 { return "READY" }
func (m *mockCluster) SetLocalStatus(status string)   {}

type mockStorage struct {
	stagedData []byte
}

func (m *mockStorage) StageObject(txID string, r io.Reader, size int64, meta s3.ObjectMeta, tx s3.Transaction) (s3.ObjectMeta, error) {
	return s3.ObjectMeta{}, nil
}
func (m *mockStorage) CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error) {
	return s3.ObjectMeta{}, nil
}
func (m *mockStorage) AbortTransaction(txID string) error { return nil }
func (m *mockStorage) GetTransaction(txID string) (s3.Transaction, error) {
	return s3.Transaction{}, nil
}
func (m *mockStorage) GetStagedObjectReader(txID string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.stagedData)), nil
}
func (m *mockStorage) CreateBucket(bucket string) error         { return nil }
func (m *mockStorage) DeleteBucket(bucket string) error         { return nil }
func (m *mockStorage) HasBucket(bucket string) (bool, error)    { return true, nil }
func (m *mockStorage) ListBuckets() ([]string, error)           { return nil, nil }
func (m *mockStorage) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	return nil, s3.ObjectMeta{}, nil
}
func (m *mockStorage) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	return s3.ObjectMeta{}, nil
}
func (m *mockStorage) DeleteObject(bucket, key string) error { return nil }
func (m *mockStorage) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	return s3.ListObjectsResult{}, nil
}
func (m *mockStorage) CreateMultipartUpload(bucket, key string) (string, error) { return "", nil }
func (m *mockStorage) SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error) {
	return s3.UploadPart{}, nil
}
func (m *mockStorage) GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockStorage) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	return nil, nil
}
func (m *mockStorage) AbortMultipartUpload(bucket, uploadID string) error { return nil }
func (m *mockStorage) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	return s3.MultipartUpload{}, nil
}
func (m *mockStorage) ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error) { return nil, nil }

func TestReplicatorParallelPhases(t *testing.T) {
	var mu sync.Mutex
	prepareCalls := make(map[string]bool)
	commitCalls := make(map[string]bool)
	abortCalls := make(map[string]bool)

	// Create test HTTP servers (followers)
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/internal/prepare" {
			prepareCalls["srv1"] = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"PREPARED"}`))
		} else if r.URL.Path == "/internal/commit" {
			commitCalls["srv1"] = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"COMMITTED"}`))
		} else if r.URL.Path == "/internal/abort" {
			abortCalls["srv1"] = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ABORTED"}`))
		}
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/internal/prepare" {
			prepareCalls["srv2"] = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"PREPARED"}`))
		} else if r.URL.Path == "/internal/commit" {
			commitCalls["srv2"] = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"COMMITTED"}`))
		} else if r.URL.Path == "/internal/abort" {
			abortCalls["srv2"] = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ABORTED"}`))
		}
	}))
	defer srv2.Close()

	client := internal_api.NewClient("test-token", 2*time.Second)
	cluster := &mockCluster{followers: []string{srv1.URL, srv2.URL}}
	storage := &mockStorage{stagedData: []byte("staged data payload")}
	replicator := NewReplicator(client, cluster, storage, 2*time.Second, zap.NewNop())

	tx := s3.Transaction{
		ID:        "tx-123",
		Operation: s3.OpPut,
		Bucket:    "mybucket",
		Key:       "mykey",
	}
	meta := s3.ObjectMeta{
		ContentLength: 19,
		CRC32:         123,
	}

	// 1. Test PrepareAll
	errs := replicator.PrepareAll(context.Background(), tx, meta)
	if len(errs) != 0 {
		t.Fatalf("PrepareAll failed with errors: %v", errs)
	}
	if !prepareCalls["srv1"] || !prepareCalls["srv2"] {
		t.Errorf("PrepareAll did not call both followers: %+v", prepareCalls)
	}

	// 2. Test CommitAll
	errs = replicator.CommitAll(context.Background(), "tx-123", "mybucket", "mykey")
	if len(errs) != 0 {
		t.Fatalf("CommitAll failed with errors: %v", errs)
	}
	if !commitCalls["srv1"] || !commitCalls["srv2"] {
		t.Errorf("CommitAll did not call both followers: %+v", commitCalls)
	}

	// 3. Test AbortAll
	errs = replicator.AbortAll(context.Background(), "tx-123")
	if len(errs) != 0 {
		t.Fatalf("AbortAll failed with errors: %v", errs)
	}
	if !abortCalls["srv1"] || !abortCalls["srv2"] {
		t.Errorf("AbortAll did not call both followers: %+v", abortCalls)
	}
}
