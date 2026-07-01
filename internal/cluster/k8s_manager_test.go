package cluster

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/config"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

func TestK8sClusterManagerLeaderElection(t *testing.T) {
	var mu sync.Mutex
	leaseFetched := false
	leaseCreated := false
	leaseRenewed := false
	endpointsFetched := false

	// Mock Kubernetes API Server
	k8sServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if r.Method == http.MethodGet && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/test-lease" {
			leaseFetched = true
			// If not created yet, return 404
			if !leaseCreated {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// Return current lease
			lease := LeaseObject{}
			lease.Metadata.Name = "test-lease"
			lease.Metadata.Namespace = "test-ns"
			lease.Metadata.ResourceVersion = "12345"
			lease.Spec = LeaseSpec{
				HolderIdentity:       "pod-1",
				LeaseDurationSeconds: 15,
				AcquireTime:          MicroTime(time.Now().Add(-10 * time.Second)),
				RenewTime:            MicroTime(time.Now().Add(-5 * time.Second)),
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(lease)

		} else if r.Method == http.MethodPost && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases" {
			leaseCreated = true
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"status":"Success"}`))

		} else if r.Method == http.MethodPut && r.URL.Path == "/apis/coordination.k8s.io/v1/namespaces/test-ns/leases/test-lease" {
			leaseRenewed = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"Success"}`))

		} else if r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/test-ns/endpoints/pod-1" {
			endpointsFetched = true
			resp := EndpointsResponse{
				Subsets: []EndpointSubset{
					{
						Addresses: []EndpointAddress{
							{
								IP: "10.0.0.1",
								TargetRef: struct {
									Kind string `json:"kind"`
									Name string `json:"name"`
								}{Kind: "Pod", Name: "pod-1"}, // ourselves
							},
							{
								IP: "10.0.0.2",
								TargetRef: struct {
									Kind string `json:"kind"`
									Name string `json:"name"`
								}{Kind: "Pod", Name: "pod-2"}, // peer follower
							},
						},
					},
				},
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
		} else if r.Method == http.MethodPatch && r.URL.Path == "/api/v1/namespaces/test-ns/pods/pod-1" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"Success"}`))
		}
	}))
	defer k8sServer.Close()

	// Create a temporary namespace file to mock the downward API
	tmpDir := t.TempDir()
	nsDir := filepath.Join(tmpDir, "serviceaccount")
	if err := os.MkdirAll(nsDir, 0755); err != nil {
		t.Fatalf("failed to create namespace dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nsDir, "namespace"), []byte("test-ns"), 0644); err != nil {
		t.Fatalf("failed to write namespace file: %v", err)
	}
	namespaceFilePath = filepath.Join(nsDir, "namespace")

	// Config pointing to mock API server
	t.Setenv("MICROS3_K8S_SERVICE_NAME", "pod-1")
	cfg := &config.Config{
		Node: config.NodeConfig{
			ID: "pod-1",
		},
		Cluster: config.ClusterConfig{
			Mode: "k8s",
			K8s: config.K8sConfig{
				LeaseName:     "test-lease",
				LeaseDuration: 15 * time.Second,
				RetryPeriod:   50 * time.Millisecond,
				InternalPort:  9001,
			},
		},
		Health: config.HealthConfig{
			Interval:    100 * time.Millisecond,
			Timeout:     50 * time.Millisecond,
			MaxFailures: 3,
		},
	}

	client := internal_api.NewClient("token", 2*time.Second)
	mgr, err := NewK8sClusterManager(cfg, client, zap.NewNop())
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	// Override K8s API client properties
	mgr.apiURL = k8sServer.URL
	// Bypass token auth for local testing
	mgr.token = "fake-token"

	// Start election loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr.Start(ctx)

	// Wait for loop to execute
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if !leaseFetched {
		t.Errorf("Expected Lease GET call")
	}
	if !leaseCreated {
		t.Errorf("Expected Lease POST call (creation)")
	}
	if !leaseRenewed {
		t.Errorf("Expected Lease PUT call (renewal)")
	}
	if !mgr.IsLeader() {
		t.Errorf("Expected pod-1 to become leader")
	}

	// Verify endpoints discovery
	if !endpointsFetched {
		t.Errorf("Expected endpoints GET call")
	}
}
