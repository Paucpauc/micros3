package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/config"
	"github.com/paucpauc/micros3/internal/domain/cluster"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

var _ s3app.ClusterManager = (*K8sClusterManager)(nil)

var namespaceFilePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

type MicroTime time.Time

func (t MicroTime) MarshalJSON() ([]byte, error) {
	ft := time.Time(t).Truncate(time.Microsecond).UTC().Format("2006-01-02T15:04:05.000000Z")
	return []byte(`"` + ft + `"`), nil
}

func (t *MicroTime) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", s)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, s)
		if err != nil {
			return err
		}
	}
	*t = MicroTime(parsed)
	return nil
}

type LeaseSpec struct {
	HolderIdentity       string    `json:"holderIdentity"`
	LeaseDurationSeconds int       `json:"leaseDurationSeconds"`
	AcquireTime          MicroTime `json:"acquireTime,omitempty"`
	RenewTime            MicroTime `json:"renewTime,omitempty"`
}

type LeaseObject struct {
	ApiVersion string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Metadata   struct {
		Name            string `json:"name"`
		Namespace       string `json:"namespace"`
		ResourceVersion string `json:"resourceVersion,omitempty"`
	} `json:"metadata"`
	Spec LeaseSpec `json:"spec"`
}

type EndpointAddress struct {
	IP        string `json:"ip"`
	TargetRef struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
	} `json:"targetRef"`
}

type EndpointSubset struct {
	Addresses []EndpointAddress `json:"addresses"`
}

type EndpointsResponse struct {
	Subsets []EndpointSubset `json:"subsets"`
}

type K8sClusterManager struct {
	localNodeID  string
	namespace    string
	leaseName    string
	serviceName  string
	internalPort int
	k8sCfg       config.K8sConfig
	healthCfg    config.HealthConfig
	client       *internal_api.Client
	logger       *zap.Logger

	apiURL     string
	token      string
	k8sClient  *http.Client
	cancelLoop context.CancelFunc

	mu               sync.RWMutex
	isLeaderFlag     bool
	leaderAddress    string
	followers        []string
	localStatus      cluster.NodeStatus
	followerStates   map[string]*nodeState
	lastLeaseRenewal time.Time
	leaseDuration    time.Duration
	lastAppliedRole  string // last role value written to the pod label
}

func NewK8sClusterManager(
	cfg *config.Config,
	client *internal_api.Client,
	logger *zap.Logger,
) (*K8sClusterManager, error) {
	// Discover K8s Namespace from downward API
	nsBytes, err := os.ReadFile(namespaceFilePath)
	ns := "default"
	if err == nil {
		ns = string(bytes.TrimSpace(nsBytes))
	}

	// Read K8s Service Account Token
	tokenBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	token := ""
	if err == nil {
		token = string(bytes.TrimSpace(tokenBytes))
	}

	// Setup K8s API Host
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		host = "kubernetes.default.svc"
		port = "443"
	}
	apiURL := fmt.Sprintf("https://%s:%s", host, port)

	// Setup TLS client
	caCertPool := x509.NewCertPool()
	caCert, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	tlsConfig := &tls.Config{}
	if err == nil {
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	} else {
		tlsConfig.InsecureSkipVerify = true
	}

	k8sHTTPClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: 5 * time.Second,
	}

	mgr := &K8sClusterManager{
		localNodeID:    cfg.Node.ID,
		namespace:      ns,
		leaseName:      cfg.Cluster.K8s.LeaseName,
		serviceName:    "micros3",
		internalPort:   cfg.Cluster.K8s.InternalPort,
		k8sCfg:         cfg.Cluster.K8s,
		healthCfg:      cfg.Health,
		client:         client,
		logger:         logger,
		apiURL:         apiURL,
		token:          token,
		k8sClient:      k8sHTTPClient,
		localStatus:    cluster.StatusReady,
		followerStates: make(map[string]*nodeState),
		leaseDuration:  cfg.Cluster.K8s.LeaseDuration,
	}

	if svcName := os.Getenv("MICROS3_K8S_SERVICE_NAME"); svcName != "" {
		mgr.serviceName = svcName
	}

	return mgr, nil
}

func (m *K8sClusterManager) Start(ctx context.Context) {
	m.logger.Info("Starting K8s Cluster Manager background loop",
		zap.String("namespace", m.namespace),
		zap.String("lease_name", m.leaseName),
		zap.String("local_pod", m.localNodeID),
	)

	loopCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancelLoop = cancel
	// Track the last role we applied to the pod label so we only patch the
	// pod when the role actually changes. This avoids a race where a
	// proactive "follower" patch overwrites a just-applied "leader" patch.
	m.lastAppliedRole = ""
	m.mu.Unlock()

	// Start lease leader election loop and discovery.
	// The pod label is set exclusively by the leader-election loop based on
	// the actual lease state — no proactive "follower" patch is issued here
	// to avoid racing with the first lease acquisition.
	go m.runLeaderElectionLoop(loopCtx)
	go m.runDiscoveryLoop(loopCtx)
}

func (m *K8sClusterManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancelLoop != nil {
		m.cancelLoop()
	}
}

// --- s3app.ClusterManager Port Implementation ---

func (m *K8sClusterManager) NodeID() string {
	return m.localNodeID
}

func (m *K8sClusterManager) IsLeader() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isLeaderFlag {
		return false
	}
	// Guard window prevents split-brain writes during temporary K8s API disconnection.
	// If we haven't successfully renewed our lease for longer than leaseDuration - guardWindow,
	// we defensively reject being the leader.
	guardWindow := 2 * time.Second
	if m.leaseDuration > guardWindow && time.Since(m.lastLeaseRenewal) > m.leaseDuration-guardWindow {
		return false
	}
	return true
}

func (m *K8sClusterManager) LeaderInternalAddress() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.leaderAddress
}

func (m *K8sClusterManager) AliveFollowers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.followers
}

// KnownFollowers returns the internal addresses of all discovered follower
// nodes, regardless of their status (READY, SYNCING, or OFFLINE). This is
// used by the EC manager to broadcast shard-discovery requests: even if a
// node is marked OFFLINE in the leader's state, it may have just rebooted
// and already be serving internal API requests. A failed request to a
// truly-down node is simply skipped by the caller.
func (m *K8sClusterManager) KnownFollowers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var list []string
	for _, ns := range m.followerStates {
		list = append(list, ns.node.InternalAddress)
	}
	return list
}

func (m *K8sClusterManager) Mode() string {
	return "k8s"
}

func (m *K8sClusterManager) MarkDead(nodeAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, ns := range m.followerStates {
		if ns.node.InternalAddress == nodeAddr {
			ns.node.Status = cluster.StatusOffline
			ns.failureCount = m.healthCfg.MaxFailures
			m.logger.Warn("K8s node marked as DEAD", zap.String("node_id", id), zap.String("address", nodeAddr))
			m.rebuildAliveFollowers()
			break
		}
	}
}

func (m *K8sClusterManager) MarkAlive(nodeID, internalAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ns, exists := m.followerStates[nodeID]
	if exists {
		ns.failureCount = 0
		// If the follower reports a new address (pod was rescheduled), update it immediately
		// so health checks don't keep pinging the old stale IP.
		if internalAddr != "" && ns.node.InternalAddress != internalAddr {
			m.logger.Info("K8s node address updated (pod rescheduled)",
				zap.String("node_id", nodeID),
				zap.String("old_addr", ns.node.InternalAddress),
				zap.String("new_addr", internalAddr),
			)
			ns.node.InternalAddress = internalAddr
		}
		if ns.node.Status != cluster.StatusReady {
			m.logger.Info("K8s node proactively marked as READY (sync completed)", zap.String("node_id", nodeID))
			ns.node.Status = cluster.StatusReady
		}
		m.rebuildAliveFollowers()
	}
}

// RegisterFollower ensures a follower node is present in the leader's
// followerStates map before sync begins. If the node is already known,
// its internal address is updated (the pod may have been rescheduled).
// If the node is unknown (e.g. discovery loop hasn't run yet after a
// cluster restart), it is added with SYNCING status so that
// KnownFollowers() includes it and ReadECObject can query it for EC
// shards during the sync process.
func (m *K8sClusterManager) RegisterFollower(nodeID, internalAddr string) {
	if nodeID == "" || nodeID == m.localNodeID {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	ns, exists := m.followerStates[nodeID]
	if exists {
		if internalAddr != "" && ns.node.InternalAddress != internalAddr {
			m.logger.Info("K8s node address updated during register",
				zap.String("node_id", nodeID),
				zap.String("old_addr", ns.node.InternalAddress),
				zap.String("new_addr", internalAddr),
			)
			ns.node.InternalAddress = internalAddr
		}
		return
	}

	m.followerStates[nodeID] = &nodeState{
		node: &cluster.Node{
			ID:              nodeID,
			InternalAddress: internalAddr,
			Role:            cluster.RoleFollower,
			Status:          cluster.StatusSyncing,
		},
	}
	m.logger.Info("K8s follower registered before sync",
		zap.String("node_id", nodeID),
		zap.String("address", internalAddr),
	)
}

func (m *K8sClusterManager) Status() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return string(m.localStatus)
}

func (m *K8sClusterManager) SetLocalStatus(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localStatus = cluster.NodeStatus(status)
}

// --- K8s API Request Helpers ---

func (m *K8sClusterManager) makeK8sRequest(ctx context.Context, method, urlPath string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, m.apiURL+urlPath, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+m.token)
	req.Header.Set("Content-Type", "application/json")
	return m.k8sClient.Do(req)
}

// --- Leader Election ---

func (m *K8sClusterManager) runLeaderElectionLoop(ctx context.Context) {
	m.logger.Info("Starting leader election Lease loop")

	// Default values
	leaseDuration := 15 * time.Second
	retryPeriod := 2 * time.Second

	if m.k8sCfg.LeaseDuration > 0 {
		leaseDuration = m.k8sCfg.LeaseDuration
	}
	if m.k8sCfg.RetryPeriod > 0 {
		retryPeriod = m.k8sCfg.RetryPeriod
	}

	ticker := time.NewTicker(retryPeriod)
	defer ticker.Stop()

	m.tryAcquireOrRenewLease(ctx, leaseDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.tryAcquireOrRenewLease(ctx, leaseDuration)
		}
	}
}

func (m *K8sClusterManager) tryAcquireOrRenewLease(ctx context.Context, leaseDuration time.Duration) {
	leasePath := fmt.Sprintf("/apis/coordination.k8s.io/v1/namespaces/%s/leases/%s", m.namespace, m.leaseName)
	resp, err := m.makeK8sRequest(ctx, http.MethodGet, leasePath, nil)
	if err != nil {
		m.logger.Error("Failed to fetch Lease object", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Create Lease
		m.createLease(ctx, leaseDuration)
		return
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		m.logger.Error("Lease fetch responded with error status", zap.Int("status", resp.StatusCode), zap.String("body", string(respBody)))
		return
	}

	var lease LeaseObject
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		m.logger.Error("Failed to decode Lease object", zap.Error(err))
		return
	}

	now := time.Now().UTC()
	isCurrentHolder := lease.Spec.HolderIdentity == m.localNodeID

	expiryTime := time.Time(lease.Spec.RenewTime).Add(time.Duration(lease.Spec.LeaseDurationSeconds) * time.Second)
	expired := now.After(expiryTime)

	if isCurrentHolder || expired {
		// We can acquire/renew
		m.renewOrAcquireLease(ctx, lease, leaseDuration)
	} else {
		// Someone else is the leader
		m.mu.Lock()
		if m.isLeaderFlag {
			m.logger.Info("Lost leadership, transitioning to follower")
		}
		m.isLeaderFlag = false

		// Leader address calculation
		// If we are a follower, leader internal address is http://{leader-id}.{service-name}.{namespace}.svc.cluster.local:{port}
		// Since K8s DNS resolves pod hostnames of headless services like this!
		m.leaderAddress = fmt.Sprintf("http://%s.%s.%s.svc.cluster.local:%d", lease.Spec.HolderIdentity, m.serviceName, m.namespace, m.internalPort)
		m.mu.Unlock()

		// Always ensure the pod label reflects follower status.
		// updatePodLabel is idempotent: it skips the API call if the label
		// is already "follower", so this is safe to call on every tick.
		go m.updatePodLabel(context.Background(), false)
	}
}

func (m *K8sClusterManager) createLease(ctx context.Context, leaseDuration time.Duration) {
	leasePath := fmt.Sprintf("/apis/coordination.k8s.io/v1/namespaces/%s/leases", m.namespace)
	now := time.Now().UTC()

	lease := LeaseObject{
		ApiVersion: "coordination.k8s.io/v1",
		Kind:       "Lease",
	}
	lease.Metadata.Name = m.leaseName
	lease.Metadata.Namespace = m.namespace
	lease.Spec = LeaseSpec{
		HolderIdentity:       m.localNodeID,
		LeaseDurationSeconds: int(leaseDuration.Seconds()),
		AcquireTime:          MicroTime(now),
		RenewTime:            MicroTime(now),
	}

	bodyBytes, _ := json.Marshal(lease)
	resp, err := m.makeK8sRequest(ctx, http.MethodPost, leasePath, bytes.NewReader(bodyBytes))
	if err != nil {
		m.logger.Error("Failed to create Lease", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusCreated {
		m.logger.Info("Successfully acquired leadership (created lease)")
		m.mu.Lock()
		wasLeader := m.isLeaderFlag
		m.isLeaderFlag = true
		m.leaderAddress = ""
		m.lastLeaseRenewal = time.Now()
		m.mu.Unlock()

		if !wasLeader {
			go m.updatePodLabel(context.Background(), true)
		}
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		m.logger.Error("Lease creation responded with error status", zap.Int("status", resp.StatusCode), zap.String("body", string(respBody)))
	}
}

func (m *K8sClusterManager) renewOrAcquireLease(ctx context.Context, existingLease LeaseObject, leaseDuration time.Duration) {
	leasePath := fmt.Sprintf("/apis/coordination.k8s.io/v1/namespaces/%s/leases/%s", m.namespace, m.leaseName)
	now := time.Now().UTC()

	isCurrentHolder := existingLease.Spec.HolderIdentity == m.localNodeID

	existingLease.Spec.HolderIdentity = m.localNodeID
	existingLease.Spec.LeaseDurationSeconds = int(leaseDuration.Seconds())
	if !isCurrentHolder {
		existingLease.Spec.AcquireTime = MicroTime(now)
		m.logger.Info("Seizing expired lease", zap.String("prev_holder", existingLease.Spec.HolderIdentity))
	}
	existingLease.Spec.RenewTime = MicroTime(now)

	bodyBytes, _ := json.Marshal(existingLease)
	resp, err := m.makeK8sRequest(ctx, http.MethodPut, leasePath, bytes.NewReader(bodyBytes))
	if err != nil {
		m.logger.Error("Failed to renew Lease", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		m.mu.Lock()
		wasLeader := m.isLeaderFlag
		if !m.isLeaderFlag {
			m.logger.Info("Acquired leadership")
		}
		m.isLeaderFlag = true
		m.leaderAddress = ""
		m.lastLeaseRenewal = time.Now()
		m.mu.Unlock()

		if !wasLeader {
			go m.updatePodLabel(context.Background(), true)
		}
	} else if resp.StatusCode == http.StatusConflict {
		m.logger.Warn("Conflict during lease renew (optimistic lock error)")
	} else {
		respBody, _ := io.ReadAll(resp.Body)
		m.logger.Error("Lease renew responded with error", zap.Int("status", resp.StatusCode), zap.String("body", string(respBody)))
	}
}

func (m *K8sClusterManager) updatePodLabel(ctx context.Context, isLeader bool) {
	roleVal := "follower"
	if isLeader {
		roleVal = "leader"
	}

	// Skip the API call if the role hasn't changed since the last successful
	// patch. This makes updatePodLabel idempotent and prevents redundant
	// patches (and races) on every lease-renew tick.
	m.mu.Lock()
	if m.lastAppliedRole == roleVal {
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	podPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", m.namespace, m.localNodeID)
	payload := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]string{
				"role": roleVal,
			},
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, m.apiURL+podPath, bytes.NewReader(bodyBytes))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+m.token)
	req.Header.Set("Content-Type", "application/strategic-merge-patch+json")

	resp, err := m.k8sClient.Do(req)
	if err != nil {
		m.logger.Warn("Failed to patch pod label", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		m.logger.Warn("Patch pod label returned error", zap.Int("status", resp.StatusCode), zap.String("body", string(respBody)))
	} else {
		m.mu.Lock()
		m.lastAppliedRole = roleVal
		m.mu.Unlock()
		m.logger.Info("Successfully updated pod role label", zap.String("role", roleVal))
	}
}

// --- Node Discovery & Health checks for Followers ---

func (m *K8sClusterManager) runDiscoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(m.healthCfg.Interval)
	defer ticker.Stop()

	m.discoverAndCheckFollowers(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.discoverAndCheckFollowers(ctx)
		}
	}
}

func (m *K8sClusterManager) discoverAndCheckFollowers(ctx context.Context) {
	if !m.IsLeader() {
		return
	}

	endpointsPath := fmt.Sprintf("/api/v1/namespaces/%s/endpoints/%s", m.namespace, m.serviceName)
	resp, err := m.makeK8sRequest(ctx, http.MethodGet, endpointsPath, nil)
	if err != nil {
		m.logger.Error("Failed to fetch K8s endpoints for node discovery", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		m.logger.Error("Endpoints fetch responded with error status", zap.Int("status", resp.StatusCode))
		return
	}

	var endpoints EndpointsResponse
	if err := json.NewDecoder(resp.Body).Decode(&endpoints); err != nil {
		m.logger.Error("Failed to decode endpoints response", zap.Error(err))
		return
	}

	discoveredNodes := make(map[string]string)
	for _, subset := range endpoints.Subsets {
		for _, addr := range subset.Addresses {
			if addr.TargetRef.Name != "" && addr.TargetRef.Name != m.localNodeID {
				// Address of follower: http://{pod-ip}:{internal_port}
				discoveredNodes[addr.TargetRef.Name] = fmt.Sprintf("http://%s:%d", addr.IP, m.internalPort)
			}
		}
	}

	m.mu.Lock()
	// Clean up states for nodes no longer in endpoints
	for id := range m.followerStates {
		if _, exists := discoveredNodes[id]; !exists {
			delete(m.followerStates, id)
		}
	}

	// Update or add states
	for id, addr := range discoveredNodes {
		if _, exists := m.followerStates[id]; !exists {
			m.followerStates[id] = &nodeState{
				node: &cluster.Node{
					ID:              id,
					InternalAddress: addr,
					Role:            cluster.RoleFollower,
					Status:          cluster.StatusOffline,
				},
			}
		}
	}
	m.mu.Unlock()

	// Check health of discovered followers in parallel
	m.checkFollowersHealth(ctx)
}

func (m *K8sClusterManager) checkFollowersHealth(ctx context.Context) {
	// 1. Copy the followers under lock to release the lock immediately
	m.mu.RLock()
	type followerTask struct {
		id   string
		addr string
		ns   *nodeState
	}
	var tasks []followerTask
	for id, ns := range m.followerStates {
		tasks = append(tasks, followerTask{id: id, addr: ns.node.InternalAddress, ns: ns})
	}
	m.mu.RUnlock()

	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(t followerTask) {
			defer wg.Done()

			reqCtx, cancel := context.WithTimeout(ctx, m.healthCfg.Timeout)
			defer cancel()

			hResp, err := m.client.Health(reqCtx, t.addr)

			// 2. Lock to update the state of this follower
			m.mu.Lock()
			defer m.mu.Unlock()

			// Check if follower still exists in the map (it might have been deleted by discovery)
			ns, exists := m.followerStates[t.id]
			if !exists {
				return
			}

			if err != nil {
				ns.failureCount++
				if ns.failureCount >= m.healthCfg.MaxFailures {
					if ns.node.Status != cluster.StatusOffline {
						m.logger.Warn("K8s Follower node went OFFLINE", zap.String("node_id", t.id), zap.String("address", t.addr))
						ns.node.Status = cluster.StatusOffline
					}
				}
			} else {
				ns.failureCount = 0
				ns.node.UptimeSeconds = hResp.UptimeSeconds
				ns.node.ObjectsCount = hResp.ObjectsCount
				ns.node.StorageUsed = hResp.StorageUsedBytes

				// If follower was offline, force it to transition to SYNCING first
				// to pull latest files before letting it participate in 2PC.
				if ns.node.Status == cluster.StatusOffline {
					m.logger.Info("K8s Follower node recovered from OFFLINE, forcing SYNC", zap.String("node_id", t.id), zap.String("address", t.addr))
					// Trigger set-status asynchronously to avoid locking m.mu during network I/O
					go func(nodeID, addr string) {
						sCtx, sCancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer sCancel()
						if err := m.client.SetStatus(sCtx, addr, string(cluster.StatusSyncing)); err != nil {
							m.logger.Warn("Failed to force SYNC on follower", zap.String("node_id", nodeID), zap.Error(err))
						}
					}(t.id, t.addr)
				} else if hResp.State == string(cluster.StatusReady) {
					// Only promote to READY if the node itself has finished syncing (reports State == READY)
					if ns.node.Status != cluster.StatusReady {
						m.logger.Info("K8s Follower node is READY", zap.String("node_id", t.id), zap.String("address", t.addr))
						ns.node.Status = cluster.StatusReady
					}
				}
			}
		}(task)
	}

	wg.Wait()

	// 3. Rebuild alive list under lock
	m.mu.Lock()
	m.rebuildAliveFollowers()
	m.mu.Unlock()
}

func (m *K8sClusterManager) rebuildAliveFollowers() {
	var list []string
	for _, ns := range m.followerStates {
		if ns.node.Status == cluster.StatusReady {
			list = append(list, ns.node.InternalAddress)
		}
	}
	m.followers = list
}
