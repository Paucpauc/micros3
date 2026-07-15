package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/config"
	"github.com/paucpauc/micros3/internal/domain/cluster"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

var _ s3app.ClusterManager = (*StaticClusterManager)(nil)

type nodeState struct {
	node         *cluster.Node
	failureCount int
}

type StaticClusterManager struct {
	localNodeID string
	staticNodes []config.StaticNode
	forceLeader string
	healthCfg   config.HealthConfig
	client      *internal_api.Client
	logger      *zap.Logger

	mu          sync.RWMutex
	nodesState  map[string]*nodeState
	cancelLoop  context.CancelFunc
	localStatus cluster.NodeStatus
}

func NewStaticClusterManager(
	cfg *config.Config,
	client *internal_api.Client,
	logger *zap.Logger,
) *StaticClusterManager {
	mgr := &StaticClusterManager{
		localNodeID: cfg.Node.ID,
		staticNodes: cfg.Cluster.Static.Nodes,
		forceLeader: cfg.Cluster.Static.ForceLeader,
		healthCfg:   cfg.Health,
		client:      client,
		logger:      logger,
		nodesState:  make(map[string]*nodeState),
		localStatus: cluster.StatusReady,
	}

	// Initialize state for all static nodes
	for _, n := range cfg.Cluster.Static.Nodes {
		role := cluster.RoleFollower
		if n.ID == cfg.Cluster.Static.ForceLeader {
			role = cluster.RoleLeader
		}

		status := cluster.StatusOffline
		if n.ID == cfg.Node.ID {
			status = cluster.StatusReady
		}

		mgr.nodesState[n.ID] = &nodeState{
			node: &cluster.Node{
				ID:              n.ID,
				InternalAddress: n.InternalAddress,
				Role:            role,
				Status:          status,
				LastSeen:        time.Now(),
			},
		}
	}

	return mgr
}

// Start runs background health checks if we are the leader
func (m *StaticClusterManager) Start(ctx context.Context) {
	if !m.IsLeader() {
		m.logger.Info("StaticClusterManager starting in Follower mode")
		return
	}

	m.logger.Info("StaticClusterManager starting in Leader mode, running health check loop")
	loopCtx, cancel := context.WithCancel(ctx)
	m.mu.Lock()
	m.cancelLoop = cancel
	m.mu.Unlock()

	go m.runHeartbeatLoop(loopCtx)
}

func (m *StaticClusterManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancelLoop != nil {
		m.cancelLoop()
	}
}

// --- s3app.ClusterManager Port Implementation ---

func (m *StaticClusterManager) NodeID() string {
	return m.localNodeID
}

func (m *StaticClusterManager) IsLeader() bool {
	// If forceLeader matches local Node ID, or single node config
	if m.forceLeader == "" {
		return true // standalone mode defaults to leader
	}
	return m.localNodeID == m.forceLeader
}

func (m *StaticClusterManager) LeaderInternalAddress() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.forceLeader == "" {
		return ""
	}

	// Find force leader internal address
	for _, n := range m.staticNodes {
		if n.ID == m.forceLeader {
			return n.InternalAddress
		}
	}

	return ""
}

func (m *StaticClusterManager) AliveFollowers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var followers []string
	for id, ns := range m.nodesState {
		if id == m.localNodeID {
			continue
		}
		if ns.node.Status == cluster.StatusReady {
			followers = append(followers, ns.node.InternalAddress)
		}
	}
	return followers
}

// KnownFollowers returns the internal addresses of all discovered follower
// nodes, regardless of their status (READY, SYNCING, or OFFLINE). This is
// used by the EC manager to broadcast shard-discovery requests: even if a
// node is marked OFFLINE in the leader's state, it may have just rebooted
// and already be serving internal API requests. A failed request to a
// truly-down node is simply skipped by the caller.
func (m *StaticClusterManager) KnownFollowers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var followers []string
	for id, ns := range m.nodesState {
		if id == m.localNodeID {
			continue
		}
		followers = append(followers, ns.node.InternalAddress)
	}
	return followers
}

func (m *StaticClusterManager) Mode() string {
	return "static"
}

func (m *StaticClusterManager) MarkDead(nodeAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, ns := range m.nodesState {
		if ns.node.InternalAddress == nodeAddr {
			ns.node.Status = cluster.StatusOffline
			ns.failureCount = m.healthCfg.MaxFailures // force dead status
			m.logger.Warn("Node marked as DEAD by transaction failure", zap.String("node_id", ns.node.ID), zap.String("address", nodeAddr))
			break
		}
	}
}

func (m *StaticClusterManager) MarkAlive(nodeID, internalAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ns, exists := m.nodesState[nodeID]
	if exists {
		ns.failureCount = 0
		if ns.node.Status != cluster.StatusReady {
			m.logger.Info("Node proactively marked as READY (sync completed)", zap.String("node_id", nodeID))
			ns.node.Status = cluster.StatusReady
		}
	}
}

// RegisterFollower ensures a follower node is present in the leader's
// nodesState map before sync begins. If the node is already known,
// its internal address is updated. If the node is unknown (e.g.
// discovery loop hasn't run yet after a cluster restart), it is added
// with SYNCING status so that KnownFollowers() includes it and
// ReadECObject can query it for EC shards during the sync process.
func (m *StaticClusterManager) RegisterFollower(nodeID, internalAddr string) {
	if nodeID == "" || nodeID == m.localNodeID {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	ns, exists := m.nodesState[nodeID]
	if exists {
		if internalAddr != "" && ns.node.InternalAddress != internalAddr {
			m.logger.Info("Node address updated during register",
				zap.String("node_id", nodeID),
				zap.String("old_addr", ns.node.InternalAddress),
				zap.String("new_addr", internalAddr),
			)
			ns.node.InternalAddress = internalAddr
		}
		return
	}

	m.nodesState[nodeID] = &nodeState{
		node: &cluster.Node{
			ID:              nodeID,
			InternalAddress: internalAddr,
			Role:            cluster.RoleFollower,
			Status:          cluster.StatusSyncing,
		},
	}
	m.logger.Info("Follower registered before sync",
		zap.String("node_id", nodeID),
		zap.String("address", internalAddr),
	)
}

// RefreshFollowers is a no-op for the static cluster manager: all nodes are
// already known from configuration and are discovered at startup. The
// background heartbeat loop keeps statuses up to date.
func (m *StaticClusterManager) RefreshFollowers(ctx context.Context) {
	// For static mode, all nodes are pre-configured — nothing to discover.
}

func (m *StaticClusterManager) Status() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return string(m.localStatus)
}

// SetLocalStatus allows setting local node status (e.g. SYNCING during sync)
func (m *StaticClusterManager) SetLocalStatus(status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localStatus = cluster.NodeStatus(status)
}

// --- Heartbeat Loop ---

func (m *StaticClusterManager) runHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(m.healthCfg.Interval)
	defer ticker.Stop()

	// Initial heartbeat immediately
	m.checkFollowers(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.checkFollowers(ctx)
		}
	}
}

func (m *StaticClusterManager) checkFollowers(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, ns := range m.nodesState {
		if id == m.localNodeID {
			continue
		}

		go func(id string, node *cluster.Node, ns *nodeState) {
			reqCtx, cancel := context.WithTimeout(ctx, m.healthCfg.Timeout)
			defer cancel()

			hResp, err := m.client.Health(reqCtx, node.InternalAddress)
			m.mu.Lock()
			defer m.mu.Unlock()

			if err != nil {
				ns.failureCount++
				m.logger.Debug("Heartbeat failure", zap.String("node_id", id), zap.Int("fails", ns.failureCount), zap.Error(err))
				if ns.failureCount >= m.healthCfg.MaxFailures {
					if node.Status != cluster.StatusOffline {
						m.logger.Warn("Node went OFFLINE", zap.String("node_id", id), zap.String("address", node.InternalAddress))
						node.Status = cluster.StatusOffline
					}
				}
			} else {
				ns.failureCount = 0
				node.UptimeSeconds = hResp.UptimeSeconds
				node.ObjectsCount = hResp.ObjectsCount
				node.StorageUsed = hResp.StorageUsedBytes

				// If follower was offline, force it to transition to SYNCING first
				// to pull latest files before letting it participate in 2PC.
				if node.Status == cluster.StatusOffline {
					m.logger.Info("Node recovered from OFFLINE, forcing SYNC", zap.String("node_id", id), zap.String("address", node.InternalAddress))
					// Trigger set-status asynchronously to avoid locking m.mu during network I/O
					go func(nodeID, addr string) {
						sCtx, sCancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer sCancel()
						if err := m.client.SetStatus(sCtx, addr, string(cluster.StatusSyncing)); err != nil {
							m.logger.Warn("Failed to force SYNC on follower", zap.String("node_id", nodeID), zap.Error(err))
						}
					}(id, node.InternalAddress)
				} else if hResp.State == string(cluster.StatusReady) {
					// Only promote to READY if the node itself has finished syncing (reports State == READY)
					if node.Status != cluster.StatusReady {
						m.logger.Info("Node is READY", zap.String("node_id", id), zap.String("address", node.InternalAddress))
						node.Status = cluster.StatusReady
					}
				}
				node.LastSeen = time.Now()
			}
		}(id, ns.node, ns)
	}
}
