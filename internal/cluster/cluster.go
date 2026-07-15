package cluster

import (
	"context"

	"github.com/paucpauc/micros3/internal/application/s3app"
)

var _ s3app.ClusterManager = (*StandaloneClusterManager)(nil)

type StandaloneClusterManager struct {
	nodeID string
}

func NewStandaloneClusterManager(nodeID string) *StandaloneClusterManager {
	if nodeID == "" {
		nodeID = "standalone-node"
	}
	return &StandaloneClusterManager{nodeID: nodeID}
}

func (m *StandaloneClusterManager) NodeID() string {
	return m.nodeID
}

func (m *StandaloneClusterManager) IsLeader() bool {
	return true
}

func (m *StandaloneClusterManager) LeaderInternalAddress() string {
	return ""
}

func (m *StandaloneClusterManager) AliveFollowers() []string {
	return nil
}

func (m *StandaloneClusterManager) KnownFollowers() []string {
	return nil
}

func (m *StandaloneClusterManager) Mode() string {
	return "single"
}

func (m *StandaloneClusterManager) MarkDead(nodeID string) {
	// Standalone mode has no followers to mark dead
}

func (m *StandaloneClusterManager) MarkAlive(nodeID, internalAddr string) {
	// Standalone mode has no followers to mark alive
}

func (m *StandaloneClusterManager) RegisterFollower(nodeID, internalAddr string) {
	// Standalone mode has no followers to register
}

func (m *StandaloneClusterManager) RefreshFollowers(_ context.Context) {
	// Standalone mode has no followers to discover
}

func (m *StandaloneClusterManager) Status() string {
	return "READY"
}

func (m *StandaloneClusterManager) SetLocalStatus(status string) {
	// No-op for standalone mode
}
