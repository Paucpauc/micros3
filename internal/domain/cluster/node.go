package cluster

import (
	"time"
)

type NodeRole string

const (
	RoleLeader   NodeRole = "leader"
	RoleFollower NodeRole = "follower"
)

type NodeStatus string

const (
	StatusOffline NodeStatus = "OFFLINE"
	StatusSyncing NodeStatus = "SYNCING"
	StatusReady   NodeStatus = "READY"
	StatusError   NodeStatus = "ERROR"
)

// Node represents a member of the cluster
type Node struct {
	ID              string     `json:"id"`
	InternalAddress string     `json:"internal_address"`
	Role            NodeRole   `json:"role"`
	Status          NodeStatus `json:"status"`
	LastSeen        time.Time  `json:"last_seen"`
	ObjectsCount    int64      `json:"objects_count"`
	StorageUsed     int64      `json:"storage_used_bytes"`
	UptimeSeconds   int64      `json:"uptime_seconds"`
}

// ClusterState holds the global view of the cluster membership
type ClusterState struct {
	LocalNodeID string
	Nodes       map[string]*Node
}

func NewClusterState(localNodeID string) *ClusterState {
	return &ClusterState{
		LocalNodeID: localNodeID,
		Nodes:       make(map[string]*Node),
	}
}

// IsHealthy returns true if the node is considered ready to participate
func (n *Node) IsHealthy() bool {
	return n.Status == StatusReady || n.Role == RoleLeader
}

// UpdateLastSeen marks the node as alive
func (n *Node) UpdateLastSeen() {
	n.LastSeen = time.Now()
}

// TransitionTo updates node status safely
func (n *Node) TransitionTo(status NodeStatus) error {
	if n.Status == status {
		return nil
	}
	// Add business rules for state transitions if needed
	n.Status = status
	return nil
}
