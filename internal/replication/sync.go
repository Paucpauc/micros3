package replication

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/cluster"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

type SyncWorker struct {
	client           *internal_api.Client
	cluster          s3app.ClusterManager
	logger           *zap.Logger
	selfInternalAddr string // own internal address sent to leader on sync request

	runningMu sync.Mutex
	running   bool
}

func NewSyncWorker(
	client *internal_api.Client,
	cluster s3app.ClusterManager,
	logger *zap.Logger,
	internalPort int,
) *SyncWorker {
	// Build self address from POD_IP env variable injected by K8s Downward API
	selfAddr := ""
	if podIP := os.Getenv("POD_IP"); podIP != "" {
		selfAddr = fmt.Sprintf("http://%s:%d", podIP, internalPort)
	}
	return &SyncWorker{
		client:           client,
		cluster:          cluster,
		logger:           logger,
		selfInternalAddr: selfAddr,
	}
}

// Start runs the synchronization process in the background
func (s *SyncWorker) Start(ctx context.Context) {
	s.runningMu.Lock()
	if s.running {
		s.runningMu.Unlock()
		return
	}
	s.running = true
	s.runningMu.Unlock()

	go s.runSyncLoop(ctx)
}

func (s *SyncWorker) runSyncLoop(ctx context.Context) {
	s.logger.Info("Starting replica sync loop")
	defer func() {
		s.runningMu.Lock()
		s.running = false
		s.runningMu.Unlock()
		s.logger.Info("Stopped replica sync loop")
	}()

	// If we start up, we should initiate sync if we are a follower
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// If we are follower and status is SYNCING (or we just want to run sync at start)
			if !s.cluster.IsLeader() && s.cluster.Status() == string(cluster.StatusSyncing) {
				s.logger.Info("Follower is in SYNCING state, requesting leader-driven sync")
				err := s.Synchronize(ctx)
				if err != nil {
					s.logger.Error("Replica synchronization failed, will retry in 5s", zap.Error(err))
					time.Sleep(5 * time.Second)
					continue
				}
				s.logger.Info("Replica synchronization completed successfully. Node is READY")
				s.cluster.SetLocalStatus(string(cluster.StatusReady))
			}
			time.Sleep(2 * time.Second)
		}
	}
}

// Synchronize sends a sync request to the leader. The leader then drives
// the entire sync process: it queries this follower's keys, pushes missing
// or updated objects, and deletes extraneous ones. This method blocks until
// the leader reports sync completion (or failure).
func (s *SyncWorker) Synchronize(ctx context.Context) error {
	leaderAddr := s.cluster.LeaderInternalAddress()
	if leaderAddr == "" {
		return fmt.Errorf("leader address is not available yet")
	}

	if s.selfInternalAddr == "" {
		return fmt.Errorf("self internal address is not configured (POD_IP env not set)")
	}

	s.logger.Info("Sending sync request to leader",
		zap.String("leader_address", leaderAddr),
		zap.String("self_address", s.selfInternalAddr),
	)

	// Send sync request to leader. The leader will block writes, query our
	// keys, push missing/updated objects, delete extraneous ones, and then
	// return. This call blocks until sync is complete.
	syncCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	err := s.client.SyncRequest(syncCtx, leaderAddr, s.cluster.NodeID(), s.selfInternalAddr)
	if err != nil {
		return fmt.Errorf("leader-driven sync failed: %w", err)
	}

	return nil
}
