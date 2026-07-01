package replication

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/cluster"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

type SyncWorker struct {
	client           *internal_api.Client
	cluster          s3app.ClusterManager
	storage          s3app.StorageRepository
	logger           *zap.Logger
	selfInternalAddr string // own internal address to send to leader on SyncDone

	runningMu sync.Mutex
	running   bool
}

func NewSyncWorker(
	client *internal_api.Client,
	cluster s3app.ClusterManager,
	storage s3app.StorageRepository,
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
		storage:          storage,
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
				s.logger.Info("Follower is in SYNCING state, starting sync process")
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

// Synchronize runs a single delta-sync pass against the leader
func (s *SyncWorker) Synchronize(ctx context.Context) error {
	leaderAddr := s.cluster.LeaderInternalAddress()
	if leaderAddr == "" {
		return fmt.Errorf("leader address is not available yet")
	}

	s.logger.Info("Initiating sync with leader", zap.String("leader_address", leaderAddr))

	// 1. Sync start (leader blocks writes)
	syncStartCtx, cancelStart := context.WithTimeout(ctx, 5*time.Second)
	err := s.client.SyncStart(syncStartCtx, leaderAddr, s.cluster.NodeID())
	cancelStart()
	if err != nil {
		return fmt.Errorf("failed to start sync with leader: %w", err)
	}

	// Start background heartbeat to keep lease active on leader
	heartbeatCtx, heartbeatCancel := context.WithCancel(ctx)
	defer heartbeatCancel()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				s.logger.Debug("Sending sync keepalive heartbeat to leader")
				_ = s.client.SyncHeartbeat(heartbeatCtx, leaderAddr, s.cluster.NodeID())
			}
		}
	}()

	// Make sure we notify the leader that we are done/aborted if we return early with an error
	// so the leader doesn't block writes forever
	var syncDoneSuccessful bool
	defer func() {
		if !syncDoneSuccessful {
			s.logger.Warn("Sync failed, informing leader to unblock writes")
			syncDoneCtx, cancelDone := context.WithTimeout(context.Background(), 5*time.Second)
			_ = s.client.SyncDone(syncDoneCtx, leaderAddr, s.cluster.NodeID(), s.selfInternalAddr)
			cancelDone()
		}
	}()

	// 2. Fetch leader keys
	getKeysCtx, cancelKeys := context.WithTimeout(ctx, 15*time.Second)
	leaderResp, err := s.client.GetKeys(getKeysCtx, leaderAddr)
	cancelKeys()
	if err != nil {
		return fmt.Errorf("failed to fetch keys from leader: %w", err)
	}

	leaderKeysMap := make(map[string]internal_api.KeyInfo)
	for _, k := range leaderResp.Keys {
		leaderKeysMap[k.Bucket+"/"+k.Key] = k
	}

	// 3. Fetch local keys
	localKeys, err := s.getLocalKeys()
	if err != nil {
		return fmt.Errorf("failed to retrieve local keys: %w", err)
	}

	localKeysMap := make(map[string]internal_api.KeyInfo)
	for _, k := range localKeys {
		localKeysMap[k.Bucket+"/"+k.Key] = k
	}

	// 4. Download missing or out-of-sync files
	for path, leaderKey := range leaderKeysMap {
		localKey, exists := localKeysMap[path]
		if !exists || localKey.CRC32 != leaderKey.CRC32 || localKey.Size != leaderKey.Size {
			s.logger.Info("Syncing file",
				zap.String("bucket", leaderKey.Bucket),
				zap.String("key", leaderKey.Key),
				zap.Bool("exists_locally", exists),
			)

			if err := s.syncFile(ctx, leaderAddr, leaderKey); err != nil {
				return fmt.Errorf("failed to sync file %s: %w", path, err)
			}
		}
	}

	// 5. Delete extraneous local files
	for path, localKey := range localKeysMap {
		if _, exists := leaderKeysMap[path]; !exists {
			s.logger.Info("Deleting extraneous file",
				zap.String("bucket", localKey.Bucket),
				zap.String("key", localKey.Key),
			)
			if err := s.storage.DeleteObject(localKey.Bucket, localKey.Key); err != nil {
				s.logger.Warn("Failed to delete extraneous file", zap.String("path", path), zap.Error(err))
			}
		}
	}

	// 6. Sync done (leader unblocks writes and updates our address)
	syncDoneCtx, cancelDone := context.WithTimeout(ctx, 5*time.Second)
	err = s.client.SyncDone(syncDoneCtx, leaderAddr, s.cluster.NodeID(), url.QueryEscape(s.selfInternalAddr))
	cancelDone()
	if err != nil {
		return fmt.Errorf("failed to signal sync completion to leader: %w", err)
	}

	syncDoneSuccessful = true
	return nil
}

func (s *SyncWorker) syncFile(ctx context.Context, leaderAddr string, keyInfo internal_api.KeyInfo) error {
	// Download object from leader with a safe 15-minute timeout to support large files
	getCtx, cancelGet := context.WithTimeout(ctx, 15*time.Minute)
	defer cancelGet()
	body, meta, err := s.client.GetObject(getCtx, leaderAddr, keyInfo.Bucket, keyInfo.Key)
	if err != nil {
		return err
	}
	defer body.Close()

	// Stage locally using 2PC mechanisms
	txID := "sync-" + uuid.New().String()
	tx := s3.Transaction{
		ID:        txID,
		Operation: s3.OpPut,
		Bucket:    keyInfo.Bucket,
		Key:       keyInfo.Key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	// Ensure local bucket exists
	if err := s.storage.CreateBucket(keyInfo.Bucket); err != nil {
		return err
	}

	_, err = s.storage.StageObject(txID, body, keyInfo.Size, meta, tx)
	if err != nil {
		_ = s.storage.AbortTransaction(txID)
		return err
	}

	// Commit transaction locally
	_, err = s.storage.CommitTransaction(txID, keyInfo.Bucket, keyInfo.Key)
	if err != nil {
		return err
	}

	return nil
}

func (s *SyncWorker) getLocalKeys() ([]internal_api.KeyInfo, error) {
	buckets, err := s.storage.ListBuckets()
	if err != nil {
		return nil, err
	}

	var keys []internal_api.KeyInfo
	for _, b := range buckets {
		res, err := s.storage.ListObjectsV2(b, "", "", "", 1000000)
		if err != nil {
			continue
		}
		for _, c := range res.Contents {
			meta, err := s.storage.GetObjectMeta(b, c.Key)
			if err != nil {
				continue
			}
			keys = append(keys, internal_api.KeyInfo{
				Bucket:     b,
				Key:        c.Key,
				CRC32:      meta.CRC32,
				Size:       c.Size,
				ModifiedAt: c.LastModified,
			})
		}
	}
	return keys, nil
}
