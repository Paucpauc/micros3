package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

var _ s3app.SyncCoordinator = (*SyncCoordinatorImpl)(nil)

// SyncCoordinatorImpl implements leader-driven synchronization. When a
// follower requests sync, the leader queries the follower's keys, compares
// them with its own, pushes missing/updated objects via 2PC, and deletes
// extraneous keys on the follower.
type SyncCoordinatorImpl struct {
	client  *internal_api.Client
	storage s3app.StorageRepository
	logger  *zap.Logger
}

// NewSyncCoordinator creates a leader-driven sync coordinator.
func NewSyncCoordinator(
	client *internal_api.Client,
	storage s3app.StorageRepository,
	logger *zap.Logger,
) *SyncCoordinatorImpl {
	return &SyncCoordinatorImpl{
		client:  client,
		storage: storage,
		logger:  logger,
	}
}

// SyncFollower drives the synchronization of a single follower. It is called
// on the leader after writes have been blocked.
func (sc *SyncCoordinatorImpl) SyncFollower(ctx context.Context, nodeID, followerAddr string) error {
	sc.logger.Info("Starting leader-driven sync of follower",
		zap.String("node_id", nodeID),
		zap.String("follower_addr", followerAddr),
	)

	// 1. Fetch follower keys
	getKeysCtx, cancelKeys := context.WithTimeout(ctx, 30*time.Second)
	followerResp, err := sc.client.GetKeys(getKeysCtx, followerAddr)
	cancelKeys()
	if err != nil {
		return fmt.Errorf("failed to fetch keys from follower: %w", err)
	}

	followerKeysMap := make(map[string]internal_api.KeyInfo)
	for _, k := range followerResp.Keys {
		followerKeysMap[k.Bucket+"/"+k.Key] = k
	}

	// 2. Fetch leader (local) keys
	leaderKeys, err := sc.getLocalKeys()
	if err != nil {
		return fmt.Errorf("failed to retrieve local keys: %w", err)
	}

	leaderKeysMap := make(map[string]internal_api.KeyInfo)
	for _, k := range leaderKeys {
		leaderKeysMap[k.Bucket+"/"+k.Key] = k
	}

	// 3. Push missing or out-of-sync objects to the follower
	for path, leaderKey := range leaderKeysMap {
		followerKey, exists := followerKeysMap[path]
		if !exists || followerKey.CRC32 != leaderKey.CRC32 || followerKey.Size != leaderKey.Size {
			sc.logger.Info("Pushing object to follower",
				zap.String("bucket", leaderKey.Bucket),
				zap.String("key", leaderKey.Key),
				zap.Bool("exists_on_follower", exists),
				zap.String("node_id", nodeID),
			)

			if err := sc.pushObject(ctx, followerAddr, leaderKey); err != nil {
				return fmt.Errorf("failed to push object %s to follower: %w", path, err)
			}
		}
	}

	// 4. Delete extraneous keys on the follower
	var extraneous []internal_api.KeyInfo
	for path, followerKey := range followerKeysMap {
		if _, exists := leaderKeysMap[path]; !exists {
			sc.logger.Info("Requesting deletion of extraneous object on follower",
				zap.String("bucket", followerKey.Bucket),
				zap.String("key", followerKey.Key),
				zap.String("node_id", nodeID),
			)
			extraneous = append(extraneous, followerKey)
		}
	}

	if len(extraneous) > 0 {
		deleteCtx, cancelDelete := context.WithTimeout(ctx, 30*time.Second)
		err := sc.client.SyncDelete(deleteCtx, followerAddr, extraneous)
		cancelDelete()
		if err != nil {
			sc.logger.Warn("Failed to delete extraneous objects on follower",
				zap.String("node_id", nodeID),
				zap.Int("count", len(extraneous)),
				zap.Error(err),
			)
			return fmt.Errorf("failed to delete extraneous objects on follower: %w", err)
		}
	}

	sc.logger.Info("Leader-driven sync of follower completed",
		zap.String("node_id", nodeID),
		zap.Int("pushed", countPushed(leaderKeysMap, followerKeysMap)),
		zap.Int("deleted", len(extraneous)),
	)
	return nil
}

// pushObject sends a single object from the leader's local storage to the
// follower using the 2PC Prepare/Commit protocol.
func (sc *SyncCoordinatorImpl) pushObject(ctx context.Context, followerAddr string, keyInfo internal_api.KeyInfo) error {
	// Read object from local storage
	rc, meta, err := sc.storage.GetObject(keyInfo.Bucket, keyInfo.Key)
	if err != nil {
		return fmt.Errorf("failed to read local object: %w", err)
	}
	defer rc.Close()

	// Prepare on follower
	txID := "sync-" + uuid.New().String()
	tx := s3.Transaction{
		ID:        txID,
		Operation: s3.OpPut,
		Bucket:    keyInfo.Bucket,
		Key:       keyInfo.Key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	prepareCtx, cancelPrepare := context.WithTimeout(ctx, 15*time.Minute)
	defer cancelPrepare()

	if err := sc.client.Prepare(prepareCtx, followerAddr, tx, meta, rc, meta.ContentLength); err != nil {
		return fmt.Errorf("prepare failed on follower: %w", err)
	}

	// Commit on follower
	commitCtx, cancelCommit := context.WithTimeout(ctx, 30*time.Second)
	defer cancelCommit()

	if err := sc.client.Commit(commitCtx, followerAddr, txID, keyInfo.Bucket, keyInfo.Key); err != nil {
		// Best-effort abort
		abortCtx, cancelAbort := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelAbort()
		_ = sc.client.Abort(abortCtx, followerAddr, txID)
		return fmt.Errorf("commit failed on follower: %w", err)
	}

	return nil
}

func (sc *SyncCoordinatorImpl) getLocalKeys() ([]internal_api.KeyInfo, error) {
	buckets, err := sc.storage.ListBuckets()
	if err != nil {
		return nil, err
	}

	var keys []internal_api.KeyInfo
	for _, b := range buckets {
		res, err := sc.storage.ListObjectsV2(b, "", "", "", 1000000)
		if err != nil {
			continue
		}
		for _, c := range res.Contents {
			meta, err := sc.storage.GetObjectMeta(b, c.Key)
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

func countPushed(leader, follower map[string]internal_api.KeyInfo) int {
	count := 0
	for path, lk := range leader {
		fk, exists := follower[path]
		if !exists || fk.CRC32 != lk.CRC32 || fk.Size != lk.Size {
			count++
		}
	}
	return count
}
