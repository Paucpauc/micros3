// Package ec provides the leader-driven erasure-coding manager.
//
// The manager runs on the leader and is responsible for:
//   - Converting replica objects into erasure-coded shards (background loop).
//   - Reconstructing object data from EC shards during reads.
//   - Repairing degraded EC objects (reconstructing and redistributing
//     missing shards).
//
// All inter-node communication uses the internal API client.
package ec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/infrastructure/storage/ec"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

// Manager coordinates erasure-coding operations on the leader.
type Manager struct {
	client  *internal_api.Client
	storage s3app.StorageRepository
	cluster s3app.ClusterManager
	codec   *ec.Codec
	minAge  time.Duration
	minSize int64
	logger  *zap.Logger

	runningMu sync.Mutex
	running   bool
}

// NewManager creates an EC manager. Only the leader should use the
// conversion and repair loops; read reconstruction may be used by any
// node that can reach the others.
func NewManager(
	client *internal_api.Client,
	storage s3app.StorageRepository,
	cluster s3app.ClusterManager,
	codec *ec.Codec,
	minAge time.Duration,
	minSize int64,
	logger *zap.Logger,
) *Manager {
	return &Manager{
		client:  client,
		storage: storage,
		cluster: cluster,
		codec:   codec,
		minAge:  minAge,
		minSize: minSize,
		logger:  logger,
	}
}

// StartConvertLoop starts the background loop that scans for eligible
// replica objects and converts them to EC. Only runs on the leader.
func (m *Manager) StartConvertLoop(ctx context.Context, interval time.Duration) {
	m.runningMu.Lock()
	if m.running {
		m.runningMu.Unlock()
		return
	}
	m.running = true
	m.runningMu.Unlock()

	go m.runConvertLoop(ctx, interval)
}

func (m *Manager) runConvertLoop(ctx context.Context, interval time.Duration) {
	m.logger.Info("Starting EC converter loop", zap.Duration("interval", interval))
	defer func() {
		m.runningMu.Lock()
		m.running = false
		m.runningMu.Unlock()
		m.logger.Info("Stopped EC converter loop")
	}()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.cluster.IsLeader() {
				continue
			}
			if err := m.scanAndConvert(ctx); err != nil {
				m.logger.Warn("EC conversion scan failed", zap.Error(err))
			}
		}
	}
}

// StartRepairLoop starts the background loop that scans for degraded EC
// objects and repairs them. Only runs on the leader.
func (m *Manager) StartRepairLoop(ctx context.Context, interval time.Duration) {
	go m.runRepairLoop(ctx, interval)
}

func (m *Manager) runRepairLoop(ctx context.Context, interval time.Duration) {
	m.logger.Info("Starting EC repair loop", zap.Duration("interval", interval))
	defer m.logger.Info("Stopped EC repair loop")

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !m.cluster.IsLeader() {
				continue
			}
			if err := m.scanAndRepair(ctx); err != nil {
				m.logger.Warn("EC repair scan failed", zap.Error(err))
			}
		}
	}
}

// scanAndConvert iterates over all local objects and converts eligible
// replicas to EC.
func (m *Manager) scanAndConvert(ctx context.Context) error {
	buckets, err := m.storage.ListBuckets()
	if err != nil {
		return fmt.Errorf("list buckets: %w", err)
	}

	followers := m.cluster.AliveFollowers()
	totalNodes := len(followers) + 1 // leader + followers
	n := m.codec.N()
	if totalNodes < n {
		// Not enough nodes to distribute shards; skip.
		m.logger.Debug("Not enough nodes for EC conversion",
			zap.Int("nodes", totalNodes),
			zap.Int("required", n),
		)
		return nil
	}

	for _, bucket := range buckets {
		res, err := m.storage.ListObjectsV2(bucket, "", "", "", 1000000)
		if err != nil {
			continue
		}
		for _, obj := range res.Contents {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			meta, err := m.storage.GetObjectMeta(bucket, obj.Key)
			if err != nil {
				continue
			}

			// Skip if already EC.
			if meta.IsEC() {
				continue
			}

			// Skip if too small.
			if meta.ContentLength < m.minSize {
				continue
			}

			// Skip if too new (avoid converting objects still being written).
			if time.Since(meta.ModifiedAt) < m.minAge {
				continue
			}

			if err := m.ConvertToEC(ctx, bucket, obj.Key); err != nil {
				m.logger.Warn("Failed to convert object to EC",
					zap.String("bucket", bucket),
					zap.String("key", obj.Key),
					zap.Error(err),
				)
			}
		}
	}
	return nil
}

// ConvertToEC converts a single replica object into erasure-coded shards
// distributed across all nodes. The leader reads the full object, encodes
// it into k+m shards, and pushes one shard to each node (including itself).
func (m *Manager) ConvertToEC(ctx context.Context, bucket, key string) error {
	reqID := s3.GetRequestID(ctx)
	m.logger.Info("Converting replica to EC",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("request_id", reqID),
	)

	// 1. Read the full object from local storage.
	rc, meta, err := m.storage.GetObject(bucket, key)
	if err != nil {
		return fmt.Errorf("read object: %w", err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("read object body: %w", err)
	}

	// 2. Encode into k+m shards.
	shards, err := m.codec.Encode(data)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	shardSize := int64(len(shards[0]))

	// 3. Build the list of target nodes: leader (self) + followers.
	followers := m.cluster.AliveFollowers()
	targets := make([]string, 0, len(followers)+1)
	// Reserve index 0 for the leader (local).
	targets = append(targets, "")
	targets = append(targets, followers...)

	n := m.codec.N()
	if len(targets) < n {
		return fmt.Errorf("not enough nodes: have %d, need %d", len(targets), n)
	}
	targets = targets[:n]

	// 4. Prepare the new EC metadata.
	ecMeta := meta
	ecMeta.StorageMode = s3.StorageModeEC
	ecMeta.ECParams = s3.ECParams{
		K:         m.codec.K(),
		M:         m.codec.M(),
		ShardSize: shardSize,
	}

	// 5. Distribute shards. Index 0 goes to the leader (local); the rest
	// are pushed to followers via the internal API.
	for i, target := range targets {
		shardMeta := ecMeta
		shardMeta.ECChunkIndex = i

		if target == "" {
			// Local (leader): write shard directly.
			if err := m.storage.PutECShard(bucket, key, i, bytes.NewReader(shards[i]), shardSize, shardMeta); err != nil {
				return fmt.Errorf("write local shard %d: %w", i, err)
			}
		} else {
			putCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := m.client.PutECShard(putCtx, target, bucket, key, i, shardMeta, bytes.NewReader(shards[i]), shardSize)
			cancel()
			if err != nil {
				m.logger.Warn("Failed to push EC shard to node",
					zap.String("bucket", bucket),
					zap.String("key", key),
					zap.Int("shard_index", i),
					zap.String("target", target),
					zap.Error(err),
				)
				return fmt.Errorf("push shard %d to %s: %w", i, target, err)
			}
		}
	}

	// 6. Remove the full replica data from every node (leader + followers).
	// Each node now stores only its EC shard plus the shared metadata.
	if err := m.removeReplicaFromAllNodes(ctx, bucket, key, followers); err != nil {
		m.logger.Warn("Failed to remove old replica data after EC conversion",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.Error(err),
		)
	}

	m.logger.Info("Successfully converted replica to EC",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int("shards", n),
		zap.String("request_id", reqID),
	)
	return nil
}

// removeReplicaFromAllNodes removes the full replica data file for an
// object that has been converted to EC from every node in the cluster.
// The metadata and EC shards are left intact — only the original full
// replica data file is deleted. This reclaims the space occupied by the
// full copy on each node.
func (m *Manager) removeReplicaFromAllNodes(ctx context.Context, bucket, key string, followers []string) error {
	var firstErr error

	// Remove the replica data on the leader (local).
	if err := m.storage.RemoveReplicaData(bucket, key); err != nil {
		m.logger.Warn("Failed to remove local replica data after EC conversion",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.Error(err),
		)
		if firstErr == nil {
			firstErr = err
		}
	}

	// Instruct each follower to remove its replica data as well.
	for _, follower := range followers {
		removeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := m.client.RemoveReplicaData(removeCtx, follower, bucket, key)
		cancel()
		if err != nil {
			m.logger.Warn("Failed to remove replica data on follower after EC conversion",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("follower", follower),
				zap.Error(err),
			)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// ReadECObject reconstructs the full object data from EC shards. The leader
// broadcasts a metadata request to all nodes, collects the available
// shards, and reconstructs the data using the Reed-Solomon codec.
func (m *Manager) ReadECObject(ctx context.Context, bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	reqID := s3.GetRequestID(ctx)

	// 1. Get local metadata to learn the EC params.
	localMeta, err := m.storage.GetObjectMeta(bucket, key)
	if err != nil {
		return nil, s3.ObjectMeta{}, fmt.Errorf("get local meta: %w", err)
	}
	if !localMeta.IsEC() {
		// Not an EC object — fall back to normal read.
		return m.storage.GetObject(bucket, key)
	}

	k := localMeta.ECParams.K
	mShards := localMeta.ECParams.M
	n := k + mShards
	shardSize := localMeta.ECParams.ShardSize

	m.logger.Debug("Reading EC object",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int("k", k),
		zap.Int("m", mShards),
		zap.Int64("shard_size", shardSize),
		zap.String("request_id", reqID),
	)

	// 2. Broadcast metadata request to all nodes to discover which shards
	// each node holds. Use KnownFollowers (which includes SYNCING nodes)
	// instead of AliveFollowers so that EC reconstruction works even when
	// followers are still synchronizing after a cluster restart.
	followers := m.cluster.KnownFollowers()
	allAddrs := append([]string{""}, followers...) // "" = local

	type metaResult struct {
		addr string
		meta s3.ObjectMeta
		err  error
	}
	metaCh := make(chan metaResult, len(allAddrs))
	var wg sync.WaitGroup

	for _, addr := range allAddrs {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			if a == "" {
				meta, err := m.storage.GetObjectMeta(bucket, key)
				metaCh <- metaResult{addr: a, meta: meta, err: err}
				return
			}
			metaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			meta, err := m.client.GetECMeta(metaCtx, a, bucket, key)
			metaCh <- metaResult{addr: a, meta: meta, err: err}
		}(addr)
	}
	wg.Wait()
	close(metaCh)

	// 3. Collect shard locations: map shardIndex -> node address.
	shardLocations := make(map[int]string)
	for res := range metaCh {
		if res.err != nil {
			continue
		}
		if !res.meta.IsEC() {
			continue
		}
		// Verify the EC params match.
		if res.meta.ECParams.K != k || res.meta.ECParams.M != mShards {
			continue
		}
		shardLocations[res.meta.ECChunkIndex] = res.addr
	}

	m.logger.Debug("EC shard locations discovered",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int("available_shards", len(shardLocations)),
		zap.Int("needed", k),
		zap.String("request_id", reqID),
	)

	if len(shardLocations) < k {
		return nil, s3.ObjectMeta{}, fmt.Errorf("insufficient shards: have %d, need %d", len(shardLocations), k)
	}

	// 4. Download k shards (prefer data shards 0..k-1 first).
	shards := make([][]byte, n)
	downloaded := 0
	for idx := 0; idx < n && downloaded < k; idx++ {
		addr, ok := shardLocations[idx]
		if !ok {
			continue
		}
		shardCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)

		if addr == "" {
			rc, err := m.storage.GetECShard(bucket, key, idx)
			if err != nil {
				cancel()
				continue
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			cancel()
			if err != nil {
				continue
			}
			shards[idx] = data
		} else {
			rc, _, err := m.client.GetECShard(shardCtx, addr, bucket, key, idx)
			if err != nil {
				cancel()
				continue
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			cancel()
			if err != nil {
				continue
			}
			shards[idx] = data
		}
		downloaded++
	}

	if downloaded < k {
		return nil, s3.ObjectMeta{}, fmt.Errorf("failed to download enough shards: got %d, need %d", downloaded, k)
	}

	// 5. Reconstruct the full data.
	data, err := m.codec.Reconstruct(shards, localMeta.ContentLength)
	if err != nil {
		return nil, s3.ObjectMeta{}, fmt.Errorf("reconstruct: %w", err)
	}

	return io.NopCloser(bytes.NewReader(data)), localMeta, nil
}

// scanAndRepair iterates over all EC objects and repairs any that are
// missing shards.
func (m *Manager) scanAndRepair(ctx context.Context) error {
	buckets, err := m.storage.ListBuckets()
	if err != nil {
		return fmt.Errorf("list buckets: %w", err)
	}

	for _, bucket := range buckets {
		res, err := m.storage.ListObjectsV2(bucket, "", "", "", 1000000)
		if err != nil {
			continue
		}
		for _, obj := range res.Contents {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			meta, err := m.storage.GetObjectMeta(bucket, obj.Key)
			if err != nil {
				continue
			}
			if !meta.IsEC() {
				continue
			}

			if err := m.RepairECObject(ctx, bucket, obj.Key); err != nil {
				m.logger.Warn("Failed to repair EC object",
					zap.String("bucket", bucket),
					zap.String("key", obj.Key),
					zap.Error(err),
				)
			}
		}
	}
	return nil
}

// RepairECObject checks if an EC object is missing shards and repairs it.
// The leader broadcasts a metadata request to all nodes, collects the
// available shards, reconstructs any missing ones, and pushes them to the
// nodes that need them.
func (m *Manager) RepairECObject(ctx context.Context, bucket, key string) error {
	reqID := s3.GetRequestID(ctx)

	localMeta, err := m.storage.GetObjectMeta(bucket, key)
	if err != nil {
		return fmt.Errorf("get local meta: %w", err)
	}
	if !localMeta.IsEC() {
		return nil
	}

	k := localMeta.ECParams.K
	mShards := localMeta.ECParams.M
	n := k + mShards

	// 1. Broadcast metadata to discover which nodes have which shards.
	// Use KnownFollowers (which includes SYNCING nodes) instead of
	// AliveFollowers so that repair works even when followers are still
	// synchronizing after a cluster restart.
	followers := m.cluster.KnownFollowers()
	allAddrs := append([]string{""}, followers...)

	type metaResult struct {
		addr string
		meta s3.ObjectMeta
		err  error
	}
	metaCh := make(chan metaResult, len(allAddrs))
	var wg sync.WaitGroup

	for _, addr := range allAddrs {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			if a == "" {
				meta, err := m.storage.GetObjectMeta(bucket, key)
				metaCh <- metaResult{addr: a, meta: meta, err: err}
				return
			}
			metaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			meta, err := m.client.GetECMeta(metaCtx, a, bucket, key)
			metaCh <- metaResult{addr: a, meta: meta, err: err}
		}(addr)
	}
	wg.Wait()
	close(metaCh)

	// 2. Map shardIndex -> node address.
	shardLocations := make(map[int]string)
	for res := range metaCh {
		if res.err != nil {
			continue
		}
		if !res.meta.IsEC() {
			continue
		}
		if res.meta.ECParams.K != k || res.meta.ECParams.M != mShards {
			continue
		}
		shardLocations[res.meta.ECChunkIndex] = res.addr
	}

	available := len(shardLocations)
	if available >= n {
		// All shards present — no repair needed.
		return nil
	}

	m.logger.Info("EC object needs repair",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int("available_shards", available),
		zap.Int("total_shards", n),
		zap.String("request_id", reqID),
	)

	if available < k {
		return fmt.Errorf("cannot repair: insufficient shards (have %d, need %d)", available, k)
	}

	// 3. Download available shards.
	shards := make([][]byte, n)
	downloaded := 0
	for idx := 0; idx < n && downloaded < k; idx++ {
		addr, ok := shardLocations[idx]
		if !ok {
			continue
		}
		shardCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)

		if addr == "" {
			rc, err := m.storage.GetECShard(bucket, key, idx)
			if err != nil {
				cancel()
				continue
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			cancel()
			if err != nil {
				continue
			}
			shards[idx] = data
		} else {
			rc, _, err := m.client.GetECShard(shardCtx, addr, bucket, key, idx)
			if err != nil {
				cancel()
				continue
			}
			data, err := io.ReadAll(rc)
			rc.Close()
			cancel()
			if err != nil {
				continue
			}
			shards[idx] = data
		}
		downloaded++
	}

	if downloaded < k {
		return fmt.Errorf("failed to download enough shards for repair: got %d, need %d", downloaded, k)
	}

	// 4. Reconstruct all missing shards.
	_, err = m.codec.Reconstruct(shards, localMeta.ContentLength)
	if err != nil {
		return fmt.Errorf("reconstruct for repair: %w", err)
	}

	// 5. Push missing shards to the nodes that should hold them.
	// We assign missing shards to nodes that don't currently hold any shard
	// for this object, or to the original node if it's back online.
	usedAddrs := make(map[string]bool)
	for _, addr := range shardLocations {
		usedAddrs[addr] = true
	}

	// Build a list of nodes that can receive missing shards.
	availableNodes := make([]string, 0, len(allAddrs))
	for _, addr := range allAddrs {
		availableNodes = append(availableNodes, addr)
	}

	repairMeta := localMeta
	nodeIdx := 0
	for idx := 0; idx < n; idx++ {
		if _, ok := shardLocations[idx]; ok {
			continue // shard already present somewhere
		}

		// Find a node to push this shard to.
		if nodeIdx >= len(availableNodes) {
			break
		}
		target := availableNodes[nodeIdx]
		nodeIdx++

		repairMeta.ECChunkIndex = idx
		shardData := shards[idx]

		if target == "" {
			if err := m.storage.PutECShard(bucket, key, idx, bytes.NewReader(shardData), int64(len(shardData)), repairMeta); err != nil {
				m.logger.Warn("Failed to write repaired shard locally",
					zap.String("bucket", bucket),
					zap.String("key", key),
					zap.Int("shard_index", idx),
					zap.Error(err),
				)
			}
		} else {
			putCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err := m.client.PutECShard(putCtx, target, bucket, key, idx, repairMeta, bytes.NewReader(shardData), int64(len(shardData)))
			cancel()
			if err != nil {
				m.logger.Warn("Failed to push repaired shard to node",
					zap.String("bucket", bucket),
					zap.String("key", key),
					zap.Int("shard_index", idx),
					zap.String("target", target),
					zap.Error(err),
				)
			}
		}
	}

	m.logger.Info("EC object repaired",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int("repaired_shards", n-available),
		zap.String("request_id", reqID),
	)
	return nil
}
