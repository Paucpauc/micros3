package s3app

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"go.uber.org/zap"
)

type Service struct {
	storage            StorageRepository
	replicator         Replicator
	cluster            ClusterManager
	metrics            MetricsRecorder
	syncCoordinator    SyncCoordinator
	ecReader           ECReader
	logger             *zap.Logger
	syncingNodes       map[string]time.Time
	activeWrites       int
	syncMutex          sync.Mutex
	writeCond          *sync.Cond
	writeBlockBehavior string
}

func NewService(storage StorageRepository, replicator Replicator, cluster ClusterManager, metrics MetricsRecorder, logger *zap.Logger) *Service {
	s := &Service{
		storage:            storage,
		replicator:         replicator,
		cluster:            cluster,
		metrics:            metrics,
		logger:             logger,
		syncingNodes:       make(map[string]time.Time),
		writeBlockBehavior: "reject",
	}
	s.writeCond = sync.NewCond(&s.syncMutex)
	return s
}

// SetSyncCoordinator injects the leader-driven sync coordinator. It is set
// after construction because the coordinator depends on the internal API
// client which is created alongside the cluster manager.
func (s *Service) SetSyncCoordinator(coord SyncCoordinator) {
	s.syncCoordinator = coord
}

// SetECReader injects the erasure-coding reader used to reconstruct object
// data from EC shards. It is set after construction to avoid a circular
// dependency between the application and replication layers.
func (s *Service) SetECReader(reader ECReader) {
	s.ecReader = reader
}

func (s *Service) SetWriteBlockBehavior(behavior string) {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	if behavior == "wait" || behavior == "reject" {
		s.logger.Info("Setting write block behavior", zap.String("behavior", behavior))
		s.writeBlockBehavior = behavior
	}
}

func (s *Service) UpdateStorageMetrics() {
	buckets, err := s.storage.ListBuckets()
	if err != nil {
		return
	}
	s.metrics.SetBucketsTotal(len(buckets))

	// Reset per-bucket gauges before re-populating.
	for _, b := range buckets {
		s.metrics.SetObjectsTotal(b, 0)
		s.metrics.SetStorageUsedBytes(b, 0)
	}

	for _, b := range buckets {
		var objectsCount int64
		var storageUsed int64
		res, err := s.storage.ListObjectsV2(b, "", "", "", 100000)
		if err == nil {
			for _, c := range res.Contents {
				objectsCount++
				storageUsed += c.Size
			}
		}
		s.metrics.SetObjectsTotal(b, objectsCount)
		s.metrics.SetStorageUsedBytes(b, storageUsed)
	}
}

func (s *Service) UpdateClusterMetrics() {
	s.metrics.SetClusterRole(s.cluster.IsLeader())
	s.metrics.SetClusterStatus(s.cluster.Status())
}

// ShouldProxyToLeader decides whether the current node should forward a
// request to the cluster leader instead of handling it locally. Returns
// true when the node is not the leader and the request cannot be served
// locally (non-read method, local reads disabled, or cluster not ready).
func (s *Service) ShouldProxyToLeader(method string, allowLocalReads bool) bool {
	if s.cluster.IsLeader() {
		return false
	}
	isRead := method == http.MethodGet || method == http.MethodHead
	if allowLocalReads && s.cluster.Status() == "READY" && isRead {
		return false
	}
	return true
}

func (s *Service) StartSyncLease(nodeID string) {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	if nodeID == "" {
		nodeID = "unknown"
	}
	s.logger.Info("Starting replica sync lease (blocking new writes)", zap.String("nodeID", nodeID))
	s.syncingNodes[nodeID] = time.Now()

	for s.activeWrites > 0 {
		s.logger.Info("Waiting for active write transactions to drain", zap.Int("activeWrites", s.activeWrites), zap.String("nodeID", nodeID))
		s.writeCond.Wait()
	}
	s.logger.Info("All active write transactions drained, sync lease started", zap.String("nodeID", nodeID))
	s.metrics.SetSyncLeaseActive(true)
	s.metrics.SetWritesBlocked(true)
}

func (s *Service) HeartbeatSyncLease(nodeID string) {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	if nodeID == "" {
		nodeID = "unknown"
	}
	s.syncingNodes[nodeID] = time.Now()
	s.logger.Debug("Sync lease heartbeat received on leader, timer reset", zap.String("nodeID", nodeID))
}

func (s *Service) EndSyncLease(nodeID string) {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	if nodeID == "" {
		nodeID = "unknown"
	}
	s.logger.Info("Ending replica sync lease", zap.String("nodeID", nodeID))
	delete(s.syncingNodes, nodeID)
	s.writeCond.Broadcast()
	if len(s.syncingNodes) == 0 {
		s.metrics.SetSyncLeaseActive(false)
		s.metrics.SetWritesBlocked(false)
	}
}

// HandleSyncRequest is called on the leader when a follower requests
// synchronization. The leader blocks new writes, drives the sync process
// via the SyncCoordinator, then unblocks writes and marks the follower as
// alive.
func (s *Service) HandleSyncRequest(ctx context.Context, nodeID, followerAddr string) error {
	if s.syncCoordinator == nil {
		return errors.New("sync coordinator is not configured")
	}

	// Block new writes and drain active writes
	s.StartSyncLease(nodeID)

	var syncErr error
	defer func() {
		s.EndSyncLease(nodeID)
	}()

	// Drive the sync process from the leader side
	syncErr = s.syncCoordinator.SyncFollower(ctx, nodeID, followerAddr)
	if syncErr != nil {
		s.logger.Error("SyncFollower failed",
			zap.String("node_id", nodeID),
			zap.String("follower_addr", followerAddr),
			zap.Error(syncErr),
		)
		return syncErr
	}

	// Mark follower as alive so it participates in 2PC going forward
	s.cluster.MarkAlive(nodeID, followerAddr)
	s.logger.Info("Follower synchronized and marked alive",
		zap.String("node_id", nodeID),
		zap.String("follower_addr", followerAddr),
	)
	return nil
}

func (s *Service) IsWritesBlocked() bool {
	s.syncMutex.Lock()
	defer s.syncMutex.Unlock()
	return s.isWritesBlockedLocked()
}

func (s *Service) isWritesBlockedLocked() bool {
	now := time.Now()
	hasActiveSync := false
	expiredAny := false
	for nodeID, lastSeen := range s.syncingNodes {
		if now.Sub(lastSeen) <= 30*time.Second {
			hasActiveSync = true
		} else {
			s.logger.Warn("Sync lease expired on leader for follower", zap.String("nodeID", nodeID))
			delete(s.syncingNodes, nodeID)
			expiredAny = true
		}
	}
	if expiredAny {
		s.writeCond.Broadcast()
		if len(s.syncingNodes) == 0 {
			s.metrics.SetSyncLeaseActive(false)
			s.metrics.SetWritesBlocked(false)
		}
	}
	return hasActiveSync
}

// --- Bucket Operations ---

func (s *Service) CreateBucket(bucket string) error {
	s.logger.Info("CreateBucket", zap.String("bucket", bucket))
	return s.storage.CreateBucket(bucket)
}

func (s *Service) DeleteBucket(bucket string) error {
	s.logger.Info("DeleteBucket", zap.String("bucket", bucket))
	return s.storage.DeleteBucket(bucket)
}

func (s *Service) HasBucket(bucket string) (bool, error) {
	return s.storage.HasBucket(bucket)
}

func (s *Service) EnsureBucket(bucket string) error {
	exists, err := s.storage.HasBucket(bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	s.logger.Info("Auto-creating bucket on write", zap.String("bucket", bucket))
	return s.storage.CreateBucket(bucket)
}

func (s *Service) ListBuckets() ([]string, error) {
	return s.storage.ListBuckets()
}

// --- Object Operations ---

func (s *Service) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64, meta s3.ObjectMeta) (s3.ObjectMeta, error) {
	reqID := s3.GetRequestID(ctx)

	if err := s.EnsureBucket(bucket); err != nil {
		return s3.ObjectMeta{}, err
	}

	s.syncMutex.Lock()
	for s.isWritesBlockedLocked() {
		if s.writeBlockBehavior == "wait" {
			s.logger.Info("Writes are blocked due to replication sync, waiting for unblock...",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("request_id", reqID),
			)
			s.writeCond.Wait()
		} else {
			s.syncMutex.Unlock()
			s.logger.Warn("PutObject rejected: writes are blocked",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("request_id", reqID),
			)
			return s3.ObjectMeta{}, errors.New("ServiceUnavailable: writes are blocked during synchronization")
		}
	}
	s.activeWrites++
	s.metrics.SetActiveWrites(s.activeWrites)
	s.syncMutex.Unlock()

	defer func() {
		s.syncMutex.Lock()
		s.activeWrites--
		s.metrics.SetActiveWrites(s.activeWrites)
		s.writeCond.Broadcast()
		s.syncMutex.Unlock()
	}()

	s.logger.Info("PutObject initiating 2PC",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int64("size", size),
		zap.String("request_id", reqID),
	)

	txID := uuid.New().String()
	tx := s3.Transaction{
		ID:        txID,
		Operation: s3.OpPut,
		Bucket:    bucket,
		Key:       key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	// 1. Stage locally (updates meta with length, CRC, ETag)
	s.logger.Debug("Staging object locally",
		zap.String("txID", txID),
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("request_id", reqID),
	)
	stagedMeta, err := s.storage.StageObject(txID, r, size, meta, tx)
	if err != nil {
		s.logger.Error("PutObject local staging failed",
			zap.String("txID", txID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		return s3.ObjectMeta{}, err
	}

	// 2. Prepare on replicas (if any followers exist)
	followers := s.cluster.AliveFollowers()
	if len(followers) > 0 {
		s.logger.Debug("Preparing transaction on replicas",
			zap.String("txID", txID),
			zap.Strings("replicas", followers),
			zap.String("request_id", reqID),
		)
		prepareErrors := s.replicator.PrepareAll(ctx, tx, stagedMeta)

		allPrepared := true
		for nodeID, err := range prepareErrors {
			if err != nil {
				s.logger.Warn("Prepare failed on replica, marking as DEAD",
					zap.String("txID", txID),
					zap.String("nodeID", nodeID),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				s.cluster.MarkDead(nodeID)
				s.metrics.IncReplicationPrepare("fail")
				allPrepared = false
			} else {
				s.metrics.IncReplicationPrepare("success")
			}
		}

		if !allPrepared {
			s.logger.Warn("2PC Prepare phase failed. Aborting transaction",
				zap.String("txID", txID),
				zap.String("request_id", reqID),
			)
			_ = s.storage.AbortTransaction(txID)
			_ = s.replicator.AbortAll(ctx, txID)
			s.metrics.IncReplicationAbort("prepare_failed")
			return s3.ObjectMeta{}, errors.New("replication prepare failed, transaction aborted")
		}
	}

	// 3. Commit locally
	s.logger.Debug("Committing transaction locally",
		zap.String("txID", txID),
		zap.String("request_id", reqID),
	)
	committedMeta, err := s.storage.CommitTransaction(txID, bucket, key)
	if err != nil {
		s.logger.Error("Local commit failed, aborting replicas",
			zap.String("txID", txID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		if len(followers) > 0 {
			_ = s.replicator.AbortAll(ctx, txID)
			s.metrics.IncReplicationAbort("local_commit_failed")
		}
		return s3.ObjectMeta{}, err
	}

	// 4. Commit on replicas
	if len(followers) > 0 {
		s.logger.Debug("Committing transaction on replicas",
			zap.String("txID", txID),
			zap.String("request_id", reqID),
		)
		commitErrors := s.replicator.CommitAll(ctx, txID, bucket, key)
		for nodeID, err := range commitErrors {
			if err != nil {
				s.logger.Warn("Commit failed on replica, marking as DEAD",
					zap.String("txID", txID),
					zap.String("nodeID", nodeID),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				s.cluster.MarkDead(nodeID)
				s.metrics.IncReplicationCommit("fail")
			} else {
				s.metrics.IncReplicationCommit("success")
			}
		}
	}

	s.logger.Info("PutObject 2PC success",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("etag", committedMeta.ETag),
		zap.String("request_id", reqID),
	)
	return committedMeta, nil
}

func (s *Service) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	// Check if the object is stored in erasure-coded form. If so, use the
	// EC reader to reconstruct the full data from shards distributed across
	// the cluster.
	meta, err := s.storage.GetObjectMeta(bucket, key)
	if err != nil {
		return nil, s3.ObjectMeta{}, err
	}
	if meta.IsEC() && s.ecReader != nil {
		return s.ecReader.ReadECObject(context.Background(), bucket, key)
	}
	return s.storage.GetObject(bucket, key)
}

func (s *Service) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	return s.storage.GetObjectMeta(bucket, key)
}

func (s *Service) DeleteObject(ctx context.Context, bucket, key string) error {
	reqID := s3.GetRequestID(ctx)

	s.syncMutex.Lock()
	for s.isWritesBlockedLocked() {
		if s.writeBlockBehavior == "wait" {
			s.logger.Info("Writes are blocked due to replication sync, waiting for unblock...",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("request_id", reqID),
			)
			s.writeCond.Wait()
		} else {
			s.syncMutex.Unlock()
			s.logger.Warn("DeleteObject rejected: writes are blocked",
				zap.String("bucket", bucket),
				zap.String("key", key),
				zap.String("request_id", reqID),
			)
			return errors.New("ServiceUnavailable: writes are blocked during synchronization")
		}
	}
	s.activeWrites++
	s.metrics.SetActiveWrites(s.activeWrites)
	s.syncMutex.Unlock()

	defer func() {
		s.syncMutex.Lock()
		s.activeWrites--
		s.metrics.SetActiveWrites(s.activeWrites)
		s.writeCond.Broadcast()
		s.syncMutex.Unlock()
	}()

	s.logger.Info("DeleteObject initiating 2PC",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("request_id", reqID),
	)

	txID := uuid.New().String()
	tx := s3.Transaction{
		ID:        txID,
		Operation: s3.OpDelete,
		Bucket:    bucket,
		Key:       key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	// 1. Stage locally
	s.logger.Debug("Staging delete locally",
		zap.String("txID", txID),
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("request_id", reqID),
	)
	_, err := s.storage.StageObject(txID, nil, 0, s3.ObjectMeta{}, tx)
	if err != nil {
		s.logger.Error("DeleteObject local staging failed",
			zap.String("txID", txID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		return err
	}

	// 2. Prepare on replicas
	followers := s.cluster.AliveFollowers()
	if len(followers) > 0 {
		s.logger.Debug("Preparing delete transaction on replicas",
			zap.String("txID", txID),
			zap.Strings("replicas", followers),
			zap.String("request_id", reqID),
		)
		prepareErrors := s.replicator.PrepareAll(ctx, tx, s3.ObjectMeta{})

		allPrepared := true
		for nodeID, err := range prepareErrors {
			if err != nil {
				s.logger.Warn("Prepare delete failed on replica, marking as DEAD",
					zap.String("txID", txID),
					zap.String("nodeID", nodeID),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				s.cluster.MarkDead(nodeID)
				s.metrics.IncReplicationPrepare("fail")
				allPrepared = false
			} else {
				s.metrics.IncReplicationPrepare("success")
			}
		}

		if !allPrepared {
			s.logger.Warn("2PC Delete Prepare phase failed. Aborting transaction",
				zap.String("txID", txID),
				zap.String("request_id", reqID),
			)
			_ = s.storage.AbortTransaction(txID)
			_ = s.replicator.AbortAll(ctx, txID)
			s.metrics.IncReplicationAbort("prepare_failed")
			return errors.New("replication prepare failed, transaction aborted")
		}
	}

	// 3. Commit locally
	s.logger.Debug("Committing delete transaction locally",
		zap.String("txID", txID),
		zap.String("request_id", reqID),
	)
	_, err = s.storage.CommitTransaction(txID, bucket, key)
	if err != nil {
		s.logger.Error("Local delete commit failed, aborting replicas",
			zap.String("txID", txID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		if len(followers) > 0 {
			_ = s.replicator.AbortAll(ctx, txID)
			s.metrics.IncReplicationAbort("local_commit_failed")
		}
		return err
	}

	// 4. Commit on replicas
	if len(followers) > 0 {
		s.logger.Debug("Committing delete transaction on replicas",
			zap.String("txID", txID),
			zap.String("request_id", reqID),
		)
		commitErrors := s.replicator.CommitAll(ctx, txID, bucket, key)
		for nodeID, err := range commitErrors {
			if err != nil {
				s.logger.Warn("Commit delete failed on replica, marking as DEAD",
					zap.String("txID", txID),
					zap.String("nodeID", nodeID),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				s.cluster.MarkDead(nodeID)
				s.metrics.IncReplicationCommit("fail")
			} else {
				s.metrics.IncReplicationCommit("success")
			}
		}
	}

	s.logger.Info("DeleteObject 2PC success",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("request_id", reqID),
	)
	return nil
}

func (s *Service) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	return s.storage.ListObjectsV2(bucket, prefix, delimiter, continuationToken, maxKeys)
}

// --- Copy Object Operation ---

func (s *Service) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) (s3.ObjectMeta, error) {
	reqID := s3.GetRequestID(ctx)
	s.logger.Info("CopyObject",
		zap.String("srcBucket", srcBucket),
		zap.String("srcKey", srcKey),
		zap.String("dstBucket", dstBucket),
		zap.String("dstKey", dstKey),
		zap.String("request_id", reqID),
	)

	// Get source object. Use s.GetObject (not s.storage.GetObject) so that
	// erasure-coded objects are transparently reconstructed from shards.
	rc, meta, err := s.GetObject(srcBucket, srcKey)
	if err != nil {
		return s3.ObjectMeta{}, fmt.Errorf("failed to get source object: %w", err)
	}
	defer rc.Close()

	// Perform standard PutObject flow
	return s.PutObject(ctx, dstBucket, dstKey, rc, meta.ContentLength, s3.ObjectMeta{
		ContentType:  meta.ContentType,
		UserMetadata: meta.UserMetadata,
		CreatedAt:    time.Now(),
		ModifiedAt:   time.Now(),
	})
}

// --- Multipart Upload Operations ---

func (s *Service) CreateMultipartUpload(bucket, key string) (string, error) {
	s.logger.Info("CreateMultipartUpload", zap.String("bucket", bucket), zap.String("key", key))
	if err := s.EnsureBucket(bucket); err != nil {
		return "", err
	}
	return s.storage.CreateMultipartUpload(bucket, key)
}

func (s *Service) SaveMultipartPart(bucket, uploadID string, partNum int, r io.Reader) (s3.UploadPart, error) {
	s.logger.Debug("SaveMultipartPart", zap.String("bucket", bucket), zap.String("uploadID", uploadID), zap.Int("partNum", partNum))
	return s.storage.SaveMultipartPart(bucket, uploadID, partNum, r)
}

func (s *Service) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	return s.storage.GetMultipartParts(bucket, uploadID)
}

func (s *Service) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, requestedParts []s3.CompletePart) (s3.ObjectMeta, error) {
	reqID := s3.GetRequestID(ctx)
	s.logger.Info("CompleteMultipartUpload",
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("uploadID", uploadID),
		zap.String("request_id", reqID),
	)

	// Get stored parts
	storedParts, err := s.storage.GetMultipartParts(bucket, uploadID)
	if err != nil {
		return s3.ObjectMeta{}, err
	}

	storedPartsMap := make(map[int]s3.UploadPart)
	for _, p := range storedParts {
		storedPartsMap[p.PartNumber] = p
	}

	// Verify requested parts match stored parts
	var sortedParts []s3.UploadPart
	for _, reqPart := range requestedParts {
		storedPart, exists := storedPartsMap[reqPart.PartNumber]
		if !exists {
			return s3.ObjectMeta{}, fmt.Errorf("requested part %d does not exist", reqPart.PartNumber)
		}
		// Strip quotes from ETags to compare
		reqETag := strings.Trim(reqPart.ETag, "\"")
		storedETag := strings.Trim(storedPart.ETag, "\"")
		if reqETag != storedETag {
			return s3.ObjectMeta{}, fmt.Errorf("part %d ETag mismatch: requested %s, stored %s", reqPart.PartNumber, reqETag, storedETag)
		}
		sortedParts = append(sortedParts, storedPart)
	}

	// Sort parts just in case
	sort.Slice(sortedParts, func(i, j int) bool {
		return sortedParts[i].PartNumber < sortedParts[j].PartNumber
	})

	var totalSize int64
	for _, part := range sortedParts {
		totalSize += part.Size
	}

	// Create reader that yields part bytes and deletes part files immediately upon EOF
	partReader := &multipartPartsReader{
		storage:  s.storage,
		bucket:   bucket,
		uploadID: uploadID,
		parts:    sortedParts,
	}
	defer partReader.Close()

	// Compute final S3 ETag: MD5(concatenated binary MD5s) + "-" + num_parts
	h := md5.New()
	for _, part := range sortedParts {
		etag := strings.Trim(part.ETag, "\"")
		binaryMD5, err := hex.DecodeString(etag)
		if err != nil {
			return s3.ObjectMeta{}, fmt.Errorf("failed to decode part %d etag: %w", part.PartNumber, err)
		}
		h.Write(binaryMD5)
	}
	finalETag := fmt.Sprintf("\"%s-%d\"", hex.EncodeToString(h.Sum(nil)), len(sortedParts))

	// Get multipart session details to copy metadata if needed
	uploadSession, err := s.storage.GetMultipartUpload(bucket, uploadID)
	if err != nil {
		return s3.ObjectMeta{}, fmt.Errorf("failed to get multipart session details: %w", err)
	}

	// Perform standard 2PC PutObject flow using final ETag
	meta := s3.ObjectMeta{
		ContentType: "application/octet-stream", // default
		ETag:        finalETag,
		CreatedAt:   time.Now(),
		ModifiedAt:  time.Now(),
	}

	committedMeta, err := s.PutObject(ctx, uploadSession.Bucket, uploadSession.Key, partReader, totalSize, meta)
	if err != nil {
		return s3.ObjectMeta{}, fmt.Errorf("failed to commit completed multipart object: %w", err)
	}

	// Clean up multipart upload session
	if err := s.storage.AbortMultipartUpload(bucket, uploadID); err != nil {
		s.logger.Warn("Failed to clean up multipart upload directory", zap.String("uploadID", uploadID), zap.Error(err))
	}

	return committedMeta, nil
}

func (s *Service) AbortMultipartUpload(bucket, uploadID string) error {
	s.logger.Info("AbortMultipartUpload", zap.String("bucket", bucket), zap.String("uploadID", uploadID))
	return s.storage.AbortMultipartUpload(bucket, uploadID)
}

type multipartPartsReader struct {
	storage   StorageRepository
	bucket    string
	uploadID  string
	parts     []s3.UploadPart
	curIndex  int
	curReader io.ReadCloser
}

func (m *multipartPartsReader) Read(p []byte) (n int, err error) {
	for {
		if m.curReader == nil {
			if m.curIndex >= len(m.parts) {
				return 0, io.EOF
			}
			part := m.parts[m.curIndex]
			pr, err := m.storage.GetMultipartPartReader(m.bucket, m.uploadID, part.PartNumber)
			if err != nil {
				return 0, fmt.Errorf("failed to open reader for part %d: %w", part.PartNumber, err)
			}
			m.curReader = pr
		}

		n, err = m.curReader.Read(p)
		if err == io.EOF {
			// Done reading this part.
			// 1. Close current part reader.
			_ = m.curReader.Close()
			m.curReader = nil

			// 2. Delete part files to save space.
			part := m.parts[m.curIndex]
			if errDel := m.storage.DeleteMultipartPart(m.bucket, m.uploadID, part.PartNumber); errDel != nil {
				return n, fmt.Errorf("failed to delete part %d: %w", part.PartNumber, errDel)
			}

			// Move to the next part
			m.curIndex++

			if n > 0 {
				return n, nil
			}
			continue
		}

		return n, err
	}
}

func (m *multipartPartsReader) Close() error {
	if m.curReader != nil {
		err := m.curReader.Close()
		m.curReader = nil
		return err
	}
	return nil
}
