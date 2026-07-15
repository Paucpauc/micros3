package internal_api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"go.uber.org/zap"
)

type InternalHandler struct {
	storage      s3app.StorageRepository
	service      *s3app.Service
	cluster      s3app.ClusterManager
	s3Handler    http.Handler
	clusterToken string
	logger       *zap.Logger
}

func NewHandler(
	storage s3app.StorageRepository,
	service *s3app.Service,
	cluster s3app.ClusterManager,
	s3Handler http.Handler,
	clusterToken string,
	logger *zap.Logger,
) http.Handler {
	mux := http.NewServeMux()
	h := &InternalHandler{
		storage:      storage,
		service:      service,
		cluster:      cluster,
		s3Handler:    s3Handler,
		clusterToken: clusterToken,
		logger:       logger,
	}

	mux.HandleFunc("/internal/health", h.handleHealth)
	mux.HandleFunc("/internal/prepare", h.handlePrepare)
	mux.HandleFunc("/internal/commit", h.handleCommit)
	mux.HandleFunc("/internal/abort", h.handleAbort)
	mux.HandleFunc("/internal/tx-status", h.handleTxStatus)
	mux.HandleFunc("/internal/keys", h.handleKeys)
	mux.HandleFunc("/internal/object", h.handleObject)
	mux.HandleFunc("/internal/sync-start", h.handleSyncStart)
	mux.HandleFunc("/internal/sync-done", h.handleSyncDone)
	mux.HandleFunc("/internal/sync-heartbeat", h.handleSyncHeartbeat)
	mux.HandleFunc("/internal/sync-request", h.handleSyncRequest)
	mux.HandleFunc("/internal/sync-delete", h.handleSyncDelete)
	mux.HandleFunc("/internal/s3-proxy", h.handleS3Proxy)
	mux.HandleFunc("/internal/set-status", h.handleSetStatus)

	return h.verifyTokenMiddleware(mux)
}

func (h *InternalHandler) verifyTokenMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("X-MicroS3-Token")
		if token != h.clusterToken {
			h.logger.Warn("Unauthorized internal request, token mismatch",
				zap.String("path", r.URL.Path),
				zap.String("remote_addr", r.RemoteAddr),
			)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("Unauthorized: Token mismatch"))
			return
		}

		// Inject X-MicroS3-RequestID header into context for request tracing
		if reqID := r.Header.Get("X-MicroS3-RequestID"); reqID != "" {
			ctx := context.WithValue(r.Context(), s3.RequestIDKey, reqID)
			r = r.WithContext(ctx)
		}

		next.ServeHTTP(w, r)
	})
}

func (h *InternalHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	role := "follower"
	if h.cluster.IsLeader() {
		role = "leader"
	}

	// Gather stats
	objectsCount := int64(0)
	storageUsed := int64(0)
	buckets, err := h.storage.ListBuckets()
	if err == nil {
		for _, b := range buckets {
			res, err := h.storage.ListObjectsV2(b, "", "", "", 100000)
			if err == nil {
				for _, c := range res.Contents {
					objectsCount++
					storageUsed += c.Size
				}
			}
		}
	}

	resp := HealthResponse{
		NodeID:           h.cluster.NodeID(),
		State:            h.cluster.Status(),
		Role:             role,
		Leader:           "", // Can be populated dynamically if needed
		ObjectsCount:     objectsCount,
		StorageUsedBytes: storageUsed,
		UptimeSeconds:    int64(time.Since(time.Unix(0, 0)).Seconds()), // Simplistic uptime fallback
		Version:          "0.1.0",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *InternalHandler) handlePrepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	txID := r.Header.Get("X-MicroS3-TxID")
	op := r.Header.Get("X-MicroS3-Operation")
	bucket := r.Header.Get("X-MicroS3-Bucket")
	key := r.Header.Get("X-MicroS3-Key")
	crcStr := r.Header.Get("X-MicroS3-CRC32")
	metaB64 := r.Header.Get("X-MicroS3-Meta")

	if txID == "" || op == "" || bucket == "" || key == "" {
		http.Error(w, "Missing required headers", http.StatusBadRequest)
		return
	}

	var meta s3.ObjectMeta
	if metaB64 != "" {
		metaBytes, err := base64.StdEncoding.DecodeString(metaB64)
		if err != nil {
			http.Error(w, "Invalid X-MicroS3-Meta base64", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			http.Error(w, "Invalid metadata JSON", http.StatusBadRequest)
			return
		}
	}

	if crcStr != "" {
		if crc, err := strconv.ParseUint(crcStr, 10, 32); err == nil {
			meta.CRC32 = uint32(crc)
		}
	}

	tx := s3.Transaction{
		ID:        txID,
		Operation: op,
		Bucket:    bucket,
		Key:       key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	reqID := s3.GetRequestID(r.Context())
	h.logger.Debug("Received 2PC Prepare request from coordinator",
		zap.String("tx_id", txID),
		zap.String("op", op),
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.String("request_id", reqID),
	)

	// Raw data payload is in request body
	_, err := h.storage.StageObject(txID, r.Body, r.ContentLength, meta, tx)
	if err != nil {
		h.logger.Error("Staging prepare failed",
			zap.String("txID", txID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.logger.Debug("2PC Prepare staging succeeded",
		zap.String("tx_id", txID),
		zap.String("request_id", reqID),
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "PREPARED",
		"tx_id":  txID,
	})
}

func (h *InternalHandler) handleCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		TxID   string `json:"tx_id"`
		Bucket string `json:"bucket"`
		Key    string `json:"key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	reqID := s3.GetRequestID(r.Context())
	h.logger.Debug("Received 2PC Commit request",
		zap.String("tx_id", body.TxID),
		zap.String("bucket", body.Bucket),
		zap.String("key", body.Key),
		zap.String("request_id", reqID),
	)

	_, err := h.storage.CommitTransaction(body.TxID, body.Bucket, body.Key)
	if err != nil {
		h.logger.Error("Commit failed",
			zap.String("txID", body.TxID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.logger.Debug("2PC Commit succeeded locally",
		zap.String("tx_id", body.TxID),
		zap.String("request_id", reqID),
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "COMMITTED",
	})
}

func (h *InternalHandler) handleAbort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		TxID string `json:"tx_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	reqID := s3.GetRequestID(r.Context())
	h.logger.Debug("Received 2PC Abort request",
		zap.String("tx_id", body.TxID),
		zap.String("request_id", reqID),
	)

	err := h.storage.AbortTransaction(body.TxID)
	if err != nil {
		h.logger.Error("Abort failed",
			zap.String("txID", body.TxID),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.logger.Debug("2PC Abort succeeded locally",
		zap.String("tx_id", body.TxID),
		zap.String("request_id", reqID),
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "ABORTED",
	})
}

func (h *InternalHandler) handleTxStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	txID := r.URL.Query().Get("tx_id")
	if txID == "" {
		http.Error(w, "Missing tx_id query parameter", http.StatusBadRequest)
		return
	}

	tx, err := h.storage.GetTransaction(txID)
	status := "UNKNOWN"
	if err == nil {
		status = tx.State
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(TxStatusResponse{
		TxID:   txID,
		Status: status,
	})
}

func (h *InternalHandler) handleKeys(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	buckets, err := h.storage.ListBuckets()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var keys []KeyInfo
	var totalCount int
	var totalSizeBytes int64

	for _, b := range buckets {
		res, err := h.storage.ListObjectsV2(b, "", "", "", 1000000)
		if err != nil {
			continue
		}
		for _, c := range res.Contents {
			meta, err := h.storage.GetObjectMeta(b, c.Key)
			if err != nil {
				continue
			}
			keys = append(keys, KeyInfo{
				Bucket:     b,
				Key:        c.Key,
				CRC32:      meta.CRC32,
				Size:       c.Size,
				ModifiedAt: c.LastModified,
			})
			totalCount++
			totalSizeBytes += c.Size
		}
	}

	resp := KeysResponse{
		Keys:           keys,
		TotalCount:     totalCount,
		TotalSizeBytes: totalSizeBytes,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *InternalHandler) handleObject(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	bucket := q.Get("bucket")
	key := q.Get("key")

	if bucket == "" || key == "" {
		http.Error(w, "Missing bucket or key query parameter", http.StatusBadRequest)
		return
	}

	rc, meta, err := h.storage.GetObject(bucket, key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer rc.Close()

	// Encode metadata in header
	metaBytes, err := json.Marshal(meta)
	if err == nil {
		w.Header().Set("X-MicroS3-Meta", base64.StdEncoding.EncodeToString(metaBytes))
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

func (h *InternalHandler) handleSyncStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("node_id")
	h.service.StartSyncLease(nodeID)
	w.WriteHeader(http.StatusOK)
}

func (h *InternalHandler) handleSyncDone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("node_id")
	internalAddr := r.URL.Query().Get("internal_addr")
	if nodeID != "" {
		h.cluster.MarkAlive(nodeID, internalAddr)
	}

	h.service.EndSyncLease(nodeID)
	w.WriteHeader(http.StatusOK)
}

func (h *InternalHandler) handleSyncHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("node_id")
	h.service.HeartbeatSyncLease(nodeID)
	w.WriteHeader(http.StatusOK)
}

// handleSyncRequest is received by the leader when a follower wants to
// synchronize. The leader drives the entire sync process: it queries the
// follower's keys, pushes missing/updated objects, and deletes extraneous
// ones. The HTTP response is sent only after the sync completes (or fails).
func (h *InternalHandler) handleSyncRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	nodeID := r.URL.Query().Get("node_id")
	internalAddr := r.URL.Query().Get("internal_addr")
	if nodeID == "" || internalAddr == "" {
		http.Error(w, "missing node_id or internal_addr parameter", http.StatusBadRequest)
		return
	}

	reqID := s3.GetRequestID(r.Context())
	h.logger.Info("Received sync request from follower",
		zap.String("node_id", nodeID),
		zap.String("internal_addr", internalAddr),
		zap.String("request_id", reqID),
	)

	if err := h.service.HandleSyncRequest(r.Context(), nodeID, internalAddr); err != nil {
		h.logger.Error("Leader-driven sync failed",
			zap.String("node_id", nodeID),
			zap.String("internal_addr", internalAddr),
			zap.Error(err),
			zap.String("request_id", reqID),
		)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.logger.Info("Leader-driven sync completed successfully",
		zap.String("node_id", nodeID),
		zap.String("request_id", reqID),
	)
	w.WriteHeader(http.StatusOK)
}

// handleSyncDelete is received by a follower when the leader instructs it to
// remove extraneous keys that exist on the follower but not on the leader.
func (h *InternalHandler) handleSyncDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Keys []KeyInfo `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	reqID := s3.GetRequestID(r.Context())
	h.logger.Info("Received sync-delete request",
		zap.Int("keys_count", len(body.Keys)),
		zap.String("request_id", reqID),
	)

	for _, k := range body.Keys {
		if err := h.storage.DeleteObject(k.Bucket, k.Key); err != nil {
			h.logger.Warn("Failed to delete extraneous object during sync",
				zap.String("bucket", k.Bucket),
				zap.String("key", k.Key),
				zap.Error(err),
			)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "DELETED"})
}

func (h *InternalHandler) handleSetStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	status := r.URL.Query().Get("status")
	if status == "" {
		http.Error(w, "missing status parameter", http.StatusBadRequest)
		return
	}

	h.logger.Info("Received status update request", zap.String("status", status))
	h.cluster.SetLocalStatus(status)
	w.WriteHeader(http.StatusOK)
}

func (h *InternalHandler) handleS3Proxy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	origMethod := r.Header.Get("X-MicroS3-Original-Method")
	origPath := r.Header.Get("X-MicroS3-Original-Path")
	origRawQuery := r.Header.Get("X-MicroS3-Original-RawQuery")

	if origMethod == "" || origPath == "" {
		http.Error(w, "Missing proxy headers", http.StatusBadRequest)
		return
	}

	// Rewrite request path, method, and query parameters
	r.Method = origMethod
	r.URL.Path = origPath
	r.URL.RawQuery = origRawQuery

	reqID := s3.GetRequestID(r.Context())
	h.logger.Debug("Dispatching proxied S3 request to S3 handler",
		zap.String("method", r.Method),
		zap.String("path", r.URL.Path),
		zap.String("request_id", reqID),
	)

	// Call S3 API Handler directly
	h.s3Handler.ServeHTTP(w, r)
}
