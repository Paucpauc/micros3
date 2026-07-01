package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type responseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	size        int
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.status = code
	rw.wroteHeader = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.wroteHeader {
		rw.WriteHeader(http.StatusOK)
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.size += n
	return n, err
}

func (rw *responseWriter) Unwrap() http.ResponseWriter {
	return rw.ResponseWriter
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx >= 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type Handler struct {
	service         *s3app.Service
	auth            *AuthValidator
	cluster         s3app.ClusterManager
	clusterToken    string
	logger          *zap.Logger
	allowLocalReads bool
}

func NewHandler(service *s3app.Service, auth *AuthValidator, cluster s3app.ClusterManager, clusterToken string, allowLocalReads bool, logger *zap.Logger) http.Handler {
	return &Handler{
		service:         service,
		auth:            auth,
		cluster:         cluster,
		clusterToken:    clusterToken,
		allowLocalReads: allowLocalReads,
		logger:          logger,
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		h.handleHealth(w, r)
		return
	}
	if r.URL.Path == "/liveness" {
		h.handleLiveness(w, r)
		return
	}
	if r.URL.Path == "/metrics" {
		promhttp.Handler().ServeHTTP(w, r)
		return
	}

	rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

	start := time.Now()

	reqID := r.Header.Get("X-Amz-Request-Id")
	if reqID == "" {
		reqID = fmt.Sprintf("req-%d", time.Now().UnixNano())
		r.Header.Set("X-Amz-Request-Id", reqID)
	}

	ctx := context.WithValue(r.Context(), s3.RequestIDKey, reqID)
	r = r.WithContext(ctx)

	var accessKey string

	if !h.cluster.IsLeader() {
		isRead := r.Method == http.MethodGet || r.Method == http.MethodHead
		if h.allowLocalReads && h.cluster.Status() == "READY" && isRead {
			h.logger.Debug("Not leader, but handling GET/HEAD request locally",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("request_id", reqID),
			)
		} else {
			h.logger.Debug("Not leader, proxying request to leader",
				zap.String("path", r.URL.Path),
				zap.String("request_id", reqID),
			)
			metrics.ProxyRequestsTotal.WithLabelValues(r.Method).Inc()
			h.ProxyToLeader(rw, r)
			h.logAccess(rw, r, accessKey, start)
			return
		}
	}

	if h.auth != nil {
		ak, err := h.auth.ValidateRequest(r)
		if err != nil {
			h.logger.Warn("S3 Signature V4 authentication failed", zap.Error(err))
			WriteError(rw, r, "SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", http.StatusForbidden)
			h.logAccess(rw, r, accessKey, start)
			return
		}
		accessKey = ak
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	if path == "" {
		if r.Method == http.MethodGet {
			h.handleListBuckets(rw, r)
		} else {
			WriteError(rw, r, "InvalidArgument", "Invalid method on root", http.StatusBadRequest)
		}
		h.logAccess(rw, r, accessKey, start)
		return
	}

	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	if key == "" {
		h.handleBucketRequest(rw, r, bucket)
	} else {
		h.handleObjectRequest(rw, r, bucket, key)
	}

	h.logAccess(rw, r, accessKey, start)
}

func (h *Handler) logAccess(rw *responseWriter, r *http.Request, accessKey string, start time.Time) {
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	var key string
	if len(parts) > 1 {
		key = parts[1]
	}

	duration := time.Since(start).Seconds()
	metrics.RequestsTotal.WithLabelValues(r.Method, bucket, strconv.Itoa(rw.status)).Inc()
	metrics.RequestDuration.WithLabelValues(r.Method, bucket).Observe(duration)

	isWrite := r.Method == http.MethodPut || r.Method == http.MethodPost
	if isWrite && rw.status < 400 {
		metrics.BytesWritten.WithLabelValues(r.Method, bucket).Add(float64(rw.size))
	}
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && rw.status < 400 {
		metrics.BytesRead.WithLabelValues(r.Method, bucket).Add(float64(rw.size))
	}

	h.logger.Info("S3 Access",
		zap.String("client_ip", clientIP(r)),
		zap.String("method", r.Method),
		zap.String("bucket", bucket),
		zap.String("key", key),
		zap.Int("status", rw.status),
		zap.Int("size", rw.size),
		zap.String("access_key", accessKey),
		zap.String("request_id", r.Header.Get("X-Amz-Request-Id")),
		zap.String("duration", fmt.Sprintf("%.6f", duration)),
	)
}

func (h *Handler) handleBucketRequest(w http.ResponseWriter, r *http.Request, bucket string) {
	switch r.Method {
	case http.MethodPut:
		h.handleCreateBucket(w, r, bucket)
	case http.MethodHead:
		h.handleHeadBucket(w, r, bucket)
	case http.MethodDelete:
		h.handleDeleteBucket(w, r, bucket)
	case http.MethodGet:
		// S3 uses GET /{bucket}?list-type=2 for ListObjectsV2
		if r.URL.Query().Get("list-type") == "2" {
			h.handleListObjectsV2(w, r, bucket)
		} else {
			// Fallback list objects
			h.handleListObjectsV2(w, r, bucket)
		}
	case http.MethodPost:
		// Delete objects batch
		if r.URL.Query().Has("delete") {
			h.handleDeleteObjects(w, r, bucket)
		} else {
			WriteError(w, r, "InvalidArgument", "Invalid post query parameter", http.StatusBadRequest)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleObjectRequest(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	q := r.URL.Query()

	switch r.Method {
	case http.MethodGet:
		if q.Has("uploadId") {
			h.handleListParts(w, r, bucket, key)
		} else {
			h.handleGetObject(w, r, bucket, key)
		}
	case http.MethodHead:
		h.handleHeadObject(w, r, bucket, key)
	case http.MethodDelete:
		if q.Has("uploadId") {
			h.handleAbortMultipartUpload(w, r, bucket, key)
		} else {
			h.handleDeleteObject(w, r, bucket, key)
		}
	case http.MethodPut:
		if q.Has("uploadId") && q.Has("partNumber") {
			h.handleUploadPart(w, r, bucket, key)
		} else if r.Header.Get("x-amz-copy-source") != "" {
			h.handleCopyObject(w, r, bucket, key)
		} else {
			h.handlePutObject(w, r, bucket, key)
		}
	case http.MethodPost:
		if q.Has("uploads") {
			h.handleCreateMultipartUpload(w, r, bucket, key)
		} else if q.Has("uploadId") {
			h.handleCompleteMultipartUpload(w, r, bucket, key)
		} else {
			WriteError(w, r, "InvalidArgument", "Invalid POST action", http.StatusBadRequest)
		}
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// --- Handler Implementations ---

func (h *Handler) handleListBuckets(w http.ResponseWriter, r *http.Request) {
	buckets, err := h.service.ListBuckets()
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	res := ListAllMyBucketsResult{
		Owner: Owner{
			ID:          "micros3",
			DisplayName: "micros3",
		},
		Buckets: []BucketInfo{},
	}

	for _, name := range buckets {
		res.Buckets = append(res.Buckets, BucketInfo{
			Name:         name,
			CreationDate: time.Now(), // Default fallback
		})
	}

	writeXMLResponse(w, r, http.StatusOK, res)
}

func (h *Handler) handleCreateBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := h.service.CreateBucket(bucket)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleHeadBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	exists, err := h.service.HasBucket(bucket)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}
	if !exists {
		WriteError(w, r, "NoSuchBucket", "The specified bucket does not exist.", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	err := h.service.DeleteBucket(bucket)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handlePutObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	// Parse content length.
	// For streaming aws-chunked uploads, X-Amz-Decoded-Content-Length holds the actual
	// payload size; Content-Length is inflated by chunk framing and must not be used.
	size := int64(-1)
	if dclStr := r.Header.Get("X-Amz-Decoded-Content-Length"); dclStr != "" {
		if dcl, err := strconv.ParseInt(dclStr, 10, 64); err == nil {
			size = dcl
		}
	} else if clStr := r.Header.Get("Content-Length"); clStr != "" {
		if cl, err := strconv.ParseInt(clStr, 10, 64); err == nil {
			size = cl
		}
	}

	// Parse custom metadata
	userMeta := make(map[string]string)
	for k, v := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			userMeta[k] = strings.Join(v, ",")
		}
	}

	meta := s3.ObjectMeta{
		ContentType:  r.Header.Get("Content-Type"),
		CreatedAt:    time.Now(),
		ModifiedAt:   time.Now(),
		UserMetadata: userMeta,
	}

	committedMeta, err := h.service.PutObject(r.Context(), bucket, key, r.Body, size, meta)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	w.Header().Set("ETag", committedMeta.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleGetObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	rc, meta, err := h.service.GetObject(bucket, key)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}
	defer rc.Close()

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.ContentLength, 10))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.ModifiedAt.UTC().Format(http.TimeFormat))

	for k, v := range meta.UserMetadata {
		w.Header().Set(k, v)
	}

	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

func (h *Handler) handleHeadObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	meta, err := h.service.GetObjectMeta(bucket, key)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.ContentLength, 10))
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Last-Modified", meta.ModifiedAt.UTC().Format(http.TimeFormat))

	for k, v := range meta.UserMetadata {
		w.Header().Set(k, v)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleDeleteObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	err := h.service.DeleteObject(r.Context(), bucket, key)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleListObjectsV2(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	continuationToken := q.Get("continuation-token")
	maxKeys := 1000

	if mkStr := q.Get("max-keys"); mkStr != "" {
		if mk, err := strconv.Atoi(mkStr); err == nil {
			maxKeys = mk
		}
	}

	res, err := h.service.ListObjectsV2(bucket, prefix, delimiter, continuationToken, maxKeys)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	xmlRes := ListBucketResult{
		Name:                  res.Name,
		Prefix:                res.Prefix,
		KeyCount:              res.KeyCount,
		MaxKeys:               res.MaxKeys,
		IsTruncated:           res.IsTruncated,
		NextContinuationToken: res.NextContinuationToken,
		Contents:              []Contents{},
		CommonPrefixes:        []CommonPrefix{},
	}

	for _, c := range res.Contents {
		xmlRes.Contents = append(xmlRes.Contents, Contents{
			Key:          c.Key,
			LastModified: c.LastModified,
			ETag:         c.ETag,
			Size:         c.Size,
			StorageClass: c.StorageClass,
		})
	}

	for _, cp := range res.CommonPrefixes {
		xmlRes.CommonPrefixes = append(xmlRes.CommonPrefixes, CommonPrefix{
			Prefix: cp,
		})
	}

	writeXMLResponse(w, r, http.StatusOK, xmlRes)
}

func (h *Handler) handleDeleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	var req DeleteRequest
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, r, "InvalidArgument", "Invalid XML request body", http.StatusBadRequest)
		return
	}

	res := DeleteResult{
		Deleted: []Deleted{},
		Error:   []ErrorResult{},
	}

	for _, obj := range req.Objects {
		err := h.service.DeleteObject(r.Context(), bucket, obj.Key)
		if err != nil {
			res.Error = append(res.Error, ErrorResult{
				Key:     obj.Key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		} else {
			res.Deleted = append(res.Deleted, Deleted{
				Key: obj.Key,
			})
		}
	}

	writeXMLResponse(w, r, http.StatusOK, res)
}

func (h *Handler) handleCopyObject(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	copySource := r.Header.Get("x-amz-copy-source")
	// Header format: /bucket/key or bucket/key
	copySource = strings.TrimPrefix(copySource, "/")
	parts := strings.SplitN(copySource, "/", 2)
	if len(parts) < 2 {
		WriteError(w, r, "InvalidArgument", "Invalid x-amz-copy-source header", http.StatusBadRequest)
		return
	}
	srcBucket := parts[0]
	srcKey := parts[1]

	committedMeta, err := h.service.CopyObject(r.Context(), srcBucket, srcKey, bucket, key)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	res := CopyObjectResult{
		LastModified: committedMeta.ModifiedAt,
		ETag:         committedMeta.ETag,
	}

	writeXMLResponse(w, r, http.StatusOK, res)
}

// --- Multipart Handlers ---

func (h *Handler) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	uploadID, err := h.service.CreateMultipartUpload(bucket, key)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	res := InitiateMultipartUploadResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	}

	writeXMLResponse(w, r, http.StatusOK, res)
}

func (h *Handler) handleUploadPart(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	q := r.URL.Query()
	uploadID := q.Get("uploadId")
	partNumStr := q.Get("partNumber")
	partNum, err := strconv.Atoi(partNumStr)
	if err != nil {
		WriteError(w, r, "InvalidArgument", "Invalid partNumber parameter", http.StatusBadRequest)
		return
	}

	part, err := h.service.SaveMultipartPart(bucket, uploadID, partNum, r.Body)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	w.Header().Set("ETag", part.ETag)
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	uploadID := r.URL.Query().Get("uploadId")

	var req CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
		WriteError(w, r, "InvalidArgument", "Invalid XML request body", http.StatusBadRequest)
		return
	}

	var parts []s3.CompletePart
	for _, p := range req.Parts {
		parts = append(parts, s3.CompletePart{
			PartNumber: p.PartNumber,
			ETag:       p.ETag,
		})
	}

	committedMeta, err := h.service.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, parts)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	// S3 complete multipart upload URL format
	location := fmt.Sprintf("http://%s/%s/%s", r.Host, bucket, key)

	res := CompleteMultipartUploadResult{
		Location: location,
		Bucket:   bucket,
		Key:      key,
		ETag:     committedMeta.ETag,
	}

	writeXMLResponse(w, r, http.StatusOK, res)
}

func (h *Handler) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	err := h.service.AbortMultipartUpload(bucket, uploadID)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) handleListParts(w http.ResponseWriter, r *http.Request, bucket string, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	parts, err := h.service.GetMultipartParts(bucket, uploadID)
	if err != nil {
		MapErrorToS3(w, r, err)
		return
	}

	res := ListPartsResult{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
		Part:     []Part{},
	}

	for _, p := range parts {
		res.Part = append(res.Part, Part{
			PartNumber:   p.PartNumber,
			LastModified: p.ModifiedAt,
			ETag:         p.ETag,
			Size:         p.Size,
		})
	}

	writeXMLResponse(w, r, http.StatusOK, res)
}

// --- Proxy Mechanism ---

func (h *Handler) ProxyToLeader(w http.ResponseWriter, r *http.Request) {
	leaderAddr := h.cluster.LeaderInternalAddress()
	if leaderAddr == "" {
		WriteError(w, r, "ServiceUnavailable", "No leader available in the cluster", http.StatusServiceUnavailable)
		return
	}

	leaderURL, err := url.Parse(leaderAddr)
	if err != nil {
		h.logger.Error("Failed to parse leader internal address", zap.String("address", leaderAddr), zap.Error(err))
		WriteError(w, r, "InternalError", "Invalid leader address", http.StatusInternalServerError)
		return
	}

	reqID := s3.GetRequestID(r.Context())

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = leaderURL.Scheme
			req.URL.Host = leaderURL.Host
			req.URL.Path = "/internal/s3-proxy"
			req.Method = http.MethodPost

			// Pass original request attributes in headers
			req.Header.Set("X-MicroS3-Original-Path", r.URL.Path)
			req.Header.Set("X-MicroS3-Original-Method", r.Method)
			req.Header.Set("X-MicroS3-Original-RawQuery", r.URL.RawQuery)
			req.Header.Set("X-MicroS3-Token", h.clusterToken)
			if reqID != "" {
				req.Header.Set("X-MicroS3-RequestID", reqID)
			}
		},
	}
	proxy.ServeHTTP(w, r)
}

// --- Response Helpers ---

func writeXMLResponse(w http.ResponseWriter, r *http.Request, statusCode int, val interface{}) {
	reqID := r.Header.Get("X-Amz-Request-Id")

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", reqID)
	w.Header().Set("x-amz-id-2", reqID)
	w.Header().Set("Server", "MicroS3")
	w.WriteHeader(statusCode)

	_, _ = w.Write([]byte(xml.Header))
	enc := xml.NewEncoder(w)
	_ = enc.Encode(val)
}

func (h *Handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	status := h.cluster.Status()
	w.Header().Set("Content-Type", "application/json")

	if h.cluster.IsLeader() || status == "READY" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"OK"}`))
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(fmt.Sprintf(`{"status":"%s"}`, status)))
}

func (h *Handler) handleLiveness(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"OK"}`))
}
