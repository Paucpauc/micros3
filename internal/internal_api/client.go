package internal_api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/paucpauc/micros3/internal/domain/s3"
)

type HealthResponse struct {
	NodeID           string `json:"node_id"`
	State            string `json:"state"`
	Role             string `json:"role"`
	Leader           string `json:"leader"`
	ObjectsCount     int64  `json:"objects_count"`
	StorageUsedBytes int64  `json:"storage_used_bytes"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
	Version          string `json:"version"`
}

type TxStatusResponse struct {
	TxID   string `json:"tx_id"`
	Status string `json:"status"` // "COMMITTED", "ABORTED", "UNKNOWN"
}

type KeyInfo struct {
	Bucket     string    `json:"bucket"`
	Key        string    `json:"key"`
	CRC32      uint32    `json:"crc32"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modified_at"`
}

type KeysResponse struct {
	Keys           []KeyInfo `json:"keys"`
	TotalCount     int       `json:"total_count"`
	TotalSizeBytes int64     `json:"total_size_bytes"`
}

type Client struct {
	httpClient   *http.Client
	clusterToken string
	idleTimeout  time.Duration
}

func NewClient(clusterToken string, timeout time.Duration) *Client {
	return &Client{
		httpClient: &http.Client{
			// Do not set a global timeout on the HTTP client. Timeouts are controlled
			// per request via the context passed to each method.
		},
		clusterToken: clusterToken,
		idleTimeout:  30 * time.Second, // Default idle timeout (traffic activity check)
	}
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("X-MicroS3-Token", c.clusterToken)
	if ctx := req.Context(); ctx != nil {
		if reqID := s3.GetRequestID(ctx); reqID != "" {
			req.Header.Set("X-MicroS3-RequestID", reqID)
		}
	}
}

func (c *Client) Health(ctx context.Context, targetAddr string) (*HealthResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetAddr+"/internal/health", nil)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	var h HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return nil, err
	}
	return &h, nil
}

func (c *Client) Prepare(ctx context.Context, targetAddr string, tx s3.Transaction, meta s3.ObjectMeta, body io.Reader, size int64) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wrappedBody io.Reader = body
	if body != nil {
		wrappedBody = NewIdleTimeoutReader(body, c.idleTimeout, cancel)
	}

	req, err := http.NewRequestWithContext(cancelCtx, http.MethodPost, targetAddr+"/internal/prepare", wrappedBody)
	if err != nil {
		return err
	}
	c.setHeaders(req)

	// Set headers specific to 2PC Prepare
	req.Header.Set("X-MicroS3-TxID", tx.ID)
	req.Header.Set("X-MicroS3-Operation", tx.Operation)
	req.Header.Set("X-MicroS3-Bucket", tx.Bucket)
	req.Header.Set("X-MicroS3-Key", tx.Key)
	req.Header.Set("X-MicroS3-CRC32", strconv.FormatUint(uint64(meta.CRC32), 10))
	if size >= 0 {
		req.ContentLength = size
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	req.Header.Set("X-MicroS3-Meta", base64.StdEncoding.EncodeToString(metaBytes))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("prepare failed on %s with status %d: %s", targetAddr, resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) Commit(ctx context.Context, targetAddr string, txID string, bucket string, key string) error {
	reqBody, err := json.Marshal(map[string]string{
		"tx_id":  txID,
		"bucket": bucket,
		"key":    key,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetAddr+"/internal/commit", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("commit failed on %s with status %d: %s", targetAddr, resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) Abort(ctx context.Context, targetAddr string, txID string) error {
	reqBody, err := json.Marshal(map[string]string{
		"tx_id": txID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetAddr+"/internal/abort", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("abort failed on %s with status %d: %s", targetAddr, resp.StatusCode, string(respBody))
	}

	return nil
}

func (c *Client) TxStatus(ctx context.Context, targetAddr string, txID string) (*TxStatusResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/internal/tx-status?tx_id=%s", targetAddr, url.QueryEscape(txID)), nil)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tx-status request failed with status: %d", resp.StatusCode)
	}

	var status TxStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, err
	}
	return &status, nil
}

func (c *Client) GetKeys(ctx context.Context, targetAddr string) (*KeysResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetAddr+"/internal/keys", nil)
	if err != nil {
		return nil, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get keys failed with status: %d", resp.StatusCode)
	}

	var keys KeysResponse
	if err := json.NewDecoder(resp.Body).Decode(&keys); err != nil {
		return nil, err
	}
	return &keys, nil
}

func (c *Client) GetObject(ctx context.Context, targetAddr string, bucket string, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	cancelCtx, cancel := context.WithCancel(ctx)

	req, err := http.NewRequestWithContext(cancelCtx, http.MethodGet, fmt.Sprintf("%s/internal/object?bucket=%s&key=%s", targetAddr, url.QueryEscape(bucket), url.QueryEscape(key)), nil)
	if err != nil {
		cancel()
		return nil, s3.ObjectMeta{}, err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		cancel()
		return nil, s3.ObjectMeta{}, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		cancel()
		return nil, s3.ObjectMeta{}, fmt.Errorf("get object failed with status: %d", resp.StatusCode)
	}

	// Parse meta header
	metaB64 := resp.Header.Get("X-MicroS3-Meta")
	var meta s3.ObjectMeta
	if metaB64 != "" {
		metaBytes, err := base64.StdEncoding.DecodeString(metaB64)
		if err == nil {
			_ = json.Unmarshal(metaBytes, &meta)
		}
	}

	wrappedBody := NewIdleTimeoutReader(resp.Body, c.idleTimeout, cancel)
	return &getObjectReadCloser{ReadCloser: wrappedBody, cancel: cancel}, meta, nil
}

func (c *Client) SyncStart(ctx context.Context, targetAddr string, nodeID string) error {
	url := fmt.Sprintf("%s/internal/sync-start?node_id=%s", targetAddr, url.QueryEscape(nodeID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sync-start failed with status: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) SyncDone(ctx context.Context, targetAddr string, nodeID string, internalAddr string) error {
	url := targetAddr + "/internal/sync-done?node_id=" + nodeID
	if internalAddr != "" {
		url += "&internal_addr=" + internalAddr
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sync-done failed with status: %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) SyncHeartbeat(ctx context.Context, targetAddr string, nodeID string) error {
	url := fmt.Sprintf("%s/internal/sync-heartbeat?node_id=%s", targetAddr, url.QueryEscape(nodeID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sync-heartbeat failed with status: %d", resp.StatusCode)
	}
	return nil
}

// SyncRequest is sent by a follower to the leader to signal that it wants to
// be synchronized. The leader will then drive the sync process: query the
// follower's keys, push missing/updated objects, and delete extraneous ones.
func (c *Client) SyncRequest(ctx context.Context, targetAddr string, nodeID string, internalAddr string) error {
	u := fmt.Sprintf("%s/internal/sync-request?node_id=%s", targetAddr, url.QueryEscape(nodeID))
	if internalAddr != "" {
		u += "&internal_addr=" + url.QueryEscape(internalAddr)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sync-request failed with status %d: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

// SyncDelete is sent by the leader to a follower to remove extraneous keys
// that exist on the follower but not on the leader.
func (c *Client) SyncDelete(ctx context.Context, targetAddr string, keys []KeyInfo) error {
	reqBody, err := json.Marshal(struct {
		Keys []KeyInfo `json:"keys"`
	}{Keys: keys})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetAddr+"/internal/sync-delete", bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("sync-delete failed on %s with status %d: %s", targetAddr, resp.StatusCode, string(respBody))
	}
	return nil
}

func (c *Client) SetStatus(ctx context.Context, targetAddr string, status string) error {
	url := fmt.Sprintf("%s/internal/set-status?status=%s", targetAddr, url.QueryEscape(status))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	c.setHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("set-status failed with status: %d", resp.StatusCode)
	}
	return nil
}

type idleTimeoutReader struct {
	r       io.Reader
	timeout time.Duration
	cancel  context.CancelFunc
	timer   *time.Timer
}

func NewIdleTimeoutReader(r io.Reader, timeout time.Duration, cancel context.CancelFunc) io.ReadCloser {
	tr := &idleTimeoutReader{
		r:       r,
		timeout: timeout,
		cancel:  cancel,
	}
	tr.timer = time.AfterFunc(timeout, func() {
		tr.cancel()
	})
	return tr
}

func (tr *idleTimeoutReader) Read(p []byte) (n int, err error) {
	tr.timer.Reset(tr.timeout)
	n, err = tr.r.Read(p)
	tr.timer.Reset(tr.timeout)
	if err != nil {
		tr.timer.Stop()
	}
	return n, err
}

func (tr *idleTimeoutReader) Close() error {
	tr.timer.Stop()
	if rc, ok := tr.r.(io.ReadCloser); ok {
		return rc.Close()
	}
	return nil
}

type getObjectReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (g *getObjectReadCloser) Close() error {
	g.cancel()
	return g.ReadCloser.Close()
}
