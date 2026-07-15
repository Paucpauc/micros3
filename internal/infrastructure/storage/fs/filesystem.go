package fs

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
)

var _ s3app.StorageRepository = (*FilesystemRepository)(nil)

type FilesystemRepository struct {
	root string
}

func NewFilesystemRepository(root string) (*FilesystemRepository, error) {
	// Ensure directories exist
	for _, dir := range []string{"data", "meta", "staging", "uploads", "ecdata"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}
	return &FilesystemRepository{root: root}, nil
}

// Helper paths
func (r *FilesystemRepository) dataDir(bucket string) string {
	return filepath.Join(r.root, "data", bucket)
}

func (r *FilesystemRepository) metaDir(bucket string) string {
	return filepath.Join(r.root, "meta", bucket)
}

func (r *FilesystemRepository) stagingDir(txID string) string {
	return filepath.Join(r.root, "staging", txID)
}

func (r *FilesystemRepository) uploadsDir(bucket string) string {
	return filepath.Join(r.root, "uploads", bucket)
}

func (r *FilesystemRepository) uploadSessionDir(bucket, uploadID string) string {
	return filepath.Join(r.root, "uploads", bucket, uploadID)
}

// ecShardDir returns the directory holding EC shards for a given bucket.
func (r *FilesystemRepository) ecShardDir(bucket string) string {
	return filepath.Join(r.root, "ecdata", bucket)
}

// ecShardPath returns the path to a specific EC shard file.
func (r *FilesystemRepository) ecShardPath(bucket, key string, shardIndex int) string {
	return filepath.Join(r.ecShardDir(bucket), fmt.Sprintf("%s.shard.%d", key, shardIndex))
}

// --- Bucket Operations ---

func (r *FilesystemRepository) CreateBucket(bucket string) error {
	if err := os.MkdirAll(r.dataDir(bucket), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(r.metaDir(bucket), 0755); err != nil {
		return err
	}
	return nil
}

func (r *FilesystemRepository) DeleteBucket(bucket string) error {
	// Check if empty
	empty, err := r.isDirEmpty(r.dataDir(bucket))
	if err != nil {
		return err
	}
	if !empty {
		return errors.New("bucket not empty")
	}

	if err := os.Remove(r.dataDir(bucket)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.RemoveAll(r.metaDir(bucket)); err != nil {
		return err
	}
	return nil
}

func (r *FilesystemRepository) HasBucket(bucket string) (bool, error) {
	_, err := os.Stat(r.dataDir(bucket))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (r *FilesystemRepository) ListBuckets() ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(r.root, "data"))
	if err != nil {
		return nil, err
	}
	var buckets []string
	for _, entry := range entries {
		if entry.IsDir() {
			buckets = append(buckets, entry.Name())
		}
	}
	return buckets, nil
}

// --- Transaction (2PC) Operations ---

func (r *FilesystemRepository) StageObject(txID string, reader io.Reader, size int64, meta s3.ObjectMeta, tx s3.Transaction) (s3.ObjectMeta, error) {
	stageDir := r.stagingDir(txID)
	if err := os.MkdirAll(stageDir, 0755); err != nil {
		return s3.ObjectMeta{}, err
	}

	// Write tx.json
	txPath := filepath.Join(stageDir, "tx.json")
	if err := r.writeJSON(txPath, tx); err != nil {
		return s3.ObjectMeta{}, err
	}

	if tx.Operation == s3.OpPut {
		// Write data (with write-to-temp + rename + sync)
		tmpName := fmt.Sprintf("data.tmp.%s", uuid.New().String())
		tmpPath := filepath.Join(stageDir, tmpName)
		dataPath := filepath.Join(stageDir, "data")

		f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return s3.ObjectMeta{}, err
		}
		defer func() {
			f.Close()
			os.Remove(tmpPath) // cleanup tmp if rename failed
		}()

		// Calculate CRC32 and MD5 on the fly
		hCRC := crc32.NewIEEE()
		hMD5 := md5.New()
		mw := io.MultiWriter(f, hCRC, hMD5)

		written, err := io.Copy(mw, reader)
		if err != nil {
			return s3.ObjectMeta{}, err
		}

		if size >= 0 && written != size {
			return s3.ObjectMeta{}, fmt.Errorf("size mismatch: expected %d, got %d", size, written)
		}

		// Update metadata fields computed on-the-fly
		meta.ContentLength = written
		meta.CRC32 = hCRC.Sum32()
		if meta.ETag == "" {
			meta.ETag = fmt.Sprintf("\"%s\"", hex.EncodeToString(hMD5.Sum(nil)))
		}

		if err := f.Sync(); err != nil {
			return s3.ObjectMeta{}, err
		}
		if err := f.Close(); err != nil {
			return s3.ObjectMeta{}, err
		}

		if err := os.Rename(tmpPath, dataPath); err != nil {
			return s3.ObjectMeta{}, err
		}

		// Write meta.json
		metaPath := filepath.Join(stageDir, "meta.json")
		if err := r.writeJSON(metaPath, meta); err != nil {
			return s3.ObjectMeta{}, err
		}
	}

	// Sync staging directory
	if err := r.syncDir(stageDir); err != nil {
		return s3.ObjectMeta{}, err
	}

	return meta, nil
}

func (r *FilesystemRepository) CommitTransaction(txID, bucket, key string) (s3.ObjectMeta, error) {
	stageDir := r.stagingDir(txID)
	txPath := filepath.Join(stageDir, "tx.json")

	var tx s3.Transaction
	if err := r.readJSON(txPath, &tx); err != nil {
		return s3.ObjectMeta{}, fmt.Errorf("failed to read transaction log: %w", err)
	}

	if tx.Bucket != bucket || tx.Key != key {
		return s3.ObjectMeta{}, fmt.Errorf("transaction %s bucket/key mismatch: expected %s/%s, got %s/%s", txID, tx.Bucket, tx.Key, bucket, key)
	}

	var meta s3.ObjectMeta

	if tx.Operation == s3.OpPut {
		// Read meta
		metaPath := filepath.Join(stageDir, "meta.json")
		if err := r.readJSON(metaPath, &meta); err != nil {
			return s3.ObjectMeta{}, fmt.Errorf("failed to read transaction meta: %w", err)
		}

		// Move data
		targetData := filepath.Join(r.dataDir(bucket), key)
		if err := os.MkdirAll(filepath.Dir(targetData), 0755); err != nil {
			return s3.ObjectMeta{}, err
		}
		if err := os.Rename(filepath.Join(stageDir, "data"), targetData); err != nil {
			return s3.ObjectMeta{}, err
		}
		if err := r.syncDir(filepath.Dir(targetData)); err != nil {
			return s3.ObjectMeta{}, err
		}

		// If the object previously existed in EC mode, remove its old
		// shards — the new PUT stores a full replica.
		_ = r.DeleteAllECShards(bucket, key)

		// Move meta
		targetMeta := filepath.Join(r.metaDir(bucket), key+".json")
		if err := os.MkdirAll(filepath.Dir(targetMeta), 0755); err != nil {
			return s3.ObjectMeta{}, err
		}
		if err := os.Rename(filepath.Join(stageDir, "meta.json"), targetMeta); err != nil {
			return s3.ObjectMeta{}, err
		}
		if err := r.syncDir(filepath.Dir(targetMeta)); err != nil {
			return s3.ObjectMeta{}, err
		}

	} else if tx.Operation == s3.OpDelete {
		// Delete data and meta files
		targetData := filepath.Join(r.dataDir(bucket), key)
		targetMeta := filepath.Join(r.metaDir(bucket), key+".json")

		if err := os.Remove(targetData); err != nil && !os.IsNotExist(err) {
			return s3.ObjectMeta{}, err
		}
		if err := os.Remove(targetMeta); err != nil && !os.IsNotExist(err) {
			return s3.ObjectMeta{}, err
		}

		// Remove any EC shards for the deleted object.
		_ = r.DeleteAllECShards(bucket, key)

		// Clean up empty parent directories
		r.cleanEmptyDirs(filepath.Dir(targetData), r.dataDir(bucket))
		r.cleanEmptyDirs(filepath.Dir(targetMeta), r.metaDir(bucket))
	}

	// Remove staging directory
	if err := os.RemoveAll(stageDir); err != nil {
		return s3.ObjectMeta{}, err
	}

	return meta, nil
}

func (r *FilesystemRepository) AbortTransaction(txID string) error {
	stageDir := r.stagingDir(txID)
	return os.RemoveAll(stageDir)
}

func (r *FilesystemRepository) GetTransaction(txID string) (s3.Transaction, error) {
	stageDir := r.stagingDir(txID)
	txPath := filepath.Join(stageDir, "tx.json")

	var tx s3.Transaction
	if err := r.readJSON(txPath, &tx); err != nil {
		return s3.Transaction{}, err
	}
	return tx, nil
}

func (r *FilesystemRepository) GetStagedObjectReader(txID string) (io.ReadCloser, error) {
	stageDir := r.stagingDir(txID)
	dataPath := filepath.Join(stageDir, "data")
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// --- Object Operations ---

func (r *FilesystemRepository) GetObject(bucket, key string) (io.ReadCloser, s3.ObjectMeta, error) {
	meta, err := r.GetObjectMeta(bucket, key)
	if err != nil {
		return nil, s3.ObjectMeta{}, err
	}

	// For EC objects the full data is not stored locally; only one shard is.
	// Return the local shard so that the internal API can serve it to the
	// leader for reconstruction. Full reconstruction happens in the service
	// layer.
	if meta.IsEC() {
		shardPath := r.ecShardPath(bucket, key, meta.ECChunkIndex)
		f, err := os.Open(shardPath)
		if err != nil {
			return nil, s3.ObjectMeta{}, err
		}
		return f, meta, nil
	}

	dataPath := filepath.Join(r.dataDir(bucket), key)
	f, err := os.Open(dataPath)
	if err != nil {
		return nil, s3.ObjectMeta{}, err
	}

	return f, meta, nil
}

func (r *FilesystemRepository) GetObjectMeta(bucket, key string) (s3.ObjectMeta, error) {
	metaPath := filepath.Join(r.metaDir(bucket), key+".json")
	var meta s3.ObjectMeta
	if err := r.readJSON(metaPath, &meta); err != nil {
		if os.IsNotExist(err) {
			return s3.ObjectMeta{}, os.ErrNotExist
		}
		return s3.ObjectMeta{}, err
	}
	return meta, nil
}

func (r *FilesystemRepository) DeleteObject(bucket, key string) error {
	// Standalone delete (direct, not 2PC; but since 2PC is expected, normally 2PC uses StageObject + Commit)
	// We still implement this just in case
	targetData := filepath.Join(r.dataDir(bucket), key)
	targetMeta := filepath.Join(r.metaDir(bucket), key+".json")

	if err := os.Remove(targetData); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(targetMeta); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove any EC shards that may exist for this object.
	_ = r.DeleteAllECShards(bucket, key)

	r.cleanEmptyDirs(filepath.Dir(targetData), r.dataDir(bucket))
	r.cleanEmptyDirs(filepath.Dir(targetMeta), r.metaDir(bucket))
	return nil
}

func (r *FilesystemRepository) ListObjectsV2(bucket, prefix, delimiter, continuationToken string, maxKeys int) (s3.ListObjectsResult, error) {
	result := s3.ListObjectsResult{
		Name:     bucket,
		Prefix:   prefix,
		MaxKeys:  maxKeys,
		Contents: []s3.ObjectInfo{},
	}

	// Walk the metadata directory instead of the data directory. This
	// ensures that erasure-coded objects (whose data file has been
	// removed and replaced by EC shards in ecdata/) are still listed,
	// because every object — replica or EC — has a metadata file.
	bucketMetaDir := r.metaDir(bucket)
	if _, err := os.Stat(bucketMetaDir); os.IsNotExist(err) {
		return result, os.ErrNotExist
	}

	// Recursively collect all keys from metadata files (*.json)
	var keys []string
	err := filepath.WalkDir(bucketMetaDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			relPath, err := filepath.Rel(bucketMetaDir, path)
			if err != nil {
				return err
			}
			// Strip the ".json" suffix to get the object key.
			if strings.HasSuffix(relPath, ".json") {
				keys = append(keys, strings.TrimSuffix(relPath, ".json"))
			}
		}
		return nil
	})
	if err != nil {
		return result, err
	}

	sort.Strings(keys)

	// Filter and paginate
	startIdx := 0
	if continuationToken != "" {
		startIdx = sort.Search(len(keys), func(i int) bool {
			return keys[i] > continuationToken
		})
	}

	commonPrefixesMap := make(map[string]bool)
	count := 0

	for i := startIdx; i < len(keys); i++ {
		key := keys[i]

		if prefix != "" && !strings.HasPrefix(key, prefix) {
			continue
		}

		if delimiter != "" {
			rem := key[len(prefix):]
			idx := strings.Index(rem, delimiter)
			if idx != -1 {
				// Found delimiter, group as common prefix
				cPrefix := prefix + rem[:idx+len(delimiter)]
				commonPrefixesMap[cPrefix] = true
				continue
			}
		}

		// Retrieve metadata
		meta, err := r.GetObjectMeta(bucket, key)
		if err != nil {
			// If meta is missing, skip or log (might be inconsistent)
			continue
		}

		info := s3.ObjectInfo{
			Key:          key,
			LastModified: meta.ModifiedAt,
			ETag:         meta.ETag,
			Size:         meta.ContentLength,
			StorageClass: "STANDARD",
		}

		result.Contents = append(result.Contents, info)
		count++

		if count >= maxKeys {
			if i+1 < len(keys) {
				result.IsTruncated = true
				result.NextContinuationToken = key
			}
			break
		}
	}

	for cp := range commonPrefixesMap {
		result.CommonPrefixes = append(result.CommonPrefixes, cp)
	}
	sort.Strings(result.CommonPrefixes)
	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)

	return result, nil
}

// --- EC Shard Operations ---

// PutECShard writes a single erasure-coded shard to disk and updates the
// object metadata file with the provided meta (which must carry the EC
// parameters and the local shard index).
func (r *FilesystemRepository) PutECShard(bucket, key string, shardIndex int, reader io.Reader, size int64, meta s3.ObjectMeta) error {
	shardPath := r.ecShardPath(bucket, key, shardIndex)
	if err := os.MkdirAll(filepath.Dir(shardPath), 0755); err != nil {
		return err
	}

	tmpName := fmt.Sprintf("shard.tmp.%s", uuid.New().String())
	tmpPath := shardPath + "." + tmpName

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(tmpPath)
	}()

	written, err := io.Copy(f, reader)
	if err != nil {
		return err
	}
	if size >= 0 && written != size {
		return fmt.Errorf("ec shard size mismatch: expected %d, got %d", size, written)
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, shardPath); err != nil {
		return err
	}
	if err := r.syncDir(filepath.Dir(shardPath)); err != nil {
		return err
	}

	// Update metadata file.
	return r.UpdateObjectMeta(bucket, key, meta)
}

// GetECShard opens a single EC shard for reading.
func (r *FilesystemRepository) GetECShard(bucket, key string, shardIndex int) (io.ReadCloser, error) {
	f, err := os.Open(r.ecShardPath(bucket, key, shardIndex))
	if err != nil {
		return nil, err
	}
	return f, nil
}

// HasECShard reports whether a shard exists on the local node.
func (r *FilesystemRepository) HasECShard(bucket, key string, shardIndex int) (bool, error) {
	_, err := os.Stat(r.ecShardPath(bucket, key, shardIndex))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// DeleteECShard removes a single EC shard from disk.
func (r *FilesystemRepository) DeleteECShard(bucket, key string, shardIndex int) error {
	shardPath := r.ecShardPath(bucket, key, shardIndex)
	if err := os.Remove(shardPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	r.cleanEmptyDirs(filepath.Dir(shardPath), r.ecShardDir(bucket))
	return nil
}

// UpdateObjectMeta overwrites the metadata file for an existing object.
func (r *FilesystemRepository) UpdateObjectMeta(bucket, key string, meta s3.ObjectMeta) error {
	metaPath := filepath.Join(r.metaDir(bucket), key+".json")
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return err
	}
	if err := r.writeJSON(metaPath, meta); err != nil {
		return err
	}
	return r.syncDir(filepath.Dir(metaPath))
}

// RemoveReplicaData deletes only the full replica data file for an object,
// leaving the metadata and any EC shards intact. It is used after a
// successful replica -> EC conversion to reclaim the space occupied by the
// original full copy. If the data file does not exist, it is a no-op.
func (r *FilesystemRepository) RemoveReplicaData(bucket, key string) error {
	dataPath := filepath.Join(r.dataDir(bucket), key)
	if err := os.Remove(dataPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	r.cleanEmptyDirs(filepath.Dir(dataPath), r.dataDir(bucket))
	return nil
}

// DeleteAllECShards removes every EC shard for a given object (used when
// converting back to replica or deleting an EC object).
func (r *FilesystemRepository) DeleteAllECShards(bucket, key string) error {
	dir := r.ecShardDir(bucket)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	prefix := key + ".shard."
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			_ = os.Remove(filepath.Join(dir, e.Name()))
		}
	}
	r.cleanEmptyDirs(dir, r.ecShardDir(bucket))
	return nil
}

// --- Multipart Upload Operations ---

func (r *FilesystemRepository) CreateMultipartUpload(bucket, key string) (string, error) {
	uploadID := uuid.New().String()
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		return "", err
	}

	uploadMeta := s3.MultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		Initiated: time.Now(),
	}

	metaPath := filepath.Join(sessionDir, "upload.json")
	if err := r.writeJSON(metaPath, uploadMeta); err != nil {
		return "", err
	}

	return uploadID, nil
}

func (r *FilesystemRepository) SaveMultipartPart(bucket, uploadID string, partNum int, reader io.Reader) (s3.UploadPart, error) {
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	if _, err := os.Stat(sessionDir); os.IsNotExist(err) {
		return s3.UploadPart{}, fmt.Errorf("multipart upload %s not found", uploadID)
	}

	partName := fmt.Sprintf("%05d", partNum)
	partPath := filepath.Join(sessionDir, partName)
	tmpPath := partPath + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return s3.UploadPart{}, err
	}
	defer func() {
		f.Close()
		os.Remove(tmpPath)
	}()

	hCRC := crc32.NewIEEE()
	hMD5 := md5.New()
	mw := io.MultiWriter(f, hCRC, hMD5)

	written, err := io.Copy(mw, reader)
	if err != nil {
		return s3.UploadPart{}, err
	}

	if err := f.Sync(); err != nil {
		return s3.UploadPart{}, err
	}
	if err := f.Close(); err != nil {
		return s3.UploadPart{}, err
	}

	if err := os.Rename(tmpPath, partPath); err != nil {
		return s3.UploadPart{}, err
	}

	partMeta := s3.UploadPart{
		PartNumber: partNum,
		Size:       written,
		ETag:       fmt.Sprintf("\"%s\"", hex.EncodeToString(hMD5.Sum(nil))),
		CRC32:      hCRC.Sum32(),
		ModifiedAt: time.Now(),
	}

	metaPath := partPath + ".json"
	if err := r.writeJSON(metaPath, partMeta); err != nil {
		return s3.UploadPart{}, err
	}

	return partMeta, nil
}

func (r *FilesystemRepository) GetMultipartPartReader(bucket, uploadID string, partNum int) (io.ReadCloser, error) {
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	partPath := filepath.Join(sessionDir, fmt.Sprintf("%05d", partNum))
	f, err := os.Open(partPath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (r *FilesystemRepository) DeleteMultipartPart(bucket, uploadID string, partNum int) error {
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	partPath := filepath.Join(sessionDir, fmt.Sprintf("%05d", partNum))

	// Delete the part file
	if err := os.Remove(partPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Delete the part metadata file
	if err := os.Remove(partPath + ".json"); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (r *FilesystemRepository) GetMultipartParts(bucket, uploadID string) ([]s3.UploadPart, error) {
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	if _, err := os.Stat(sessionDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("multipart upload %s not found", uploadID)
	}

	entries, err := os.ReadDir(sessionDir)
	if err != nil {
		return nil, err
	}

	var parts []s3.UploadPart
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") && entry.Name() != "upload.json" {
			var part s3.UploadPart
			if err := r.readJSON(filepath.Join(sessionDir, entry.Name()), &part); err == nil {
				parts = append(parts, part)
			}
		}
	}

	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	return parts, nil
}

func (r *FilesystemRepository) AbortMultipartUpload(bucket, uploadID string) error {
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	return os.RemoveAll(sessionDir)
}

func (r *FilesystemRepository) GetMultipartUpload(bucket, uploadID string) (s3.MultipartUpload, error) {
	sessionDir := r.uploadSessionDir(bucket, uploadID)
	var upload s3.MultipartUpload
	err := r.readJSON(filepath.Join(sessionDir, "upload.json"), &upload)
	if err != nil {
		return s3.MultipartUpload{}, err
	}
	return upload, nil
}

func (r *FilesystemRepository) ListMultipartUploads(bucket string) ([]s3.MultipartUpload, error) {
	dir := r.uploadsDir(bucket)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var uploads []s3.MultipartUpload
	for _, entry := range entries {
		if entry.IsDir() {
			var upload s3.MultipartUpload
			err := r.readJSON(filepath.Join(dir, entry.Name(), "upload.json"), &upload)
			if err == nil {
				uploads = append(uploads, upload)
			}
		}
	}
	return uploads, nil
}

// --- Maintenance Operations ---

var _ s3app.MaintenanceRepository = (*FilesystemRepository)(nil)

// CleanupExpiredTransactions aborts prepared transactions older than maxAge.
func (r *FilesystemRepository) CleanupExpiredTransactions(maxAge time.Duration) ([]s3.Transaction, error) {
	stagingPath := filepath.Join(r.root, "staging")
	entries, err := os.ReadDir(stagingPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var aborted []s3.Transaction
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		txID := entry.Name()
		tx, err := r.GetTransaction(txID)
		if err != nil {
			// Corrupt staging transaction — abort it.
			_ = r.AbortTransaction(txID)
			continue
		}

		if tx.State == s3.TxPrepared && time.Since(tx.CreatedAt) > maxAge {
			aborted = append(aborted, tx)
			_ = r.AbortTransaction(txID)
		}
	}
	return aborted, nil
}

// CleanupOrphanedObjects removes object data that has no corresponding metadata
// and is older than minAge.
func (r *FilesystemRepository) CleanupOrphanedObjects(minAge time.Duration) (int, error) {
	dataPath := filepath.Join(r.root, "data")
	metaPath := filepath.Join(r.root, "meta")

	removed := 0
	err := filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}

		rel, err := filepath.Rel(dataPath, path)
		if err != nil {
			return nil
		}

		parts := strings.SplitN(rel, string(filepath.Separator), 2)
		if len(parts) < 2 {
			return nil
		}
		bucket := parts[0]
		key := parts[1]

		mPath := filepath.Join(metaPath, bucket, key+".json")
		if _, err := os.Stat(mPath); os.IsNotExist(err) {
			if time.Since(info.ModTime()) > minAge {
				_ = os.Remove(path)
				removed++

				dir := filepath.Dir(path)
				bucketDir := filepath.Join(dataPath, bucket)
				for dir != bucketDir && dir != dataPath && len(dir) > len(bucketDir) {
					entries, err := os.ReadDir(dir)
					if err == nil && len(entries) == 0 {
						_ = os.Remove(dir)
					} else {
						break
					}
					dir = filepath.Dir(dir)
				}
			}
		}
		return nil
	})
	return removed, err
}

// CleanupExpiredMultipartUploads aborts multipart uploads older than maxAge.
func (r *FilesystemRepository) CleanupExpiredMultipartUploads(maxAge time.Duration) ([]s3.MultipartUpload, error) {
	buckets, err := r.ListBuckets()
	if err != nil {
		return nil, err
	}

	var aborted []s3.MultipartUpload
	for _, b := range buckets {
		uploads, err := r.ListMultipartUploads(b)
		if err != nil {
			continue
		}
		for _, up := range uploads {
			if time.Since(up.Initiated) > maxAge {
				aborted = append(aborted, up)
				_ = r.AbortMultipartUpload(b, up.UploadID)
			}
		}
	}
	return aborted, nil
}

// --- Internal Helper Methods ---

func (r *FilesystemRepository) isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func (r *FilesystemRepository) cleanEmptyDirs(startDir, limitDir string) {
	current := filepath.Clean(startDir)
	limit := filepath.Clean(limitDir)

	for current != limit && current != "." && current != "/" {
		empty, err := r.isDirEmpty(current)
		if err != nil || !empty {
			break
		}
		if err := os.Remove(current); err != nil {
			break
		}
		current = filepath.Dir(current)
	}
}

func (r *FilesystemRepository) syncDir(dirPath string) error {
	df, err := os.Open(dirPath)
	if err != nil {
		return err
	}
	defer df.Close()
	return df.Sync()
}

func (r *FilesystemRepository) writeJSON(path string, val interface{}) error {
	tmpPath := path + ".tmp." + strconv.FormatInt(time.Now().UnixNano(), 10)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(tmpPath)
	}()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(val); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

func (r *FilesystemRepository) readJSON(path string, target interface{}) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(target)
}
