package fs

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/paucpauc/micros3/internal/domain/s3"
)

func TestBucketOperations(t *testing.T) {
	tempDir := t.TempDir()
	repo, err := NewFilesystemRepository(tempDir)
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}

	bucket := "test-bucket"

	// HasBucket should return false initially
	exists, err := repo.HasBucket(bucket)
	if err != nil {
		t.Fatalf("HasBucket failed: %v", err)
	}
	if exists {
		t.Errorf("bucket should not exist yet")
	}

	// Create bucket
	if err := repo.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	// HasBucket should return true now
	exists, err = repo.HasBucket(bucket)
	if err != nil {
		t.Fatalf("HasBucket failed: %v", err)
	}
	if !exists {
		t.Errorf("bucket should exist")
	}

	// List buckets
	buckets, err := repo.ListBuckets()
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 1 || buckets[0] != bucket {
		t.Errorf("unexpected buckets: %v", buckets)
	}

	// Delete bucket
	if err := repo.DeleteBucket(bucket); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	// HasBucket should be false again
	exists, err = repo.HasBucket(bucket)
	if err != nil {
		t.Fatalf("HasBucket failed: %v", err)
	}
	if exists {
		t.Errorf("bucket should not exist after deletion")
	}
}

func TestPutObject2PC(t *testing.T) {
	tempDir := t.TempDir()
	repo, err := NewFilesystemRepository(tempDir)
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}

	bucket := "test-bucket"
	key := "path/to/my/object.txt"
	content := []byte("hello micros3 2pc test")

	if err := repo.CreateBucket(bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	txID := "tx-12345"
	tx := s3.Transaction{
		ID:        txID,
		Operation: s3.OpPut,
		Bucket:    bucket,
		Key:       key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	meta := s3.ObjectMeta{
		ContentType:  "text/plain",
		CreatedAt:    time.Now(),
		ModifiedAt:   time.Now(),
		UserMetadata: map[string]string{"env": "test"},
	}

	// Stage object
	_, err = repo.StageObject(txID, bytes.NewReader(content), int64(len(content)), meta, tx)
	if err != nil {
		t.Fatalf("StageObject failed: %v", err)
	}

	// Check staging dir exists and contains data/meta/tx files
	stageDir := filepath.Join(tempDir, "staging", txID)
	if _, err := os.Stat(filepath.Join(stageDir, "data")); err != nil {
		t.Errorf("staging data file not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stageDir, "meta.json")); err != nil {
		t.Errorf("staging meta file not found: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stageDir, "tx.json")); err != nil {
		t.Errorf("staging tx file not found: %v", err)
	}

	// Commit transaction
	committedMeta, err := repo.CommitTransaction(txID, bucket, key)
	if err != nil {
		t.Fatalf("CommitTransaction failed: %v", err)
	}

	// Verify metadata generated on-the-fly
	if committedMeta.ContentLength != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), committedMeta.ContentLength)
	}
	if committedMeta.ETag == "" {
		t.Errorf("ETag should not be empty")
	}
	if committedMeta.CRC32 == 0 {
		t.Errorf("CRC32 should not be 0")
	}

	// Staging dir should be deleted
	if _, err := os.Stat(stageDir); !os.IsNotExist(err) {
		t.Errorf("staging directory was not cleaned up")
	}

	// Verify file is stored in data and meta
	dataPath := filepath.Join(tempDir, "data", bucket, key)
	metaPath := filepath.Join(tempDir, "meta", bucket, key+".json")

	if _, err := os.Stat(dataPath); err != nil {
		t.Errorf("committed data file not found: %v", err)
	}
	if _, err := os.Stat(metaPath); err != nil {
		t.Errorf("committed meta file not found: %v", err)
	}

	// GetObject and verify content
	rc, readMeta, err := repo.GetObject(bucket, key)
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer rc.Close()

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("failed to read object data: %v", err)
	}

	if !bytes.Equal(readData, content) {
		t.Errorf("data mismatch: expected %q, got %q", content, readData)
	}
	if readMeta.ETag != committedMeta.ETag {
		t.Errorf("metadata ETag mismatch")
	}
}

func TestDeleteObject2PC(t *testing.T) {
	tempDir := t.TempDir()
	repo, err := NewFilesystemRepository(tempDir)
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}

	bucket := "test-bucket"
	key := "path/to/delete.txt"
	content := []byte("delete me")

	_ = repo.CreateBucket(bucket)

	// Direct stage & commit a put first
	txID1 := "tx-1"
	tx1 := s3.Transaction{ID: txID1, Operation: s3.OpPut, Bucket: bucket, Key: key, State: s3.TxPrepared}
	_, _ = repo.StageObject(txID1, bytes.NewReader(content), int64(len(content)), s3.ObjectMeta{}, tx1)
	_, _ = repo.CommitTransaction(txID1, bucket, key)

	// Now stage a delete transaction
	txID2 := "tx-2"
	tx2 := s3.Transaction{
		ID:        txID2,
		Operation: s3.OpDelete,
		Bucket:    bucket,
		Key:       key,
		State:     s3.TxPrepared,
		CreatedAt: time.Now(),
	}

	_, err = repo.StageObject(txID2, nil, 0, s3.ObjectMeta{}, tx2)
	if err != nil {
		t.Fatalf("StageObject for Delete failed: %v", err)
	}

	// Commit delete transaction
	_, err = repo.CommitTransaction(txID2, bucket, key)
	if err != nil {
		t.Fatalf("CommitTransaction for Delete failed: %v", err)
	}

	// Check files are deleted and empty directories cleaned
	dataPath := filepath.Join(tempDir, "data", bucket, key)
	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Errorf("data file was not deleted")
	}

	// The parent directories under test-bucket should be deleted as well
	parentDataDir := filepath.Join(tempDir, "data", bucket, "path")
	if _, err := os.Stat(parentDataDir); !os.IsNotExist(err) {
		t.Errorf("empty parent directory was not cleaned up: %v", parentDataDir)
	}
}

func TestMultipartUploads(t *testing.T) {
	tempDir := t.TempDir()
	repo, err := NewFilesystemRepository(tempDir)
	if err != nil {
		t.Fatalf("failed to create repo: %v", err)
	}

	bucket := "test-bucket"
	key := "multipart.bin"
	_ = repo.CreateBucket(bucket)

	// Create Multipart Upload
	uploadID, err := repo.CreateMultipartUpload(bucket, key)
	if err != nil {
		t.Fatalf("CreateMultipartUpload failed: %v", err)
	}
	if uploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// Save Parts
	part1Content := []byte("part one data ")
	part2Content := []byte("part two data")

	p1, err := repo.SaveMultipartPart(bucket, uploadID, 1, bytes.NewReader(part1Content))
	if err != nil {
		t.Fatalf("SaveMultipartPart 1 failed: %v", err)
	}
	if p1.PartNumber != 1 || p1.Size != int64(len(part1Content)) {
		t.Errorf("unexpected part 1 metadata: %+v", p1)
	}

	p2, err := repo.SaveMultipartPart(bucket, uploadID, 2, bytes.NewReader(part2Content))
	if err != nil {
		t.Fatalf("SaveMultipartPart 2 failed: %v", err)
	}
	if p2.PartNumber != 2 || p2.Size != int64(len(part2Content)) {
		t.Errorf("unexpected part 2 metadata: %+v", p2)
	}

	// List Parts
	parts, err := repo.GetMultipartParts(bucket, uploadID)
	if err != nil {
		t.Fatalf("GetMultipartParts failed: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	if parts[0].PartNumber != 1 || parts[1].PartNumber != 2 {
		t.Errorf("parts not sorted: %+v", parts)
	}

	// Abort Multipart Upload
	err = repo.AbortMultipartUpload(bucket, uploadID)
	if err != nil {
		t.Fatalf("AbortMultipartUpload failed: %v", err)
	}

	// Verify directory is deleted
	sessionDir := filepath.Join(tempDir, "uploads", bucket, uploadID)
	if _, err := os.Stat(sessionDir); !os.IsNotExist(err) {
		t.Errorf("uploads session directory not cleaned up")
	}
}
