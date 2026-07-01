package replication

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/domain/s3"
	"github.com/paucpauc/micros3/internal/internal_api"
	"go.uber.org/zap"
)

var _ s3app.Replicator = (*ReplicatorImpl)(nil)

type ReplicatorImpl struct {
	client  *internal_api.Client
	cluster s3app.ClusterManager
	storage s3app.StorageRepository
	timeout time.Duration
	logger  *zap.Logger
}

func NewReplicator(
	client *internal_api.Client,
	cluster s3app.ClusterManager,
	storage s3app.StorageRepository,
	timeout time.Duration,
	logger *zap.Logger,
) *ReplicatorImpl {
	return &ReplicatorImpl{
		client:  client,
		cluster: cluster,
		storage: storage,
		timeout: timeout,
		logger:  logger,
	}
}

func (r *ReplicatorImpl) PrepareAll(ctx context.Context, tx s3.Transaction, meta s3.ObjectMeta) map[string]error {
	ctx = s3.WithoutCancel(ctx)
	followers := r.cluster.AliveFollowers()
	if len(followers) == 0 {
		return nil
	}

	reqID := s3.GetRequestID(ctx)
	r.logger.Info("Replicating Prepare phase",
		zap.String("tx_id", tx.ID),
		zap.Int("followers_count", len(followers)),
		zap.String("request_id", reqID),
	)

	errs := make(map[string]error)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range followers {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()

			r.logger.Debug("Sending Prepare to follower",
				zap.String("tx_id", tx.ID),
				zap.String("address", address),
				zap.String("request_id", reqID),
			)

			subCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()

			var reader io.ReadCloser
			var err error

			if tx.Operation == s3.OpPut {
				reader, err = r.storage.GetStagedObjectReader(tx.ID)
				if err != nil {
					mu.Lock()
					errs[address] = err
					mu.Unlock()
					return
				}
				defer reader.Close()
			}

			err = r.client.Prepare(subCtx, address, tx, meta, reader, meta.ContentLength)
			if err != nil {
				r.logger.Warn("Prepare failed on follower",
					zap.String("tx_id", tx.ID),
					zap.String("address", address),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				mu.Lock()
				errs[address] = err
				mu.Unlock()
			} else {
				r.logger.Debug("Prepare succeeded on follower",
					zap.String("tx_id", tx.ID),
					zap.String("address", address),
					zap.String("request_id", reqID),
				)
			}
		}(addr)
	}

	wg.Wait()
	return errs
}

func (r *ReplicatorImpl) CommitAll(ctx context.Context, txID, bucket, key string) map[string]error {
	ctx = s3.WithoutCancel(ctx)
	followers := r.cluster.AliveFollowers()
	if len(followers) == 0 {
		return nil
	}

	reqID := s3.GetRequestID(ctx)
	r.logger.Info("Replicating Commit phase",
		zap.String("tx_id", txID),
		zap.Int("followers_count", len(followers)),
		zap.String("request_id", reqID),
	)

	errs := make(map[string]error)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range followers {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()

			r.logger.Debug("Sending Commit to follower",
				zap.String("tx_id", txID),
				zap.String("address", address),
				zap.String("request_id", reqID),
			)

			subCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()

			err := r.client.Commit(subCtx, address, txID, bucket, key)
			if err != nil {
				r.logger.Warn("Commit failed on follower",
					zap.String("tx_id", txID),
					zap.String("address", address),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				mu.Lock()
				errs[address] = err
				mu.Unlock()
			} else {
				r.logger.Debug("Commit succeeded on follower",
					zap.String("tx_id", txID),
					zap.String("address", address),
					zap.String("request_id", reqID),
				)
			}
		}(addr)
	}

	wg.Wait()
	return errs
}

func (r *ReplicatorImpl) AbortAll(ctx context.Context, txID string) map[string]error {
	ctx = s3.WithoutCancel(ctx)
	followers := r.cluster.AliveFollowers()
	if len(followers) == 0 {
		return nil
	}

	reqID := s3.GetRequestID(ctx)
	r.logger.Info("Replicating Abort phase",
		zap.String("tx_id", txID),
		zap.Int("followers_count", len(followers)),
		zap.String("request_id", reqID),
	)

	errs := make(map[string]error)
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range followers {
		wg.Add(1)
		go func(address string) {
			defer wg.Done()

			r.logger.Debug("Sending Abort to follower",
				zap.String("tx_id", txID),
				zap.String("address", address),
				zap.String("request_id", reqID),
			)

			subCtx, cancel := context.WithTimeout(ctx, r.timeout)
			defer cancel()

			err := r.client.Abort(subCtx, address, txID)
			if err != nil {
				r.logger.Warn("Abort failed on follower",
					zap.String("tx_id", txID),
					zap.String("address", address),
					zap.Error(err),
					zap.String("request_id", reqID),
				)
				mu.Lock()
				errs[address] = err
				mu.Unlock()
			} else {
				r.logger.Debug("Abort succeeded on follower",
					zap.String("tx_id", txID),
					zap.String("address", address),
					zap.String("request_id", reqID),
				)
			}
		}(addr)
	}

	wg.Wait()
	return errs
}
