package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/cluster"
	"github.com/paucpauc/micros3/internal/config"
	eccodec "github.com/paucpauc/micros3/internal/infrastructure/storage/ec"
	"github.com/paucpauc/micros3/internal/infrastructure/storage/fs"
	"github.com/paucpauc/micros3/internal/internal_api"
	"github.com/paucpauc/micros3/internal/metrics"
	"github.com/paucpauc/micros3/internal/presentation/s3api"
	"github.com/paucpauc/micros3/internal/replication"
	ecrepl "github.com/paucpauc/micros3/internal/replication/ec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	configPath := flag.String("config", "", "Path to configuration YAML file")
	flag.Parse()

	// 1. Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		// Log to stderr since logger is not initialized yet
		println("Failed to load configuration:", err.Error())
		os.Exit(1)
	}

	// 2. Setup logger
	logger := initLogger(cfg.Log.Level, cfg.Log.Format)
	defer logger.Sync()

	logger.Info("Initializing MicroS3",
		zap.String("node_id", cfg.Node.ID),
		zap.String("storage_type", cfg.Storage.Type),
	)

	// 3. Initialize storage (selected by cfg.Storage.Type)
	storageRepo, err := newStorageRepository(cfg)
	if err != nil {
		logger.Fatal("Failed to initialize storage repository", zap.Error(err))
	}

	// 4. Initialize cluster & replication manager (Phase 2)
	apiClient := internal_api.NewClient(cfg.Cluster.Token, cfg.Health.Timeout)

	var clusterMgr s3app.ClusterManager
	var replicator s3app.Replicator

	if strings.ToLower(cfg.Cluster.Mode) == "static" {
		staticMgr := cluster.NewStaticClusterManager(cfg, apiClient, logger)
		if !staticMgr.IsLeader() {
			staticMgr.SetLocalStatus("SYNCING")
		}
		staticMgr.Start(context.Background())
		clusterMgr = staticMgr
		replicator = replication.NewReplicator(apiClient, staticMgr, storageRepo, 15*time.Minute, logger)

		// Start SyncWorker for follower nodes to request leader-driven sync
		syncWorker := replication.NewSyncWorker(apiClient, staticMgr, logger, cfg.Cluster.K8s.InternalPort)
		syncWorker.Start(context.Background())
	} else if strings.ToLower(cfg.Cluster.Mode) == "k8s" {
		k8sMgr, err := cluster.NewK8sClusterManager(cfg, apiClient, logger)
		if err != nil {
			logger.Fatal("Failed to initialize K8s cluster manager", zap.Error(err))
		}
		if !k8sMgr.IsLeader() {
			k8sMgr.SetLocalStatus("SYNCING")
		}
		k8sMgr.Start(context.Background())
		clusterMgr = k8sMgr
		replicator = replication.NewReplicator(apiClient, k8sMgr, storageRepo, 15*time.Minute, logger)

		// Start SyncWorker for follower nodes to request leader-driven sync
		syncWorker := replication.NewSyncWorker(apiClient, k8sMgr, logger, cfg.Cluster.K8s.InternalPort)
		syncWorker.Start(context.Background())
	} else {
		// Fallback to standalone
		clusterMgr = cluster.NewStandaloneClusterManager(cfg.Node.ID)
		replicator = replication.NewReplicator(apiClient, clusterMgr, storageRepo, 15*time.Minute, logger)
	}

	// 5. Initialize S3 Application Service
	svc := s3app.NewService(storageRepo, replicator, clusterMgr, metrics.NewPrometheusRecorder(), logger)
	svc.SetWriteBlockBehavior(cfg.Sync.WriteBlockBehavior)

	// Inject leader-driven sync coordinator (used by the leader when a
	// follower requests synchronization)
	syncCoordinator := replication.NewSyncCoordinator(apiClient, storageRepo, logger)
	svc.SetSyncCoordinator(syncCoordinator)

	// 5b. Initialize Erasure Coding manager (if enabled).
	// The EC manager runs on the leader and handles background conversion
	// of replica objects into erasure-coded shards, read reconstruction,
	// and shard repair. It is injected into the service so that GetObject
	// can transparently reconstruct EC objects.
	if cfg.EC.Enabled {
		ecCodec, err := eccodec.NewCodec(cfg.EC.K, cfg.EC.M)
		if err != nil {
			logger.Fatal("Failed to create EC codec", zap.Error(err))
		}
		ecMgr := ecrepl.NewManager(
			apiClient,
			storageRepo,
			clusterMgr,
			ecCodec,
			cfg.EC.MinAge,
			cfg.EC.MinObjectSize,
			logger,
		)
		svc.SetECReader(ecMgr)

		logger.Info("Erasure coding enabled",
			zap.Int("k", cfg.EC.K),
			zap.Int("m", cfg.EC.M),
			zap.Int64("min_object_size", cfg.EC.MinObjectSize),
			zap.Duration("min_age", cfg.EC.MinAge),
		)

		// Start background loops (convert + repair). They self-check
		// IsLeader() on every tick, so they are safe to start on all nodes.
		ecMgr.StartConvertLoop(context.Background(), cfg.EC.ConvertInterval)
		ecMgr.StartRepairLoop(context.Background(), cfg.EC.RestoreInterval)
	} else {
		logger.Info("Erasure coding disabled")
	}

	// 6. Initialize Auth Validator (if credentials are set)
	var authValidator *s3api.AuthValidator
	if len(cfg.S3.Credentials) > 0 && cfg.S3.Credentials[0].AccessKey != "" {
		authValidator = s3api.NewAuthValidator(cfg.S3.Credentials, logger)
		logger.Info("S3 Authentication enabled (AWS Signature V4)")
	} else {
		logger.Warn("S3 Authentication disabled! Anonymous access allowed")
	}

	// 7. Initialize HTTP handler
	s3Handler := s3api.NewHandler(svc, authValidator, clusterMgr, cfg.Cluster.Token, cfg.Sync.AllowLocalReads, logger)

	// 8. Run S3 & Internal Servers with Graceful Shutdown
	srv := &http.Server{
		Addr:    cfg.Server.S3Listen,
		Handler: s3Handler,
	}

	internalHandler := internal_api.NewHandler(storageRepo, svc, clusterMgr, s3Handler, cfg.Cluster.Token, logger)
	internalSrv := internal_api.NewServer(cfg.Server.InternalListen, internalHandler, logger)

	// Background maintenance workers (only if the storage backend supports maintenance)
	if maint, ok := storageRepo.(s3app.MaintenanceRepository); ok {
		// expired multipart upload cleanup worker
		go func() {
			ticker := time.NewTicker(cfg.Multipart.CleanupInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					logger.Debug("Running expired multipart uploads cleanup...")
					aborted, err := maint.CleanupExpiredMultipartUploads(cfg.Multipart.UploadExpiry)
					if err != nil {
						logger.Warn("Multipart upload cleanup failed", zap.Error(err))
						continue
					}
					for _, up := range aborted {
						logger.Info("Aborted expired multipart upload",
							zap.String("key", up.Key),
							zap.String("upload_id", up.UploadID),
							zap.Time("initiated", up.Initiated),
						)
					}
				}
			}
		}()

		// 2PC Prepared Transaction Janitor + Orphan cleanup
		go func() {
			runJanitor := func() {
				aborted, err := maint.CleanupExpiredTransactions(2 * time.Minute)
				if err != nil {
					logger.Warn("Expired transactions cleanup failed", zap.Error(err))
				}
				for _, tx := range aborted {
					logger.Info("Aborted expired/dangling prepared transaction (coordinator timeout)",
						zap.String("txID", tx.ID),
						zap.String("bucket", tx.Bucket),
						zap.String("key", tx.Key),
						zap.Time("created_at", tx.CreatedAt),
					)
				}

				removed, err := maint.CleanupOrphanedObjects(5 * time.Minute)
				if err != nil {
					logger.Warn("Orphaned objects cleanup failed", zap.Error(err))
				}
				if removed > 0 {
					logger.Info("Cleaned up orphaned objects", zap.Int("count", removed))
				}
			}

			runJanitor()

			ticker := time.NewTicker(1 * time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					logger.Debug("Running expired prepared transactions cleanup...")
					runJanitor()
				}
			}
		}()
	} else {
		logger.Info("Storage backend does not support MaintenanceRepository, skipping background maintenance workers")
	}

	go func() {
		svc.UpdateStorageMetrics()
		svc.UpdateClusterMetrics()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				svc.UpdateStorageMetrics()
				svc.UpdateClusterMetrics()
			}
		}
	}()

	go func() {
		logger.Info("Starting S3 API server",
			zap.String("address", cfg.Server.S3Listen),
			zap.Bool("tls", cfg.TLS.S3.Enabled),
		)
		var err error
		if cfg.TLS.S3.Enabled {
			err = srv.ListenAndServeTLS(cfg.TLS.S3.CertFile, cfg.TLS.S3.KeyFile)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal("S3 server failed to start", zap.Error(err))
		}
	}()

	go func() {
		// TLS support for internal API is handled similarly, for now we run HTTP
		// in K8s internal networks, or ListenAndServeTLS if enabled.
		var err error
		if cfg.TLS.Internal.Enabled {
			// To keep it simple, we can construct standard server and use ListenAndServeTLS
			internalSrvTLS := &http.Server{
				Addr:    cfg.Server.InternalListen,
				Handler: internalHandler,
			}
			logger.Info("Starting Internal API server (HTTPS)", zap.String("address", cfg.Server.InternalListen))
			err = internalSrvTLS.ListenAndServeTLS(cfg.TLS.Internal.CertFile, cfg.TLS.Internal.KeyFile)
		} else {
			err = internalSrv.Start()
		}
		if err != nil && err != http.ErrServerClosed {
			logger.Fatal("Internal API server failed to start", zap.Error(err))
		}
	}()

	// Signal handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("Shutting down servers gracefully...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Shutdown S3 server
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("S3 server shutdown failed", zap.Error(err))
	}

	// Shutdown Internal server
	if err := internalSrv.Shutdown(ctx); err != nil {
		logger.Error("Internal API server shutdown failed", zap.Error(err))
	}

	// Stop background loops if static/k8s manager was started
	if staticMgr, ok := clusterMgr.(*cluster.StaticClusterManager); ok {
		staticMgr.Stop()
	}
	if k8sMgr, ok := clusterMgr.(*cluster.K8sClusterManager); ok {
		k8sMgr.Stop()
	}

	logger.Info("MicroS3 server stopped")
}

func initLogger(levelStr, formatStr string) *zap.Logger {
	var logCfg zap.Config
	if strings.ToLower(formatStr) == "json" {
		logCfg = zap.NewProductionConfig()
	} else {
		logCfg = zap.NewDevelopmentConfig()
		logCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	var level zapcore.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = zap.DebugLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	logCfg.Level.SetLevel(level)
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := logCfg.Build()
	if err != nil {
		println("Failed to build logger:", err.Error())
		os.Exit(1)
	}
	return logger
}

// newStorageRepository constructs the storage backend selected by cfg.Storage.Type.
// To add a new backend, add a case here and implement s3app.StorageRepository.
func newStorageRepository(cfg *config.Config) (s3app.StorageRepository, error) {
	switch strings.ToLower(cfg.Storage.Type) {
	case "fs", "":
		return fs.NewFilesystemRepository(cfg.Storage.Root)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}
}
