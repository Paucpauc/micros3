package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/paucpauc/micros3/internal/application/s3app"
	"github.com/paucpauc/micros3/internal/cluster"
	"github.com/paucpauc/micros3/internal/config"
	"github.com/paucpauc/micros3/internal/infrastructure/storage/fs"
	"github.com/paucpauc/micros3/internal/internal_api"
	"github.com/paucpauc/micros3/internal/metrics"
	"github.com/paucpauc/micros3/internal/presentation/s3api"
	"github.com/paucpauc/micros3/internal/replication"
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

	logger.Info("Initializing MicroS3 in standalone mode",
		zap.String("node_id", cfg.Node.ID),
		zap.String("storage_root", cfg.Storage.Root),
	)

	// 3. Initialize storage
	storageRepo, err := fs.NewFilesystemRepository(cfg.Storage.Root)
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

		// Start SyncWorker for follower nodes to catch up
		syncWorker := replication.NewSyncWorker(apiClient, staticMgr, storageRepo, logger, cfg.Cluster.K8s.InternalPort)
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

		// Start SyncWorker for follower nodes to catch up
		syncWorker := replication.NewSyncWorker(apiClient, k8sMgr, storageRepo, logger, cfg.Cluster.K8s.InternalPort)
		syncWorker.Start(context.Background())
	} else {
		// Fallback to standalone
		clusterMgr = cluster.NewStandaloneClusterManager(cfg.Node.ID)
		replicator = replication.NewReplicator(apiClient, clusterMgr, storageRepo, 15*time.Minute, logger)
	}

	// 5. Initialize S3 Application Service
	svc := s3app.NewService(storageRepo, replicator, clusterMgr, logger)
	svc.SetWriteBlockBehavior(cfg.Sync.WriteBlockBehavior)

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

	// background deduplication worker
	if cfg.Storage.Dedup.Enabled {
		go func() {
			ticker := time.NewTicker(cfg.Storage.Dedup.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					logger.Debug("Running background deduplication...")
					metrics.DedupRunsTotal.Inc()
					linked, err := storageRepo.Deduplicate()
					if err != nil {
						logger.Warn("Deduplication failed", zap.Error(err))
						continue
					}
					metrics.DedupLinksTotal.Add(float64(linked))
					if linked > 0 {
						logger.Info("Deduplication completed",
							zap.Int("files_linked", linked),
						)
					}
				}
			}
		}()
		logger.Info("Background deduplication enabled",
			zap.String("interval", cfg.Storage.Dedup.IntervalStr),
		)
	}

	// expired multipart upload cleanup worker
	go func() {
		ticker := time.NewTicker(cfg.Multipart.CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logger.Debug("Running expired multipart uploads cleanup...")
				buckets, err := storageRepo.ListBuckets()
				if err != nil {
					continue
				}
				for _, b := range buckets {
					uploads, err := storageRepo.ListMultipartUploads(b)
					if err != nil {
						continue
					}
					for _, up := range uploads {
						if time.Since(up.Initiated) > cfg.Multipart.UploadExpiry {
							logger.Info("Aborting expired multipart upload",
								zap.String("bucket", b),
								zap.String("key", up.Key),
								zap.String("upload_id", up.UploadID),
								zap.Time("initiated", up.Initiated),
							)
							_ = storageRepo.AbortMultipartUpload(b, up.UploadID)
						}
					}
				}
			}
		}
	}()

	// 2PC Prepared Transaction Janitor
	go func() {
		runCleanup := func() {
			stagingPath := filepath.Join(cfg.Storage.Root, "staging")
			entries, err := os.ReadDir(stagingPath)
			if err != nil {
				if !os.IsNotExist(err) {
					logger.Warn("Failed to read staging directory", zap.Error(err))
				}
				return
			}

			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				txID := entry.Name()
				tx, err := storageRepo.GetTransaction(txID)
				if err != nil {
					logger.Warn("Corrupt staging transaction found, aborting", zap.String("txID", txID), zap.Error(err))
					_ = storageRepo.AbortTransaction(txID)
					continue
				}

				if tx.State == "PREPARED" && time.Since(tx.CreatedAt) > 2*time.Minute {
					logger.Info("Aborting expired/dangling prepared transaction (coordinator timeout)",
						zap.String("txID", txID),
						zap.String("bucket", tx.Bucket),
						zap.String("key", tx.Key),
						zap.Time("created_at", tx.CreatedAt),
					)
					_ = storageRepo.AbortTransaction(txID)
				}
			}
		}

		runOrphanCleanup := func() {
			dataPath := filepath.Join(cfg.Storage.Root, "data")
			metaPath := filepath.Join(cfg.Storage.Root, "meta")

			_ = filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
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
					if time.Since(info.ModTime()) > 5*time.Minute {
						logger.Warn("Found orphaned S3 data file without metadata, deleting",
							zap.String("bucket", bucket),
							zap.String("key", key),
							zap.String("path", path),
							zap.Time("mod_time", info.ModTime()),
						)
						_ = os.Remove(path)

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
		}

		runCleanup()
		runOrphanCleanup()

		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				logger.Debug("Running expired prepared transactions cleanup...")
				runCleanup()
				runOrphanCleanup()
			}
		}
	}()

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
