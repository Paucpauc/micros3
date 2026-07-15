package config

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	ID string `yaml:"id"`
}

type ServerConfig struct {
	S3Listen       string `yaml:"s3_listen"`
	InternalListen string `yaml:"internal_listen"`
}

type StorageConfig struct {
	// Type selects the storage backend implementation. Currently supported:
	//   "fs" — local filesystem (default).
	// Future backends (e.g. "s3", "postgres") will be added here.
	Type string `yaml:"type"`
	// Root is the filesystem root path, used when Type == "fs".
	Root string `yaml:"root"`
	// DSN is an optional connection string for non-filesystem backends.
	DSN string `yaml:"dsn"`
}

type K8sConfig struct {
	LeaseName     string        `yaml:"lease_name"`
	LeaseDuration time.Duration `yaml:"-"`
	RenewDeadline time.Duration `yaml:"-"`
	RetryPeriod   time.Duration `yaml:"-"`

	// YAML fields to parse strings
	LeaseDurationStr string `yaml:"lease_duration"`
	RenewDeadlineStr string `yaml:"renew_deadline"`
	RetryPeriodStr   string `yaml:"retry_period"`

	LabelSelector string `yaml:"label_selector"`
	InternalPort  int    `yaml:"internal_port"`
}

type StaticNode struct {
	ID              string `yaml:"id"`
	InternalAddress string `yaml:"internal_address"`
}

type StaticConfig struct {
	Nodes       []StaticNode `yaml:"nodes"`
	ForceLeader string       `yaml:"force_leader"`
}

type ClusterConfig struct {
	Mode   string       `yaml:"mode"` // "k8s" | "static" | "single"
	Token  string       `yaml:"token"`
	K8s    K8sConfig    `yaml:"k8s"`
	Static StaticConfig `yaml:"static"`
}

type Credentials struct {
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type S3Config struct {
	Credentials []Credentials `yaml:"credentials"`
	Region      string        `yaml:"region"`
}

type TLSParams struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type TLSConfig struct {
	S3       TLSParams `yaml:"s3"`
	Internal TLSParams `yaml:"internal"`
}

type MultipartConfig struct {
	MaxPartSize     string        `yaml:"max_part_size"`
	UploadExpiry    time.Duration `yaml:"-"`
	CleanupInterval time.Duration `yaml:"-"`

	UploadExpiryStr    string `yaml:"upload_expiry"`
	CleanupIntervalStr string `yaml:"cleanup_interval"`
}

type SyncConfig struct {
	BlockWrites        bool   `yaml:"block_writes"`
	WriteBlockBehavior string `yaml:"write_block_behavior"` // "reject" | "wait"
	AllowLocalReads    bool   `yaml:"allow_local_reads"`
}

// ECConfig configures erasure-coding (Reed-Solomon) background conversion.
type ECConfig struct {
	Enabled bool `yaml:"enabled"`
	// K is the number of data shards.
	K int `yaml:"k"`
	// M is the number of parity shards.
	M int `yaml:"m"`
	// MinObjectSize is the minimum object size (in bytes) eligible for EC
	// conversion. Objects smaller than this stay as full replicas.
	MinObjectSize int64 `yaml:"min_object_size"`
	// MinAge is the minimum age of an object before it is considered for
	// conversion (prevents converting objects that are still being written
	// or are very new).
	MinAge    time.Duration `yaml:"-"`
	MinAgeStr string        `yaml:"min_age"`
	// ConvertInterval is how often the background converter scans for
	// eligible replica objects.
	ConvertInterval    time.Duration `yaml:"-"`
	ConvertIntervalStr string        `yaml:"convert_interval"`
	// RestoreInterval is how often the background restorer scans for
	// degraded EC objects that need repair.
	RestoreInterval    time.Duration `yaml:"-"`
	RestoreIntervalStr string        `yaml:"restore_interval"`
}

type HealthConfig struct {
	Interval    time.Duration `yaml:"-"`
	Timeout     time.Duration `yaml:"-"`
	MaxFailures int           `yaml:"max_failures"`

	IntervalStr string `yaml:"interval"`
	TimeoutStr  string `yaml:"timeout"`
}

type LogConfig struct {
	Level  string `yaml:"level"`  // "debug" | "info" | "warn" | "error"
	Format string `yaml:"format"` // "json" | "text"
}

type Config struct {
	Node      NodeConfig      `yaml:"node"`
	Server    ServerConfig    `yaml:"server"`
	Storage   StorageConfig   `yaml:"storage"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	S3        S3Config        `yaml:"s3"`
	TLS       TLSConfig       `yaml:"tls"`
	Multipart MultipartConfig `yaml:"multipart"`
	Sync      SyncConfig      `yaml:"sync"`
	Health    HealthConfig    `yaml:"health"`
	Log       LogConfig       `yaml:"log"`
	EC        ECConfig        `yaml:"ec"`
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			S3Listen:       ":9000",
			InternalListen: ":9001",
		},
		Storage: StorageConfig{
			Type: "fs",
			Root: "/data/micros3",
		},
		Cluster: ClusterConfig{
			Mode: "single",
			K8s: K8sConfig{
				LeaseName:        "micros3-leader",
				LeaseDurationStr: "15s",
				RenewDeadlineStr: "10s",
				RetryPeriodStr:   "2s",
				LabelSelector:    "app=micros3",
				InternalPort:     9001,
			},
			Static: StaticConfig{
				Nodes: []StaticNode{},
			},
		},
		S3: S3Config{
			Credentials: []Credentials{
				{
					AccessKey: "micros3",
					SecretKey: "micros3secret",
				},
			},
			Region: "us-east-1",
		},
		Multipart: MultipartConfig{
			MaxPartSize:        "5GB",
			UploadExpiryStr:    "24h",
			CleanupIntervalStr: "1h",
		},
		Sync: SyncConfig{
			BlockWrites:        true,
			WriteBlockBehavior: "reject",
			AllowLocalReads:    false,
		},
		Health: HealthConfig{
			IntervalStr: "5s",
			TimeoutStr:  "3s",
			MaxFailures: 3,
		},
		Log: LogConfig{
			Level:  "info",
			Format: "json",
		},
		EC: ECConfig{
			Enabled:            false,
			K:                  3,
			M:                  2,
			MinObjectSize:      1 << 20, // 1 MB
			MinAgeStr:          "10m",
			ConvertIntervalStr: "5m",
			RestoreIntervalStr: "5m",
		},
	}
}

// Load loads the configuration from a file and overrides it with environment variables
func Load(configPath string) (*Config, error) {
	cfg := DefaultConfig()

	if configPath != "" {
		file, err := os.Open(configPath)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("failed to open config file: %w", err)
			}
		} else {
			defer file.Close()
			dec := yaml.NewDecoder(file)
			if err := dec.Decode(cfg); err != nil && err != io.EOF {
				return nil, fmt.Errorf("failed to parse yaml config: %w", err)
			}
		}
	}

	cfg.OverrideWithEnv()

	if err := cfg.parseDurations(); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// OverrideWithEnv overrides configuration fields with values from environment variables
func (c *Config) OverrideWithEnv() {
	if val := os.Getenv("MICROS3_NODE_ID"); val != "" {
		c.Node.ID = val
	}
	if val := os.Getenv("MICROS3_SERVER_S3_LISTEN"); val != "" {
		c.Server.S3Listen = val
	}
	if val := os.Getenv("MICROS3_SERVER_INTERNAL_LISTEN"); val != "" {
		c.Server.InternalListen = val
	}
	if val := os.Getenv("MICROS3_STORAGE_TYPE"); val != "" {
		c.Storage.Type = val
	}
	if val := os.Getenv("MICROS3_STORAGE_ROOT"); val != "" {
		c.Storage.Root = val
	}
	if val := os.Getenv("MICROS3_STORAGE_DSN"); val != "" {
		c.Storage.DSN = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_MODE"); val != "" {
		c.Cluster.Mode = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_TOKEN"); val != "" {
		c.Cluster.Token = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_K8S_LEASE_NAME"); val != "" {
		c.Cluster.K8s.LeaseName = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_K8S_LEASE_DURATION"); val != "" {
		c.Cluster.K8s.LeaseDurationStr = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_K8S_RENEW_DEADLINE"); val != "" {
		c.Cluster.K8s.RenewDeadlineStr = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_K8S_RETRY_PERIOD"); val != "" {
		c.Cluster.K8s.RetryPeriodStr = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_K8S_LABEL_SELECTOR"); val != "" {
		c.Cluster.K8s.LabelSelector = val
	}
	if val := os.Getenv("MICROS3_CLUSTER_K8S_INTERNAL_PORT"); val != "" {
		if p, err := strconv.Atoi(val); err == nil {
			c.Cluster.K8s.InternalPort = p
		}
	}
	if val := os.Getenv("MICROS3_CLUSTER_STATIC_FORCE_LEADER"); val != "" {
		c.Cluster.Static.ForceLeader = val
	}
	if val := os.Getenv("MICROS3_S3_REGION"); val != "" {
		c.S3.Region = val
	}
	if val := os.Getenv("MICROS3_LOG_LEVEL"); val != "" {
		c.Log.Level = val
	}
	if val := os.Getenv("MICROS3_LOG_FORMAT"); val != "" {
		c.Log.Format = val
	}
	if val := os.Getenv("MICROS3_TLS_S3_ENABLED"); val != "" {
		c.TLS.S3.Enabled = strings.ToLower(val) == "true"
	}
	if val := os.Getenv("MICROS3_TLS_S3_CERT_FILE"); val != "" {
		c.TLS.S3.CertFile = val
	}
	if val := os.Getenv("MICROS3_TLS_S3_KEY_FILE"); val != "" {
		c.TLS.S3.KeyFile = val
	}
	if val := os.Getenv("MICROS3_TLS_INTERNAL_ENABLED"); val != "" {
		c.TLS.Internal.Enabled = strings.ToLower(val) == "true"
	}
	if val := os.Getenv("MICROS3_TLS_INTERNAL_CERT_FILE"); val != "" {
		c.TLS.Internal.CertFile = val
	}
	if val := os.Getenv("MICROS3_TLS_INTERNAL_KEY_FILE"); val != "" {
		c.TLS.Internal.KeyFile = val
	}

	// Dynamic credentials override support for at least index 0
	if val := os.Getenv("MICROS3_S3_CREDENTIALS_0_ACCESS_KEY"); val != "" {
		if len(c.S3.Credentials) == 0 {
			c.S3.Credentials = append(c.S3.Credentials, Credentials{})
		}
		c.S3.Credentials[0].AccessKey = val
	}
	if val := os.Getenv("MICROS3_S3_CREDENTIALS_0_SECRET_KEY"); val != "" {
		if len(c.S3.Credentials) == 0 {
			c.S3.Credentials = append(c.S3.Credentials, Credentials{})
		}
		c.S3.Credentials[0].SecretKey = val
	}

	// EC configuration overrides
	if val := os.Getenv("MICROS3_EC_ENABLED"); val != "" {
		c.EC.Enabled = strings.ToLower(val) == "true"
	}
	if val := os.Getenv("MICROS3_EC_K"); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			c.EC.K = n
		}
	}
	if val := os.Getenv("MICROS3_EC_M"); val != "" {
		if n, err := strconv.Atoi(val); err == nil {
			c.EC.M = n
		}
	}
	if val := os.Getenv("MICROS3_EC_MIN_OBJECT_SIZE"); val != "" {
		if n, err := strconv.ParseInt(val, 10, 64); err == nil {
			c.EC.MinObjectSize = n
		}
	}
	if val := os.Getenv("MICROS3_EC_MIN_AGE"); val != "" {
		c.EC.MinAgeStr = val
	}
	if val := os.Getenv("MICROS3_EC_CONVERT_INTERVAL"); val != "" {
		c.EC.ConvertIntervalStr = val
	}
	if val := os.Getenv("MICROS3_EC_RESTORE_INTERVAL"); val != "" {
		c.EC.RestoreIntervalStr = val
	}
}

func (c *Config) parseDurations() error {
	var err error
	if c.Cluster.K8s.LeaseDuration, err = time.ParseDuration(c.Cluster.K8s.LeaseDurationStr); err != nil {
		return fmt.Errorf("invalid cluster.k8s.lease_duration: %w", err)
	}
	if c.Cluster.K8s.RenewDeadline, err = time.ParseDuration(c.Cluster.K8s.RenewDeadlineStr); err != nil {
		return fmt.Errorf("invalid cluster.k8s.renew_deadline: %w", err)
	}
	if c.Cluster.K8s.RetryPeriod, err = time.ParseDuration(c.Cluster.K8s.RetryPeriodStr); err != nil {
		return fmt.Errorf("invalid cluster.k8s.retry_period: %w", err)
	}
	if c.Multipart.UploadExpiry, err = time.ParseDuration(c.Multipart.UploadExpiryStr); err != nil {
		return fmt.Errorf("invalid multipart.upload_expiry: %w", err)
	}
	if c.Multipart.CleanupInterval, err = time.ParseDuration(c.Multipart.CleanupIntervalStr); err != nil {
		return fmt.Errorf("invalid multipart.cleanup_interval: %w", err)
	}
	if c.Health.Interval, err = time.ParseDuration(c.Health.IntervalStr); err != nil {
		return fmt.Errorf("invalid health.interval: %w", err)
	}
	if c.Health.Timeout, err = time.ParseDuration(c.Health.TimeoutStr); err != nil {
		return fmt.Errorf("invalid health.timeout: %w", err)
	}
	if c.EC.MinAge, err = time.ParseDuration(c.EC.MinAgeStr); err != nil {
		return fmt.Errorf("invalid ec.min_age: %w", err)
	}
	if c.EC.ConvertInterval, err = time.ParseDuration(c.EC.ConvertIntervalStr); err != nil {
		return fmt.Errorf("invalid ec.convert_interval: %w", err)
	}
	if c.EC.RestoreInterval, err = time.ParseDuration(c.EC.RestoreIntervalStr); err != nil {
		return fmt.Errorf("invalid ec.restore_interval: %w", err)
	}
	return nil
}

func (c *Config) Validate() error {
	if c.Node.ID == "" && c.Cluster.Mode != "single" {
		// Attempt to get hostname as fallback
		hostname, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("node.id is required when cluster mode is not single")
		}
		c.Node.ID = hostname
	}

	mode := strings.ToLower(c.Cluster.Mode)
	if mode != "k8s" && mode != "static" && mode != "single" {
		return fmt.Errorf("invalid cluster.mode: %s, must be one of [k8s, static, single]", c.Cluster.Mode)
	}

	storageType := strings.ToLower(c.Storage.Type)
	if storageType == "" {
		storageType = "fs"
		c.Storage.Type = storageType
	}
	switch storageType {
	case "fs":
		if c.Storage.Root == "" {
			return fmt.Errorf("storage.root directory is required when storage.type is 'fs'")
		}
	default:
		if c.Storage.DSN == "" {
			return fmt.Errorf("storage.dsn is required when storage.type is '%s'", c.Storage.Type)
		}
	}

	c.Sync.WriteBlockBehavior = strings.ToLower(c.Sync.WriteBlockBehavior)
	if c.Sync.WriteBlockBehavior == "" {
		c.Sync.WriteBlockBehavior = "reject"
	}
	if c.Sync.WriteBlockBehavior != "reject" && c.Sync.WriteBlockBehavior != "wait" {
		return fmt.Errorf("invalid sync.write_block_behavior: %s, must be 'reject' or 'wait'", c.Sync.WriteBlockBehavior)
	}

	if c.EC.Enabled {
		if c.EC.K < 1 {
			return fmt.Errorf("invalid ec.k: must be >= 1")
		}
		if c.EC.M < 1 {
			return fmt.Errorf("invalid ec.m: must be >= 1")
		}
		if c.EC.K+c.EC.M > 256 {
			return fmt.Errorf("invalid ec.k+ec.m: must be <= 256")
		}
		if c.EC.MinObjectSize < 0 {
			return fmt.Errorf("invalid ec.min_object_size: must be >= 0")
		}
	}

	return nil
}
