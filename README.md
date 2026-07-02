# MicroS3

MicroS3 is a lightweight, high-performance distributed S3-compatible object storage server written in Go. Designed specifically for Kubernetes environments, it offers strong consistency using a two-phase commit (2PC) protocol, automatic leader election, and replica synchronization during recovery.

## Core Features
* **S3-Compatible API**: Supports operations like PutObject, GetObject, DeleteObject, Multipart Uploads, CopyObject, ListObjectsV2, and more.
* **Strong Replication (2PC)**: Two-phase commit ensures data consistency across all active replica nodes.
* **Automatic Recovery**: Reconnected or offline replicas transition to `SYNCING` state upon startup, block incoming client writes on the leader, pull missing deltas via CRC32 validation, and transition back to `READY`.
* **Background Deduplication**: Optional hardlink-based deduplication reduces storage for identical objects without changing S3 behavior.
* **Kubernetes-Native**:
  - Leader election via the standard **K8s Lease API**.
  - Node discovery via the **Endpoints API** (no external systems like Consul or etcd needed).

---

## 1. Running Standalone (For Local Development)

In this mode, the server runs as a single standalone instance without replication or external dependencies.

### Build
To build the binary:
```bash
make build
```

### Launching with Configuration
Create a simple configuration file `config.yaml`:
```yaml
node:
  id: "dev-node"
server:
  s3_listen: ":9000"
  internal_listen: ":9001"
storage:
  root: "./storage-data"
  dedup:
    enabled: false
    interval: "1h"
cluster:
  mode: "single" # Single-node mode
s3:
  credentials:
    - access_key: "admin"
      secret_key: "supersecret"
  region: "us-east-1"
log:
  level: "debug"
  format: "text"
```

And start the server:
```bash
./micros3 -config config.yaml
```

The S3 API will be available at `http://localhost:9000`. You can use any S3-compatible client (e.g., `aws-cli` or `minio-go`), pointing it to this endpoint.

### Connecting via aws-cli (No SSL)

To configure an `aws-cli` profile, run:
```bash
aws configure --profile micros3
# AWS Access Key ID [None]: admin
# AWS Secret Access Key [None]: supersecret
# Default region name [None]: us-east-1
# Default output format [None]: json
```

Once configured, you can perform bucket and object operations by specifying the `--endpoint-url` parameter:

```bash
# 1. Create a bucket
aws --endpoint-url=http://localhost:9000 s3 mb s3://test-bucket --profile micros3

# 2. Upload an object
echo "hello from cli" > hello.txt
aws --endpoint-url=http://localhost:9000 s3 cp hello.txt s3://test-bucket/hello.txt --profile micros3

# 3. List objects
aws --endpoint-url=http://localhost:9000 s3 ls s3://test-bucket --profile micros3

# 4. Download the object back
aws --endpoint-url=http://localhost:9000 s3 cp s3://test-bucket/hello.txt downloaded.txt --profile micros3
```

---

## 2. Running a Static Cluster (Static Mode)

For local multi-replica cluster testing without deploying to Kubernetes, you can use the static clustering mode. The leader is statically defined via the `force_leader` parameter.

### 3-Node Example Configuration

Create three separate configuration files.

**Node 1 (`node1.yaml` - Leader)**:
```yaml
node:
  id: "node1"
server:
  s3_listen: ":9000"
  internal_listen: ":9001"
storage:
  root: "./storage-node1"
cluster:
  mode: "static"
  token: "my-shared-cluster-token"
  static:
    force_leader: "node1" # Statically set the leader node
    nodes:
      - id: "node1"
        internal_address: "http://localhost:9001"
      - id: "node2"
        internal_address: "http://localhost:9003"
      - id: "node3"
        internal_address: "http://localhost:9005"
s3:
  credentials:
    - access_key: "admin"
      secret_key: "supersecret"
log:
  level: "info"
  format: "text"
```

**Node 2 (`node2.yaml` - Follower)**:
Set the S3 listen port to `9002`, the internal port to `9003`, and the storage directory to `./storage-node2`.

**Node 3 (`node3.yaml` - Follower)**:
Set the S3 listen port to `9004`, the internal port to `9005`, and the storage directory to `./storage-node3`.

### Starting the Nodes
Start each node in a separate terminal window:
```bash
# Terminal 1
./micros3 -config node1.yaml

# Terminal 2
./micros3 -config node2.yaml

# Terminal 3
./micros3 -config node3.yaml
```

Any S3 write requests (`PUT`/`DELETE`) sent to follower nodes (ports `9002` and `9004`) will be automatically reverse-proxied to the leader (`node1` on port `9000`), replicated via 2PC, and saved locally across all active nodes.

---

## 3. Running in Kubernetes (k8s)

In Kubernetes, cluster nodes are deployed as a `StatefulSet`. Leader election is dynamically handled using the **Kubernetes Coordination Lease API**, while replica node discovery uses a headless Service and the **Endpoints API**.

### Option A: Helm Chart (Recommended)

Add the Helm repository:
```bash
helm repo add micros3 https://paucpauc.github.io/micros3
helm repo update
```

Install with default values (3 replicas, K8s leader election):
```bash
helm install micros3 micros3/micros3
```

Customize values:
```bash
helm install micros3 micros3/micros3 \
  --set replicaCount=5 \
  --set image.repository=paucpauc/micros3 \
  --set image.tag=latest \
  --set config.s3.credentials[0].accessKey=admin \
  --set config.s3.credentials[0].secretKey=supersecret \
  --set config.cluster.token=my-cluster-token \
  --set persistence.size=10Gi
```

Or use a custom values file:
```bash
helm install micros3 micros3/micros3 -f my-values.yaml
```

The full list of configurable values:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of StatefulSet replicas | `3` |
| `image.repository` | Container image repository | `micros3` |
| `image.tag` | Container image tag | `latest` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `nameOverride` | Override chart name | `""` |
| `fullnameOverride` | Override full release name | `""` |
| `serviceAccount.create` | Create ServiceAccount | `true` |
| `serviceAccount.name` | ServiceAccount name | `micros3-sa` |
| `rbac.create` | Create Role and RoleBinding | `true` |
| `config.node.id` | Node ID (overridden by Downward API) | `"placeholder"` |
| `config.server.s3Listen` | S3 API listen address | `":9000"` |
| `config.server.internalListen` | Internal replication listen address | `":9001"` |
| `config.storage.root` | Data storage directory | `"/data"` |
| `config.storage.dedup.enabled` | Enable deduplication | `false` |
| `config.storage.dedup.interval` | Deduplication cycle interval | `"1h"` |
| `config.cluster.mode` | Cluster mode (`k8s`/`static`/`single`) | `"k8s"` |
| `config.cluster.token` | Shared cluster auth token | `"secret-cluster-token"` |
| `config.cluster.k8s.leaseName` | K8s Lease name for leader election | `"micros3-leader-lease"` |
| `config.cluster.k8s.leaseDuration` | Lease duration | `"15s"` |
| `config.cluster.k8s.renewDeadline` | Lease renew deadline | `"10s"` |
| `config.cluster.k8s.retryPeriod` | Lease retry period | `"2s"` |
| `config.cluster.k8s.internalPort` | Internal port for K8s discovery | `9001` |
| `config.s3.credentials` | List of S3 access/secret key pairs | see defaults |
| `config.s3.region` | S3 region | `"us-east-1"` |
| `config.health.interval` | Health check interval | `"5s"` |
| `config.health.timeout` | Health check timeout | `"2s"` |
| `config.health.maxFailures` | Max consecutive health check failures | `3` |
| `config.sync.blockWrites` | Block writes during sync | `true` |
| `config.sync.writeBlockBehavior` | Write block behavior (`wait`/`reject`) | `"wait"` |
| `config.sync.allowLocalReads` | Allow local reads without proxying | `true` |
| `config.log.level` | Log level (`debug`/`info`/`warn`/`error`) | `"debug"` |
| `config.log.format` | Log format (`json`/`text`) | `"text"` |
| `service.headless.s3.port` | Headless Service S3 port | `9000` |
| `service.headless.internal.port` | Headless Service internal port | `9001` |
| `service.leader.enabled` | Create leader NodePort Service | `true` |
| `service.leader.type` | Leader Service type | `ClusterIP` |
| `service.leader.s3.port` | Leader Service S3 port | `9000` |
| `service.readOnly.enabled` | Create read-only Service | `true` |
| `service.readOnly.type` | Read-only Service type | `ClusterIP` |
| `service.readOnly.s3.port` | Read-only Service S3 port | `9000` |
| `persistence.enabled` | Enable PVC for data | `true` |
| `persistence.accessMode` | PVC access mode | `ReadWriteOnce` |
| `persistence.size` | PVC size | `2Gi` |
| `persistence.storageClassName` | Storage class name | `""` |
| `podMonitor.enabled` | Create PodMonitor for Prometheus | `true` |
| `podMonitor.interval` | Scrape interval | `30s` |
| `podMonitor.scrapeTimeout` | Scrape timeout | `10s` |
| `probe.readiness.httpGet.path` | Readiness probe path | `/health` |
| `probe.readiness.initialDelaySeconds` | Readiness initial delay | `5` |
| `probe.readiness.periodSeconds` | Readiness period | `15` |
| `probe.readiness.timeoutSeconds` | Readiness timeout | `10` |
| `probe.liveness.httpGet.path` | Liveness probe path | `/liveness` |
| `probe.liveness.initialDelaySeconds` | Liveness initial delay | `10` |
| `probe.liveness.periodSeconds` | Liveness period | `25` |
| `probe.liveness.timeoutSeconds` | Liveness timeout | `15` |
| `resources` | Container resource requests/limits | `{}` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |

### Option B: Raw Manifests

All deployment manifests are located in the `deploy/` directory.

#### Step 1. Build the Docker Image
```bash
docker build -t micros3:latest .
```

> **Note**: For local Kubernetes clusters (e.g. minikube / k3d), import the image into the cluster container registry:
> ```bash
> minikube image load micros3:latest
> ```

#### Step 2. Deploy RBAC Roles
```bash
kubectl apply -f deploy/rbac.yaml
```

#### Step 3. Create the ConfigMap and Headless Service
The headless Service provides stable network identities for StatefulSet replicas (e.g. `micros3-0.micros3.default.svc.cluster.local`).
```bash
kubectl apply -f deploy/configmap.yaml
kubectl apply -f deploy/service.yaml
```

#### Step 4. Deploy the StatefulSet
```bash
kubectl apply -f deploy/statefulset.yaml
```

Pods will run as:
- `micros3-0`
- `micros3-1`
- `micros3-2`

Each pod discovers its identity using the `MICROS3_NODE_ID` environment variable (injected via the Downward API). The node that successfully acquires the `micros3-leader-lease` Lease becomes the leader; the remaining nodes connect to it as followers and synchronize.

---

## Monitoring and Metrics

Each MicroS3 node exposes endpoints on its main S3 API port:
* **`/health`**: Returns `{"status":"OK"}`. Used for K8s `livenessProbe` and `readinessProbe`.
* **`/liveness`**: Returns HTTP 200. Lightweight liveness check.
* **`/metrics`**: Exports Prometheus-format metrics:
  - `micros3_requests_total{method,bucket,code}` — Total S3 API request count by HTTP method, bucket, and status code.
  - `micros3_request_duration_seconds{method,bucket}` — Histogram of S3 API request durations.
  - `micros3_bytes_written_total{method,bucket}` — Total bytes written (PUT/POST) per method and bucket.
  - `micros3_bytes_read_total{method,bucket}` — Total bytes read (GET/HEAD) per method and bucket.
  - `micros3_objects_total{bucket}` — Number of objects per bucket.
  - `micros3_storage_used_bytes{bucket}` — Storage used per bucket (in bytes).
  - `micros3_buckets_total` — Total number of buckets.
  - `micros3_cluster_role` — Node role (1=leader, 0=follower).
  - `micros3_cluster_status{status}` — Node status (1 for the current status: OFFLINE, SYNCING, READY, ERROR).
  - `micros3_writes_blocked` — Whether writes are blocked by sync lease (1=blocked).
  - `micros3_active_writes` — Number of in-flight write transactions.
  - `micros3_replication_prepare_total{result}` — 2PC prepare attempts (result=success/fail).
  - `micros3_replication_commit_total{result}` — 2PC commit attempts (result=success/fail).
  - `micros3_replication_abort_total{result}` — 2PC aborts (result=prepare_failed/local_commit_failed).
  - `micros3_sync_lease_active` — Whether a sync lease is currently active.
  - `micros3_proxy_requests_total{method}` — Requests proxied to leader.
  - `micros3_multipart_uploads_active` — Number of active multipart uploads.
  - `micros3_dedup_total{result}` — Deduplication cycle results (result=success/error).
  - `micros3_dedup_links_created` — Total hardlinks created by deduplication.
  - `micros3_dedup_space_saved_bytes` — Storage space saved by deduplication (in bytes).

A `PodMonitor` for Prometheus Operator is included in both the Helm chart and the raw manifests (`deploy/podmonitor.yaml`).

Retrieve metrics using curl:
```bash
curl http://localhost:9000/metrics
```
