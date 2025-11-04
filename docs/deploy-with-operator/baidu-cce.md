# Deploy EloqDoc on Baidu Cloud CCE with the Operator

This guide walks you through deploying EloqDoc on Baidu Cloud CCE using the Eloq Operator. 

## Prerequisites
- Baidu Cloud account with permissions to create CCE clusters, CCR (Container Registry) namespaces, and BOS (Object Storage) buckets
- kubectl installed and configured locally (v1.28+ recommended)
- helm installed (v3+)

## Quick overview (high level steps)
1. Create a CCE cluster.
2. Create a node group using instances with local SSD (for local RocksDB storage), e.g. `bcc.l5d.c8m32.1d`, and use an Ubuntu 24.04 image.
3. Push all required images to Baidu CCR (enable CCR service first). Use CCR-hosted image tags when installing cert-manager, OpenEBS, and the Eloq operator.
4. Create BOS (S3 compatible) AK/SK credentials and store them as a Kubernetes secret.
5. Apply the EloqDoc CustomResource (CR) to create the cluster.

## Step 1 — Create the CCE cluster
1. Create a cluster via the Baidu Cloud Console or the CLI. Configure VPC/subnets, cluster network plugin, and choose the Kubernetes version compatible with the operator.
2. **Install required CCE components** during cluster creation:
   - **CCE CSI CDS Plugin**: Required for managing Baidu Cloud Disk (CDS) volumes, equivalent to AWS EBS. See [CCE CSI CDS Plugin Documentation](https://cloud.baidu.com/doc/CCE/s/Llc7917cx) for details.
   - **CCE Credential Controller**: Enables password-free image pulling from associated CCR (Container Registry). See [CCE Credential Controller Documentation](https://cloud.baidu.com/doc/CCE/s/4m0kru8g5) for details.
3. Ensure the cluster control plane can reach CCR and BOS endpoints for image pulls and object storage access.

## Step 2 — Create node group with local SSD
1. Create a node group (CCE instance group) and choose instances with local SSD (example: `bcc.l5d.c8m32.1d`).
2. Select an Ubuntu 24.04 image for nodes.
3. **Configure post-deployment script** to prepare local SSDs on each node:
   - In the CCE node group settings, find the "Post-deployment script" or "User data" section
   - Use the script below to automatically format and mount local SSDs with XFS filesystem (with quota support)
   - The script will run on each node after it joins the cluster
4. Add node labels used by the operator schedulePolicy (for example: `instance-group-id: <your-ig-id>`).

### Node Setup Script

This script will:
- Detect and wait for local SSD disks to appear
- Format the disk with XFS filesystem (with quota support enabled)
- Mount the disk to `/mnt/xfs-quota`
- Add the mount to `/etc/fstab` for persistence across reboots
- Configure containerd for io_uring support (requires `LimitMEMLOCK=infinity`)

**Usage in CCE**:
1. Copy the adapted script above
2. In CCE console, when creating the node group, paste this script into the "Post-deployment script" field
3. The script will run automatically on each node after it boots and joins the cluster


```bash
#!/bin/bash

# Robust EC2 data-disk setup + mount
# - Waits for non-root, unmounted block device >= MIN_BYTES
# - Accepts nvme/xvd/sd (Nitro and non-Nitro)
# - Idempotent: skips mkfs if filesystem exists,
#   skips fstab duplicates, etc.

set -euo pipefail

###########################################################################
# Configuration
###########################################################################

CONTAINER_RUNTIME="containerd"

# Minimum size to qualify as "data disk" (default 800 GiB)
MIN_BYTES=$((800 * 1024 * 1024 * 1024))

# Where to mount the data disk
MNT_DIR="/mnt/xfs-quota"

# Filesystem and mount options
FS_TYPE="xfs"
FS_OPTS="defaults,uquota,pquota,discard"

# run with DEBUG=1 for verbose logs
DEBUG=${DEBUG:-0}
RETRIES="${RETRIES:-60}"
SLEEP_SECONDS="${SLEEP_SECONDS:-2}"

###########################################################################
# Helper: print log lines with timestamp
###########################################################################

log() {
  printf '[%s] %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

[[ $DEBUG -eq 1 ]] && set -x

###########################################################################
# Helper: find root disk (e.g., nvme0n1) so we can exclude it
###########################################################################
get_root_disk() {
  df --output=source / | tail -n1 | xargs lsblk -no PKNAME
}

###########################################################################
# Helper: wait for a suitable data disk to appear
# Criteria:
#   - block device (TYPE=disk)
#   - not the root disk (and not a partition of it)
#   - unmounted
#   - name starts with nvme/xvd/sd
#   - size >= MIN_BYTES
# Returns /dev/<name> to stdout
###########################################################################
wait_for_data_disk() {
  local root="$1" min="$2" tries="$3" sleep_s="$4"

  for ((i=1; i<=tries; i++)); do
    while read -r name size type mnt pk; do
      # Skip if not a disk device
      [[ "$type" != "disk" ]] && continue
      # Skip the root disk itself
      [[ "$name" == "$root" ]] && continue
      # Skip mounted devices
      [[ -n "$mnt" ]] && continue
      # Accept common device name prefixes
      [[ "$name" =~ ^(nvme|xvd|sd) ]] || continue
      # Enforce minimum size
      if (( size >= min )); then
        echo "/dev/$name"
        return 0
      fi
    done < <(lsblk -b -dn -o NAME,SIZE,TYPE,MOUNTPOINT,PKNAME)

    log "Waiting for data disk to appear ($i/$tries)..."
    sudo udevadm settle || true
    sleep "$sleep_s"
  done

  return 1
}

###########################################################################
# Helper: if the disk has partitions, prefer the first partition node
###########################################################################
pick_target_node() {
  local dev_path="$1"
  local base part
  base="$(basename "$dev_path")"
  # Find the first partition whose PKNAME equals the base device
  part="$(lsblk -nr -o NAME,TYPE,PKNAME | awk -v d="$base" '$2=="part" && $3==d{print $1; exit}')"
  if [[ -n "$part" ]]; then
    echo "/dev/$part"
  else
    echo "$dev_path"
  fi
}

###########################################################################
# 1. Detect root disk
###########################################################################
ROOT_DISK="$(get_root_disk)"
if [[ -z "${ROOT_DISK:-}" ]]; then
  log "ERROR: failed to detect root disk"
  lsblk -b -o NAME,SIZE,TYPE,MOUNTPOINT,PKNAME
  exit 1
fi
log "Root disk   : $ROOT_DISK"

###########################################################################
# 2. Find candidate data disks (wait for attachment/udev)
###########################################################################
DATA_DISK="$(wait_for_data_disk "$ROOT_DISK" "$MIN_BYTES" "$RETRIES" "$SLEEP_SECONDS")" || {
  log "ERROR: no unmounted data disk ≥ $((MIN_BYTES / 1024 / 1024 / 1024)) GiB found after waiting"
  log "lsblk snapshot:"
  lsblk -b -o NAME,SIZE,TYPE,MOUNTPOINT,PKNAME
  exit 1
}

log "Selected disk: ${DATA_DISK}"

###########################################################################
# 3. If a partition exists, prefer it (avoids clobbering existing partition tables)
###########################################################################
TARGET_NODE="$(pick_target_node "$DATA_DISK")"
[[ "$TARGET_NODE" != "$DATA_DISK" ]] && log "Using partition node: $TARGET_NODE"

###########################################################################
# 4. Install xfsprogs if needed (Ubuntu 24.04 doesn't include it by default)
###########################################################################
if ! command -v mkfs.xfs &>/dev/null; then
  log "Installing xfsprogs package..."
  sudo apt-get update -qq
  sudo apt-get install -y xfsprogs
fi

###########################################################################
# 5. Create filesystem if missing (idempotent)
###########################################################################
FSTYPE="$(lsblk -no FSTYPE "$TARGET_NODE" || true)"
if [[ -z "${FSTYPE}" ]]; then
  log "No filesystem detected on ${TARGET_NODE}; creating ${FS_TYPE}"
  sudo mkfs."${FS_TYPE}" -f "${TARGET_NODE}"
else
  log "Filesystem ${FSTYPE} already exists on ${TARGET_NODE}; skipping mkfs"
fi

###########################################################################
# 6. Resolve UUID with retries
###########################################################################
UUID=""
for _ in {1..10}; do
  UUID="$(lsblk -no UUID "${TARGET_NODE}" || true)"
  [[ -n "${UUID}" ]] && break
  sleep 1
done

if [[ -z "${UUID}" ]]; then
  log "ERROR: failed to read UUID for ${TARGET_NODE}"
  exit 1
fi
log "Detected UUID : ${UUID}"

###########################################################################
# 7. Mount and persist in /etc/fstab (idempotent)
###########################################################################
sudo mkdir -p "${MNT_DIR}"

if ! mountpoint -q "${MNT_DIR}"; then
  log "Mounting ${TARGET_NODE} on ${MNT_DIR}"
  sudo mount -o "${FS_OPTS}" "UUID=${UUID}" "${MNT_DIR}"
else
  log "${MNT_DIR} already mounted"
fi

if ! grep -q "UUID=${UUID}[[:space:]]\+${MNT_DIR}[[:space:]]" /etc/fstab; then
  log "Adding entry to /etc/fstab"
  # Use tee to ensure sudo applies to the file write
  echo "UUID=${UUID}  ${MNT_DIR}  ${FS_TYPE}  ${FS_OPTS},nofail  0  2" | sudo tee -a /etc/fstab >/dev/null
else
  log "UUID already present in /etc/fstab; skipping"
fi

###########################################################################
# 8. Configure containerd for io_uring (requires LimitMEMLOCK=infinity)
###########################################################################
log "Configuring containerd for io_uring support"
# Create a systemd override directory for the containerd service
sudo mkdir -p /etc/systemd/system/containerd.service.d

# Create the override configuration file, directly setting LimitMEMLOCK to infinity for the service. (For io_uring)
cat <<EOF | sudo tee /etc/systemd/system/containerd.service.d/override.conf > /dev/null
[Service]
LimitMEMLOCK=infinity
EOF

# Reload the systemd configuration and restart the containerd service to apply the change
sudo systemctl daemon-reload
sudo systemctl restart containerd

log "Done."
```

## Step 3 — Enable CCR and push images

> **Important**: CCE clusters may not be able to directly pull images from public registries (Docker Hub, Quay.io, gcr.io, etc.). Therefore, you must mirror all required images to Baidu CCR (private registry) first, then use custom Helm values to install cert-manager, OpenEBS, and the Eloq operator with CCR-hosted images.

### 3.1 Enable CCR service and configure access
1. Enable CCR (Container Registry) service in Baidu Cloud and create a namespace/repository.
2. Configure CCR access restrictions to allow connectivity:
   - **VPC access**: Enable VPC access restriction to allow CCE cluster to pull images via private network. See [CCR Access Restriction Documentation](https://cloud.baidu.com/doc/CCR/s/4kwdm4oxv) for details.
   - **Public network access** (optional): If you need to push images from your local machine via public network, also enable public network access restriction.

### 3.2 Get CCR registry endpoints
After enabling CCR, you will have two registry endpoints:
- **VPC endpoint** (for CCE cluster): `ccr-<instance-id>-vpc.cnc.<region>.baidubce.com`
- **Public endpoint** (for pushing images): `ccr-<instance-id>.cnc.<region>.baidubce.com`

Replace the following placeholders in this guide:
- `<CCR_VPC_ENDPOINT>`: Your VPC registry endpoint (e.g., `ccr-xxxxx-vpc.cnc.bj.baidubce.com`)
- `<CCR_NAMESPACE>`: Your CCR namespace (e.g., `default`)

### 3.3 Push images to CCR
Mirror the following image groups into CCR and tag them with your CCR registry prefix.

#### Required Images

The table below lists all images that need to be pushed to your private CCR registry:

| Category          | Source Image                                                     | Target Name in CCR                     | Notes                   |
| ----------------- | ---------------------------------------------------------------- | -------------------------------------- | ----------------------- |
| **cert-manager**  | `quay.io/jetstack/cert-manager-controller:v1.19.0`               | `cert-manager-controller:v1.19.0`      | Controller component    |
|                   | `quay.io/jetstack/cert-manager-webhook:v1.19.0`                  | `cert-manager-webhook:v1.19.0`         | Webhook component       |
|                   | `quay.io/jetstack/cert-manager-cainjector:v1.19.0`               | `cert-manager-cainjector:v1.19.0`      | CA injector component   |
|                   | `quay.io/jetstack/cert-manager-startupapicheck:v1.19.0`          | `cert-manager-startupapicheck:v1.19.0` | Startup API check       |
| **OpenEBS**       | `docker.io/grafana/alloy:v1.8.1`                                 | `alloy:v1.8.1`                         | Alloy component         |
|                   | `openebs/provisioner-localpv:4.3.0`                              | `provisioner-localpv:4.3.0`            | Local PV provisioner    |
|                   | `openebs/linux-utils:4.2.0`                                      | `linux-utils:4.2.0`                    | Linux utilities         |
|                   | `quay.io/prometheus-operator/prometheus-config-reloader:v0.81.0` | `prometheus-config-reloader:v0.81.0`   | Config reloader         |
|                   | `docker.io/openebs/kubectl:1.25.15`                              | `kubectl:1.25.15`                      | Kubectl utility         |
|                   | `docker.io/grafana/loki:3.4.2`                                   | `loki:3.4.2`                           | Loki (optional)         |
|                   | `kiwigrid/k8s-sidecar:1.30.2`                                    | `k8s-sidecar:1.30.2`                   | K8s sidecar (optional)  |
|                   | `quay.io/minio/minio:RELEASE.2024-12-18T13-15-44Z`               | `minio:RELEASE.2024-12-18T13-15-44Z`   | MinIO (optional)        |
|                   | `quay.io/minio/mc:RELEASE.2024-11-21T17-21-54Z`                  | `mc:RELEASE.2024-11-21T17-21-54Z`      | MinIO client (optional) |
| **Eloq Operator** | `eloqdata/eloq-operator:lz-20251104-v1`                          | `eloq-operator:lz-20251104-v1`         | Operator controller     |
|                   | `quay.io/brancz/kube-rbac-proxy:v0.13.1`                         | `kube-rbac-proxy:v0.13.1`              | RBAC proxy              |
| **EloqDoc**       | `eloqdata/eloqdoc-rocks-cloud:release-0.2.6-new`                 | `eloqdoc-rocks-cloud:0.2.6`            | EloqDoc runtime         |

**Notes**:
- Images marked as "optional" are for features like logging (Loki) or S3-compatible storage (MinIO) that you may not need
- All images must be pushed to your CCR namespace (e.g., `default`)
- Use the VPC endpoint in Helm values for CCE cluster to pull images


When installing Helm charts, you will override default image repositories to use the **VPC endpoint** (`<CCR_VPC_ENDPOINT>`) so that CCE nodes can pull images via private network.

## Step 4 — Install required components (Helm)
Install cert-manager, OpenEBS (or your chosen PV provider), and the Eloq operator. For each Helm install you will provide value overrides to use CCR images.

> **Note**: The values files referenced in this section (`cert-manager-values.yaml`, `openebs-values.yaml`, and `operator-values.yaml`) are available in the same directory as this guide.

### 4.1 Install cert-manager
Add the Jetstack Helm repository and install cert-manager using the custom values file that points to CCR images.

```bash
# Add Jetstack Helm repository
helm repo add jetstack https://charts.jetstack.io
helm repo update

# Install cert-manager with custom values
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --create-namespace \
  --version v1.19.0 \
  -f cert-manager-values.yaml

# Verify installation
kubectl get pods -n cert-manager
```

The `cert-manager-values.yaml` file should contain:
```yaml
# cert-manager-values.yaml
image:
  repository: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/cert-manager-controller
  tag: v1.19.0
  pullPolicy: IfNotPresent

webhook:
  image:
    repository: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/cert-manager-webhook
    tag: v1.19.0
    pullPolicy: IfNotPresent

cainjector:
  image:
    repository: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/cert-manager-cainjector
    tag: v1.19.0
    pullPolicy: IfNotPresent

startupapicheck:
  image:
    repository: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/cert-manager-startupapicheck
    tag: v1.19.0
    pullPolicy: IfNotPresent

# Install CRDs
installCRDs: true
```

### 4.2 Install OpenEBS
Add the OpenEBS Helm repository and install OpenEBS using the custom values file for local PV provisioning.

```bash
# Add OpenEBS Helm repository
helm repo add openebs https://openebs.github.io/openebs
helm repo update

# Install OpenEBS with custom values
helm install openebs openebs/openebs \
  --namespace openebs \
  --create-namespace \
  --version 4.3.0 \
  -f openebs-values.yaml

# Verify installation
kubectl get pods -n openebs
```

The `openebs-values.yaml` file should contain:
```yaml
# openebs-values.yaml
# OpenEBS configuration for Baidu Cloud CCE using internal registry

# Disable Alloy to avoid insufficient IP resources
alloy:
  enabled: false
  image:
    registry: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>
    repository: alloy
    tag: "v1.8.1"
  
  configReloader:
    image:
      registry: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>
      repository: prometheus-config-reloader
      tag: "v0.81.0"

# Disable unnecessary storage engines
engines:
  local:
    lvm:
      enabled: false
    zfs:
      enabled: false
  replicated:
    mayastor:
      enabled: false

# LocalPV Provisioner configuration
localpv-provisioner:
  localpv:
    image:
      registry: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>
      repository: provisioner-localpv
      tag: "4.3.0"
    resources: {}
  
  helperPod:
    image:
      registry: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>
      repository: linux-utils
      tag: "4.2.0"
    resources: {}

# Disable Loki to reduce resource usage
loki:
  enabled: false

# kubectl pre-upgrade hook
preUpgradeHook:
  image:
    registry: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>
    repo: kubectl
    tag: "1.25.15"
```

## Step 5 — Install the Eloq Operator
Install the operator via Helm using the local chart tarball and custom values file.

```bash
# Install operator from local tarball with custom values
helm install eloq-operator \
  eloq-operator-2.1.8.tgz \
  --namespace eloq-operator-system \
  --create-namespace \
  -f operator-values.yaml

# Verify installation
kubectl get pods -n eloq-operator-system

# Check operator logs
kubectl logs -l control-plane=controller-manager -n eloq-operator-system -f
```

The `operator-values.yaml` file should contain:
```yaml
# EloQ Operator configuration for Baidu Cloud CCE using internal registry

# K8s distribution type for Baidu Cloud CCE
k8sDistribution: "cce"

controllerManager:
  image:
    repository: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/eloq-operator
    tag: lz-20251105-v1
  imagePullPolicy: IfNotPresent
  
  # Configure kube-rbac-proxy image
  kubeRbacProxy:
    image:
      repository: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/kube-rbac-proxy
      tag: v0.13.1
    resources:
      limits:
        cpu: 200m
        memory: 256Mi
      requests:
        cpu: 100m
        memory: 128Mi
  
  # Resource requests
  resources:
    requests:
      cpu: 200m
      memory: 512Mi
    limits:
      cpu: 500m
      memory: 512Mi

# Disable cert-manager (already installed separately)
cert-manager:
  enabled: false
```

Verify the operator is running and CRD `EloqDBCluster` is installed:
```bash
kubectl get crd eloqdbclusters.eloqdbcluster.eloqdata.com
```

## Step 6 — Create BOS credentials and Kubernetes secret
1. Create an AK/SK pair in Baidu Cloud for BOS access.
2. Create a Kubernetes secret containing the AK/SK, following the format accepted by the operator.

Example secret creation:
```bash
kubectl create secret generic aws-credentials \
  --from-literal=AWS_ACCESS_KEY_ID=<YOUR_BAIDU_AK> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<YOUR_BAIDU_SK> \
  --namespace default
```

**Notes**:
- The secret name `aws-credentials` is referenced in the CR's `spec.awsSecretName` field. You can use any name you prefer, but make sure it matches in both places.
- The keys `awsAccessKeyId` and `awsSecretAccessKey` are the standard keys expected by the operator for S3-compatible storage (BOS is S3-compatible).
- Replace `<YOUR_BAIDU_AK>` and `<YOUR_BAIDU_SK>` with your actual Baidu Cloud Access Key and Secret Key.
- BOS endpoints are typically `https://s3.<region>.bcebos.com` (e.g., `https://s3.bj.bcebos.com` for Beijing region).

## Step 7 — Apply the EloqDoc CustomResource

### 7.1 Understanding BOS Bucket Configuration

EloqDoc uses BOS (Baidu Object Storage) for persistent storage with the following configuration parameters:

- **`txLogBucketName`**: Base name for the transaction log bucket
- **`objectStoreBucketName`**: Base name for the object store bucket (can be the same as `txLogBucketName`)
- **`bucketPrefix`**: Prefix that will be prepended to bucket names
- **`txLogObjectPath`**: Path prefix for transaction logs within the bucket
- **`objectStoreObjectPath`**: Path prefix for object store data within the bucket
- **`region`**: Baidu Cloud region where buckets will be created (e.g., `bj`, `gz`, `su`)
- **`endpointUrl`**: BOS endpoint URL for the specified region

**Bucket Naming Convention:**

The actual BOS bucket names are formed by combining the prefix and base name:
```
Actual Bucket Name = bucketPrefix + bucketName
```

For example, with the configuration:
```yaml
bucketPrefix: eloqdoc-
txLogBucketName: my-cluster-data
objectStoreBucketName: my-cluster-data
txLogObjectPath: eloqdoc-rocksdb-s3-log
objectStoreObjectPath: eloqdoc-rocksdb-s3-store
region: bj
endpointUrl: "https://s3.bj.bcebos.com"
```

The created bucket will be:
- Bucket name: `eloqdoc-my-cluster-data`

Within this bucket, data is organized by paths:
- Transaction logs: `bos://eloqdoc-my-cluster-data/eloqdoc-rocksdb-s3-log/`
- Object store data: `bos://eloqdoc-my-cluster-data/eloqdoc-rocksdb-s3-store/`

> **Tip:** You can use the same bucket for both transaction logs and object store data. The different paths ensure proper data separation and organization.

**BOS Endpoints by Region:**

Common Baidu Cloud regions and their BOS endpoints:
- Beijing (bj): `https://s3.bj.bcebos.com`
- Guangzhou (gz): `https://s3.gz.bcebos.com`
- Suzhou (su): `https://s3.su.bcebos.com`
- Hong Kong (hkg): `https://s3.hkg.bcebos.com`

**Automatic Bucket Creation:**

EloqDoc will automatically create the BOS buckets if they don't exist. Ensure your AK/SK has permissions to create buckets.

**Important Bucket Naming Rules:**
- Must be globally unique across all Baidu Cloud accounts
- Must be between 3-63 characters long
- Can contain only lowercase letters, numbers, and hyphens
- Must start and end with a letter or number
- Cannot contain consecutive hyphens

### 7.2 Create EloqDoc Cluster Configuration

Below is the CR example (use CCR image references, AK/SK secret name, and correct BOS endpoint/region values). Edit fields such as `image`, `schedulePolicy`, `storageClassDiskType`, and bucket names before applying.

**Key fields to customize**:
- `spec.awsSecretName`: Must match the secret name created in Step 6 (e.g., `aws-credentials`)
- `spec.tx.image`: Use your CCR VPC endpoint and namespace
- `spec.tx.schedulePolicy.labelSelector`: Use your node group ID
- `spec.store.rocksdbCloud.cloudObjectStorage`: Configure BOS bucket names, region, and endpoint

```yaml
apiVersion: eloqdbcluster.eloqdata.com/v1alpha1
kind: EloqDBCluster
metadata:
  name: eloqdoc-rocksdbcloud-s3
  namespace: default
spec:
  clusterDeployMode: txWithInternalLog
  awsSecretName: aws-credentials  # Must match the secret name created in Step 6
  frontend:
    module: "eloqdoc"
    port: 27017
    config:
      operation: upsert
      rawConfig: |
        # MongoDB configuration file for eloqdoc
        systemLog:
          verbosity: 0
  tx:
    exposedService: true
    replica: 1
    resources:
      requests:
        memory: "1Gi"
        cpu: "500m"
      limits:
        memory: "1Gi"
        cpu: "500m"
    keySpaceName: test
    image: <CCR_VPC_ENDPOINT>/<CCR_NAMESPACE>/eloqdoc-rocks-cloud:0.2.6
    imagePullPolicy: Always
    schedulePolicy:
      policyType: required
      preferredZone: zoneD
      labelSelector:
        matchExpressions:
          - key: instance-group-id
            operator: "In"
            values:
              - <YOUR_NODE_GROUP_ID>  # Replace with your CCE node group ID (e.g., cce-ig-xxxxx)
    storageClassDiskType: hp1
    dataStore:
      ephemeral:
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 5Gi
            limits:
              storage: 10Gi
      pvc:
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 5Gi
            limits:
              storage: 10Gi
          volumeMode: Filesystem
  store:
    storageType: objectStorage
    rocksdbCloud:
      sstFileCacheSize: 2Gi
      readyTimeout: 10
      fileDeletionDelay: 3600
      cloudObjectStorage:
        cloudStoreType: s3
        txLogBucketName: test  # Base name for transaction log bucket
        objectStoreBucketName: test  # Base name for object store bucket (can be same as txLogBucketName)
        bucketPrefix: eloqdoc-  # Prefix prepended to bucket names (actual bucket: eloqdoc-test)
        region: bj  # Baidu Cloud region code (bj=Beijing, gz=Guangzhou, su=Suzhou, etc.)
        txLogObjectPath: eloqdoc-rocksdb-s3-log  # Path for transaction logs within bucket
        objectStoreObjectPath: eloqdoc-rocksdb-s3-store  # Path for object store data within bucket
        endpointUrl: "https://s3.bj.bcebos.com"  # BOS endpoint URL for the region

```

Apply the CR and watch the operator create resources:

```bash
kubectl apply -f eloqdoc-cluster.yaml
kubectl -n default get eloqdbcluster eloqdoc-rocksdbcloud-s3 -o yaml
```

### 7.3 Retrieve Admin Credentials

After deployment, the operator creates a secret with admin credentials.

```bash
# View the secret
kubectl get secret eloqdoc-rocksdbcloud-s3-admin-user -n default -o yaml

# Extract username
export ELOQDOC_USERNAME=$(kubectl get secret eloqdoc-rocksdbcloud-s3-admin-user -n default -o jsonpath='{.data.username}' | base64 -d)

# Extract password
export ELOQDOC_PASSWORD=$(kubectl get secret eloqdoc-rocksdbcloud-s3-admin-user -n default -o jsonpath='{.data.password}' | base64 -d)

# Display credentials
echo "Username: $ELOQDOC_USERNAME"
echo "Password: $ELOQDOC_PASSWORD"
```

## Step 8 — Test and verify

### 8.1 Create a Test Pod

Deploy a MongoDB shell pod for testing:

```yaml
# mongosh-test.yaml
apiVersion: v1
kind: Pod
metadata:
  name: mongosh-test
  namespace: default
spec:
  containers:
  - name: mongosh
    image: mongo:5.0
    command:
      - sleep
      - "3600"
    resources:
      requests:
        memory: "128Mi"
        cpu: "100m"
      limits:
        memory: "256Mi"
        cpu: "200m"
  restartPolicy: Never
```

```bash
# Deploy the test pod
kubectl apply -f mongosh-test.yaml

# Wait for the pod to be ready
kubectl wait --for=condition=Ready pod/mongosh-test -n default --timeout=60s
```

### 8.2 Connect to EloqDoc

#### Option 1: Internal Connection (ClusterIP Service)

Connect from within the cluster using the internal service:

```bash
# Exec into the mongosh pod
kubectl exec -it mongosh-test -n default -- bash

# Inside the pod, connect to EloqDoc
mongosh "mongodb://$ELOQDOC_USERNAME:$ELOQDOC_PASSWORD@eloq-srv-tx-eloqdoc-rocksdbcloud-s3.default.svc.cluster.local:27017"

# Test basic operations
use testdb
db.testcol.insertOne({name: "test", value: 123})
db.testcol.find()
```

#### Option 2: External Connection (LoadBalancer Service)

To connect from outside the cluster, the service should be exposed via a LoadBalancer (configured by `spec.tx.exposedService: true` in the CR):

```bash
# Get the LoadBalancer IP or hostname
export LB_ADDRESS=$(kubectl get service eloq-srv-tx-eloqdoc-rocksdbcloud-s3-exposed -n default -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

echo "LoadBalancer Address: $LB_ADDRESS"

# Wait for the LoadBalancer to be provisioned
kubectl wait --for=jsonpath='{.status.loadBalancer.ingress}' \
  service/eloq-srv-tx-eloqdoc-rocksdbcloud-s3-exposed \
  -n default --timeout=300s

# Connect from your local machine (if mongosh is installed locally)
mongosh "mongodb://$ELOQDOC_USERNAME:$ELOQDOC_PASSWORD@$LB_ADDRESS:27017"
```

> **Security Note:** Exposing EloqDoc via a public LoadBalancer makes it accessible from the internet. Consider:
> - Using security groups or Baidu Cloud SLB access control to restrict access to specific IP addresses
> - Implementing network policies
> - Using a VPN or private network connection for production environments

### 8.3 Verify BOS Storage

Check that data is being written to BOS buckets:

```bash
# List BOS buckets (if you have Baidu Cloud CLI configured)
# Or check in Baidu Cloud Console under BOS service

# Verify transaction logs are being written
# Check bucket: eloqdoc-test
# Path: eloqdoc-rocksdb-s3-log/

# Verify object store data
# Check bucket: eloqdoc-test
# Path: eloqdoc-rocksdb-s3-store/
```

### 8.4 Monitor Deployment

```bash
# Check pod status
kubectl get pods -n default -l app=eloqdoc-rocksdbcloud-s3

# Check pod logs
kubectl logs -n default -l app=eloqdoc-rocksdbcloud-s3 -f

# Check events
kubectl get events -n default --sort-by='.lastTimestamp'

# Verify pods are scheduled on correct nodes with local SSDs
kubectl get pods -n default -o wide
```

## Step 9 — Cleanup

Remove the EloqDoc deployment and associated resources when done:

```bash
# Delete the EloqDoc cluster
kubectl delete -f eloqdoc-cluster.yaml

# Delete the test pod
kubectl delete pod mongosh-test -n default

# Uninstall the operator (optional)
helm uninstall eloq-operator -n eloq-operator-system

# Delete the operator namespace (optional)
kubectl delete namespace eloq-operator-system

# Uninstall OpenEBS (optional)
helm uninstall openebs -n openebs
kubectl delete namespace openebs

# Uninstall cert-manager (optional)
helm uninstall cert-manager -n cert-manager
kubectl delete namespace cert-manager
```

> **Note:** BOS buckets and their data are not automatically deleted. If you want to delete the buckets and data, do so manually via the Baidu Cloud Console or CLI.