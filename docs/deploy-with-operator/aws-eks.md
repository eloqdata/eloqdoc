# Deploy EloqDoc on AWS EKS with Eloq Operator

This guide walks you through deploying EloqDoc on AWS EKS using the Eloq Operator. The operator simplifies management and provides a declarative way to deploy and configure EloqDoc clusters.

## Prerequisites

Before you begin, ensure you have:

- `kubectl` installed (v1.28 or later)
- `helm` installed (v3.0 or later)
- An AWS account with permissions to create EKS clusters, IAM policies, and S3 buckets
- AWS CLI configured with appropriate credentials
- `eksctl` installed (v0.150.0 or later)

## Step 1: Create EKS Cluster

### 1.1 Create Cluster Configuration File

Create an EKS cluster with i4i instance types for optimal performance. These instances provide local NVMe storage that EloqDoc can leverage.
Create a file named `eloqdb-demo.yaml` with the following configuration:

```yaml
# eloqdb-demo.yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig

metadata:
  name: eloqdb-demo
  region: ap-northeast-1
  version: "1.32"

managedNodeGroups:
  - name: ap-northeast-1a-i4i-xlarge
    privateNetworking: true
    availabilityZones: ['ap-northeast-1a']
    instanceType: i4i.xlarge
    spot: false
    volumeSize: 50
    ami: ami-0421a6503852f2cdb
    amiFamily: Ubuntu2204
    labels:
      xfsQuota: enabled
    minSize: 0
    desiredCapacity: 0
    maxSize: 3

    overrideBootstrapCommand: |
      #!/bin/bash
      
      # Robust EC2 data-disk setup + mount for EKS nodes (XFS + quota),
      # then bootstrap.
      # - Waits for non-root, unmounted block device >= MIN_BYTES
      # - Accepts nvme/xvd/sd (Nitro and non-Nitro)
      # - Idempotent: skips mkfs if filesystem exists,
      #   skips fstab duplicates, etc.
     
      set -euo pipefail

      ###########################################################################
      # Configuration
      ###########################################################################
      
      CLUSTER_NAME="eloqdb-demo"
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
      # 4. Create filesystem if missing (idempotent)
      ###########################################################################
      FSTYPE="$(lsblk -no FSTYPE "$TARGET_NODE" || true)"
      if [[ -z "${FSTYPE}" ]]; then
        log "No filesystem detected on ${TARGET_NODE}; creating ${FS_TYPE}"
        sudo mkfs."${FS_TYPE}" -f "${TARGET_NODE}"
      else
        log "Filesystem ${FSTYPE} already exists on ${TARGET_NODE}; skipping mkfs"
      fi

      ###########################################################################
      # 5. Resolve UUID with retries
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
      # 6. Mount and persist in /etc/fstab (idempotent)
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
      # 7. Bootstrap EKS (start kubelet after mount is ready)
      #    If you prefer the original order, move this *above* the disk steps.
      ###########################################################################
      log "Running EKS bootstrap for cluster '${CLUSTER_NAME}' (runtime: ${CONTAINER_RUNTIME})"
      sudo /etc/eks/bootstrap.sh "${CLUSTER_NAME}" --container-runtime "${CONTAINER_RUNTIME}"

      log "Done."

    iam:
      attachPolicyARNs:
        - arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly
        - arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy
        - arn:aws:iam::aws:policy/AmazonEC2FullAccess
        - arn:aws:iam::aws:policy/ElasticLoadBalancingFullAccess
        - arn:aws:iam::aws:policy/AmazonEKSClusterPolicy
        - arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy
        - arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
        - arn:aws:iam::<YOUR_ACCOUNT_ID>:policy/EKSFullAccess

iamIdentityMappings:
  - arn: arn:aws:iam::<YOUR_ACCOUNT_ID>:user/<YOUR_IAM_USER>
    groups:
      - system:masters
    username: <YOUR_IAM_USER>
    noDuplicateARNs: true
```

> **Note:** Replace the following placeholders:
> - `<YOUR_ACCOUNT_ID>`: Your AWS account ID
> - `<YOUR_IAM_USER>`: Your IAM username
> - Adjust the `region`, `availabilityZones`, and `ami` values according to your requirements

The bootstrap script automatically:
- Detects and formats attached instance store disks (≥800 GiB)
- Creates an XFS filesystem with quota support
- Mounts the disk to `/mnt/xfs-quota`
- Makes the mount persistent across reboots

### 1.2 Create the Cluster

```bash
# Create the EKS cluster
eksctl create cluster -f eloqdb-demo.yaml

# Configure kubectl to use the new cluster
aws eks update-kubeconfig --name eloqdb-demo --region ap-northeast-1

# Verify the configuration
kubectl config get-contexts

# Switch to the cluster context
kubectl config use-context <YOUR_CLUSTER_CONTEXT>
```


## Step 2: Configure IAM OIDC Provider

The IAM OIDC provider allows Kubernetes service accounts to assume IAM roles, enabling pods to access AWS services securely.

```bash
# Check if OIDC issuer URL exists
aws eks describe-cluster --name eloqdb-demo --query "cluster.identity.oidc.issuer" --region ap-northeast-1 --output text

# Associate IAM OIDC provider with the cluster
eksctl utils associate-iam-oidc-provider --cluster eloqdb-demo --region ap-northeast-1 --approve
```

## Step 3: Install Required Components

### 3.1 Install AWS EBS CSI Driver

The EBS CSI driver enables dynamic provisioning of EBS volumes for persistent storage.

```bash
# Create IAM service account for EBS CSI driver
eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster eloqdb-demo \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --region ap-northeast-1 \
  --approve

# Add Helm repository
helm repo add aws-ebs-csi-driver https://kubernetes-sigs.github.io/aws-ebs-csi-driver
helm repo update

# Install the driver
helm upgrade --install aws-ebs-csi-driver \
    --namespace kube-system \
    --set controller.serviceAccount.create=false \
    --set controller.serviceAccount.name=ebs-csi-controller-sa \
    aws-ebs-csi-driver/aws-ebs-csi-driver

# Verify installation
kubectl get pod -n kube-system -l "app.kubernetes.io/name=aws-ebs-csi-driver,app.kubernetes.io/instance=aws-ebs-csi-driver"
```

### 3.2 Install AWS Load Balancer Controller

The AWS Load Balancer Controller manages ALB and NLB for Kubernetes services.

#### Create IAM Policy

```bash
cat > aws-lb-controller-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:CreateServiceLinkedRole"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "iam:AWSServiceName": "elasticloadbalancing.amazonaws.com"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeAccountAttributes",
                "ec2:DescribeAddresses",
                "ec2:DescribeAvailabilityZones",
                "ec2:DescribeInternetGateways",
                "ec2:DescribeVpcs",
                "ec2:DescribeVpcPeeringConnections",
                "ec2:DescribeSubnets",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeInstances",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeTags",
                "ec2:GetCoipPoolUsage",
                "ec2:DescribeCoipPools",
                "elasticloadbalancing:DescribeLoadBalancers",
                "elasticloadbalancing:DescribeLoadBalancerAttributes",
                "elasticloadbalancing:DescribeListeners",
                "elasticloadbalancing:DescribeListenerAttributes",
                "elasticloadbalancing:DescribeListenerCertificates",
                "elasticloadbalancing:DescribeSSLPolicies",
                "elasticloadbalancing:DescribeRules",
                "elasticloadbalancing:DescribeTargetGroups",
                "elasticloadbalancing:DescribeTargetGroupAttributes",
                "elasticloadbalancing:DescribeTargetHealth",
                "elasticloadbalancing:DescribeTags"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "cognito-idp:DescribeUserPoolClient",
                "acm:ListCertificates",
                "acm:DescribeCertificate",
                "iam:ListServerCertificates",
                "iam:GetServerCertificate",
                "waf-regional:GetWebACL",
                "waf-regional:GetWebACLForResource",
                "waf-regional:AssociateWebACL",
                "waf-regional:DisassociateWebACL",
                "wafv2:GetWebACL",
                "wafv2:GetWebACLForResource",
                "wafv2:AssociateWebACL",
                "wafv2:DisassociateWebACL",
                "shield:GetSubscriptionState",
                "shield:DescribeProtection",
                "shield:CreateProtection",
                "shield:DeleteProtection"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:RevokeSecurityGroupIngress"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateSecurityGroup"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags"
            ],
            "Resource": "arn:aws:ec2:*:*:security-group/*",
            "Condition": {
                "StringEquals": {
                    "ec2:CreateAction": "CreateSecurityGroup"
                },
                "Null": {
                    "aws:RequestTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Resource": "arn:aws:ec2:*:*:security-group/*",
            "Condition": {
                "Null": {
                    "aws:RequestTag/elbv2.k8s.aws/cluster": "true",
                    "aws:ResourceTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:AuthorizeSecurityGroupIngress",
                "ec2:RevokeSecurityGroupIngress",
                "ec2:DeleteSecurityGroup"
            ],
            "Resource": "*",
            "Condition": {
                "Null": {
                    "aws:ResourceTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:CreateLoadBalancer",
                "elasticloadbalancing:CreateTargetGroup"
            ],
            "Resource": "*",
            "Condition": {
                "Null": {
                    "aws:RequestTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:CreateListener",
                "elasticloadbalancing:DeleteListener",
                "elasticloadbalancing:CreateRule",
                "elasticloadbalancing:DeleteRule"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:AddTags",
                "elasticloadbalancing:RemoveTags"
            ],
            "Resource": [
                "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*",
                "arn:aws:elasticloadbalancing:*:*:loadbalancer/net/*/*",
                "arn:aws:elasticloadbalancing:*:*:loadbalancer/app/*/*"
            ],
            "Condition": {
                "Null": {
                    "aws:RequestTag/elbv2.k8s.aws/cluster": "true",
                    "aws:ResourceTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:AddTags",
                "elasticloadbalancing:RemoveTags"
            ],
            "Resource": [
                "arn:aws:elasticloadbalancing:*:*:listener/net/*/*/*",
                "arn:aws:elasticloadbalancing:*:*:listener/app/*/*/*",
                "arn:aws:elasticloadbalancing:*:*:listener-rule/net/*/*/*",
                "arn:aws:elasticloadbalancing:*:*:listener-rule/app/*/*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:AddTags"
            ],
            "Resource": [
                "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*",
                "arn:aws:elasticloadbalancing:*:*:loadbalancer/net/*/*",
                "arn:aws:elasticloadbalancing:*:*:loadbalancer/app/*/*"
            ],
            "Condition": {
                "StringEquals": {
                    "elasticloadbalancing:CreateAction": [
                        "CreateTargetGroup",
                        "CreateLoadBalancer"
                    ]
                },
                "Null": {
                    "aws:RequestTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:ModifyLoadBalancerAttributes",
                "elasticloadbalancing:SetIpAddressType",
                "elasticloadbalancing:SetSecurityGroups",
                "elasticloadbalancing:SetSubnets",
                "elasticloadbalancing:DeleteLoadBalancer",
                "elasticloadbalancing:ModifyTargetGroup",
                "elasticloadbalancing:ModifyTargetGroupAttributes",
                "elasticloadbalancing:DeleteTargetGroup"
            ],
            "Resource": "*",
            "Condition": {
                "Null": {
                    "aws:ResourceTag/elbv2.k8s.aws/cluster": "false"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:RegisterTargets",
                "elasticloadbalancing:DeregisterTargets"
            ],
            "Resource": "arn:aws:elasticloadbalancing:*:*:targetgroup/*/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticloadbalancing:SetWebAcl",
                "elasticloadbalancing:ModifyListener",
                "elasticloadbalancing:AddListenerCertificates",
                "elasticloadbalancing:RemoveListenerCertificates",
                "elasticloadbalancing:ModifyRule"
            ],
            "Resource": "*"
        }
    ]
}

EOF

# Create the IAM policy
aws iam create-policy \
  --policy-name AWSLoadBalancerControllerIAMPolicy \
  --policy-document file://aws-lb-controller-policy.json
```

#### Deploy the Load Balancer Controller

```bash
# Create IAM service account
eksctl create iamserviceaccount \
    --cluster=eloqdb-demo \
    --namespace=kube-system \
    --name=aws-load-balancer-controller \
    --attach-policy-arn=arn:aws:iam::<YOUR_ACCOUNT_ID>:policy/AWSLoadBalancerControllerIAMPolicy \
    --region ap-northeast-1 \
    --approve

# Add Helm repository
helm repo add eks https://aws.github.io/eks-charts
helm repo update

# Install the controller
helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  -n kube-system \
  --set clusterName=eloqdb-demo \
  --set serviceAccount.create=false \
  --set serviceAccount.name=aws-load-balancer-controller

# Verify installation
kubectl get deployment -n kube-system aws-load-balancer-controller
```

### 3.3 Install OpenEBS

OpenEBS provides local persistent volumes with XFS quota support.

```bash
# Add Helm repository
helm repo add openebs https://openebs.github.io/openebs
helm repo update

# Install OpenEBS (local PV provisioner only)
helm install openebs --namespace openebs openebs/openebs \
  --set engines.local.lvm.enabled=false \
  --set engines.local.zfs.enabled=false \
  --set engines.replicated.mayastor.enabled=false \
  --create-namespace

# Verify installation
kubectl get pods -n openebs
```

### 3.4 Install cert-manager

cert-manager is required by the Eloq Operator for webhook certificate management.

```bash
# Install cert-manager
helm install cert-manager oci://quay.io/jetstack/charts/cert-manager \
  --version v1.19.0 \
  --namespace cert-manager \
  --create-namespace \
  --set crds.enabled=true

# Verify installation
kubectl get pods -n cert-manager
```

## Step 4: Configure Storage

Create storage classes for EloqDoc to use.

### 4.1 Create Local Storage Class (for instance store)

```yaml
# local-storage-class.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: tx-eloq-data-local-sc
  annotations:
    cas.openebs.io/config: |-
      - name: StorageType
        value: hostpath
      - name: BasePath
        value: /mnt/xfs-quota
      - name: XFSQuota
        enabled: "true"
        data:
          softLimitGrace: "0%"
          hardLimitGrace: "0%"
    openebs.io/cas-type: local
provisioner: openebs.io/local
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
```

### 4.2 Create EBS Storage Class

```yaml
# ebs-storage-class.yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: tx-eloq-data-sc
mountOptions:
  - nodelalloc
  - noatime
parameters:
  type: gp3
provisioner: ebs.csi.aws.com
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
```

```bash
# Apply storage classes
kubectl apply -f local-storage-class.yaml
kubectl apply -f ebs-storage-class.yaml
```

## Step 5: Set Up IAM for EloqDoc

### 5.1 Create IAM Policy for EloqDoc

EloqDoc requires access to S3 for storing data and transaction logs.

```bash
cat > EloqDBResourceIAMPolicy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3Access",
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "arn:aws:s3:::*"
        },
        {
            "Sid": "EC2Permissions",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeSubnets",
                "ec2:DescribeNetworkInterfaces",
                "ec2:CreateNetworkInterface"
            ],
            "Resource": "*"
        },
        {
            "Sid": "EKSAccess",
            "Effect": "Allow",
            "Action": [
                "eks:DescribeCluster"
            ],
            "Resource": "*"
        }
    ]
}
EOF

# Create the IAM policy
aws iam create-policy \
  --policy-name EloqDBResourceIAMPolicy \
  --policy-document file://EloqDBResourceIAMPolicy.json
```

### 5.2 Create Kubernetes ServiceAccount with IAM Role

Create a namespace and service account that can assume the IAM role.

```bash
# Create namespace for EloqDoc
kubectl create namespace ns-eloqdoc

# Create service account with IAM role binding
eksctl create iamserviceaccount \
  --cluster eloqdb-demo \
  --namespace ns-eloqdoc \
  --name eloq-aws-access \
  --attach-policy-arn arn:aws:iam::<YOUR_ACCOUNT_ID>:policy/EloqDBResourceIAMPolicy \
  --region ap-northeast-1 \
  --approve

# Verify service account creation
kubectl get sa -n ns-eloqdoc eloq-aws-access -o yaml
```

## Step 6: Install Eloq Operator

The Eloq Operator manages the lifecycle of EloqDoc clusters.

```bash
# Add EloqData Helm repository
helm repo add eloqdata https://eloqdata.github.io/eloq-charts/
helm repo update

# Install the operator
helm install eloq-operator eloqdata/eloq-operator \
  --namespace eloq-operator-system \
  --create-namespace

# Verify operator installation
kubectl get pods -n eloq-operator-system
```

## Step 7: Deploy EloqDoc

### 7.1 Understanding S3 Bucket Configuration

EloqDoc uses S3 for persistent storage with the following configuration parameters:

- **`txLogBucketName`**: Base name for the transaction log bucket
- **`objectStoreBucketName`**: Base name for the object store bucket (can be the same as `txLogBucketName`)
- **`bucketPrefix`**: Prefix that will be prepended to bucket names
- **`txLogObjectPath`**: Path prefix for transaction logs within the bucket
- **`objectStoreObjectPath`**: Path prefix for object store data within the bucket
- **`region`**: AWS region where buckets will be created

**Bucket Naming Convention:**

The actual S3 bucket names are formed by combining the prefix and base name:
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
```

The created bucket will be:
- Bucket name: `eloqdoc-my-cluster-data`

Within this bucket, data is organized by paths:
- Transaction logs: `s3://eloqdoc-my-cluster-data/eloqdoc-rocksdb-s3-log/`
- Object store data: `s3://eloqdoc-my-cluster-data/eloqdoc-rocksdb-s3-store/`

> **Tip:** You can use the same bucket for both transaction logs and object store data. The different paths ensure proper data separation and organization.

**Automatic Bucket Creation:**

EloqDoc will automatically create the S3 buckets if they don't exist. Ensure your IAM policy includes `s3:CreateBucket` permission (included in `s3:*` in the policy we created earlier).

### 7.2 Create EloqDoc Cluster Configuration

Create a file named `eloqdoc-cluster.yaml` with the following configuration:

```yaml
# eloqdoc-cluster.yaml
apiVersion: eloqdbcluster.eloqdata.com/v1alpha1
kind: EloqDBCluster
metadata:
  name: eloqdoc-rocksdbcloud-s3
  namespace: ns-eloqdoc
spec:
  clusterDeployMode: txWithInternalLog
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
        memory: "512Mi"
        cpu: "1"
      limits:
        memory: "512Mi"
        cpu: "1"
    keySpaceName: e2e
    image: eloqdata/eloqdoc-rocks-cloud:release-0.2.6
    imagePullPolicy: Always
    serviceAccountName: eloq-aws-access
    schedulePolicy:
      policyType: required
      preferredZone: ap-northeast-1a
      labelSelector:
        matchExpressions:
          - key: alpha.eksctl.io/nodegroup-name
            operator: "In"
            values:
              - ap-northeast-1a-i4i-xlarge
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
              storage: 500Mi
            limits:
              storage: 3Gi
          volumeMode: Filesystem
  store:
    storageType: objectStorage
    rocksdbCloud:
      sstFileCacheSize: 2Gi
      readyTimeout: 10
      fileDeletionDelay: 3600
      cloudObjectStorage:
        cloudStoreType: s3
        txLogBucketName: <YOUR_S3_BUCKET_BASE_NAME>
        objectStoreBucketName: <YOUR_S3_BUCKET_BASE_NAME>
        bucketPrefix: eloqdoc-
        region: ap-northeast-1
        txLogObjectPath: eloqdoc-rocksdb-s3-log
        objectStoreObjectPath: eloqdoc-rocksdb-s3-store
```

> **Note:** Update the following values:
> - `<YOUR_S3_BUCKET_BASE_NAME>`: Base name for S3 buckets (e.g., `my-eloqdoc-data`)
> - `txLogBucketName` and `objectStoreBucketName`: **Can use the same value**. The data will be separated by different paths (`txLogObjectPath` and `objectStoreObjectPath`)
> - `bucketPrefix`: This prefix will be prepended to the bucket names. The actual S3 bucket names created will be:
>   - Transaction log bucket: `<bucketPrefix><txLogBucketName>` (e.g., `eloqdoc-my-eloqdoc-data`)
>   - Object store bucket: `<bucketPrefix><objectStoreBucketName>` (e.g., `eloqdoc-my-eloqdoc-data`)
> - If using the same bucket name, the data will be organized as:
>   - Transaction logs: `s3://<bucketPrefix><bucketName>/<txLogObjectPath>/`
>   - Object store: `s3://<bucketPrefix><bucketName>/<objectStoreObjectPath>/`
> - The buckets will be **automatically created** if they don't exist
> - Adjust resource limits, replica count, and storage sizes according to your requirements

**Important:** Ensure your bucket names comply with S3 naming rules:
- Must be globally unique across all AWS accounts
- Must be between 3-63 characters long
- Can contain only lowercase letters, numbers, hyphens, and periods
- Must start and end with a letter or number

### 7.3 Deploy the Cluster

```bash
# Apply the EloqDoc cluster configuration
kubectl apply -f eloqdoc-cluster.yaml

# Monitor the deployment
kubectl get pods -n ns-eloqdoc -w
```

### 7.4 Retrieve Admin Credentials

After deployment, the operator creates a secret with admin credentials.

```bash
# View the secret
kubectl get secret eloqdoc-rocksdbcloud-s3-admin-user -n ns-eloqdoc -o yaml

# Extract username
export ELOQDOC_USERNAME=$(kubectl get secret eloqdoc-rocksdbcloud-s3-admin-user -n ns-eloqdoc -o jsonpath='{.data.username}' | base64 -d)

# Extract password
export ELOQDOC_PASSWORD=$(kubectl get secret eloqdoc-rocksdbcloud-s3-admin-user -n ns-eloqdoc -o jsonpath='{.data.password}' | base64 -d)

# Display credentials
echo "Username: $ELOQDOC_USERNAME"
echo "Password: $ELOQDOC_PASSWORD"
```

## Step 8: Test the Deployment

### 8.1 Create a Test Pod

Deploy a MongoDB shell pod for testing:

```yaml
# mongosh-test.yaml
apiVersion: v1
kind: Pod
metadata:
  name: mongosh-test
  namespace: ns-eloqdoc
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
kubectl wait --for=condition=Ready pod/mongosh-test -n ns-eloqdoc --timeout=60s
```

### 8.2 Connect to EloqDoc

#### Option 1: Internal Connection (ClusterIP Service)

Connect from within the cluster using the internal service:

```bash
# Exec into the mongosh pod
kubectl exec -it mongosh-test -n ns-eloqdoc -- bash

# Inside the pod, connect to EloqDoc
mongosh "mongodb://$ELOQDOC_USERNAME:$ELOQDOC_PASSWORD@eloq-srv-tx-eloqdoc-rocksdbcloud-s3.ns-eloqdoc.svc.cluster.local:27017"

# Test basic operations
use testdb
db.testcol.insertOne({name: "test", value: 123})
db.testcol.find()
```

#### Option 2: External Connection (LoadBalancer Service)

To connect from outside the cluster, expose the service via an internet-facing LoadBalancer:

```bash
# Make LoadBalancer internet-facing
kubectl annotate service eloq-srv-tx-eloqdoc-rocksdbcloud-s3-exposed \
  -n ns-eloqdoc \
  service.beta.kubernetes.io/aws-load-balancer-scheme=internet-facing \
  --overwrite

# Get the LoadBalancer DNS name
export LB_DNS=$(kubectl get service eloq-srv-tx-eloqdoc-rocksdbcloud-s3-exposed -n ns-eloqdoc -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

echo "LoadBalancer DNS: $LB_DNS"

# Wait for the LoadBalancer to be provisioned (may take 2-3 minutes)
kubectl wait --for=jsonpath='{.status.loadBalancer.ingress}' \
  service/eloq-srv-tx-eloqdoc-rocksdbcloud-s3-exposed \
  -n ns-eloqdoc --timeout=300s

# Connect from your local machine
mongosh "mongodb://$ELOQDOC_USERNAME:$ELOQDOC_PASSWORD@$LB_DNS:27017"
```

> **Security Note:** Making the LoadBalancer internet-facing exposes your EloqDoc instance to the public internet. Consider:
> - Using security groups to restrict access to specific IP addresses
> - Implementing network policies
> - Using a VPN or AWS PrivateLink for production environments


## Cleanup

To remove the EloqDoc deployment and associated resources:

```bash
# Delete the EloqDoc cluster
kubectl delete -f eloqdoc-cluster.yaml

# Delete the namespace
kubectl delete namespace ns-eloqdoc

# Uninstall the operator
helm uninstall eloq-operator -n eloq-operator-system

# Delete the operator namespace
kubectl delete namespace eloq-operator-system
```
