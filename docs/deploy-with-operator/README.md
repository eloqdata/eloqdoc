# Deploy EloqDoc with Eloq Operator

This directory contains deployment guides for running EloqDoc on different cloud platforms using the Eloq Operator.

## Overview

The Eloq Operator simplifies the deployment and management of EloqDoc clusters by providing:
- Declarative configuration through Kubernetes Custom Resources (CR)
- Automated lifecycle management (deployment, scaling, updates)
- Cloud-native integration with various cloud providers
- Simplified storage configuration (local SSD, cloud object storage)

## Deployment Guides by Cloud Provider

| Cloud Provider  | Platform | Guide                                 |
| --------------- | -------- | ------------------------------------- |
| **AWS**         | EKS      | [Deploy on AWS EKS](./aws-eks.md)     |
| **Baidu Cloud** | CCE      | [Deploy on Baidu CCE](./baidu-cce.md) |

1. **Choose your cloud platform** from the guides above
2. **Follow the platform-specific guide** for detailed step-by-step instructions
3. **Deploy the Eloq Operator** and required components
4. **Apply the EloqDoc CustomResource** to create your cluster
5. **Connect and test** your EloqDoc deployment

## Common Prerequisites

Before deploying on any platform, ensure you have:

- `kubectl` installed (v1.28 or later)
- `helm` installed (v3.0 or later)
- Access to a Kubernetes cluster (v1.28 or later)
- Appropriate cloud provider CLI tools and credentials

## Common Components

All deployments require the following components:

1. **cert-manager** (v1.19.0+)
   - Manages TLS certificates for webhook endpoints
   - Required by the Eloq Operator

2. **OpenEBS** (v4.3.0+)
   - Provides local persistent volume provisioning
   - Supports XFS filesystem with quota

3. **Eloq Operator**
   - Manages EloqDoc cluster lifecycle
   - Deploys and configures EloqDoc instances
   - Handles storage and networking configuration

## Deployment Architecture

EloqDoc uses a hybrid storage approach:

- **Block Storage (Raft Log)**
  - Cloud block storage (EBS/CDS) for raft consensus logs
  - Used by the log service for distributed consensus
  - Persistent volumes for durability and consistency
  - Provisioned via CSI drivers (EBS CSI/CDS CSI)

- **Local Storage (Hot Data)**
  - Fast local SSDs for cache and active data
  - XFS filesystem with quota support
  - Provisioned via OpenEBS local PV

- **Object Storage (Cold Data)**
  - Cloud object storage (S3/BOS) for persistent data
  - Transaction logs and SST files
  - Automatic bucket creation and lifecycle management


```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                    │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │              Eloq Operator                      │    │
│  │  (Namespace: eloq-operator-system)             │    │
│  └────────────────────────────────────────────────┘    │
│                         │                               │
│                         │ Manages                       │
│                         ▼                               │
│  ┌────────────────────────────────────────────────┐    │
│  │           EloqDoc Cluster (CR)                  │    │
│  │  ┌──────────────────────────────────────────┐  │    │
│  │  │  Frontend (MongoDB Protocol)             │  │    │
│  │  └──────────────────────────────────────────┘  │    │
│  │  ┌──────────────────────────────────────────┐  │    │
│  │  │  TX Nodes (Transaction Processing)       │  │    │
│  │  │  - Local SSD (XFS with quota)            │  │    │
│  │  │  - Object Storage (S3/BOS)               │  │    │
│  │  └──────────────────────────────────────────┘  │    │
│  └────────────────────────────────────────────────┘    │
│                                                          │
│  ┌────────────────────────────────────────────────┐    │
│  │         Supporting Components                   │    │
│  │  - cert-manager (TLS certificates)             │    │
│  │  - OpenEBS (Local PV provisioning)             │    │
│  │  - CSI Drivers (Cloud disk management)         │    │
│  └────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
                         │
                         │ Storage
                         ▼
         ┌───────────────────────────────┐
         │   Object Storage              │
         │  - Transaction Logs           │
         │  - Object Store Data          │
         └───────────────────────────────┘
```
