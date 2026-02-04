# Kubernetes Deployment Guide

## Overview

This directory contains Kubernetes manifests for deploying the Bulletin Data DAG as a Dagster code location. The deployment connects to a central Dagster instance via gRPC on port 4000.

## Prerequisites

- Kubernetes cluster (cloud or local k3d)
- `kubectl` configured to access your cluster
- Access to GitHub Container Registry (`ghcr.io`)
- Central Dagster instance already running in the `dagster` namespace

## Manifests

| File | Purpose |
|------|---------|
| `deployment.yaml` | Pod specification and deployment configuration |
| `service.yaml` | ClusterIP service exposing gRPC port 4000 |
| `configmap.yaml` | Non-sensitive configuration (WordPress API URLs, paths) |
| `secret.yaml` | Sensitive credentials (AWS S3 access keys) |

## Initial Setup

### 1. Create GitHub Container Registry Secret

The deployment requires authentication to pull images from `ghcr.io/bu-ili/de-datalake-bulletin-dataload`.

```bash
# Create the ghcr-secret for image pulls
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username=YOUR_GITHUB_USERNAME \
  --docker-password=YOUR_GITHUB_PAT \
  --namespace=dagster

# Verify secret was created
kubectl get secret ghcr-secret -n dagster
```

**Note:** Create a GitHub Personal Access Token (PAT) with `read:packages` scope at https://github.com/settings/tokens

### 2. Update Secrets with Real Values

The `secret.yaml` file contains placeholder values. Update them before deploying:

```bash
# Edit secret.yaml and replace placeholders with actual values
vim secret.yaml

# Or apply secrets directly via kubectl
kubectl create secret generic bulletin-dataload-secret \
  --from-literal=AWS_S3_BUCKET_NAME='your-actual-bucket' \
  --from-literal=AWS_ACCESS_KEY_ID='your-actual-key-id' \
  --from-literal=AWS_SECRET_ACCESS_KEY='your-actual-secret' \
  --from-literal=AWS_REGION_NAME='us-east-1' \
  --namespace=dagster \
  --dry-run=client -o yaml | kubectl apply -f -
```

**Security Note:** Never commit real secrets to git. The checked-in `secret.yaml` uses placeholders for safety.

## Deployment

### Deploy All Manifests

```bash
# From the k8s/ directory
kubectl apply -f configmap.yaml
kubectl apply -f secret.yaml
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml

# Or apply all at once
kubectl apply -f .
```

### Verify Deployment

```bash
# Check deployment status
kubectl get deployment bulletin-dataload -n dagster

# Check pod status
kubectl get pods -n dagster -l app=bulletin-dataload

# View pod logs
kubectl logs -n dagster -l app=bulletin-dataload -f

# Check service
kubectl get service bulletin-dataload -n dagster
```

### Verify gRPC Connection

The Dagster daemon should automatically detect this code location via the service endpoint:

```
bulletin-dataload.dagster.svc.cluster.local:4000
```

Check the Dagster webserver UI to verify the code location is loaded.

## Image Tags

The deployment uses different image tags based on the branch:

| Branch | Image Tag | Environment |
|--------|-----------|-------------|
| `dev` | `:dev` | Development |
| `test` | `:test` | Test/Staging |
| `main` | `:latest` | Production |
| `main` | `:YYYYMMDD-{sha}` | Production (pinned) |

### Update to Specific Tag

```bash
# Update to latest from main branch
kubectl set image deployment/bulletin-dataload \
  bulletin-dataload=ghcr.io/bu-ili/de-datalake-bulletin-dataload:latest \
  -n dagster

# Update to specific CalVer tag (for rollback or pinning)
kubectl set image deployment/bulletin-dataload \
  bulletin-dataload=ghcr.io/bu-ili/de-datalake-bulletin-dataload:20260204-a1b2c3d \
  -n dagster

# Update to dev tag for testing
kubectl set image deployment/bulletin-dataload \
  bulletin-dataload=ghcr.io/bu-ili/de-datalake-bulletin-dataload:dev \
  -n dagster
```

## Configuration

### ConfigMap Values

Edit [configmap.yaml](configmap.yaml) to adjust non-sensitive settings:

```yaml
BULLETIN_WP_BASE_URL: "https://www.bu.edu/academics/wp-json/wp/v2/"
USER_AGENT: "de-datalake-bulletin-dataload-1.2.0"
PARQUET_EXPORT_FOLDER_PATH: "/app/data/parquet/"
PARQUET_EXPORT_FILE_NAME: "de_bulletin_data.parquet"
```

After editing, apply changes:

```bash
kubectl apply -f configmap.yaml
kubectl rollout restart deployment/bulletin-dataload -n dagster
```

### Resource Limits

Current resource configuration:

```yaml
resources:
  requests:
    memory: "512Mi"
    cpu: "250m"
  limits:
    memory: "2Gi"
    cpu: "1000m"
```

Adjust in [deployment.yaml](deployment.yaml) based on observed usage.

## Troubleshooting

### Pod Won't Start

```bash
# Check pod events
kubectl describe pod -n dagster -l app=bulletin-dataload

# Check if image pull is failing
kubectl get events -n dagster --sort-by='.lastTimestamp'
```

Common issues:
- Missing `ghcr-secret` → See "Initial Setup" above
- Incorrect image tag → Check GitHub Actions for available tags
- Resource constraints → Check node resources with `kubectl describe nodes`

### gRPC Connection Issues

```bash
# Test service DNS resolution
kubectl run -it --rm debug --image=busybox --restart=Never -n dagster -- \
  nslookup bulletin-dataload.dagster.svc.cluster.local

# Test port connectivity
kubectl run -it --rm debug --image=nicolaka/netshoot --restart=Never -n dagster -- \
  nc -zv bulletin-dataload.dagster.svc.cluster.local 4000
```

### View Logs

```bash
# Follow logs in real-time
kubectl logs -n dagster -l app=bulletin-dataload -f

# View logs from previous pod (if crashed)
kubectl logs -n dagster -l app=bulletin-dataload --previous
```

### Access Pod Shell

```bash
# Open interactive shell in running pod
kubectl exec -it -n dagster deployment/bulletin-dataload -- /bin/sh
```

## Local Development (k3d)

For local testing with k3d:

1. **Start k3d cluster** with registry:
   ```bash
   k3d cluster create mycluster --registry-create mycluster-registry:5000
   ```

2. **Build and push to local registry**:
   ```bash
   docker build -t localhost:5000/de-bulletin:dev .
   docker push localhost:5000/de-bulletin:dev
   ```

3. **Update deployment** to use local image:
   ```bash
   # Edit deployment.yaml temporarily
   image: localhost:5000/de-bulletin:dev
   
   kubectl apply -f k8s/
   ```

## Rollback

### To Previous Version

```bash
# View rollout history
kubectl rollout history deployment/bulletin-dataload -n dagster

# Rollback to previous version
kubectl rollout undo deployment/bulletin-dataload -n dagster

# Rollback to specific revision
kubectl rollout undo deployment/bulletin-dataload -n dagster --to-revision=2
```

### To Specific CalVer Tag

```bash
# Find available tags at: https://github.com/bu-ili/de-datalake-bulletin-dataload/pkgs/container/de-datalake-bulletin-dataload

# Update to specific date/commit
kubectl set image deployment/bulletin-dataload \
  bulletin-dataload=ghcr.io/bu-ili/de-datalake-bulletin-dataload:20260201-f8e9a2b \
  -n dagster
```

## Cleanup

```bash
# Delete all resources
kubectl delete -f .

# Or delete individually
kubectl delete deployment bulletin-dataload -n dagster
kubectl delete service bulletin-dataload -n dagster
kubectl delete configmap bulletin-dataload-config -n dagster
kubectl delete secret bulletin-dataload-secret -n dagster
```

## Additional Resources

- [Dagster Deployment Docs](https://docs.dagster.io/deployment/guides/kubernetes)
- [GitHub Container Registry Docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)

### gRPC connection issues
```bash
# Test from another pod
kubectl run -it --rm debug --image=curlimages/curl --restart=Never -n dagster \
  -- curl -v bulletin-dataload.dagster.svc.cluster.local:4000
```

## Security Considerations
- ✅ Runs as non-root user (UID 1000)
- ✅ Read-only root filesystem where possible
- ✅ Drops all capabilities
- ✅ Uses service account with minimal permissions
- ✅ Secrets managed externally (not in git)

## Updating
```bash
# Update image
kubectl set image deployment/bulletin-dataload \
  bulletin-dataload=your-registry.azurecr.io/bulletin-dataload:v1.2.1 \
  -n dagster

# Rollback if needed
kubectl rollout undo deployment/bulletin-dataload -n dagster
```
