# Kubernetes Manifests

This document explains the Kubernetes YAML files in the repository.

## 1. Core Stack

### 1.1 `k8s/allocator-stack.yaml`

Purpose:

- deploy the allocator service and node agent

Contents:

- namespace `mac-allocator`
- PostgreSQL `StatefulSet/net-identity-allocator-postgres`
- PostgreSQL `Service/net-identity-allocator-postgres`
- service accounts
- RBAC for allocator
- RBAC for node agent
- `Deployment/net-identity-allocator`
- `Service/net-identity-allocator`
- `DaemonSet/cmxsafemac-ipv6-node-agent`

Important notes:

- allocator storage now lives in PostgreSQL, not in `emptyDir`
- PostgreSQL persistence comes from the StatefulSet PVC
- node agent is privileged and host-mounted
- managed IPv6 prefix is currently `fd42:4242:4242:10::/64`
- node agent now distinguishes `MANAGED_IFACE=eth0` from `EXPLICIT_IFACE=net1`
- this manifest also carries the main explicit-performance tuning knobs:
  - allocator backlog size
  - allocator create and move batch sizes
  - move dispatch shards
  - node-agent request backlog
  - per-pod explicit batch parallelism
- this manifest assumes Tetragon and Multus are already available in the cluster

RBAC included in this manifest:

- `ServiceAccount/net-identity-allocator`
- `Role/net-identity-allocator`
- `RoleBinding/net-identity-allocator`
- `ServiceAccount/cmxsafemac-ipv6-node-agent`
- `ClusterRole/cmxsafemac-ipv6-node-agent`
- `ClusterRoleBinding/cmxsafemac-ipv6-node-agent`

Current permission split:

- allocator RBAC is namespace-scoped to `mac-allocator`
  useful for discovering node-agent pods in its own namespace
- node-agent RBAC is cluster-scoped for `pods`
  useful because managed pods may live in other namespaces and the agent patches pod annotations there

### 1.2 `k8s/net-identity-allocator-postgres-secret.yaml`

Purpose:

- define the Secret template used by both PostgreSQL and the allocator deployment

Contents:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_SSLMODE`

Important note:

- this file is intended to be edited for the target environment before deployment

## 2. PHP Monitor

### 2.1 `k8s/php-monitor-deployment.yaml`

Purpose:

- deploy the PHP monitor as a separate service

Contents:

- `Deployment/net-identity-allocator-php-monitor`
- `Service/net-identity-allocator-php-monitor`

Important note:

- this deployment now uses a dedicated PHP image, not a ConfigMap-mounted PHP payload
- it expects:
  - `MAC_ALLOCATOR_BASE_URL`
  - `TRAFFIC_COLLECTOR_BASE_URL`
  - `EXPLICIT_IPV6_ROUTE_PREFIX_LEN`

## 3. Traffic Collector

### 3.1 `k8s/traffic-collector.yaml`

Purpose:

- deploy the separate tshark-based traffic collector

Contents:

- `DaemonSet/cmxsafemac-ipv6-traffic-collector`
- `Service/cmxsafemac-ipv6-traffic-collector`

Important notes:

- runs with `hostNetwork: true`
- captures IPv6 traffic on `br-explicit-v6`
- is observability-only; it does not assign MAC or IPv6 state

## 4. Sample Workloads

### 4.0 `k8s/explicit-v6-network.yaml`

Purpose:

- create the sample namespaces
- create an `explicit-v6-lan` `NetworkAttachmentDefinition` in each sample namespace

Why it exists:

- sample pods need a shared secondary `net1` interface for explicit IPv6 traffic
- the current short-term design is single-node and uses a bridge-backed Multus network for that purpose
- explicit IPv6 communication in the samples depends on this manifest
- the `bridge` CNI plugin must exist on the node for this manifest to work

### 4.1 `k8s/demo-statefulset.yaml`

Purpose:

- sample `StatefulSet`

Why it exists:

- provides a `StatefulSet`-shaped sample workload
- shows a managed workload that carries both the management label and the Multus `net1` annotation

### 4.2 `k8s/demo-deployment.yaml`

Purpose:

- sample `Deployment`

Why it exists:

- shows that deployment replicas can also be managed
- useful for testing multiple replicas with explicit IPv6 communication
- shows multiple replicas sharing the same `explicit-v6-lan` secondary network

### 4.3 `k8s/busybox-portable-openssh-test.yaml`

Purpose:

- provide a first-pass test pod for mounted portable OpenSSH inside `busybox:1.36`

Why it exists:

- lets us test a read-only `/etc/ssh` mount without replacing all of `/etc`
- mounts an external account directory at `/external-etc`
- creates `/etc/passwd` and `/etc/group` symlinks at container startup before `sshd` runs
- mounts `/home` read-only from an external volume
- keeps `/var/run` and `/tmp` writable through `emptyDir`
- is focused on verifying whether mounted `sshd`, key auth, and forwarding can work in a minimal BusyBox environment

Important notes:

- the portable OpenSSH runtime is expected on one external read-only volume that contains both:
  - `opt/openssh`
  - `var/empty`
- the pod now mounts the runtime bundle read-only at `/seed-openssh`, then stages it into writable `/opt/openssh` before launch so the compiled helper paths line up and executable bits can be restored on Windows-backed storage
- the pod now prepares `/var/empty` as a local `emptyDir` at startup and explicitly sets the required root ownership and `0755` mode for OpenSSH privilege separation
- account files are expected on a separate external read-only volume mounted at `/external-etc`
- the startup command then links:
  - `/etc/passwd -> /external-etc/passwd`
  - `/etc/group -> /external-etc/group`
  - `/etc/shadow -> /external-etc/shadow` when present
- `/etc/ssh` is mounted as one projected read-only directory made from:
  - ConfigMap `portable-openssh-etc`
  - Secret `portable-openssh-host-keys`
- the projected `/etc/ssh` volume uses mode `0600` so the private host key is acceptable to `sshd`
- that Secret is intentionally not embedded in the sample manifest; create it separately from real generated host-key files
- the external account files must include an `sshd` user and group for privilege separation
- the tested static `10.2p1` bundle does not accept `UsePAM` in `sshd_config`, so the sample config intentionally leaves that option out
- the pod keeps a small capability set (`DAC_OVERRIDE`, `SETGID`, `SETUID`, `SYS_CHROOT`) because OpenSSH privilege separation fails if every capability is dropped
- the sample now also mounts `/opt/ssh-policy/forward-only.sh` from a ConfigMap so `authorized_keys` policies can use one stable forced-command target
- `/home` is expected to be pre-created externally with correct ownership and SSH permissions
- this manifest is a test scaffold; it assumes the external volumes already exist and are populated

### 4.4 `k8s/portable-openssh-dashboard.yaml`

Purpose:

- deploy the first dashboard-backed control plane for the Portable OpenSSH sample

Why it exists:

- keeps desired SSH state in PostgreSQL
- mounts the sample account and home PVCs so a background worker can render:
  - `passwd`
  - `group`
  - `authorized_keys`
- can patch the sample SSH ConfigMap and reload or restart the SSH workload as needed
- replaces the earlier one-shot seed-pod idea with a reusable dashboard plus reconcile worker model

Important notes:

- this first version is intentionally scoped to the mounted sample target
- it uses a separate `ssh_admin` schema inside the existing PostgreSQL service
- it needs namespace-scoped RBAC for:
  - ConfigMap patch/update
  - pod listing
  - `pods/exec`
  - Deployment patch/update
- the PostgreSQL credentials are carried by a dedicated Secret in the sample namespace because Secrets cannot be mounted across namespaces

## 5. Tetragon-Related Files

### 5.1 `k8s/kind-tetragon-config.yaml`

Purpose:

- cluster configuration used for a kind-based environment

### 5.2 `k8s/tetragon-values.yaml`

Purpose:

- Helm values used to install Tetragon with the expected options

## 6. Toolbox And Debug Helpers

### 6.1 `k8s/toolbox.yaml`

Purpose:

- deploy the optional long-lived general Linux toolbox pod

Why it exists:

- provides an in-cluster shell with `bash`, `curl`, `jq`, `kubectl`, `python3`, and network tools
- useful for running benchmarks from inside the cluster instead of from the Windows host

### 6.2 `k8s/CMXsafeMAC-IPv6-node-agent-toolbox.yaml`

Purpose:

- deploy the optional privileged node-agent debugging toolbox

Why it exists:

- mirrors the node-agent pod mounts and privileges closely enough for direct Tetragon and runtime experiments
- useful when debugging node-agent behavior without rebuilding the production DaemonSet image

## 7. Which Manifest To Apply

Install the sample secondary network:

```powershell
kubectl apply -f .\k8s\explicit-v6-network.yaml
```

Core service stack:

```powershell
kubectl create namespace mac-allocator --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f .\k8s\net-identity-allocator-postgres-secret.yaml
kubectl apply -f .\k8s\allocator-stack.yaml
```

PHP monitor:

```powershell
kubectl apply -f .\k8s\php-monitor-deployment.yaml
```

Traffic collector:

```powershell
kubectl apply -f .\k8s\traffic-collector.yaml
```

General in-cluster toolbox:

```powershell
kubectl apply -f .\k8s\toolbox.yaml
```

Privileged node-agent toolbox:

```powershell
kubectl apply -f .\k8s\CMXsafeMAC-IPv6-node-agent-toolbox.yaml
```

StatefulSet sample:

```powershell
kubectl apply -f .\k8s\demo-statefulset.yaml
```

Deployment sample:

```powershell
kubectl apply -f .\k8s\demo-deployment.yaml
```
