# net-identity-allocator

This document describes the allocator service component.

Primary file:

- `net-identity-allocator/app.py`

Supporting files:

- `net-identity-allocator/Dockerfile`

## 1. Responsibility

The allocator service is the central control-plane component for:

- deterministic MAC allocation
- deterministic managed IPv6 allocation
- derivation of the automatic managed `net1` IPv6
- storage of explicit extra IPv6 assignments
- monitor data exposure
- forwarding explicit IPv6 apply requests to the correct node agent

It does not directly enter pod network namespaces. That is the node agent's job.

## 2. Main Functions

The allocator service:

- stores state in PostgreSQL for the Kubernetes deployment
- creates or reuses managed allocation rows
- releases or marks rows stale
- stores explicit IPv6 requests
- resolves explicit IPv6 requests to the correct live pod
- stores the current node-agent pod name, UID, and IP on each managed allocation row
- stores the latest pod runtime snapshot on each managed allocation row:
  - `sandbox_id`
  - `sandbox_pid`
  - `sandbox_pid_start_time`
  - `netns_inode`
  - `runtime_observed_at`
- copies that same node-agent endpoint metadata onto explicit IPv6 assignment rows
- copies the managed runtime snapshot onto explicit IPv6 assignment rows too
- exposes JSON APIs
- batches explicit create and move apply work before handing it to the node agent

Allocator concurrency note:

- the allocator no longer holds a process-wide store write lock
- write concurrency is now delegated to PostgreSQL transactions and uniqueness constraints
- explicit IPv6 writes still use retry-on-conflict behavior so different canonical explicit IPv6 requests can proceed in parallel while same-address conflicts are rejected cleanly
- queued explicit-apply workers now dispatch the already-written assignment snapshot directly instead of rereading that row first
- create and move are now batched separately so the allocator can keep the write path precise while still reducing node-agent call count

## 3. Managed Allocation Model

Managed MAC format:

```text
GW1:GW2:GW3:GW4:CTR1:CTR2
```

Meaning:

- `GW1:GW2:GW3:GW4`
  the first 4 bytes of the configured canonical gateway MAC when present, otherwise the original gateway MAC seen on the node
- `CTR1:CTR2`
  the 2-byte collision counter for the allocation row

Why the 2-byte counter matters:

- it gives each allocation a stable differentiator under the same gateway MAC head
- it allows many managed pods on the same node or under the same gateway-MAC-derived space
- it is especially useful for replicas, because different replicas can share the same `GW_HEAD_4` while still getting unique deterministic MACs and managed IPv6 host indexes
- the same counter is kept as allocator metadata for the currently targeted explicit IPv6 assignment

Managed IPv6 format:

- prefix = configured `/64`
- host part = `counter + 1`

Example:

- `gw_mac = f6:db:2b:39:78:94`
- `counter = 3`
- assigned MAC = `f6:db:2b:39:00:03`
- managed IPv6 with `fd42:4242:4242:10::/64` = `fd42:4242:4242:10::4`

Automatic managed `net1` IPv6 format:

- prefix tag = `AUTO_MANAGED_EXPLICIT_TAG`
- embedded counter = `counter + 1`
- embedded device bytes = `00:00:00:00:00:00`

Example with `AUTO_MANAGED_EXPLICIT_TAG=fd00`:

- `gw_mac = f6:db:2b:39:78:94`
- `counter = 3`
- automatic managed `net1` IPv6 = `fd00:f6db:2b39:7894:0004::`

Operational note:

- this address is exposed on allocation rows as `auto_managed_explicit_ipv6`
- it is not stored in `explicit_ipv6_assignments`, because it is automatic managed state rather than a caller-driven movable identity
- during managed assignment, the node agent also reports the current runtime snapshot so explicit create / move can reuse it later without a fresh Kubernetes pod lookup
- `canonical_gateway_mac` is stored in the shared `cmxsafe_system_settings` table and can also be seeded with the allocator `CANONICAL_GATEWAY_MAC` environment variable; the dashboard mirrors its Server Settings value into the same table

## 4. Explicit IPv6 Model

Managed IPv6, automatic managed `net1` IPv6, and caller-driven explicit IPv6 are intentionally different:

- managed IPv6
  - one allocator-owned primary IPv6 per managed allocation row
  - derived from the configured managed `/64` plus the stable counter
  - attached on `eth0`
- automatic managed `net1` IPv6
  - one deterministic secondary-lane IPv6 per managed allocation row
  - derived from `AUTO_MANAGED_EXPLICIT_TAG + GW_MAC + (counter + 1) + 00..00`
  - attached on `net1`
- explicit IPv6
  - zero or more extra IPv6s per managed pod
  - derived from the canonical encoded `Prefix + canonical_gateway_mac + 0000 + MAC_DEV`
  - attached on `net1` in the current single-node design

Explicit IPv6 format:

```text
Prefix-2-bytes | canonical_gateway_mac-6-bytes | 0000 | MAC_DEV-6-bytes
```

Meaning:

- `Prefix-2-bytes`
  a caller-chosen 2-byte tag used to group or categorize explicit IPv6 addresses
- `canonical_gateway_mac-6-bytes`
  the stable logical gateway identity root stored in `cmxsafe_system_settings`; if unset, the allocator falls back to the managed allocation `gw_mac`
- `MAC_DEV-6-bytes`
  a caller-defined 6-byte differentiator for multiple explicit IPv6 identities under the same prefix and gateway-MAC scope

Example:

```text
6666:f6db:2b39:7894:0000:aabb:ccdd:3101
```

Practical interpretation:

- `Prefix-2-bytes` is useful in the current implementation because the node agent groups explicit routes by prefix length, and with the current default `EXPLICIT_IPV6_ROUTE_PREFIX_LEN=16`, the first 2 bytes act as a route bucket such as `4444::/16` or `6666::/16`
- that route grouping helps east-west communication between managed pods for explicit IPv6s in this design because the node agent installs one on-link route per active prefix bucket on the shared `net1` interface
- the explicit IPv6 itself always uses canonical counter `0000`
- the real managed allocation counter is stored only as metadata in `target_counter`
- the allocator therefore treats explicit IPv6 as a unique movable identity that can be reassigned between managed pods

Parallel explicit-assignment assumptions:

- no two concurrent requests assign the same canonical explicit IPv6 to different pods
- the relevant explicit prefix route already exists on the managed pods
- under those conditions, the allocator only needs to preserve uniqueness of each canonical explicit IPv6, not serialize all explicit writes together

Operational note:

- in the current single-node design, explicit IPv6s are attached on `net1`, not `eth0`
- `net1` is supplied by a Multus `NetworkAttachmentDefinition`
- the allocator keeps the identity and forwarding logic the same, while the node agent decides which interface receives the explicit IPv6
- once the node agent marks an explicit IPv6 as applied, the stored `container_iface` for that explicit assignment becomes `net1`

The allocator can accept this either:

- directly as `ipv6_address`
- or indirectly via `pod_uid + gw_tag + mac_dev`

## 5. Tables

### 5.1 `mac_allocations`

Purpose:

- one row per managed pod allocation

Important fields:

- `assigned_mac`
- `gw_mac`
- `counter`
- `assigned_ipv6`
- derived response field `auto_managed_explicit_ipv6`
- `namespace`
- `pod_name`
- `pod_uid`
- `node_name`
- `node_agent_pod_name`
- `node_agent_pod_uid`
- `node_agent_pod_ip`
- `sandbox_id`
- `sandbox_pid`
- `sandbox_pid_start_time`
- `netns_inode`
- `runtime_observed_at`
- `status`
- `container_iface`

Operational note:

- the allocator records the node-agent pod name, UID, and IP that was current when the managed MAC assignment was created or refreshed
- this deliberately duplicates node-agent endpoint data per managed pod row so later explicit IPv6 apply work can read it directly without doing a fresh Kubernetes node-agent lookup first
- it also records the last runtime snapshot observed by the node agent so explicit create / move can hand that snapshot straight back to the node agent as a fast-path hint

### 5.2 `explicit_ipv6_assignments`

Purpose:

- one row per extra IPv6 address

Important fields:

- `requested_ipv6`
- `gw_tag_hex`
- `target_gw_mac`
- `target_counter`
- `target_assigned_mac`
- `mac_dev`
- `namespace`
- `pod_name`
- `pod_uid`
- `node_name`
- `node_agent_pod_name`
- `node_agent_pod_uid`
- `node_agent_pod_ip`
- `sandbox_id`
- `sandbox_pid`
- `sandbox_pid_start_time`
- `netns_inode`
- `runtime_observed_at`
- `status`
- `container_iface`

Operational note:

- explicit IPv6 rows inherit the node-agent endpoint metadata from the target managed allocation row
- that lets the allocator try the stored node-agent IP first when forwarding explicit work, including the current bulk `/explicit-ipv6/bulk-apply` and `/explicit-ipv6/bulk-move` paths
- explicit IPv6 rows also inherit the last managed runtime snapshot, so the explicit hot path can use a single allocator read and avoid a fresh pod lookup in Kubernetes

## 6. API Endpoints

Health and read APIs:

- `GET /healthz`
  simple liveness check for Kubernetes probes and quick operational checks
- `GET /stats`
  returns summary counters such as allocated, stale, and released rows for dashboards or health summaries
- `GET /allocations`
  returns the current managed allocation table, including MAC, managed IPv6, derived automatic managed `net1` IPv6, pod identity, and status
- `GET /explicit-ipv6-assignments`
  returns the stored extra IPv6 assignment rows and their target pod mapping

Managed allocation:

- `POST /allocations/ensure`
  creates the managed MAC and managed IPv6 allocation for a pod identity, or refreshes the same active row while that exact `pod_uid` remains alive
- `POST /allocations/release`
  marks a managed allocation as no longer active when the pod has ended or been removed
- `POST /allocations/touch`
  refreshes the allocator's last-seen timestamp for a still-live managed pod
- `POST /reconcile/live-pods`
  compares the allocator table with a caller-supplied live pod set and marks orphaned rows appropriately
- `POST /allocations/clear-stale`
  deletes stale rows from the allocator table when you want to clean up old orphaned entries
- `POST /admin/reset`
  clears both allocator tables so automated tests can start from a clean state without deleting the PostgreSQL PVC
- `POST /admin/reset-explicit`
  marks active explicit IPv6 rows as `RELEASED`, optionally scoped by `namespace`; when called with `clear_runtime=true`, it also asks the relevant node-agent pod to remove those explicit `/128`s, shared prefix routes, and stale neighbor entries from live pod runtime state

Explicit IPv6:

- `POST /explicit-ipv6-assignments/ensure`
  accepts a canonical explicit IPv6, and either reuses the currently mapped target or requires pod identity metadata such as `pod_uid` or `target_assigned_mac` to resolve the target pod before forwarding apply work to the correct node agent; the allocator first uses the node-agent IP stored on the row and only falls back to a live Kubernetes lookup if that stored endpoint is missing or stale
- `POST /explicit-ipv6-assignments/ensure-by-pod`
  accepts `pod_uid + gw_tag + mac_dev`, derives the canonical explicit IPv6 internally with counter `0000`, stores it, and forwards apply work to the correct node agent; again, the allocator prefers the node-agent IP already stored on the managed allocation row
- `POST /explicit-ipv6-assignments/applied`
  records that a node agent successfully attached the explicit IPv6 to the live pod
- `POST /explicit-ipv6-assignments/applied-batch`
  internal bulk variant used when the allocator or node agent needs to mark many explicit IPv6 rows as applied in one database update

Runtime behavior note:

- by default, the allocator queues explicit apply work on a background worker pool and returns `202 Accepted` with `applied.status = queued`
- if asynchronous explicit apply is disabled, the same endpoints wait for the node-agent call inline and can return an immediate applied result instead
- in the common asynchronous path, create requests are regrouped into allocator bulk calls to the node-agent `/explicit-ipv6/bulk-apply` endpoint
- move requests are regrouped into allocator bulk calls to the node-agent `/explicit-ipv6/bulk-move` endpoint
- for PostgreSQL-backed deployments, those explicit writes use retry-on-conflict behavior so distinct explicit IPv6 writes can progress in parallel while the unique canonical address remains protected

## 7. Configuration

Main environment variables:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_SSLMODE`
- `DATABASE_URL`
- `MAC_ALLOCATOR_HOST`
- `ALLOCATOR_HTTP_PORT`
- `NODE_AGENT_NAMESPACE`
- `NODE_AGENT_LABEL_SELECTOR`
- `NODE_AGENT_HTTP_PORT`
- `AUTO_MANAGED_EXPLICIT_TAG`
- `EXPLICIT_WRITE_RETRY_ATTEMPTS`
- `EXPLICIT_WRITE_RETRY_BASE_SECONDS`
- `ASYNC_EXPLICIT_APPLY_ENABLED`
- `ALLOCATOR_REQUEST_QUEUE_SIZE`
- `EXPLICIT_APPLY_WORKERS`
- `EXPLICIT_APPLY_BATCH_WINDOW_MS`
- `EXPLICIT_APPLY_BATCH_MAX_ITEMS`
- `EXPLICIT_MOVE_BATCH_WINDOW_MS`
- `EXPLICIT_MOVE_BATCH_MAX_ITEMS`
- `EXPLICIT_MOVE_MIN_BATCH_ITEMS`
- `EXPLICIT_MOVE_DISPATCH_SHARDS`
- `DB_POOL_MIN_SIZE`
- `DB_POOL_MAX_SIZE`
- `DB_POOL_TIMEOUT_SECONDS`
- `DB_POOL_MAX_IDLE_SECONDS`

Current in-cluster values come from:

- `k8s/net-identity-allocator-postgres-secret.yaml`
- `k8s/allocator-stack.yaml`

## 8. RBAC Context

The allocator runs with a namespace-scoped service account and role.

Current RBAC shape:

- ServiceAccount:
  `net-identity-allocator` in namespace `mac-allocator`
- Role:
  `get`, `list` on `pods` in namespace `mac-allocator`
- RoleBinding:
  binds that Role to the `net-identity-allocator` ServiceAccount

Why the allocator needs this:

- to discover the node-agent pods in namespace `mac-allocator` when a managed allocation row is first created or refreshed
- to fall back safely if a stored node-agent IP later becomes stale after a node-agent pod restart or rollout

The allocator does not have cluster-wide pod mutation rights. It does not patch arbitrary workload pods itself.

## 9. PHP Monitor

The PHP monitor is separate and consumes this allocator API.

Files:

- `CMXsafeMAC-IPv6-php-monitor/index.php`
- `CMXsafeMAC-IPv6-php-monitor/api.php`

It renders allocator information plus live traffic-collector flow data server-side for easier PHP integration.

## 10. Local Run

Build:

```powershell
docker build -t net-identity-allocator:docker-desktop-v10 .\net-identity-allocator
```

Run:

```powershell
docker run -d --name net-identity-allocator `
  -p 8080:8080 `
  -e POSTGRES_HOST=host.docker.internal `
  -e POSTGRES_PORT=5432 `
  -e POSTGRES_DB=cmxsafemac_ipv6 `
  -e POSTGRES_USER=allocator `
  -e POSTGRES_PASSWORD=change-me-in-production `
  -e ALLOCATOR_HTTP_PORT=8080 `
  net-identity-allocator:docker-desktop-v10
```

## 11. Notes

- In Kubernetes, the allocator now uses PostgreSQL inside the cluster.
- PostgreSQL is deployed as a StatefulSet with a Service and PVC.
- Credentials are supplied through [net-identity-allocator-postgres-secret.yaml](../k8s/net-identity-allocator-postgres-secret.yaml).
- Direct local runs also require a reachable PostgreSQL instance.
