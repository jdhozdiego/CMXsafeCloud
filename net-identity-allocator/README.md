# net-identity-allocator

This service manages deterministic pod MAC allocations, optional managed pod IPv6 addresses, explicit externally applied IPv6 addresses, and exposes the JSON API consumed by the node agent, tooling, and the separate PHP dashboard.

Detailed architecture and operations documentation:

- [docs/README.md](../docs/README.md)
- [docs/CMXsafeMAC-IPv6-architecture.md](../docs/CMXsafeMAC-IPv6-architecture.md)

## Features

- PostgreSQL-backed allocation table for the Kubernetes deployment
- deterministic MAC generation with `gw-head-4 + counter-2`
- deterministic managed IPv6 generation with `prefix-64 + (counter + 1)`
- canonical explicit IPv6 identities encoded as `Prefix + GW_MAC + 0000 + MAC_DEV`
- a higher-level `pod_uid` helper endpoint that resolves the current target pod and keeps the real managed counter only as metadata
- pod identity tracking with `namespace`, `pod_name`, `pod_uid`, and `node_name`
- HTTP endpoints to allocate, release, touch, reconcile, and list rows
- summary endpoint at `/stats`
- `POST /admin/reset-explicit` for explicit-only cleanup used by reused-replica benchmarks
- node-agent forwarding so explicit IPv6s are attached from outside the pod namespace
- background explicit apply queue enabled by default so explicit IPv6 requests usually return after the row is stored and the node-agent job is queued
- separate bulk forwarding paths for create and move so the allocator can reduce node-agent call count during larger bursts
- support for the current single-node split-interface model:
  - managed MAC and managed IPv6 on `eth0`
  - explicit IPv6 on `net1`

## Run

Build:

```powershell
docker build -t net-identity-allocator .
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
  net-identity-allocator
```

In Kubernetes, the allocator deployment now uses PostgreSQL through:

- [k8s/net-identity-allocator-postgres-secret.yaml](../k8s/net-identity-allocator-postgres-secret.yaml)
- [k8s/allocator-stack.yaml](../k8s/allocator-stack.yaml)

Example API health check:

- [http://localhost:8080/healthz](http://localhost:8080/healthz)

PHP monitor alternative:

- the PHP monitor now lives in `../CMXsafeMAC-IPv6-php-monitor/`.
- it is a separate image and reads from the existing allocator API plus the separate traffic collector.
- set `MAC_ALLOCATOR_BASE_URL` and `TRAFFIC_COLLECTOR_BASE_URL` if those APIs are not reachable at their defaults.

In Kubernetes, deploy the dedicated PHP monitor image from:

- `k8s/php-monitor-deployment.yaml`

## Example API calls

Create an allocation:

```powershell
$body = @{
  gw_mac = "3c:52:82:aa:bb:cc"
  gw_iface = "eno1"
  node_name = "worker-1"
  namespace = "default"
  pod_name = "web-0"
  pod_uid = "7b1d8d79-2f4f-46e1-9345-c0f537f8b3c9"
  container_iface = "eth0"
  ipv6_prefix = "fd42:4242:4242:10::/64"
  owner_kind = "StatefulSet"
  owner_name = "web"
  pod_ordinal = 0
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/allocations/ensure -ContentType "application/json" -Body $body
```

Optional metadata:

- `mac_dev` can still be sent and stored for correlation, but it is no longer part of the MAC format.
- the first four bytes of the assigned MAC come directly from the host gateway MAC, and only the last two bytes are used for collision indexing.
- if `ipv6_prefix` is provided, the allocator also returns `assigned_ipv6` from that `/64` using the same stable collision counter.

List the current table:

```powershell
Invoke-RestMethod http://localhost:8080/allocations
```

Create a canonical explicit IPv6 assignment:

Format:

- the IPv6 is exactly `#-2-bytes | original GW_MAC-6-bytes | 0000 | MAC_DEV-6-bytes`
- that maps directly to the 8 IPv6 hextets as:

```text
hextet1 : #
hextet2 : original GW_MAC bytes 1-2
hextet3 : original GW_MAC bytes 3-4
hextet4 : original GW_MAC bytes 5-6
hextet5 : canonical explicit counter (always 0000)
hextet6 : MAC_DEV bytes 1-2
hextet7 : MAC_DEV bytes 3-4
hextet8 : MAC_DEV bytes 5-6
```

Meaning:

- `#` is a 2-byte tag that you choose
- `canonical_gateway_mac` is the stable 6-byte logical gateway identity root when configured; otherwise the allocator falls back to the underlying gateway MAC stored in the allocation row
- `MAC_DEV` is a 6-byte value that you choose and that is stored as metadata with the explicit IPv6 request
- the explicit IPv6 itself always uses `0000`
- the real managed allocation counter is kept only as allocator metadata for the pod currently targeted by that canonical explicit IPv6

Example mapping:

```text
#                     = 1111
canonical_gateway_mac = f6:db:2b:39:78:94
MAC_DEV               = aa:bb:cc:dd:ee:02

IPv6                  = 1111:f6db:2b39:7894:0000:aabb:ccdd:ee02
```

Notes:

- for a new canonical explicit IPv6, the service needs target identity such as `pod_uid` or `target_assigned_mac`, unless that same explicit IPv6 already exists in the table and can reuse its current target mapping
- `#` is not sent as a separate JSON field when using the raw `ipv6_address` endpoint; it is encoded directly inside the IPv6 address string
- `MAC_DEV` is also not sent separately. Its 6 bytes are encoded in the last 3 hextets of the IPv6 address string.
- normal IPv6 compression is fine. For example, `1111:f6db:2b39:7894:0000:aabb:ccdd:ee02` may be rendered as `1111:f6db:2b39:7894:0:aabb:ccdd:ee02`.
- the request body can be just `ipv6_address` for an already known canonical explicit IPv6, or it can also include target identity such as `pod_uid`
- by default the allocator returns `202 Accepted` with `applied.status = queued`, because the live node-agent work runs asynchronously after the row is stored

Example:

```powershell
$body = @{
  ipv6_address = "1111:f6db:2b39:7894:0000:aabb:ccdd:ee02"
  pod_uid = "5d214bae-c5d5-42fd-9ffd-b6be791cbe6a"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/explicit-ipv6-assignments/ensure -ContentType "application/json" -Body $body
```

Higher-level alternative by pod UID:

- send `pod_uid`, `gw_tag`, and `mac_dev`
- the service uses configured `canonical_gateway_mac` as the identity root, then looks up the real managed `counter` from the active allocation row for that pod
- `gw_tag` is the 2-byte `#` tag, and `mac_dev` is the 6-byte `MAC_DEV` value
- then it builds the canonical explicit IPv6 internally as `#-2-bytes | canonical_gateway_mac-6-bytes | 0000 | MAC_DEV-6-bytes`, stores the real managed counter only as metadata, and applies it

```powershell
$body = @{
  pod_uid = "5d214bae-c5d5-42fd-9ffd-b6be791cbe6a"
  gw_tag = "1111"
  mac_dev = "aa:bb:cc:dd:ee:02"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/explicit-ipv6-assignments/ensure-by-pod -ContentType "application/json" -Body $body
```

List explicit IPv6 requests:

```powershell
Invoke-RestMethod http://localhost:8080/explicit-ipv6-assignments
```

Reset only explicit IPv6 state for one namespace and clear live runtime state too:

```powershell
$body = @{
  namespace = "mac-deployment-demo"
  clear_runtime = $true
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/admin/reset-explicit -ContentType "application/json" -Body $body
```

Release by pod UID:

```powershell
$body = @{ pod_uid = "7b1d8d79-2f4f-46e1-9345-c0f537f8b3c9" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8080/allocations/release -ContentType "application/json" -Body $body
```

Clear stale allocation rows:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8080/allocations/clear-stale -ContentType "application/json" -Body "{}"
```

The monitor page also exposes this as a `Clear Stale` button.

Status semantics:

- `ALLOCATED`: the row is active and currently assigned to a live pod
- `RELEASED`: the row was explicitly retired, for example because the agent observed pod deletion
- `STALE`: the row appears obsolete and was marked indirectly during reconciliation because no live pod matches it anymore

Mark missing pod UIDs as stale:

```powershell
$body = @{
  node_name = "worker-1"
  live_pod_uids = @("7b1d8d79-2f4f-46e1-9345-c0f537f8b3c9")
  status = "STALE"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri http://localhost:8080/reconcile/live-pods -ContentType "application/json" -Body $body
```
