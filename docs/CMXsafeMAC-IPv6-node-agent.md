# CMXsafeMAC-IPv6-node-agent

This document describes the node-local agent.

Primary file:

- `CMXsafeMAC-IPv6-node-agent/agent.py`

Supporting file:

- `CMXsafeMAC-IPv6-node-agent/Dockerfile`
- `CMXsafeMAC-IPv6-node-agent/Dockerfile.dev`
- `CMXsafeMAC-IPv6-node-agent/debug_tetragon.py`

Naming note:

- the component name is `CMXsafeMAC-IPv6-node-agent`
- Kubernetes object names and Docker image names use the lowercase-safe runtime form `cmxsafemac-ipv6-node-agent`

## 1. Responsibility

The node agent is responsible for applying network state from outside the pod.

It:

- runs as a privileged `DaemonSet`
- observes selected pods on its node
- calls the allocator service
- finds the pod sandbox PID
- enters the pod network namespace or opens its network namespace for netlink work
- sets the deterministic MAC address on the managed interface
- adds the managed allocator IPv6 on the managed interface
- adds the automatic managed `net1` IPv6 derived from `counter + 1`
- adds explicit IPv6 addresses on the secondary explicit interface
- installs prefix-level on-link routes needed for explicit IPv6 communication on that secondary interface
- batches explicit create and move work by pod so many IPv6 operations can reuse the same namespace session
- releases rows when pods are deleted

Explicit hot-path note:

- managed MAC / IPv6 assignment is the point where the agent is still allowed to learn pod/runtime details from Kubernetes and CRI
- at that moment, the agent reports the current runtime snapshot to the allocator and also records it locally
- later explicit IPv6 create / move requests reuse:
  - the allocator-stored runtime snapshot
  - the allocator-stored node-agent endpoint
  - the agent's own in-memory managed-pod registry
- so the explicit create / move path no longer needs a fresh Kubernetes pod `GET` or pod `LIST`

## 2. Trigger Sources

The node agent no longer uses a Kubernetes pod watch.

It now uses:

- Tetragon gRPC `GetEvents` as the live event source
- a low-frequency safety reconcile loop as the missed-event fallback

### 2.1 Tetragon gRPC stream

Purpose:

- primary event source for pod runtime activity

Role:

- connects directly to the Tetragon gRPC socket
- subscribes to `FineGuidanceSensors/GetEvents`
- reacts to pod `process_exec` events for create / assignment
- reacts to runtime delete execs from `containerd-shim-runc-v2` for pod release
- still accepts `process_exit` as a secondary signal when those events are usable
- schedules bounded retries for MAC / IPv6 assignment
- schedules release checks after delete-side signals

### 2.1.1 Exact Events The Agent Uses

The agent subscribes to these Tetragon event types:

- `process_exec`
- `process_exit`

The important detail is that it does not treat all of those events the same way.

Create / assignment path:

- trigger type: `process_exec`
- expected shape: the event is enriched with Kubernetes pod identity
- typical binaries seen:
  - `/kind/bin/mount-product-files.sh`
  - `/usr/bin/jq`
  - `/usr/bin/cp`
  - application-side binaries like `/bin/sh`, `/bin/sleep`, `/bin/true`
- use: when the event belongs to a managed pod, the agent resolves the pod and tries MAC / IPv6 assignment

Delete / release path:

- trigger type: `process_exec`
- expected shape: runtime teardown event from the host side, not from inside the pod
- primary binary:
  - `/usr/local/bin/containerd-shim-runc-v2`
- required argument pattern:
  - arguments contain `delete`
  - arguments contain `-id <sandbox-id>`
- use:
  - the agent extracts the sandbox ID from the arguments
  - it resolves that sandbox ID back to the Kubernetes pod through CRI metadata
  - then it runs the normal release check flow

Secondary delete-side signal:

- trigger type: `process_exit`
- use:
  - still subscribed and still accepted
  - not the main delete trigger anymore in this Docker Desktop `kind` environment

Simple mental model:

- pod `process_exec` -> assign
- runtime `containerd-shim-runc-v2 ... delete` exec -> release
- `process_exit` -> secondary / opportunistic signal

### 2.2 Safety reconcile

Purpose:

- recover if the local environment drops or delays Tetragon events

Role:

- periodically lists currently managed pods on the node
- re-applies missing MAC / IPv6 state
- marks rows `RELEASED` when the pod is still visible but already deleting or terminal
- marks rows `STALE` only when they are no longer visible and can only be inferred as orphaned

## 3. Why it is external to the pod

The target application pod does not need:

- `ip`
- `NET_ADMIN`
- root

The node agent performs network changes using:

- `crictl`
- sandbox PID lookup
- namespace entry for managed-interface work
- Python netlink operations for the optimized explicit hot path

This keeps the application image minimal.

## 4. Network Operations Performed

For managed state:

- set MAC on `MANAGED_IFACE` (currently `eth0`)
- add the managed allocator IPv6 on `MANAGED_IFACE`
- replace any old managed IPv6 from the same managed `/64`
- keep the host-peer route behavior for the managed IPv6 path
- on reconcile or agent restart, re-assert the host-side `/128` routes for managed IPv6s even when pod annotations already match, so traffic keeps working after the node agent is restarted

For explicit IPv6 state:

- require `EXPLICIT_IFACE` (currently `net1`) to exist in the pod
- add the automatic managed `net1` IPv6 in the form `AUTO_MANAGED_EXPLICIT_TAG + GW_MAC + (counter + 1) + 00..00`
- add each explicit IPv6 on `EXPLICIT_IFACE`
- keep existing explicit IPv6s
- install on-link prefix routes such as `4444::/16 dev net1`
- avoid the older per-address global route redistribution model
- serialize explicit apply/move work by canonical `requested_ipv6`, not by a broad global lock
- keep a local managed-pod registry keyed by `pod_uid` so neighbor flush and prefix fan-out can iterate local known managed pods instead of relisting pods from Kubernetes
- use persistent per-pod explicit command batchers so `addr-add`, `addr-del`, and `neigh-flush` work can be applied through reused namespace-aware netlink sessions instead of spawning one shell command per operation

Parallel explicit-assignment assumptions:

- no two concurrent requests try to assign the same canonical explicit IPv6 to different pods
- the relevant prefix route is already present on the managed pods
- under those conditions, different explicit IPv6s can be applied in parallel while the same explicit IPv6 still stays serialized for safe canonical moves

## 5. Route Behavior

Explicit IPv6 prefixes are grouped using:

- `EXPLICIT_IPV6_ROUTE_PREFIX_LEN`

Current value:

- `16`

This means prefixes such as `4444::/16`, `5555::/16`, and `6666::/16` are installed as on-link routes on the shared explicit interface across managed pods.

Practical meaning of the explicit IPv6 fields:

- `prefix-2-bytes`
  acts as the route-grouping tag in the current design when `EXPLICIT_IPV6_ROUTE_PREFIX_LEN=16`
- that makes it useful for east-west communication because explicit IPv6s that share the same first 2 bytes fall into the same route bucket on `net1`
- the explicit IPv6 itself now uses canonical counter `0000`
- the real managed allocation counter is tracked only as metadata for the currently targeted pod
- before the agent applies a canonical explicit IPv6 to a pod, it evicts that same explicit IPv6 from any other managed pod that still owns it
- when that canonical explicit IPv6 moves, the agent also flushes the matching `net1` neighbor-cache entry on the other managed pods so traffic does not keep following the old pod MAC
- this cache is the IPv6 neighbor cache used by NDP, not ARP
- tracked explicit IPv6 replays are revalidated against the latest allocator row right before reattach, so a pod skips reapplying an address that has already been moved elsewhere
- with the current single-node design, the node agent provisions one on-link route per active prefix bucket instead of pushing per-address routes for every explicit IPv6
- for the common existing-prefix case, the agent can therefore process different canonical explicit IPv6s in parallel because it only serializes by `requested_ipv6`

## 6. Configuration

Main environment variables:

- `NODE_NAME`
- `ALLOCATOR_URL`
- `GW_IFACE`
- `MANAGED_IFACE`
- `EXPLICIT_IFACE`
- `MANAGED_IPV6_PREFIX`
- `AUTO_MANAGED_EXPLICIT_TAG`
- `SELECTOR_KEY`
- `SELECTOR_VALUE`
- `HOST_SYS_PATH`
- `CONTAINER_RUNTIME_ENDPOINT`
- `TETRAGON_GRPC_ADDRESS`
- `STREAM_RESTART_SECONDS`
- `STARTUP_RETRY_SECONDS`
- `SAFETY_RECONCILE_SECONDS`
- `AGENT_HTTP_PORT`
- `AGENT_REQUEST_QUEUE_SIZE`
- `EXPLICIT_IPV6_ROUTE_PREFIX_LEN`
- `EXPLICIT_OP_BATCH_WINDOW_MS`
- `EXPLICIT_OP_BATCH_MAX_COMMANDS`
- `EXPLICIT_POD_BATCH_SHARDS`
- `EXPLICIT_MOVE_SUBBATCH_MAX_ITEMS`
- `EXPLICIT_MOVE_SUBBATCH_WORKERS`
- `EXPLICIT_BULK_MOVE_BROAD_FLUSH_THRESHOLD`
- `ASYNC_APPLIED_CALLBACK_ENABLED`
- `APPLIED_CALLBACK_BATCH_WINDOW_MS`
- `APPLIED_CALLBACK_BATCH_MAX_ITEMS`
- `APPLIED_CALLBACK_WORKERS`
- `POD_RUNTIME_CACHE_TTL_SECONDS`

These are currently set in:

- `k8s/allocator-stack.yaml`

## 7. RBAC

The node agent uses a cluster-wide service account and cluster role because managed pods can live in different namespaces.

Current RBAC shape:

- ServiceAccount:
  `cmxsafemac-ipv6-node-agent` in namespace `mac-allocator`
- ClusterRole:
  `get`, `list`, `patch` on `pods`
- ClusterRoleBinding:
  binds that ClusterRole to the `cmxsafemac-ipv6-node-agent` ServiceAccount

Why the node agent needs this:

- `get` and `list`
  to resolve managed pods by label, node, pod UID, and metadata across namespaces during managed-assignment and safety-reconcile flows
- `patch`
  to write pod annotations with the current assigned MAC, managed IPv6, explicit IPv6 state markers, and status

This cluster-wide RBAC is the reason the allocator stack does not need to be deployed in the same namespace as the managed workloads.

Important distinction:

- RBAC gives the agent Kubernetes API rights over pod objects
- it does not give the agent network namespace mutation power
- the actual host-side MAC and IPv6 changes come from the DaemonSet running as privileged with `hostPID` and host mounts

## 8. Required Privileges And Mounts

The agent needs:

- `hostPID: true`
- `privileged: true`
- host `/sys`
- host `/run/containerd`
- host `/var/run/cilium/tetragon`

Without these it cannot:

- resolve host interfaces
- talk to the runtime
- connect to the Tetragon gRPC Unix socket
- enter pod namespaces

## 9. Local Agent HTTP Endpoint

The node agent exposes:

- `GET /healthz`
  simple liveness endpoint used for quick checks and readiness-style validation that the node-local HTTP server is up
- `POST /explicit-ipv6/apply`
  receives a concrete single explicit IPv6 apply request from the allocator, validates the stored runtime snapshot locally, adds the IPv6 to the pod interface, and updates apply status back into allocator state when appropriate
- `POST /explicit-ipv6/bulk-apply`
  receives grouped create work for one target pod and applies many explicit IPv6 adds in one node-agent request
- `POST /explicit-ipv6/bulk-move`
  receives grouped move work, regroups it by old-owner, new-owner, and observer pod, and then executes the move in per-pod batches
- `POST /explicit-ipv6/clear`
  receives a list of explicit IPv6 runtime entries to remove, validates those stored runtime snapshots locally, deletes the `/128`s from `net1`, removes now-unused shared explicit prefix routes from the same pod, and flushes `net1` neighbor state so repeated benchmark scenarios can reuse the same replicas cleanly

The allocator uses these endpoints when it needs the correct node agent to attach an explicit IPv6 to a live pod, or to clear explicit runtime state during reused-replica benchmark cleanup.

Operational note:

- the allocator now queues explicit apply work by default, so node-agent explicit calls usually arrive from allocator background workers rather than from the original caller's still-open HTTP request
- the fast path now prefers `/explicit-ipv6/bulk-apply` for creates and `/explicit-ipv6/bulk-move` for moves; the single-item `/explicit-ipv6/apply` endpoint remains useful for narrow or fallback paths
- if the apply request introduces the first live use of a new prefix, the agent may still run one-time prefix-route propagation work
- if the prefix is already known, the explicit apply path is reduced to canonical-owner eviction, `/128` attachment on `net1`, optional neighbor flush, and allocator apply confirmation
- the agent no longer replays `ip -6 route replace <prefix> dev net1` on every explicit `/128` add
- prefix-route correctness is instead owned by managed setup, one-time new-prefix fan-out, and the existing repair/reconcile flows
- first-prefix fan-out and cleanup use the node agent's local managed-pod registry first, but they can also complement it from allocator-managed rows on the same node when the in-memory registry is incomplete
- the agent first tries the runtime snapshot already stored on the assignment row or in its local registry
- it validates that snapshot cheaply with `/proc`
- only if that snapshot is stale does it fall back to CRI `inspectp`
- for explicit bulk work, the agent now reuses persistent per-pod netlink sessions so repeated `addr-add`, `addr-del`, and `neigh-flush` operations for the same pod do not pay full process-spawn overhead each time

Operational note for cleanup:

- `/explicit-ipv6/clear` removes the requested `/128` addresses from the owning pods
- if a cleared prefix no longer has any active explicit rows in the allocator, the agent also retracts that distributed prefix route from the other managed pods on the node

## 10. Build

Build:

```powershell
docker build -t cmxsafemac-ipv6-node-agent:docker-desktop-v23 .\CMXsafeMAC-IPv6-node-agent
```

Use the same tag that [allocator-stack.yaml](../k8s/allocator-stack.yaml) currently references for the DaemonSet image and pair it with the current `net-identity-allocator` image tag used by the stack.

## 11. Current Runtime Behavior

Startup:

1. reconcile live pods
2. apply managed state to already-running managed pods
3. start HTTP server
4. start the safety reconcile thread
5. start the Tetragon gRPC event loop

Delete:

- clean pod deletion can become `RELEASED` when a runtime delete-triggered release check succeeds
- safety reconcile also marks visible deleting or completed pods `RELEASED`
- indirect orphan detection during safety reconciliation becomes `STALE` only when the pod is already gone

## 12. Toolbox Pod For Faster gRPC Testing

Files:

- `k8s/CMXsafeMAC-IPv6-node-agent-toolbox.yaml`
- `CMXsafeMAC-IPv6-node-agent/Dockerfile.dev`
- `CMXsafeMAC-IPv6-node-agent/debug_tetragon.py`

Purpose:

- provide a persistent privileged pod with the same host mounts and env as the node agent
- let us test Tetragon gRPC manually without rebuilding the production agent every time
- allow in-place edits and ad hoc Python experiments inside the container

Typical use:

```powershell
docker build -t cmxsafemac-ipv6-node-agent-dev:docker-desktop-v2 -f .\CMXsafeMAC-IPv6-node-agent\Dockerfile.dev .\CMXsafeMAC-IPv6-node-agent
kubectl apply -f .\k8s\CMXsafeMAC-IPv6-node-agent-toolbox.yaml
kubectl get pods -n mac-allocator -l app=cmxsafemac-ipv6-node-agent-toolbox
kubectl exec -it -n mac-allocator <toolbox-pod> -- bash
python /app/debug_tetragon.py --namespace tetragon-agent-test --timeout 30
```

Inside the toolbox pod you can:

- edit `/app/agent.py` in place with `vim`
- run `python /app/agent.py` manually
- inspect runtime state with `crictl`, `ip`, `nsenter`, `ss`, `jq`, `ping`, and `strace`

Operational note:

- the toolbox DaemonSet is for debugging only
- it sleeps by default and does not run another active agent unless you start it manually
- keep only one active agent process mutating managed pods at a time

## 13. Current gRPC Caveat

In this local Docker Desktop `kind` setup, the direct Tetragon gRPC stream is real, but `PROCESS_EXEC` is not a fully reliable startup trigger for every pod.

Observed signal:

- Tetragon metrics show a high `pod_info` fetch-failure count for `PROCESS_EXEC`

Example check:

```powershell
kubectl exec -n kube-system tetragon-nsxrj -c tetragon -- sh -lc "wget -qO- http://127.0.0.1:2112/metrics | grep 'tetragon_event_cache_fetch_failures_total{entry_type=\"pod_info\",event_type=\"PROCESS_EXEC\"}'"
```

Why it matters:

- when a fresh pod gets a usable early `PROCESS_EXEC` with pod metadata, the agent can assign MAC and IPv6 immediately
- when that exec event does not get usable pod info, the pod is only converged later by the safety reconcile loop

So the current practical model is:

- gRPC first
- pod `PROCESS_EXEC` drives most assignment
- runtime-side `containerd-shim-runc-v2 ... delete` execs drive clean release more reliably than plain `PROCESS_EXIT` in this environment
- `PROCESS_EXIT` can still converge a still-live unassigned pod when those events are available
- safety reconcile remains as the last fallback for missed startup cases
- in this local setup, safety reconcile remains the fallback if the runtime delete signal is missed
