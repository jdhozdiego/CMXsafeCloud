import ipaddress
import json
import os
import queue
import re
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack, contextmanager
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc
from pyroute2 import IPRoute, NetlinkError
from tetragon import events_pb2, sensors_pb2_grpc


API_HOST = os.environ["KUBERNETES_SERVICE_HOST"]
API_PORT = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
NODE_NAME = os.environ["NODE_NAME"]
ALLOCATOR_URL = os.environ.get("ALLOCATOR_URL", "http://net-identity-allocator.mac-allocator.svc.cluster.local:8080")
GW_IFACE = os.environ.get("GW_IFACE", "eth0")
HOST_SYS = os.environ.get("HOST_SYS_PATH", "/host-sys")
MANAGED_IFACE = os.environ.get("MANAGED_IFACE", os.environ.get("TARGET_IFACE", "eth0"))
EXPLICIT_IFACE = os.environ.get("EXPLICIT_IFACE", "net1")
MANAGED_IPV6_PREFIX = os.environ.get("MANAGED_IPV6_PREFIX", "").strip()
AUTO_MANAGED_EXPLICIT_TAG = str(os.environ.get("AUTO_MANAGED_EXPLICIT_TAG", "") or "").strip()
SELECTOR_KEY = os.environ.get("SELECTOR_KEY", "pods-mac-allocator/enabled")
SELECTOR_VALUE = os.environ.get("SELECTOR_VALUE", "true")
RUNTIME_ENDPOINT = os.environ.get("CONTAINER_RUNTIME_ENDPOINT", "unix:///host-run/containerd/containerd.sock")
TETRAGON_GRPC_ADDRESS = os.environ.get("TETRAGON_GRPC_ADDRESS", "unix:///var/run/cilium/tetragon/tetragon.sock")
STREAM_RESTART_SECONDS = int(os.environ.get("STREAM_RESTART_SECONDS", "3"))
STARTUP_RETRY_SECONDS = int(os.environ.get("STARTUP_RETRY_SECONDS", "5"))
SAFETY_RECONCILE_SECONDS = int(os.environ.get("SAFETY_RECONCILE_SECONDS", "15"))
AGENT_HTTP_PORT = int(os.environ.get("AGENT_HTTP_PORT", "8081"))
AGENT_REQUEST_QUEUE_SIZE = max(1, int(os.environ.get("AGENT_REQUEST_QUEUE_SIZE", "4096")))
EXPLICIT_IPV6_ROUTE_PREFIX_LEN = int(os.environ.get("EXPLICIT_IPV6_ROUTE_PREFIX_LEN", "16"))
EXPLICIT_OP_BATCH_WINDOW_MS = max(0.0, float(os.environ.get("EXPLICIT_OP_BATCH_WINDOW_MS", "5")))
EXPLICIT_OP_BATCH_MAX_COMMANDS = max(1, int(os.environ.get("EXPLICIT_OP_BATCH_MAX_COMMANDS", "256")))
EXPLICIT_POD_BATCH_SHARDS = max(1, int(os.environ.get("EXPLICIT_POD_BATCH_SHARDS", "4")))
EXPLICIT_MOVE_SUBBATCH_MAX_ITEMS = max(1, int(os.environ.get("EXPLICIT_MOVE_SUBBATCH_MAX_ITEMS", "64")))
EXPLICIT_MOVE_SUBBATCH_WORKERS = max(1, int(os.environ.get("EXPLICIT_MOVE_SUBBATCH_WORKERS", "4")))
EXPLICIT_BULK_MOVE_BROAD_FLUSH_THRESHOLD = max(
    1, int(os.environ.get("EXPLICIT_BULK_MOVE_BROAD_FLUSH_THRESHOLD", "128"))
)
ASYNC_APPLIED_CALLBACK_ENABLED = str(os.environ.get("ASYNC_APPLIED_CALLBACK_ENABLED", "true")).strip().lower() not in {
    "0",
    "false",
    "no",
}
APPLIED_CALLBACK_BATCH_WINDOW_MS = max(0.0, float(os.environ.get("APPLIED_CALLBACK_BATCH_WINDOW_MS", "10")))
APPLIED_CALLBACK_BATCH_MAX_ITEMS = max(1, int(os.environ.get("APPLIED_CALLBACK_BATCH_MAX_ITEMS", "256")))
APPLIED_CALLBACK_WORKERS = max(1, int(os.environ.get("APPLIED_CALLBACK_WORKERS", "4")))
APPLIED_CALLBACK_RETRY_DELAYS = (0.1, 0.5, 1.0, 2.0, 5.0)
MANAGE_RETRY_DELAYS = (0.0, 0.5, 1.0, 2.0, 4.0, 8.0, 15.0)
RELEASE_RETRY_DELAYS = (0.5, 1.0, 2.0, 5.0)

ASSIGNED_ANN = "pods-mac-allocator/assigned-mac"
CURRENT_ANN = "pods-mac-allocator/current-mac"
STATUS_ANN = "pods-mac-allocator/status"
# Retained only so we can clear the legacy annotation from already-managed pods.
STABLE_ANN = "pods-mac-allocator/stable-key"
MACDEV_ANN = "pods-mac-allocator/mac-dev"
ASSIGNED_IPV6_ANN = "pods-mac-allocator/assigned-ipv6"
CURRENT_IPV6_ANN = "pods-mac-allocator/current-ipv6"
IPV6_PREFIX_ANN = "pods-mac-allocator/ipv6-prefix"
AUTO_EXPLICIT_IPV6_ANN = "pods-mac-allocator/auto-explicit-ipv6"
AUTO_EXPLICIT_TAG_ANN = "pods-mac-allocator/auto-explicit-tag"


TOKEN = Path("/var/run/secrets/kubernetes.io/serviceaccount/token").read_text(encoding="utf-8").strip()
CA_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
SSL_CONTEXT = ssl.create_default_context(cafile=CA_FILE)
INFLIGHT = set()
INFLIGHT_LOCK = threading.Lock()
MANAGE_RETRYING = set()
MANAGE_RETRY_LOCK = threading.Lock()
RELEASE_CHECKING = set()
RELEASE_CHECK_LOCK = threading.Lock()
KNOWN_MANAGED_UIDS = set()
KNOWN_MANAGED_UIDS_LOCK = threading.Lock()
MANAGED_POD_REGISTRY = {}
MANAGED_POD_REGISTRY_LOCK = threading.Lock()
KNOWN_EXPLICIT_PREFIXES = set()
KNOWN_EXPLICIT_PREFIXES_LOCK = threading.Lock()
EXPLICIT_PREFIXES_SEEDED = False
EXPLICIT_PREFIXES_SEEDED_LOCK = threading.Lock()
EXPLICIT_STATE_LOCKS = {}
EXPLICIT_STATE_LOCK_REFS = {}
EXPLICIT_STATE_LOCKS_GUARD = threading.Lock()
EXPLICIT_POD_BATCHERS = {}
EXPLICIT_POD_BATCHERS_GUARD = threading.Lock()
APPLIED_CALLBACK_QUEUE = queue.Queue()
APPLIED_CALLBACK_WORKERS_STARTED = False
APPLIED_CALLBACK_WORKERS_LOCK = threading.Lock()
POD_RUNTIME_CACHE = {}
POD_RUNTIME_CACHE_LOCK = threading.Lock()
HOST_NETNS = ["nsenter", "-t", "1", "-n"]
POD_RUNTIME_CACHE_TTL_SECONDS = float(os.environ.get("POD_RUNTIME_CACHE_TTL_SECONDS", "60"))


def log(message):
    print(message, flush=True)


def elapsed_ms(start_time):
    return round((time.perf_counter() - start_time) * 1000.0, 2)


def log_explicit_trace(**fields):
    parts = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, bool):
            rendered = "1" if value else "0"
        elif isinstance(value, float):
            rendered = f"{value:.2f}"
        elif isinstance(value, int):
            rendered = str(value)
        else:
            rendered = str(value).strip().replace(" ", "_")
        if rendered:
            parts.append(f"{key}={rendered}")
    if parts:
        log("explicit-trace " + " ".join(parts))


TRACE_CONTEXT_FIELDS = (
    "trace_id",
    "trace_phase",
    "trace_batch_size",
    "trace_request_index",
    "trace_client_started_at_ms",
    "trace_allocator_sent_at_ms",
)


def current_epoch_ms():
    return int(time.time() * 1000)


def normalize_optional_trace_int(value):
    if value in (None, ""):
        return None
    return int(value)


def extract_trace_context(source):
    if not source:
        return {}
    context = {}
    for field in TRACE_CONTEXT_FIELDS:
        value = source.get(field)
        if field.endswith("_ms"):
            value = normalize_optional_trace_int(value)
        elif value not in (None, ""):
            value = str(value).strip()
        if value not in (None, ""):
            context[field] = value
    return context


def trace_log_fields(trace_context):
    if not trace_context:
        return {}
    return {
        key: trace_context[key]
        for key in ("trace_id", "trace_phase", "trace_batch_size", "trace_request_index")
        if trace_context.get(key) not in (None, "")
    }


def normalize_ipv6_address(value):
    return ipaddress.IPv6Address(str(value).strip()).compressed.lower()


def normalize_tag_hex(value):
    raw = str(value or "").strip().lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if len(raw) > 4:
        raise ValueError("Explicit IPv6 tag must fit in 2 bytes.")
    if raw and not re.fullmatch(r"[0-9a-f]+", raw):
        raise ValueError("Explicit IPv6 tag must be hexadecimal.")
    return raw.rjust(4, "0")


def build_explicit_ipv6(gw_tag_hex, gw_mac, mac_dev, counter):
    tag = bytes.fromhex(normalize_tag_hex(gw_tag_hex))
    gw_bytes = bytes.fromhex(str(gw_mac or "").strip().replace(":", ""))
    if int(counter) < 0 or int(counter) > 0xFFFF:
        raise ValueError(f"counter must fit in 2 bytes: {counter}")
    counter_bytes = int(counter).to_bytes(2, byteorder="big")
    dev_bytes = bytes.fromhex(str(mac_dev or "").strip().replace(":", ""))
    return ipaddress.IPv6Address(tag + gw_bytes + counter_bytes + dev_bytes).compressed.lower()


def auto_managed_explicit_ipv6(allocation):
    if not AUTO_MANAGED_EXPLICIT_TAG:
        return None
    if not allocation:
        return None
    counter = allocation.get("counter")
    gw_mac = allocation.get("gw_mac")
    if counter is None or not gw_mac:
        return None
    return build_explicit_ipv6(AUTO_MANAGED_EXPLICIT_TAG, gw_mac, "00:00:00:00:00:00", int(counter) + 1)


@contextmanager
def explicit_state_lock(requested_ipv6):
    key = normalize_ipv6_address(requested_ipv6)
    with EXPLICIT_STATE_LOCKS_GUARD:
        lock = EXPLICIT_STATE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            EXPLICIT_STATE_LOCKS[key] = lock
            EXPLICIT_STATE_LOCK_REFS[key] = 0
        EXPLICIT_STATE_LOCK_REFS[key] += 1
    lock.acquire()
    try:
        yield
    finally:
        lock.release()
        with EXPLICIT_STATE_LOCKS_GUARD:
            remaining = EXPLICIT_STATE_LOCK_REFS.get(key, 0) - 1
            if remaining <= 0:
                EXPLICIT_STATE_LOCK_REFS.pop(key, None)
                EXPLICIT_STATE_LOCKS.pop(key, None)
            else:
                EXPLICIT_STATE_LOCK_REFS[key] = remaining


class ExplicitPodBatchItem:
    def __init__(self, value):
        self.value = value
        self.completed = threading.Event()
        self.error = None


class ExplicitPodCommandBatcher:
    def __init__(self, batch_key, op_name):
        self.batch_key = str(batch_key or "unknown")
        self.op_name = str(op_name or "op")
        self.pid = None
        self.pid_guard = threading.Lock()
        self.base_ns_fd = None
        self.target_pid = None
        self.target_ns_inode = None
        self.target_ns_fd = None
        self.ipr = None
        self.ifindex = None
        self.queue = queue.Queue()
        self.thread = threading.Thread(
            target=self._run,
            name=f"explicit-{self.op_name}-{self.batch_key}",
            daemon=True,
        )
        self.thread.start()

    def submit(self, pid, value):
        item = ExplicitPodBatchItem(value)
        with self.pid_guard:
            self.pid = str(pid)
        self.queue.put(item)
        return item

    def _current_pid(self):
        with self.pid_guard:
            return self.pid

    def _close_netlink_session(self):
        if self.ipr is not None:
            try:
                self.ipr.close()
            except Exception:
                pass
            self.ipr = None
        if self.base_ns_fd is not None:
            try:
                os.setns(self.base_ns_fd, 0)
            except OSError:
                pass
        if self.target_ns_fd is not None:
            try:
                os.close(self.target_ns_fd)
            except OSError:
                pass
            self.target_ns_fd = None
        self.ifindex = None
        self.target_pid = None
        self.target_ns_inode = None

    def _ensure_netlink_session(self, pid):
        pid_value = int(pid)
        target_inode = netns_inode(pid_value)
        if self.ipr is not None and self.ifindex is not None and self.target_pid == pid_value and self.target_ns_inode == target_inode:
            return True
        self._close_netlink_session()
        if self.base_ns_fd is None:
            self.base_ns_fd = os.open("/proc/thread-self/ns/net", os.O_RDONLY)
        try:
            target_ns_fd = os.open(f"/proc/{pid_value}/ns/net", os.O_RDONLY)
        except OSError:
            return False
        try:
            os.setns(target_ns_fd, 0)
            ipr = IPRoute()
            link_indexes = ipr.link_lookup(ifname=EXPLICIT_IFACE)
            if not link_indexes:
                ipr.close()
                os.setns(self.base_ns_fd, 0)
                os.close(target_ns_fd)
                return False
            self.target_pid = pid_value
            self.target_ns_inode = target_inode
            self.target_ns_fd = target_ns_fd
            self.ipr = ipr
            self.ifindex = int(link_indexes[0])
            return True
        except Exception:
            try:
                os.setns(self.base_ns_fd, 0)
            except OSError:
                pass
            try:
                os.close(target_ns_fd)
            except OSError:
                pass
            raise

    def _run(self):
        self.base_ns_fd = os.open("/proc/thread-self/ns/net", os.O_RDONLY)
        while True:
            item = self.queue.get()
            batch = [item]
            if EXPLICIT_OP_BATCH_WINDOW_MS > 0:
                deadline = time.perf_counter() + (EXPLICIT_OP_BATCH_WINDOW_MS / 1000.0)
                while len(batch) < EXPLICIT_OP_BATCH_MAX_COMMANDS:
                    timeout = deadline - time.perf_counter()
                    if timeout <= 0:
                        break
                    try:
                        batch.append(self.queue.get(timeout=timeout))
                    except queue.Empty:
                        break
            pid = self._current_pid()
            error = None
            if not pid:
                error = RuntimeError(f"No pid is available for explicit {self.op_name} batching.")
            else:
                try:
                    session_ready = self._ensure_netlink_session(pid)
                    if not session_ready or self.ipr is None or self.ifindex is None:
                        if self.op_name in {"addr-del", "neigh-flush"}:
                            error = None
                        else:
                            raise RuntimeError(f"Explicit interface {EXPLICIT_IFACE} is not present")
                    else:
                        apply_explicit_netlink_batch(self.ipr, self.ifindex, self.op_name, [entry.value for entry in batch])
                except Exception as exc:
                    error = RuntimeError(
                        f"Explicit {self.op_name} batch failed for {self.batch_key}: {exc}"
                    )
            for entry in batch:
                entry.error = error
                entry.completed.set()


def explicit_pod_shard(value):
    if EXPLICIT_POD_BATCH_SHARDS <= 1:
        return 0
    normalized = normalize_ipv6_address(value)
    if not normalized:
        return 0
    return hash(normalized) % EXPLICIT_POD_BATCH_SHARDS


def explicit_pod_batcher(batch_key, op_name, shard=0):
    key = (str(batch_key or "").strip(), str(op_name or "").strip(), int(shard))
    with EXPLICIT_POD_BATCHERS_GUARD:
        batcher = EXPLICIT_POD_BATCHERS.get(key)
        if batcher is None:
            batcher = ExplicitPodCommandBatcher(f"{key[0]}-s{key[2]}", key[1])
            EXPLICIT_POD_BATCHERS[key] = batcher
        return batcher


def wait_batched_explicit_commands(items):
    first_error = None
    for item in items:
        item.completed.wait()
        if item.error and first_error is None:
            first_error = item.error
    if first_error is not None:
        raise first_error


@contextmanager
def explicit_state_locks(requested_ipv6s):
    keys = sorted(
        {
            normalize_ipv6_address(value)
            for value in (requested_ipv6s or [])
            if str(value or "").strip()
        }
    )
    with ExitStack() as stack:
        for key in keys:
            stack.enter_context(explicit_state_lock(key))
        yield keys


def submit_explicit_pod_commands(batch_key, pid, op_name, values):
    items = queue_explicit_pod_commands(batch_key, pid, op_name, values)
    wait_batched_explicit_commands(items)
    return len(items)


def queue_explicit_pod_commands(batch_key, pid, op_name, values):
    if op_name == "neigh-flush-all":
        return [explicit_pod_batcher(batch_key, op_name, 0).submit(pid, "__all__")]
    requested = sorted(
        {
            normalize_ipv6_address(value)
            for value in (values or [])
            if str(value or "").strip()
        }
    )
    if not requested:
        return []
    items = []
    for shard in range(EXPLICIT_POD_BATCH_SHARDS):
        shard_values = [value for value in requested if explicit_pod_shard(value) == shard]
        if not shard_values:
            continue
        batcher = explicit_pod_batcher(batch_key, op_name, shard)
        items.extend(batcher.submit(pid, value) for value in shard_values)
    return items


def chunked_entries(values, size):
    values = list(values or [])
    chunk_size = max(1, int(size))
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]


def json_request(method, url, body=None, content_type="application/json"):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = content_type
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = response.read()
        if not payload:
            return None
        return json.loads(payload.decode("utf-8"))


def kube_request(method, path, body=None, content_type="application/json"):
    url = f"https://{API_HOST}:{API_PORT}{path}"
    data = None
    headers = {"Accept": "application/json", "Authorization": f"Bearer {TOKEN}"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = content_type
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(request, context=SSL_CONTEXT, timeout=30) as response:
        payload = response.read()
        if not payload:
            return None
        return json.loads(payload.decode("utf-8"))


def get_pod(namespace, name):
    ns = urllib.parse.quote(str(namespace).strip(), safe="")
    pod_name = urllib.parse.quote(str(name).strip(), safe="")
    return kube_request("GET", f"/api/v1/namespaces/{ns}/pods/{pod_name}")


def get_pod_if_current(namespace, name, uid):
    try:
        pod = get_pod(namespace, name)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            invalidate_pod_runtime_cache(uid)
            return None
        raise
    metadata = pod.get("metadata") or {}
    if metadata.get("uid") != uid:
        invalidate_pod_runtime_cache(uid)
        return None
    return pod


def host_gateway_mac():
    return Path(HOST_SYS, "class", "net", GW_IFACE, "address").read_text(encoding="utf-8").strip().lower()


def matches_selector(labels):
    value = (labels or {}).get(SELECTOR_KEY)
    return str(value).strip().lower() == SELECTOR_VALUE.lower()


def label_selector():
    selector = f"{SELECTOR_KEY}={SELECTOR_VALUE}"
    return urllib.parse.quote(selector, safe="")


def field_selector():
    return urllib.parse.quote(f"spec.nodeName={NODE_NAME}", safe="")


def list_target_pods(include_terminal=False):
    path = f"/api/v1/pods?fieldSelector={field_selector()}&labelSelector={label_selector()}"
    payload = kube_request("GET", path)
    items = payload.get("items", [])
    pods = []
    for pod in items:
        meta = pod.get("metadata", {})
        phase = (pod.get("status", {}) or {}).get("phase")
        if not include_terminal:
            if meta.get("deletionTimestamp"):
                continue
            if phase not in {"Pending", "Running"}:
                continue
        if matches_selector(meta.get("labels") or {}):
            pods.append(pod)
    return pods, ((payload.get("metadata") or {}).get("resourceVersion"))


def patch_pod_annotations(namespace, name, annotations):
    body = {"metadata": {"annotations": annotations}}
    path = f"/api/v1/namespaces/{namespace}/pods/{name}"
    return kube_request("PATCH", path, body, "application/merge-patch+json")


def runtime_snapshot_from_network_info(network_info):
    if not network_info:
        return {}
    return {
        "sandbox_id": str(network_info.get("sandbox_id") or "").strip() or None,
        "sandbox_pid": int(network_info["pid"]) if network_info.get("pid") else None,
        "sandbox_pid_start_time": int(network_info["pid_start_time"]) if network_info.get("pid_start_time") else None,
        "netns_inode": int(network_info["netns_inode"]) if network_info.get("netns_inode") else None,
        "runtime_observed_at": str(network_info.get("runtime_observed_at") or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
    }


def explicit_identity_details(source):
    if not source:
        return None
    metadata = source.get("metadata") if isinstance(source, dict) else None
    base = metadata if isinstance(metadata, dict) else source
    return {
        "namespace": str(base.get("namespace") or "").strip() or None,
        "pod_name": str(base.get("pod_name") or base.get("name") or "").strip() or None,
        "pod_uid": str(base.get("pod_uid") or base.get("uid") or "").strip() or None,
        "node_name": str(source.get("node_name") or "").strip() or None,
        "assigned_mac": str(source.get("assigned_mac") or source.get("target_assigned_mac") or ((base.get("annotations") or {}).get(ASSIGNED_ANN)) or "").strip().lower() or None,
        "sandbox_id": str(source.get("sandbox_id") or "").strip() or None,
        "sandbox_pid": int(source["sandbox_pid"]) if source.get("sandbox_pid") not in (None, "") else None,
        "sandbox_pid_start_time": int(source["sandbox_pid_start_time"]) if source.get("sandbox_pid_start_time") not in (None, "") else None,
        "netns_inode": int(source["netns_inode"]) if source.get("netns_inode") not in (None, "") else None,
        "runtime_observed_at": str(source.get("runtime_observed_at") or "").strip() or None,
    }


def allocator_ensure(pod, gw_mac, network_info=None):
    metadata = pod["metadata"]
    annotations = metadata.get("annotations", {}) or {}
    body = {
        "gw_mac": gw_mac,
        "gw_iface": GW_IFACE,
        "node_name": NODE_NAME,
        "namespace": metadata["namespace"],
        "pod_name": metadata["name"],
        "pod_uid": metadata["uid"],
        "container_iface": MANAGED_IFACE,
    }
    if MANAGED_IPV6_PREFIX:
        body["ipv6_prefix"] = MANAGED_IPV6_PREFIX
    if MACDEV_ANN in annotations:
        body["mac_dev"] = annotations[MACDEV_ANN]
    owner_refs = metadata.get("ownerReferences", []) or []
    if owner_refs:
        body["owner_kind"] = owner_refs[0].get("kind")
        body["owner_name"] = owner_refs[0].get("name")
        body["owner_uid"] = owner_refs[0].get("uid")
    body.update(runtime_snapshot_from_network_info(network_info))
    url = f"{ALLOCATOR_URL}/allocations/ensure"
    return json_request("POST", url, body)


def allocator_release(pod, status="RELEASED"):
    metadata = pod.get("metadata", {}) or {}
    body = {"pod_uid": metadata.get("uid"), "status": status}
    assigned_mac = ((metadata.get("annotations") or {}).get(ASSIGNED_ANN))
    if assigned_mac:
        body["assigned_mac"] = assigned_mac
    url = f"{ALLOCATOR_URL}/allocations/release"
    return json_request("POST", url, body)


def allocator_reconcile(live_pod_uids):
    body = {"node_name": NODE_NAME, "live_pod_uids": live_pod_uids, "status": "STALE"}
    url = f"{ALLOCATOR_URL}/reconcile/live-pods"
    return json_request("POST", url, body)


def allocator_list_explicit_ipv6(assign_mac):
    params = urllib.parse.urlencode({"status": "ACTIVE", "target_assigned_mac": assign_mac})
    url = f"{ALLOCATOR_URL}/explicit-ipv6-assignments?{params}"
    return json_request("GET", url) or []


def allocator_list_all_explicit_ipv6():
    params = urllib.parse.urlencode({"status": "ACTIVE"})
    url = f"{ALLOCATOR_URL}/explicit-ipv6-assignments?{params}"
    return json_request("GET", url) or []


def allocator_list_allocations():
    url = f"{ALLOCATOR_URL}/allocations"
    return json_request("GET", url) or []


def allocator_get_explicit_ipv6(requested_ipv6):
    params = urllib.parse.urlencode({"status": "ACTIVE", "requested_ipv6": normalize_ipv6_address(requested_ipv6)})
    url = f"{ALLOCATOR_URL}/explicit-ipv6-assignments?{params}"
    rows = json_request("GET", url) or []
    return rows[0] if rows else None


def active_explicit_prefixes():
    with KNOWN_EXPLICIT_PREFIXES_LOCK:
        return sorted(KNOWN_EXPLICIT_PREFIXES)


def explicit_applied_payload(requested_ipv6, pod):
    metadata = explicit_identity_details(pod) or {}
    trace_context = extract_trace_context(pod if isinstance(pod, dict) else {})
    body = {
        "requested_ipv6": requested_ipv6,
        "namespace": metadata.get("namespace"),
        "pod_name": metadata.get("pod_name"),
        "pod_uid": metadata.get("pod_uid"),
        "node_name": metadata.get("node_name") or NODE_NAME,
        "container_iface": EXPLICIT_IFACE,
        "target_assigned_mac": metadata.get("assigned_mac"),
    }
    if trace_context:
        body.update(trace_context)
    return body


def allocator_mark_explicit_ipv6_applied(requested_ipv6, pod):
    body = explicit_applied_payload(requested_ipv6, pod)
    body["trace_node_callback_sent_at_ms"] = current_epoch_ms()
    url = f"{ALLOCATOR_URL}/explicit-ipv6-assignments/applied"
    return json_request("POST", url, body)


def allocator_mark_explicit_ipv6_applied_batch(entries):
    payloads = []
    for entry in entries:
        payload = dict(entry)
        payload["trace_node_callback_sent_at_ms"] = current_epoch_ms()
        payloads.append(payload)
    url = f"{ALLOCATOR_URL}/explicit-ipv6-assignments/applied-batch"
    return json_request("POST", url, {"entries": payloads})


class AppliedCallbackQueueItem:
    def __init__(self, payload, attempt=0):
        self.payload = dict(payload)
        self.attempt = int(attempt)


def ensure_applied_callback_workers():
    global APPLIED_CALLBACK_WORKERS_STARTED
    with APPLIED_CALLBACK_WORKERS_LOCK:
        if APPLIED_CALLBACK_WORKERS_STARTED:
            return
        APPLIED_CALLBACK_WORKERS_STARTED = True
        for index in range(APPLIED_CALLBACK_WORKERS):
            thread = threading.Thread(
                target=run_applied_callback_worker,
                args=(index,),
                name=f"explicit-applied-{index}",
                daemon=True,
            )
            thread.start()


def queue_explicit_ipv6_applied(requested_ipv6, pod):
    payload = explicit_applied_payload(requested_ipv6, pod)
    ensure_applied_callback_workers()
    APPLIED_CALLBACK_QUEUE.put(AppliedCallbackQueueItem(payload))


def requeue_applied_callback_items(items):
    for item in items:
        next_attempt = item.attempt + 1
        if next_attempt > len(APPLIED_CALLBACK_RETRY_DELAYS):
            payload = item.payload
            log(
                f"explicit-applied-callback-drop ipv6={payload.get('requested_ipv6')} "
                f"pod_uid={payload.get('pod_uid')} attempt={item.attempt}"
            )
            log_explicit_trace(
                source="node-agent-applied-error",
                requested_ipv6=payload.get("requested_ipv6"),
                pod_uid=payload.get("pod_uid"),
                error="callback-drop",
                **trace_log_fields(extract_trace_context(payload)),
            )
            continue
        time.sleep(APPLIED_CALLBACK_RETRY_DELAYS[next_attempt - 1])
        APPLIED_CALLBACK_QUEUE.put(AppliedCallbackQueueItem(item.payload, attempt=next_attempt))


def run_applied_callback_worker(worker_index):
    while True:
        item = APPLIED_CALLBACK_QUEUE.get()
        batch = [item]
        if APPLIED_CALLBACK_BATCH_WINDOW_MS > 0:
            deadline = time.perf_counter() + (APPLIED_CALLBACK_BATCH_WINDOW_MS / 1000.0)
            while len(batch) < APPLIED_CALLBACK_BATCH_MAX_ITEMS:
                timeout = deadline - time.perf_counter()
                if timeout <= 0:
                    break
                try:
                    batch.append(APPLIED_CALLBACK_QUEUE.get(timeout=timeout))
                except queue.Empty:
                    break
        started = time.perf_counter()
        try:
            allocator_mark_explicit_ipv6_applied_batch([entry.payload for entry in batch])
            callback_ms = elapsed_ms(started)
            log_explicit_trace(
                source="node-agent-applied-batch",
                batch_size=len(batch),
                total_ms=callback_ms,
            )
        except Exception as exc:
            log(
                f"explicit-applied-callback-error worker={worker_index} "
                f"batch_size={len(batch)} error={type(exc).__name__}: {exc}"
            )
            requeue_applied_callback_items(batch)


def replace_known_explicit_prefixes(prefixes):
    with KNOWN_EXPLICIT_PREFIXES_LOCK:
        KNOWN_EXPLICIT_PREFIXES.clear()
        KNOWN_EXPLICIT_PREFIXES.update(prefixes)


def refresh_known_explicit_prefixes_from_allocator():
    rows = allocator_list_all_explicit_ipv6()
    replace_known_explicit_prefixes({explicit_route_network(row["requested_ipv6"]) for row in rows})
    return rows


def seed_known_explicit_prefixes_from_allocator():
    global EXPLICIT_PREFIXES_SEEDED
    with EXPLICIT_PREFIXES_SEEDED_LOCK:
        if EXPLICIT_PREFIXES_SEEDED:
            return []
        EXPLICIT_PREFIXES_SEEDED = True
    return refresh_known_explicit_prefixes_from_allocator()


def remember_known_explicit_prefix(prefix):
    with KNOWN_EXPLICIT_PREFIXES_LOCK:
        is_new = prefix not in KNOWN_EXPLICIT_PREFIXES
        KNOWN_EXPLICIT_PREFIXES.add(prefix)
        return is_new


def pid_start_time(pid):
    payload = Path(f"/proc/{int(pid)}/stat").read_text(encoding="utf-8")
    tail = payload[payload.rfind(")") + 2 :].split()
    return int(tail[19])


def netns_inode(pid):
    return Path(f"/proc/{int(pid)}/ns/net").stat().st_ino


def runtime_cache_key(pod):
    metadata = pod.get("metadata") or {}
    return str(metadata.get("uid") or "").strip()


def invalidate_pod_runtime_cache(uid):
    if not uid:
        return
    with POD_RUNTIME_CACHE_LOCK:
        POD_RUNTIME_CACHE.pop(str(uid).strip(), None)


def cache_runtime_snapshot(uid, namespace, pod_name, sandbox_id, pid, pid_start=None, netns=None, observed_at=None):
    if POD_RUNTIME_CACHE_TTL_SECONDS <= 0:
        return None
    if not uid or not sandbox_id or not pid:
        return None
    try:
        pid_value = int(pid)
        entry = {
            "sandbox_id": str(sandbox_id).strip(),
            "pid": pid_value,
            "pid_start_time": int(pid_start) if pid_start is not None else pid_start_time(pid_value),
            "netns_inode": int(netns) if netns is not None else netns_inode(pid_value),
            "namespace": namespace,
            "pod_name": pod_name,
            "runtime_observed_at": observed_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "expires_at": time.monotonic() + POD_RUNTIME_CACHE_TTL_SECONDS,
        }
    except Exception:
        return None
    with POD_RUNTIME_CACHE_LOCK:
        POD_RUNTIME_CACHE[str(uid).strip()] = entry
    return dict(entry)


def cached_runtime_snapshot(uid, namespace=None, pod_name=None):
    if POD_RUNTIME_CACHE_TTL_SECONDS <= 0 or not uid:
        return None
    with POD_RUNTIME_CACHE_LOCK:
        entry = POD_RUNTIME_CACHE.get(str(uid).strip())
    if not entry:
        return None
    if namespace and entry.get("namespace") != namespace:
        invalidate_pod_runtime_cache(uid)
        return None
    if pod_name and entry.get("pod_name") != pod_name:
        invalidate_pod_runtime_cache(uid)
        return None
    if time.monotonic() >= entry.get("expires_at", 0):
        invalidate_pod_runtime_cache(uid)
        return None
    pid = entry.get("pid")
    try:
        if pid_start_time(pid) != entry.get("pid_start_time"):
            raise RuntimeError("pid start time changed")
        if netns_inode(pid) != entry.get("netns_inode"):
            raise RuntimeError("netns inode changed")
    except Exception:
        invalidate_pod_runtime_cache(uid)
        return None
    return {
        "sandbox_id": entry.get("sandbox_id"),
        "pid": pid,
        "pid_start_time": entry.get("pid_start_time"),
        "netns_inode": entry.get("netns_inode"),
        "runtime_observed_at": entry.get("runtime_observed_at"),
    }


def cached_pod_network_info(pod):
    uid = runtime_cache_key(pod)
    metadata = pod.get("metadata") or {}
    return cached_runtime_snapshot(uid, metadata.get("namespace"), metadata.get("name"))


def owner_pod_details(owner):
    return explicit_identity_details(owner)


def managed_registry_entry(uid):
    if not uid:
        return None
    with MANAGED_POD_REGISTRY_LOCK:
        entry = MANAGED_POD_REGISTRY.get(str(uid).strip())
    return dict(entry) if entry else None


def managed_registry_entries(exclude_uid=None):
    excluded = str(exclude_uid or "").strip() or None
    with MANAGED_POD_REGISTRY_LOCK:
        items = list(MANAGED_POD_REGISTRY.items())
    entries = []
    for uid, entry in items:
        if excluded and uid == excluded:
            continue
        if entry.get("node_name") and entry.get("node_name") != NODE_NAME:
            continue
        entries.append(dict(entry))
    return entries


def managed_runtime_identities(exclude_uid=None):
    excluded = str(exclude_uid or "").strip() or None
    identities = {}
    for entry in managed_registry_entries(exclude_uid=excluded):
        uid = str((explicit_identity_details(entry) or {}).get("pod_uid") or "").strip()
        if uid:
            identities[uid] = entry
    try:
        for row in allocator_list_allocations():
            if row.get("status") != "ALLOCATED" or str(row.get("node_name") or "").strip() != NODE_NAME:
                continue
            uid = str(row.get("pod_uid") or "").strip()
            if not uid or (excluded and uid == excluded) or uid in identities:
                continue
            identities[uid] = row
    except Exception as exc:
        log(f"managed-runtime-identities allocator refresh skipped: {exc}")
    return [dict(entry) for entry in identities.values()]


def register_managed_pod(source, network_info=None):
    entry = explicit_identity_details(source) or {}
    if network_info:
        entry.update(runtime_snapshot_from_network_info(network_info))
    uid = entry.get("pod_uid")
    if not uid:
        return None
    with MANAGED_POD_REGISTRY_LOCK:
        MANAGED_POD_REGISTRY[uid] = dict(entry)
    return dict(entry)


def retain_managed_registry_uids(uids):
    live = {str(uid).strip() for uid in uids if str(uid).strip()}
    with MANAGED_POD_REGISTRY_LOCK:
        stale = [uid for uid in MANAGED_POD_REGISTRY.keys() if uid not in live]
        for uid in stale:
            MANAGED_POD_REGISTRY.pop(uid, None)


def inspect_runtime_by_sandbox_id(sandbox_id):
    if not sandbox_id:
        return None
    payload = crictl_json("inspectp", str(sandbox_id).strip())
    info = payload.get("info") if isinstance(payload, dict) else payload[0].get("info")
    pid = info.get("pid")
    if not pid:
        return None
    pid_value = int(pid)
    return {
        "sandbox_id": str(sandbox_id).strip(),
        "pid": pid_value,
        "pid_start_time": pid_start_time(pid_value),
        "netns_inode": netns_inode(pid_value),
        "runtime_observed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def crictl_json(*args):
    command = ["crictl", "--runtime-endpoint", RUNTIME_ENDPOINT, *args]
    output = subprocess.check_output(command, text=True)
    return json.loads(output)


def sandbox_id_for_pod(pod):
    namespace = pod["metadata"]["namespace"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]
    items = crictl_json("pods", "-o", "json").get("items", [])
    for item in items:
        metadata = item.get("metadata", {}) or {}
        labels = item.get("labels", {}) or {}
        if metadata.get("name") == name and metadata.get("namespace") == namespace:
            if labels.get("io.kubernetes.pod.uid") in {None, uid}:
                return item.get("id")
    return None


def pod_network_info(pod):
    cached = cached_pod_network_info(pod)
    if cached:
        return cached
    sandbox_id = sandbox_id_for_pod(pod)
    if not sandbox_id:
        return None
    result = inspect_runtime_by_sandbox_id(sandbox_id)
    if not result:
        return None
    cache_runtime_snapshot(
        runtime_cache_key(pod),
        (pod.get("metadata") or {}).get("namespace"),
        (pod.get("metadata") or {}).get("name"),
        result.get("sandbox_id"),
        result.get("pid"),
        pid_start=result.get("pid_start_time"),
        netns=result.get("netns_inode"),
        observed_at=result.get("runtime_observed_at"),
    )
    return result


def resolve_runtime_for_identity(identity):
    details = explicit_identity_details(identity) or {}
    registry = managed_registry_entry(details.get("pod_uid"))
    if registry:
        merged = dict(registry)
        merged.update({key: value for key, value in details.items() if value not in (None, "")})
        details = merged
    cached = cached_runtime_snapshot(details.get("pod_uid"), details.get("namespace"), details.get("pod_name"))
    if cached:
        details.update(runtime_snapshot_from_network_info(cached))
        return details, cached
    pid = details.get("sandbox_pid")
    start_time = details.get("sandbox_pid_start_time")
    inode = details.get("netns_inode")
    if pid and start_time is not None and inode is not None:
        try:
            if pid_start_time(pid) == int(start_time) and netns_inode(pid) == int(inode):
                runtime = {
                    "sandbox_id": details.get("sandbox_id"),
                    "pid": int(pid),
                    "pid_start_time": int(start_time),
                    "netns_inode": int(inode),
                    "runtime_observed_at": details.get("runtime_observed_at"),
                }
                cache_runtime_snapshot(
                    details.get("pod_uid"),
                    details.get("namespace"),
                    details.get("pod_name"),
                    runtime.get("sandbox_id"),
                    runtime.get("pid"),
                    pid_start=runtime.get("pid_start_time"),
                    netns=runtime.get("netns_inode"),
                    observed_at=runtime.get("runtime_observed_at"),
                )
                register_managed_pod(details, runtime)
                return details, runtime
        except Exception:
            invalidate_pod_runtime_cache(details.get("pod_uid"))
    refreshed = inspect_runtime_by_sandbox_id(details.get("sandbox_id"))
    if not refreshed:
        return details, None
    details.update(runtime_snapshot_from_network_info(refreshed))
    cache_runtime_snapshot(
        details.get("pod_uid"),
        details.get("namespace"),
        details.get("pod_name"),
        refreshed.get("sandbox_id"),
        refreshed.get("pid"),
        pid_start=refreshed.get("pid_start_time"),
        netns=refreshed.get("netns_inode"),
        observed_at=refreshed.get("runtime_observed_at"),
    )
    register_managed_pod(details, refreshed)
    return details, refreshed


def can_enter_pid(pid, iface=MANAGED_IFACE):
    probe = subprocess.run(
        ["nsenter", "-t", str(pid), "-n", "ip", "link", "show", "dev", iface],
        text=True,
        capture_output=True,
    )
    return probe.returncode == 0


def interface_exists(pid, iface):
    return can_enter_pid(pid, iface=iface)


def read_int(path):
    return int(Path(path).read_text(encoding="utf-8").strip())


def container_peer_ifindex(pid, iface):
    output = subprocess.check_output(
        ["nsenter", "-t", str(pid), "-n", "ip", "-o", "link", "show", "dev", iface],
        text=True,
    ).strip()
    match = re.search(r"@if(\d+):", output)
    if match:
        return int(match.group(1))
    output = subprocess.check_output(
        ["nsenter", "-t", str(pid), "-n", "cat", f"/sys/class/net/{iface}/iflink"],
        text=True,
    )
    return int(output.strip())


def host_interface_name_by_index(ifindex):
    for candidate in Path(HOST_SYS, "class", "net").iterdir():
        try:
            current = read_int(candidate / "ifindex")
        except (FileNotFoundError, ValueError):
            continue
        if current == int(ifindex):
            return candidate.name
    return None


def host_peer_for_container_iface(pid, iface):
    iflink = container_peer_ifindex(pid, iface)
    host_iface = host_interface_name_by_index(iflink)
    if not host_iface:
        raise RuntimeError(f"Unable to determine host peer for container interface {iface}")
    return host_iface


def host_link_local(host_veth):
    output = subprocess.check_output(
        [*HOST_NETNS, "ip", "-6", "-o", "addr", "show", "dev", host_veth, "scope", "link"],
        text=True,
    )
    match = re.search(r"inet6 ([0-9a-f:]+)/\d+", output, re.IGNORECASE)
    if not match:
        raise RuntimeError(f"Unable to find link-local IPv6 on host interface {host_veth}")
    return match.group(1).lower()


def ensure_host_ipv6_forwarding():
    subprocess.check_call([*HOST_NETNS, "sh", "-lc", "echo 1 > /proc/sys/net/ipv6/conf/all/forwarding"])


def set_mac(pid, assigned_mac):
    direct = subprocess.run(
        ["nsenter", "-t", str(pid), "-n", "ip", "link", "set", "dev", MANAGED_IFACE, "address", assigned_mac],
        text=True,
        capture_output=True,
    )
    if direct.returncode == 0:
        return
    recovery = (
        f"ip link set dev {MANAGED_IFACE} down && "
        f"ip link set dev {MANAGED_IFACE} address {assigned_mac} && "
        f"ip link set dev {MANAGED_IFACE} up"
    )
    subprocess.check_call(["nsenter", "-t", str(pid), "-n", "sh", "-lc", recovery])


def current_global_ipv6s(pid, iface):
    output = subprocess.check_output(
        ["nsenter", "-t", str(pid), "-n", "ip", "-6", "-o", "addr", "show", "dev", iface, "scope", "global"],
        text=True,
    )
    addresses = []
    for line in output.splitlines():
        match = re.search(r"inet6 ([0-9a-f:]+)/(\d+)", line, re.IGNORECASE)
        if not match:
            continue
        addresses.append((match.group(1).lower(), int(match.group(2))))
    return addresses


def netlink_ignore_missing(exc):
    if not isinstance(exc, NetlinkError):
        return False
    return int(getattr(exc, "code", -1)) in {2, 3, 6, 19, 22, 99}

def apply_explicit_netlink_batch(ipr, ifindex, op_name, values):
    requested = [normalize_ipv6_address(value) for value in values if str(value or "").strip()]
    if op_name != "neigh-flush-all" and not requested:
        return
    requested = sorted(set(requested))
    if op_name == "addr-add":
        for address in requested:
            try:
                ipr.addr("add", index=ifindex, address=address, prefixlen=128)
            except NetlinkError as exc:
                if int(getattr(exc, "code", -1)) not in {17}:
                    raise
        return
    if op_name == "addr-del":
        for address in requested:
            try:
                ipr.addr("del", index=ifindex, address=address, prefixlen=128)
            except NetlinkError as exc:
                if not netlink_ignore_missing(exc):
                    raise
        return
    if op_name == "neigh-flush":
        for address in requested:
            try:
                ipr.neigh("del", dst=address, ifindex=ifindex, family=socket.AF_INET6)
            except NetlinkError as exc:
                if not netlink_ignore_missing(exc):
                    raise
        return
    if op_name == "neigh-flush-all":
        for neighbor in ipr.get_neighbours(ifindex=ifindex, family=socket.AF_INET6):
            attrs = dict(neighbor.get("attrs") or [])
            dst = normalize_ipv6_address(attrs.get("NDA_DST"))
            if not dst:
                continue
            try:
                ipr.neigh("del", dst=dst, ifindex=ifindex, family=socket.AF_INET6)
            except NetlinkError as exc:
                if not netlink_ignore_missing(exc):
                    raise
        return
    raise RuntimeError(f"Unsupported explicit batch op: {op_name}")


def current_managed_ipv6s(pid, iface, network):
    current = []
    for address, prefix_len in current_global_ipv6s(pid, iface):
        if ipaddress.IPv6Address(address) in network:
            current.append((address, prefix_len))
    return current


def apply_managed_ipv6_address(pid, ipv6_address, route_network=None, replace_network=None):
    host_veth = host_peer_for_container_iface(pid, MANAGED_IFACE)
    ensure_host_ipv6_forwarding()
    host_peer_ll = host_link_local(host_veth)
    desired = normalize_ipv6_address(ipv6_address)
    subprocess.check_call([*HOST_NETNS, "ip", "-6", "route", "replace", f"{desired}/128", "dev", host_veth])
    if replace_network:
        network = ipaddress.IPv6Network(replace_network, strict=False)
        for address, prefix_len in current_managed_ipv6s(pid, MANAGED_IFACE, network):
            if address != desired:
                subprocess.check_call(
                    ["nsenter", "-t", str(pid), "-n", "ip", "-6", "addr", "del", f"{address}/{prefix_len}", "dev", MANAGED_IFACE]
                )
    current = current_global_ipv6s(pid, MANAGED_IFACE)
    if not any(address == desired for address, _ in current):
        subprocess.check_call(
            ["nsenter", "-t", str(pid), "-n", "ip", "-6", "addr", "add", f"{desired}/128", "dev", MANAGED_IFACE]
        )
    if route_network:
        subprocess.check_call(
            ["nsenter", "-t", str(pid), "-n", "ip", "-6", "route", "replace", route_network, "via", host_peer_ll, "dev", MANAGED_IFACE]
        )


def ensure_onlink_route(pid, route_network, iface):
    subprocess.check_call(
        ["nsenter", "-t", str(pid), "-n", "ip", "-6", "route", "replace", route_network, "dev", iface]
    )


def apply_explicit_ipv6_address(pid, ipv6_address, route_network=None):
    desired = normalize_ipv6_address(ipv6_address)
    current = current_global_ipv6s(pid, EXPLICIT_IFACE)
    if not any(address == desired for address, _ in current):
        subprocess.check_call(
            ["nsenter", "-t", str(pid), "-n", "ip", "-6", "addr", "add", f"{desired}/128", "dev", EXPLICIT_IFACE]
        )
    if route_network:
        ensure_onlink_route(pid, route_network, EXPLICIT_IFACE)


def remove_explicit_ipv6_address(pid, ipv6_address):
    desired = normalize_ipv6_address(ipv6_address)
    current = current_global_ipv6s(pid, EXPLICIT_IFACE)
    for address, prefix_len in current:
        if address != desired:
            continue
        subprocess.check_call(
            ["nsenter", "-t", str(pid), "-n", "ip", "-6", "addr", "del", f"{desired}/{prefix_len}", "dev", EXPLICIT_IFACE]
        )
        return True
    return False


def delete_explicit_route(pid, route_network):
    route_network = str(route_network or "").strip()
    if not route_network:
        return False
    result = subprocess.run(
        ["nsenter", "-t", str(pid), "-n", "ip", "-6", "route", "del", route_network, "dev", EXPLICIT_IFACE],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def flush_all_explicit_neighbors(pid):
    subprocess.run(
        ["nsenter", "-t", str(pid), "-n", "ip", "-6", "neigh", "flush", "dev", EXPLICIT_IFACE],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def set_ipv6(pid, network_info, assigned_ipv6, allocation=None):
    if not MANAGED_IPV6_PREFIX or not assigned_ipv6:
        managed_applied = False
    else:
        apply_managed_ipv6_address(
            pid,
            assigned_ipv6,
            route_network=MANAGED_IPV6_PREFIX,
            replace_network=MANAGED_IPV6_PREFIX,
        )
        managed_applied = True
    auto_explicit_ipv6 = auto_managed_explicit_ipv6(allocation)
    if auto_explicit_ipv6 and interface_exists(pid, EXPLICIT_IFACE):
        apply_explicit_ipv6_address(
            pid,
            auto_explicit_ipv6,
            route_network=explicit_route_network(auto_explicit_ipv6),
        )
    return {
        "managed_ipv6": assigned_ipv6 if managed_applied else None,
        "auto_explicit_ipv6": auto_explicit_ipv6,
    }


def explicit_route_network(ipv6_address):
    address = ipaddress.IPv6Address(normalize_ipv6_address(ipv6_address))
    network = ipaddress.IPv6Network((address, EXPLICIT_IPV6_ROUTE_PREFIX_LEN), strict=False)
    return network.compressed


def sync_explicit_prefix_routes_for_pod(pod, pid=None, network_info=None):
    network_info = network_info or (pod_network_info(pod) or {})
    pid = pid if pid and can_enter_pid(pid) else network_info.get("pid")
    if not pid:
        return False
    if not interface_exists(pid, EXPLICIT_IFACE):
        raise RuntimeError(f"Explicit interface {EXPLICIT_IFACE} is not present")
    prefixes = active_explicit_prefixes()
    for prefix in prefixes:
        ensure_onlink_route(pid, prefix, EXPLICIT_IFACE)
    return True


def sync_explicit_prefix_route_to_all_pods(route_network):
    for entry in managed_runtime_identities():
        details = explicit_identity_details(entry) or {}
        try:
            details, runtime = resolve_runtime_for_identity(entry)
            pid = (runtime or {}).get("pid")
            if not pid or not interface_exists(pid, EXPLICIT_IFACE):
                continue
            ensure_onlink_route(pid, route_network, EXPLICIT_IFACE)
        except Exception as exc:
            log(
                f"explicit-route-sync skipped {details.get('namespace')}/{details.get('pod_name')} "
                f"for prefix={route_network}: {exc}"
            )


def clear_explicit_prefix_route_from_all_pods(route_network):
    for entry in managed_runtime_identities():
        details = explicit_identity_details(entry) or {}
        try:
            details, runtime = resolve_runtime_for_identity(entry)
            pid = (runtime or {}).get("pid")
            if not pid or not interface_exists(pid, EXPLICIT_IFACE):
                continue
            delete_explicit_route(pid, route_network)
        except Exception as exc:
            log(
                f"explicit-route-clear skipped {details.get('namespace')}/{details.get('pod_name')} "
                f"for prefix={route_network}: {exc}"
            )


def sync_explicit_prefix_routes_all_pods():
    for entry in managed_runtime_identities():
        details = explicit_identity_details(entry) or {}
        try:
            details, runtime = resolve_runtime_for_identity(entry)
            pid = (runtime or {}).get("pid")
            if not pid:
                continue
            for prefix in active_explicit_prefixes():
                ensure_onlink_route(pid, prefix, EXPLICIT_IFACE)
        except Exception as exc:
            log(
                f"explicit-route-sync skipped {details.get('namespace')}/{details.get('pod_name')}: {exc}"
            )


def set_explicit_ipv6(pid, network_info, requested_ipv6):
    # The steady-state explicit hot path only adds/removes /128 addresses.
    # Prefix-route correctness is owned by managed setup, one-time new-prefix
    # fan-out, and repair/reconcile flows rather than replaying route replace
    # on every explicit assignment.
    submit_explicit_pod_commands(f"pid-{pid}", pid, "addr-add", [requested_ipv6])


def evict_explicit_ipv6_from_previous_owner(requested_ipv6, previous_owner, target_uid):
    requested_ipv6 = normalize_ipv6_address(requested_ipv6)
    owner = owner_pod_details(previous_owner)
    if not owner:
        return []
    owner_uid = owner.get("pod_uid")
    if not owner_uid or owner_uid == target_uid:
        return []
    owner_node = owner.get("node_name")
    if owner_node and owner_node != NODE_NAME:
        log(
            f"explicit-ipv6-evict-direct skipped {requested_ipv6} "
            f"reason=remote-owner owner_node={owner_node} target_node={NODE_NAME}"
        )
        return []
    owner, runtime = resolve_runtime_for_identity(owner)
    namespace = owner.get("namespace")
    name = owner.get("pod_name")
    pid = (runtime or {}).get("pid")
    if not pid:
        return []
    submit_explicit_pod_commands(owner_uid or f"pid-{pid}", pid, "addr-del", [requested_ipv6])
    log(f"explicit-ipv6-evicted {requested_ipv6} from {namespace}/{name} uid={owner_uid} mode=python-batched")
    return [{"namespace": namespace, "pod_name": name, "pod_uid": owner_uid}]


def evict_explicit_ipv6_from_other_pods(requested_ipv6, target_uid):
    requested_ipv6 = normalize_ipv6_address(requested_ipv6)
    removed = []
    queued = []
    for entry in managed_registry_entries(exclude_uid=target_uid):
        details = explicit_identity_details(entry) or {}
        runtime = None
        uid = details.get("pod_uid")
        namespace = details.get("namespace")
        name = details.get("pod_name")
        try:
            details, runtime = resolve_runtime_for_identity(entry)
            uid = details.get("pod_uid")
            namespace = details.get("namespace")
            name = details.get("pod_name")
            pid = (runtime or {}).get("pid")
            if not pid:
                continue
            items = queue_explicit_pod_commands(uid or f"pid-{pid}", pid, "addr-del", [requested_ipv6])
            queued.extend((item, {"namespace": namespace, "pod_name": name, "pod_uid": uid}) for item in items)
        except Exception as exc:
            log(f"explicit-ipv6-evict skipped {namespace}/{name} for {requested_ipv6}: {exc}")
    wait_batched_explicit_commands([item for item, _ in queued])
    for _, pod_ref in queued:
        removed.append(pod_ref)
        log(
            f"explicit-ipv6-evicted {requested_ipv6} from "
            f"{pod_ref['namespace']}/{pod_ref['pod_name']} uid={pod_ref['pod_uid']} mode=python-batched"
        )
    return removed


def flush_explicit_ipv6_neighbors(requested_ipv6, target_uid=None):
    requested_ipv6 = normalize_ipv6_address(requested_ipv6)
    flushed = []
    queued = []
    for entry in managed_registry_entries(exclude_uid=target_uid):
        details = explicit_identity_details(entry) or {}
        runtime = None
        uid = details.get("pod_uid")
        namespace = details.get("namespace")
        name = details.get("pod_name")
        try:
            details, runtime = resolve_runtime_for_identity(entry)
            uid = details.get("pod_uid")
            namespace = details.get("namespace")
            name = details.get("pod_name")
            pid = (runtime or {}).get("pid")
            if not pid:
                continue
            items = queue_explicit_pod_commands(uid or f"pid-{pid}", pid, "neigh-flush", [requested_ipv6])
            queued.extend((item, {"namespace": namespace, "pod_name": name, "pod_uid": uid}) for item in items)
        except Exception as exc:
            log(f"explicit-ipv6-neigh-flush skipped {namespace}/{name} for {requested_ipv6}: {exc}")
    wait_batched_explicit_commands([item for item, _ in queued])
    for _, pod_ref in queued:
        flushed.append(pod_ref)
    if flushed:
        log(f"explicit-ipv6-neigh-flush {requested_ipv6} on {len(flushed)} managed pods")
    return flushed


def begin_inflight(uid):
    with INFLIGHT_LOCK:
        if uid in INFLIGHT:
            return False
        INFLIGHT.add(uid)
        return True


def end_inflight(uid):
    with INFLIGHT_LOCK:
        INFLIGHT.discard(uid)


def desired_state_matches(annotations, assigned_mac, assigned_ipv6=None):
    matches = (
        annotations.get(ASSIGNED_ANN) == assigned_mac
        and annotations.get(CURRENT_ANN) == assigned_mac
        and annotations.get(STATUS_ANN) == "assigned"
    )
    if not MANAGED_IPV6_PREFIX:
        return matches
    return (
        matches
        and annotations.get(ASSIGNED_IPV6_ANN) == assigned_ipv6
        and annotations.get(CURRENT_IPV6_ANN) == assigned_ipv6
        and annotations.get(IPV6_PREFIX_ANN) == MANAGED_IPV6_PREFIX
    )


def annotations_show_assignment(annotations):
    annotations = annotations or {}
    if annotations.get(STATUS_ANN) != "assigned":
        return False
    if not annotations.get(ASSIGNED_ANN) or not annotations.get(CURRENT_ANN):
        return False
    if MANAGED_IPV6_PREFIX and (not annotations.get(ASSIGNED_IPV6_ANN) or not annotations.get(CURRENT_IPV6_ANN)):
        return False
    return True


def apply_tracked_explicit_ipv6s(pod, pid, network_info, assigned_mac, source):
    current_explicit = {address for address, _ in current_global_ipv6s(pid, EXPLICIT_IFACE)}
    for assignment in allocator_list_explicit_ipv6(assigned_mac):
        requested_ipv6 = normalize_ipv6_address(assignment["requested_ipv6"])
        with explicit_state_lock(requested_ipv6):
            latest = allocator_get_explicit_ipv6(requested_ipv6)
            if not latest:
                log(
                    f"explicit-ipv6-skip {pod['metadata']['namespace']}/{pod['metadata']['name']} "
                    f"uid={pod['metadata']['uid']} ipv6={requested_ipv6} reason=missing-row"
                )
                continue
            if latest.get("target_assigned_mac") != assigned_mac or latest.get("pod_uid") != pod["metadata"]["uid"]:
                log(
                    f"explicit-ipv6-skip {pod['metadata']['namespace']}/{pod['metadata']['name']} "
                    f"uid={pod['metadata']['uid']} ipv6={requested_ipv6} "
                    f"reason=target-moved target_uid={latest.get('pod_uid')} target_mac={latest.get('target_assigned_mac')}"
                )
                continue
            route_network = explicit_route_network(requested_ipv6)
            if requested_ipv6 in current_explicit:
                if remember_known_explicit_prefix(route_network):
                    sync_explicit_prefix_route_to_all_pods(route_network)
                continue
            evicted = evict_explicit_ipv6_from_other_pods(requested_ipv6, pod["metadata"]["uid"])
            set_explicit_ipv6(pid, network_info, requested_ipv6)
            current_explicit.add(requested_ipv6)
            if evicted:
                flush_explicit_ipv6_neighbors(requested_ipv6, target_uid=pod["metadata"]["uid"])
            if ASYNC_APPLIED_CALLBACK_ENABLED:
                queue_explicit_ipv6_applied(requested_ipv6, pod)
            else:
                allocator_mark_explicit_ipv6_applied(requested_ipv6, pod)
            log(
                f"explicit-ipv6 {pod['metadata']['namespace']}/{pod['metadata']['name']} "
                f"uid={pod['metadata']['uid']} ipv6={requested_ipv6} source={source}"
            )


def apply_explicit_ipv6_request(target, requested_ipv6, previous_owner=None, preferred_pid=None, source="api"):
    total_started = time.perf_counter()
    runtime_started = time.perf_counter()
    trace_context = extract_trace_context(target)
    skip_allocator_applied_callback = bool(target.get("skip_allocator_applied_callback")) if isinstance(target, dict) else False
    details, network_info = resolve_runtime_for_identity(target)
    resolve_runtime_ms = elapsed_ms(runtime_started)
    namespace = details.get("namespace")
    name = details.get("pod_name")
    uid = details.get("pod_uid")
    pid = preferred_pid if preferred_pid and can_enter_pid(preferred_pid) else (network_info or {}).get("pid")
    if not pid:
        raise RuntimeError(f"No usable pid is available yet for {namespace}/{name}.")
    requested_ipv6 = normalize_ipv6_address(requested_ipv6)
    lock_wait_started = time.perf_counter()
    with explicit_state_lock(requested_ipv6):
        lock_wait_ms = elapsed_ms(lock_wait_started)
        route_network = explicit_route_network(requested_ipv6)
        evict_started = time.perf_counter()
        evicted = evict_explicit_ipv6_from_previous_owner(requested_ipv6, previous_owner, uid)
        evict_ms = elapsed_ms(evict_started)
        set_started = time.perf_counter()
        set_explicit_ipv6(pid, network_info, requested_ipv6)
        set_ms = elapsed_ms(set_started)
        flush_ms = 0.0
        if evicted:
            flush_started = time.perf_counter()
            flush_explicit_ipv6_neighbors(requested_ipv6, target_uid=uid)
            flush_ms = elapsed_ms(flush_started)
        register_managed_pod(details, network_info)
        mark_applied_started = time.perf_counter()
        if skip_allocator_applied_callback:
            pass
        elif ASYNC_APPLIED_CALLBACK_ENABLED:
            queue_explicit_ipv6_applied(requested_ipv6, {**details, **trace_context})
        else:
            allocator_mark_explicit_ipv6_applied(requested_ipv6, {**details, **trace_context})
        mark_applied_ms = elapsed_ms(mark_applied_started)
        prefix_sync_ms = 0.0
        if remember_known_explicit_prefix(route_network):
            prefix_sync_started = time.perf_counter()
            sync_explicit_prefix_route_to_all_pods(route_network)
            prefix_sync_ms = elapsed_ms(prefix_sync_started)
        log(f"explicit-ipv6 {namespace}/{name} uid={uid} ipv6={requested_ipv6} source={source}")
        log_explicit_trace(
            source="node-agent",
            requested_ipv6=requested_ipv6,
            pod_uid=uid,
            namespace=namespace,
            pod_name=name,
            lock_wait_ms=lock_wait_ms,
            resolve_runtime_ms=resolve_runtime_ms,
            evict_ms=evict_ms,
            evicted_count=len(evicted),
            set_ms=set_ms,
            flush_ms=flush_ms,
            mark_applied_ms=mark_applied_ms,
            prefix_sync_ms=prefix_sync_ms,
            total_ms=elapsed_ms(total_started),
            **trace_log_fields(trace_context),
        )
        return {
            "status": "applied",
            "namespace": namespace,
            "pod_name": name,
            "pod_uid": uid,
            "node_name": NODE_NAME,
            "requested_ipv6": requested_ipv6,
            "route_network": route_network,
            "evicted": evicted,
        }


def apply_explicit_ipv6_move_requests_bulk(entries, source="api-move-bulk"):
    if not isinstance(entries, list):
        raise ValueError("entries must be a list.")
    total_started = time.perf_counter()
    resolve_started = time.perf_counter()
    resolved_cache = {}
    prepared = []
    results = []

    def resolve_cached(identity):
        details = explicit_identity_details(identity) or {}
        key = str(details.get("pod_uid") or f"{details.get('namespace')}/{details.get('pod_name')}").strip()
        cached = resolved_cache.get(key)
        if cached is not None:
            return cached
        cached = resolve_runtime_for_identity(identity)
        resolved_cache[key] = cached
        return cached

    for raw_entry in entries:
        trace_context = extract_trace_context(raw_entry if isinstance(raw_entry, dict) else None)
        requested_ipv6 = None
        try:
            if not isinstance(raw_entry, dict):
                raise ValueError("entry must be an object")
            requested_ipv6 = normalize_ipv6_address(raw_entry.get("requested_ipv6") or raw_entry.get("ipv6_address"))
            if not requested_ipv6:
                raise ValueError("requested_ipv6 is required")
            target = raw_entry.get("target")
            if not isinstance(target, dict):
                raise ValueError("target must be an object")
            target_details, target_runtime = resolve_cached(target)
            target_uid = target_details.get("pod_uid")
            target_pid = (target_runtime or {}).get("pid")
            if not target_pid:
                raise RuntimeError(
                    f"No usable pid is available yet for {target_details.get('namespace')}/{target_details.get('pod_name')}."
                )
            register_managed_pod(target_details, target_runtime)
            route_network = explicit_route_network(requested_ipv6)
            previous_owner = owner_pod_details(raw_entry.get("previous_owner"))
            owner_details = None
            owner_runtime = None
            owner_pid = None
            owner_uid = str((previous_owner or {}).get("pod_uid") or "").strip() or None
            owner_node = str((previous_owner or {}).get("node_name") or "").strip() or None
            if previous_owner and owner_uid and owner_uid != target_uid and (not owner_node or owner_node == NODE_NAME):
                owner_details, owner_runtime = resolve_cached(previous_owner)
                owner_pid = (owner_runtime or {}).get("pid")
            prepared.append(
                {
                    "requested_ipv6": requested_ipv6,
                    "trace_context": trace_context,
                    "target_details": target_details,
                    "target_runtime": target_runtime,
                    "target_pid": target_pid,
                    "target_uid": target_uid,
                    "target_batch_key": target_uid or f"pid-{target_pid}",
                    "previous_owner_details": owner_details,
                    "previous_owner_runtime": owner_runtime,
                    "previous_owner_pid": owner_pid,
                    "previous_owner_uid": owner_uid,
                    "route_network": route_network,
                    "evicted": [],
                }
            )
        except Exception as exc:
            log_explicit_trace(
                source="node-agent-error",
                requested_ipv6=requested_ipv6,
                total_ms=elapsed_ms(total_started),
                error=type(exc).__name__,
                error_message=str(exc),
                batch_size=len(entries),
                **trace_log_fields(trace_context),
            )
            results.append(
                {
                    "status": "error",
                    "requested_ipv6": requested_ipv6,
                    "error": str(exc),
                }
            )

    resolve_runtime_ms = elapsed_ms(resolve_started)
    if not prepared:
        return {
            "status": "partial" if results else "applied",
            "applied_count": 0,
            "failed_count": len(results),
            "results": results,
        }
    observer_infos = []
    for observer in managed_registry_entries():
        observer_hint = explicit_identity_details(observer) or {}
        try:
            observer_details, observer_runtime = resolve_cached(observer)
            observer_pid = (observer_runtime or {}).get("pid")
            if not observer_pid:
                continue
            observer_infos.append(
                {
                    "uid": observer_details.get("pod_uid"),
                    "pid": observer_pid,
                    "batch_key": observer_details.get("pod_uid") or f"pid-{observer_pid}",
                    "namespace": observer_details.get("namespace"),
                    "pod_name": observer_details.get("pod_name"),
                }
            )
        except Exception as exc:
            log(
                f"explicit-ipv6-neigh-flush skipped "
                f"{observer_hint.get('namespace')}/{observer_hint.get('pod_name')} for move setup: {exc}"
            )

    def execute_move_subbatch(subbatch_entries):
        sub_started = time.perf_counter()
        all_requested_ipv6s = [entry["requested_ipv6"] for entry in subbatch_entries]
        route_networks = sorted({entry["route_network"] for entry in subbatch_entries})
        target_uid_to_addresses = {}
        for entry in subbatch_entries:
            target_uid_to_addresses.setdefault(entry["target_uid"], set()).add(entry["requested_ipv6"])

        with explicit_state_locks(all_requested_ipv6s):
            evict_started = time.perf_counter()
            delete_groups = {}
            for entry in subbatch_entries:
                owner_pid = entry["previous_owner_pid"]
                if not owner_pid:
                    continue
                owner_details = entry["previous_owner_details"] or {}
                owner_uid = entry["previous_owner_uid"] or f"pid-{owner_pid}"
                group = delete_groups.setdefault(
                    owner_uid,
                    {
                        "pid": owner_pid,
                        "batch_key": owner_uid,
                        "requested_ipv6s": set(),
                    },
                )
                group["requested_ipv6s"].add(entry["requested_ipv6"])
                entry["evicted"] = [
                    {
                        "namespace": owner_details.get("namespace"),
                        "pod_name": owner_details.get("pod_name"),
                        "pod_uid": owner_details.get("pod_uid"),
                    }
                ]
            queued_delete_items = []
            try:
                for group in delete_groups.values():
                    queued_delete_items.extend(
                        queue_explicit_pod_commands(group["batch_key"], group["pid"], "addr-del", group["requested_ipv6s"])
                    )
                wait_batched_explicit_commands(queued_delete_items)
            except Exception as exc:
                raise RuntimeError(f"evict phase failed: {exc}") from exc
            evict_ms = elapsed_ms(evict_started)

            set_started = time.perf_counter()
            add_groups = {}
            for entry in subbatch_entries:
                group = add_groups.setdefault(
                    entry["target_batch_key"],
                    {
                        "pid": entry["target_pid"],
                        "batch_key": entry["target_batch_key"],
                        "requested_ipv6s": set(),
                    },
                )
                group["requested_ipv6s"].add(entry["requested_ipv6"])
            queued_add_items = []
            try:
                for group in add_groups.values():
                    queued_add_items.extend(
                        queue_explicit_pod_commands(group["batch_key"], group["pid"], "addr-add", group["requested_ipv6s"])
                    )
                wait_batched_explicit_commands(queued_add_items)
            except Exception as exc:
                raise RuntimeError(f"set phase failed: {exc}") from exc
            set_ms = elapsed_ms(set_started)

            flush_started = time.perf_counter()
            all_requested_set = set(all_requested_ipv6s)
            queued_flush_items = []
            use_broad_flush = len(subbatch_entries) >= EXPLICIT_BULK_MOVE_BROAD_FLUSH_THRESHOLD
            try:
                for observer_info in observer_infos:
                    if use_broad_flush:
                        queued_flush_items.extend(
                            queue_explicit_pod_commands(
                                observer_info["batch_key"],
                                observer_info["pid"],
                                "neigh-flush-all",
                                ["__all__"],
                            )
                        )
                    else:
                        flush_values = all_requested_set - target_uid_to_addresses.get(observer_info["uid"], set())
                        if not flush_values:
                            continue
                        queued_flush_items.extend(
                            queue_explicit_pod_commands(
                                observer_info["batch_key"],
                                observer_info["pid"],
                                "neigh-flush",
                                flush_values,
                            )
                        )
                wait_batched_explicit_commands(queued_flush_items)
            except Exception as exc:
                raise RuntimeError(f"flush phase failed: {exc}") from exc
            flush_ms = elapsed_ms(flush_started)

        return {
            "entries": subbatch_entries,
            "route_networks": route_networks,
            "evict_ms": evict_ms,
            "set_ms": set_ms,
            "flush_ms": flush_ms,
            "broad_flush": use_broad_flush,
            "total_ms": elapsed_ms(sub_started),
        }

    subbatches = list(chunked_entries(prepared, EXPLICIT_MOVE_SUBBATCH_MAX_ITEMS))
    subresults = []
    if len(subbatches) == 1 or EXPLICIT_MOVE_SUBBATCH_WORKERS <= 1:
        for subbatch in subbatches:
            try:
                subresults.append(execute_move_subbatch(subbatch))
            except Exception as exc:
                for entry in subbatch:
                    trace_context = entry["trace_context"]
                    requested_ipv6 = entry["requested_ipv6"]
                    log_explicit_trace(
                        source="node-agent-error",
                        requested_ipv6=requested_ipv6,
                        pod_uid=entry["target_details"].get("pod_uid"),
                        total_ms=elapsed_ms(total_started),
                        error=type(exc).__name__,
                        error_message=str(exc),
                        batch_size=len(subbatch),
                        **trace_log_fields(trace_context),
                    )
                    results.append(
                        {
                            "status": "error",
                            "requested_ipv6": requested_ipv6,
                            "error": str(exc),
                        }
                    )
    else:
        with ThreadPoolExecutor(
            max_workers=min(EXPLICIT_MOVE_SUBBATCH_WORKERS, len(subbatches)),
            thread_name_prefix="explicit-move-subbatch",
        ) as executor:
            future_map = {executor.submit(execute_move_subbatch, subbatch): subbatch for subbatch in subbatches}
            for future in as_completed(future_map):
                subbatch = future_map[future]
                try:
                    subresults.append(future.result())
                except Exception as exc:
                    for entry in subbatch:
                        trace_context = entry["trace_context"]
                        requested_ipv6 = entry["requested_ipv6"]
                        log_explicit_trace(
                            source="node-agent-error",
                            requested_ipv6=requested_ipv6,
                            pod_uid=entry["target_details"].get("pod_uid"),
                            total_ms=elapsed_ms(total_started),
                            error=type(exc).__name__,
                            error_message=str(exc),
                            batch_size=len(subbatch),
                            **trace_log_fields(trace_context),
                        )
                        results.append(
                            {
                                "status": "error",
                                "requested_ipv6": requested_ipv6,
                                "error": str(exc),
                            }
                        )

    all_route_networks = sorted({route for subresult in subresults for route in subresult["route_networks"]})
    prefix_sync_started = time.perf_counter()
    new_prefixes = []
    for route_network in all_route_networks:
        if remember_known_explicit_prefix(route_network):
            new_prefixes.append(route_network)
    for route_network in new_prefixes:
        sync_explicit_prefix_route_to_all_pods(route_network)
    prefix_sync_ms = elapsed_ms(prefix_sync_started) if all_route_networks else 0.0

    for subresult in subresults:
        sub_entries = subresult["entries"]
        sub_batch_size = len(sub_entries)
        if sub_batch_size <= 0:
            continue
        per_entry_resolve_ms = round(resolve_runtime_ms / len(prepared), 2)
        per_entry_evict_ms = round(subresult["evict_ms"] / sub_batch_size, 2)
        per_entry_set_ms = round(subresult["set_ms"] / sub_batch_size, 2)
        per_entry_flush_ms = round(subresult["flush_ms"] / sub_batch_size, 2)
        per_entry_total_ms = round(subresult["total_ms"] / sub_batch_size, 2)
        for entry in sub_entries:
            target_details = entry["target_details"]
            requested_ipv6 = entry["requested_ipv6"]
            trace_context = entry["trace_context"]
            log(
                f"explicit-ipv6 {target_details.get('namespace')}/{target_details.get('pod_name')} "
                f"uid={target_details.get('pod_uid')} ipv6={requested_ipv6} source={source}"
            )
            log_explicit_trace(
                source="node-agent",
                requested_ipv6=requested_ipv6,
                pod_uid=target_details.get("pod_uid"),
                namespace=target_details.get("namespace"),
                pod_name=target_details.get("pod_name"),
                lock_wait_ms=0.0,
                resolve_runtime_ms=per_entry_resolve_ms,
                evict_ms=per_entry_evict_ms,
                evicted_count=len(entry["evicted"]),
                set_ms=per_entry_set_ms,
                flush_ms=per_entry_flush_ms,
                mark_applied_ms=0.0,
        prefix_sync_ms=round(prefix_sync_ms / len(prepared), 2) if prepared else 0.0,
        total_ms=per_entry_total_ms,
        batch_size=sub_batch_size,
        broad_flush=1 if subresult.get("broad_flush") else 0,
        **trace_log_fields(trace_context),
    )
            results.append(
                {
                    "status": "applied",
                    "namespace": target_details.get("namespace"),
                    "pod_name": target_details.get("pod_name"),
                    "pod_uid": target_details.get("pod_uid"),
                    "node_name": NODE_NAME,
                    "requested_ipv6": requested_ipv6,
                    "route_network": entry["route_network"],
                    "evicted": entry["evicted"],
                }
            )

    failed_count = sum(1 for entry in results if str(entry.get("status") or "").strip().lower() != "applied")
    applied_count = len(results) - failed_count
    log_explicit_trace(
        source="node-agent-bulk-move",
        batch_size=len(prepared),
        applied_count=applied_count,
        failed_count=failed_count,
        resolve_runtime_ms=resolve_runtime_ms,
        evict_ms=round(sum(subresult["evict_ms"] for subresult in subresults), 2) if subresults else 0.0,
        set_ms=round(sum(subresult["set_ms"] for subresult in subresults), 2) if subresults else 0.0,
        flush_ms=round(sum(subresult["flush_ms"] for subresult in subresults), 2) if subresults else 0.0,
        prefix_sync_ms=prefix_sync_ms,
        total_ms=elapsed_ms(total_started),
        subbatch_count=len(subbatches),
        broad_flush_batches=sum(1 for subresult in subresults if subresult.get("broad_flush")),
    )
    return {
        "status": "applied" if failed_count == 0 else "partial",
        "applied_count": applied_count,
        "failed_count": failed_count,
        "results": results,
    }


def apply_explicit_ipv6_requests_bulk(target, entries, source="api-bulk"):
    if not isinstance(entries, list):
        raise ValueError("entries must be a list.")
    total_started = time.perf_counter()
    runtime_started = time.perf_counter()
    target_trace_context = extract_trace_context(target)
    skip_allocator_applied_callback = bool(target.get("skip_allocator_applied_callback")) if isinstance(target, dict) else False
    details, network_info = resolve_runtime_for_identity(target)
    resolve_runtime_ms = elapsed_ms(runtime_started)
    namespace = details.get("namespace")
    name = details.get("pod_name")
    uid = details.get("pod_uid")
    pid = (network_info or {}).get("pid")
    if not pid:
        raise RuntimeError(f"No usable pid is available yet for {namespace}/{name}.")
    register_managed_pod(details, network_info)
    results = []
    new_prefixes = []
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            results.append({"status": "error", "error": "entry must be an object"})
            continue
        trace_context = extract_trace_context(raw_entry)
        requested_ipv6 = normalize_ipv6_address(raw_entry.get("requested_ipv6") or raw_entry.get("ipv6_address"))
        if not requested_ipv6:
            results.append({"status": "error", "error": "requested_ipv6 is required"})
            continue
        lock_wait_started = time.perf_counter()
        try:
            with explicit_state_lock(requested_ipv6):
                lock_wait_ms = elapsed_ms(lock_wait_started)
                route_network = explicit_route_network(requested_ipv6)
                evict_started = time.perf_counter()
                evicted = evict_explicit_ipv6_from_previous_owner(requested_ipv6, raw_entry.get("previous_owner"), uid)
                evict_ms = elapsed_ms(evict_started)
                set_started = time.perf_counter()
                set_explicit_ipv6(pid, network_info, requested_ipv6)
                set_ms = elapsed_ms(set_started)
                flush_ms = 0.0
                if evicted:
                    flush_started = time.perf_counter()
                    flush_explicit_ipv6_neighbors(requested_ipv6, target_uid=uid)
                    flush_ms = elapsed_ms(flush_started)
                mark_applied_started = time.perf_counter()
                if skip_allocator_applied_callback:
                    pass
                elif ASYNC_APPLIED_CALLBACK_ENABLED:
                    queue_explicit_ipv6_applied(requested_ipv6, {**details, **target_trace_context, **trace_context})
                else:
                    allocator_mark_explicit_ipv6_applied(requested_ipv6, {**details, **target_trace_context, **trace_context})
                mark_applied_ms = elapsed_ms(mark_applied_started)
                if remember_known_explicit_prefix(route_network):
                    new_prefixes.append(route_network)
                log(f"explicit-ipv6 {namespace}/{name} uid={uid} ipv6={requested_ipv6} source={source}")
                log_explicit_trace(
                    source="node-agent",
                    requested_ipv6=requested_ipv6,
                    pod_uid=uid,
                    namespace=namespace,
                    pod_name=name,
                    lock_wait_ms=lock_wait_ms,
                    resolve_runtime_ms=resolve_runtime_ms,
                    evict_ms=evict_ms,
                    evicted_count=len(evicted),
                    set_ms=set_ms,
                    flush_ms=flush_ms,
                    mark_applied_ms=mark_applied_ms,
                    prefix_sync_ms=0.0,
                    total_ms=elapsed_ms(total_started),
                    batch_size=len(entries),
                    **trace_log_fields(trace_context),
                )
                results.append(
                    {
                        "status": "applied",
                        "namespace": namespace,
                        "pod_name": name,
                        "pod_uid": uid,
                        "node_name": NODE_NAME,
                        "requested_ipv6": requested_ipv6,
                        "route_network": route_network,
                        "evicted": evicted,
                    }
                )
        except Exception as exc:
            log_explicit_trace(
                source="node-agent-error",
                requested_ipv6=requested_ipv6,
                pod_uid=uid,
                total_ms=elapsed_ms(total_started),
                error=type(exc).__name__,
                batch_size=len(entries),
                **trace_log_fields(trace_context),
            )
            results.append(
                {
                    "status": "error",
                    "requested_ipv6": requested_ipv6,
                    "error": str(exc),
                }
            )
    prefix_sync_started = time.perf_counter()
    for route_network in sorted(set(new_prefixes)):
        sync_explicit_prefix_route_to_all_pods(route_network)
    prefix_sync_ms = elapsed_ms(prefix_sync_started) if new_prefixes else 0.0
    applied_count = sum(1 for entry in results if str(entry.get("status") or "").strip().lower() == "applied")
    failed_count = len(results) - applied_count
    log_explicit_trace(
        source="node-agent-bulk",
        pod_uid=uid,
        namespace=namespace,
        pod_name=name,
        batch_size=len(entries),
        applied_count=applied_count,
        failed_count=failed_count,
        resolve_runtime_ms=resolve_runtime_ms,
        prefix_sync_ms=prefix_sync_ms,
        total_ms=elapsed_ms(total_started),
    )
    return {
        "status": "applied" if failed_count == 0 else "partial",
        "namespace": namespace,
        "pod_name": name,
        "pod_uid": uid,
        "node_name": NODE_NAME,
        "applied_count": applied_count,
        "failed_count": failed_count,
        "results": results,
    }


def clear_explicit_ipv6_runtime(entries):
    if not isinstance(entries, list):
        raise ValueError("entries must be a list.")
    grouped = {}
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            raise ValueError("each cleanup entry must be an object.")
        namespace = str(raw_entry.get("namespace") or "").strip()
        pod_name = str(raw_entry.get("pod_name") or "").strip()
        pod_uid = str(raw_entry.get("pod_uid") or "").strip()
        requested_ipv6 = normalize_ipv6_address(raw_entry.get("requested_ipv6"))
        if not namespace or not pod_name or not pod_uid:
            raise ValueError("cleanup entries require namespace, pod_name, pod_uid, and requested_ipv6.")
        group = grouped.setdefault(
            pod_uid,
            {
                "namespace": namespace,
                "pod_name": pod_name,
                "pod_uid": pod_uid,
                "requested_ipv6s": set(),
            },
        )
        for key, value in (explicit_identity_details(raw_entry) or {}).items():
            if value not in (None, ""):
                group[key] = value
        group["requested_ipv6s"].add(requested_ipv6)

    cleared = []
    cleared_prefixes = set()
    for entry in grouped.values():
        namespace = entry["namespace"]
        pod_name = entry["pod_name"]
        pod_uid = entry["pod_uid"]
        requested_ipv6s = sorted(entry["requested_ipv6s"])
        details, network_info = resolve_runtime_for_identity(entry)
        pid = (network_info or {}).get("pid")
        if not pid:
            cleared.append(
                {
                    "namespace": namespace,
                    "pod_name": pod_name,
                    "pod_uid": pod_uid,
                    "status": "missing",
                    "removed_ipv6": [],
                    "removed_routes": [],
                }
            )
            continue
        if not pid or not interface_exists(pid, EXPLICIT_IFACE):
            cleared.append(
                {
                    "namespace": namespace,
                    "pod_name": pod_name,
                    "pod_uid": pod_uid,
                    "status": "no-explicit-iface",
                    "removed_ipv6": [],
                    "removed_routes": [],
                }
            )
            continue
        removed_ipv6 = []
        route_networks = sorted({explicit_route_network(ipv6) for ipv6 in requested_ipv6s})
        cleared_prefixes.update(route_networks)
        for requested_ipv6 in requested_ipv6s:
            if remove_explicit_ipv6_address(pid, requested_ipv6):
                removed_ipv6.append(requested_ipv6)
        remaining_explicit = {address for address, _ in current_global_ipv6s(pid, EXPLICIT_IFACE)}
        removed_routes = []
        for route_network in route_networks:
            if any(explicit_route_network(address) == route_network for address in remaining_explicit):
                continue
            if delete_explicit_route(pid, route_network):
                removed_routes.append(route_network)
        flush_all_explicit_neighbors(pid)
        cleared.append(
            {
                "namespace": namespace,
                "pod_name": pod_name,
                "pod_uid": pod_uid,
                "status": "cleared",
                "removed_ipv6": removed_ipv6,
                "removed_routes": removed_routes,
            }
        )
    remaining_rows = refresh_known_explicit_prefixes_from_allocator()
    remaining_prefixes = {explicit_route_network(row["requested_ipv6"]) for row in remaining_rows}
    for route_network in sorted(cleared_prefixes - remaining_prefixes):
        clear_explicit_prefix_route_from_all_pods(route_network)
    return {
        "status": "cleared",
        "node_name": NODE_NAME,
        "pods": cleared,
    }


def manage_pod(pod, preferred_pid=None, source="event"):
    namespace = pod["metadata"]["namespace"]
    name = pod["metadata"]["name"]
    uid = pod["metadata"]["uid"]
    annotations = (pod["metadata"].get("annotations") or {})
    if not begin_inflight(uid):
        return
    try:
        network_info = pod_network_info(pod) or {}
        pid = preferred_pid if preferred_pid and can_enter_pid(preferred_pid) else network_info.get("pid")
        if not pid:
            log(f"skip {namespace}/{name}: no usable pid yet (source={source})")
            return False
        allocation = allocator_ensure(pod, host_gateway_mac(), network_info=network_info)
        register_managed_pod(allocation, network_info)
        assigned_mac = allocation["assigned_mac"].lower()
        assigned_ipv6 = (allocation.get("assigned_ipv6") or "").lower()
        auto_explicit_ipv6 = (allocation.get("auto_managed_explicit_ipv6") or auto_managed_explicit_ipv6(allocation) or "").lower()
        if desired_state_matches(annotations, assigned_mac, assigned_ipv6):
            # An annotation match is not enough after agent restarts; re-ensure
            # the managed IPv6 path so host-side /128 routes are restored.
            set_ipv6(pid, network_info, assigned_ipv6, allocation=allocation)
            apply_tracked_explicit_ipv6s(pod, pid, network_info, assigned_mac, f"{source}:tracked")
            sync_explicit_prefix_routes_for_pod(pod, pid=pid, network_info=network_info)
            return True
        set_mac(pid, assigned_mac)
        set_ipv6(pid, network_info, assigned_ipv6, allocation=allocation)
        apply_tracked_explicit_ipv6s(pod, pid, network_info, assigned_mac, f"{source}:tracked")
        sync_explicit_prefix_routes_for_pod(pod, pid=pid, network_info=network_info)
        patch = {
            ASSIGNED_ANN: assigned_mac,
            CURRENT_ANN: assigned_mac,
            STATUS_ANN: "assigned",
            STABLE_ANN: None,
        }
        if assigned_ipv6:
            patch.update(
                {
                    ASSIGNED_IPV6_ANN: assigned_ipv6,
                    CURRENT_IPV6_ANN: assigned_ipv6,
                    IPV6_PREFIX_ANN: MANAGED_IPV6_PREFIX,
                }
            )
        if auto_explicit_ipv6:
            patch.update(
                {
                    AUTO_EXPLICIT_IPV6_ANN: auto_explicit_ipv6,
                    AUTO_EXPLICIT_TAG_ANN: normalize_tag_hex(AUTO_MANAGED_EXPLICIT_TAG),
                }
            )
        patch_pod_annotations(namespace, name, patch)
        mark_uid_managed(uid)
        if assigned_ipv6 and auto_explicit_ipv6:
            log(
                f"managed {namespace}/{name} uid={uid} mac={assigned_mac} "
                f"ipv6={assigned_ipv6} auto-net1-ipv6={auto_explicit_ipv6} source={source}"
            )
        elif assigned_ipv6:
            log(f"managed {namespace}/{name} uid={uid} mac={assigned_mac} ipv6={assigned_ipv6} source={source}")
        else:
            log(f"managed {namespace}/{name} uid={uid} mac={assigned_mac} source={source}")
        return True
    finally:
        end_inflight(uid)


def reconcile_existing_pods():
    pods, resource_version = list_target_pods(include_terminal=True)
    live_uids = []
    for pod in pods:
        metadata = pod.get("metadata") or {}
        phase = (pod.get("status") or {}).get("phase")
        if metadata.get("deletionTimestamp") or phase in {"Succeeded", "Failed", "Unknown"}:
            allocator_release(pod, status="RELEASED")
            mark_uid_unmanaged(metadata.get("uid"))
            continue
        if phase in {"Pending", "Running"}:
            live_uids.append(metadata["uid"])
    replace_known_managed_uids(live_uids)
    retain_managed_registry_uids(live_uids)
    allocator_reconcile(live_uids)
    seed_known_explicit_prefixes_from_allocator()
    for pod in pods:
        metadata = pod.get("metadata") or {}
        phase = (pod.get("status") or {}).get("phase")
        if metadata.get("deletionTimestamp") or phase not in {"Pending", "Running"}:
            continue
        manage_pod(pod, source="startup")
    return resource_version


def safety_reconcile_loop():
    if SAFETY_RECONCILE_SECONDS <= 0:
        return
    while True:
        time.sleep(SAFETY_RECONCILE_SECONDS)
        try:
            reconcile_existing_pods()
        except Exception as exc:
            log(f"safety reconcile error: {exc}")


def wrapper_value(message, field_name):
    if not message.HasField(field_name):
        return None
    return getattr(message, field_name).value


def parse_process_event(response, field_name):
    if not response.HasField(field_name):
        return None
    envelope = getattr(response, field_name)
    process = envelope.process
    if not process.HasField("pod"):
        return None
    pod = process.pod
    namespace = pod.namespace
    name = pod.name
    uid = pod.uid
    pid = wrapper_value(process, "pid")
    if not namespace or not name or not uid or not pid:
        return None
    container_pid = None
    if pod.HasField("container"):
        container_pid = wrapper_value(pod.container, "pid")
    return {
        "namespace": namespace,
        "name": name,
        "uid": uid,
        "pid": int(pid),
        "binary": process.binary or "",
        "labels": dict(pod.pod_labels),
        "annotations": dict(pod.pod_annotations),
        "workload": pod.workload or "",
        "workload_kind": pod.workload_kind or "",
        "container_pid": int(container_pid) if container_pid else None,
        "is_container_init": bool(container_pid and pid == container_pid),
        "event_key": field_name,
    }


def parse_exec_event(response):
    return parse_process_event(response, "process_exec")


def parse_exit_event(response):
    return parse_process_event(response, "process_exit")


def pod_stub_from_event(details):
    owner_refs = []
    workload_kind = str(details.get("workload_kind") or "").strip()
    workload_name = str(details.get("workload") or "").strip()
    if workload_kind and workload_name and workload_kind.lower() != "pod":
        owner_refs.append({"kind": workload_kind, "name": workload_name})
    return {
        "metadata": {
            "namespace": details["namespace"],
            "name": details["name"],
            "uid": details["uid"],
            "labels": details.get("labels") or {},
            "annotations": details.get("annotations") or {},
            "ownerReferences": owner_refs,
        },
        "status": {"phase": "Running"},
    }


def sandbox_record_by_id(runtime_id):
    if not runtime_id:
        return None
    items = crictl_json("pods", "-o", "json").get("items", [])
    for item in items:
        sandbox_id = item.get("id") or ""
        if sandbox_id == runtime_id or sandbox_id.startswith(runtime_id):
            return item
    return None


def runtime_delete_details(runtime_id, pid, binary, arguments):
    sandbox = sandbox_record_by_id(runtime_id)
    if not sandbox:
        return None
    metadata = sandbox.get("metadata", {}) or {}
    namespace = metadata.get("namespace")
    name = metadata.get("name")
    uid = metadata.get("uid")
    if not namespace or not name or not uid:
        return None
    return {
        "namespace": namespace,
        "name": name,
        "uid": uid,
        "pid": int(pid) if pid else 0,
        "binary": binary or "",
        "arguments": arguments or "",
        "labels": dict(sandbox.get("labels") or {}),
        "annotations": dict(sandbox.get("annotations") or {}),
        "workload": "",
        "workload_kind": "",
        "container_pid": None,
        "is_container_init": False,
        "event_key": "runtime_delete_exec",
        "runtime_id": runtime_id,
        "source": f"tetragon-runtime-delete:{binary or 'process'}",
    }


def begin_manage_retry(uid):
    with MANAGE_RETRY_LOCK:
        if uid in MANAGE_RETRYING:
            return False
        MANAGE_RETRYING.add(uid)
        return True


def end_manage_retry(uid):
    with MANAGE_RETRY_LOCK:
        MANAGE_RETRYING.discard(uid)


def manage_retry_worker(details):
    uid = details["uid"]
    namespace = details["namespace"]
    name = details["name"]
    preferred_pid = details["pid"]
    try:
        for index, delay in enumerate(MANAGE_RETRY_DELAYS):
            if delay:
                time.sleep(delay)
            pod = get_pod_if_current(namespace, name, uid)
            if not pod:
                log(f"skip {namespace}/{name}: pod no longer current during Tetragon retry")
                return
            metadata = pod.get("metadata") or {}
            if metadata.get("deletionTimestamp"):
                log(f"skip {namespace}/{name}: pod is deleting during Tetragon retry")
                return
            phase = (pod.get("status") or {}).get("phase")
            if phase not in {"Pending", "Running"}:
                continue
            log(
                f"tetragon-manage-attempt {namespace}/{name} uid={uid} "
                f"attempt={index + 1} phase={phase} preferred-pid={'yes' if index == 0 and preferred_pid else 'no'} "
                f"binary={details['binary'] or 'process'}"
            )
            succeeded = manage_pod(
                pod,
                preferred_pid=preferred_pid if index == 0 else None,
                source=f"tetragon-exec:{details['binary'] or 'process'}",
            )
            if succeeded:
                return
        log(f"manage retry exhausted for {namespace}/{name} uid={uid}")
    except Exception as exc:
        log(f"manage retry error for {namespace}/{name} uid={uid}: {exc}")
    finally:
        end_manage_retry(uid)


def schedule_manage_retry(details):
    uid = details["uid"]
    if not begin_manage_retry(uid):
        return
    thread = threading.Thread(target=manage_retry_worker, args=(details,), daemon=True)
    thread.start()


def begin_release_check(uid):
    with RELEASE_CHECK_LOCK:
        if uid in RELEASE_CHECKING:
            return False
        RELEASE_CHECKING.add(uid)
        return True


def end_release_check(uid):
    with RELEASE_CHECK_LOCK:
        RELEASE_CHECKING.discard(uid)


def replace_known_managed_uids(uids):
    with KNOWN_MANAGED_UIDS_LOCK:
        KNOWN_MANAGED_UIDS.clear()
        KNOWN_MANAGED_UIDS.update(uids)
    retain_managed_registry_uids(uids)


def mark_uid_managed(uid):
    with KNOWN_MANAGED_UIDS_LOCK:
        KNOWN_MANAGED_UIDS.add(uid)


def mark_uid_unmanaged(uid):
    with KNOWN_MANAGED_UIDS_LOCK:
        KNOWN_MANAGED_UIDS.discard(uid)
        live = set(KNOWN_MANAGED_UIDS)
    retain_managed_registry_uids(live)


def is_known_managed_uid(uid):
    with KNOWN_MANAGED_UIDS_LOCK:
        return uid in KNOWN_MANAGED_UIDS


def release_check_worker(details):
    uid = details["uid"]
    namespace = details["namespace"]
    name = details["name"]
    source = details.get("source") or f"tetragon-exit:{details['binary'] or 'process'}"
    try:
        for delay in RELEASE_RETRY_DELAYS:
            time.sleep(delay)
            pod = get_pod_if_current(namespace, name, uid)
            if not pod:
                allocator_release(pod_stub_from_event(details), status="RELEASED")
                mark_uid_unmanaged(uid)
                log(f"released {namespace}/{name} uid={uid} source={source}:missing")
                return
            metadata = pod.get("metadata") or {}
            phase = (pod.get("status") or {}).get("phase")
            if metadata.get("deletionTimestamp") or phase in {"Succeeded", "Failed", "Unknown"}:
                allocator_release(pod, status="RELEASED")
                mark_uid_unmanaged(uid)
                log(f"released {namespace}/{name} uid={uid} source={source}:{phase or 'deleting'}")
                return
            annotations = metadata.get("annotations") or {}
            if phase in {"Pending", "Running"} and not annotations_show_assignment(annotations):
                succeeded = manage_pod(
                    pod,
                    source=source,
                )
                if succeeded:
                    return
        log(f"release check skipped for {namespace}/{name} uid={uid}: pod is still live")
    except Exception as exc:
        log(f"release check error for {namespace}/{name} uid={uid}: {exc}")
    finally:
        end_release_check(uid)


def schedule_release_check(details):
    uid = details["uid"]
    if not begin_release_check(uid):
        return
    thread = threading.Thread(target=release_check_worker, args=(details,), daemon=True)
    thread.start()


def grpc_error_text(exc):
    code = exc.code() if hasattr(exc, "code") else None
    details = exc.details() if hasattr(exc, "details") else str(exc)
    if code is None:
        return str(details)
    return f"{code.name}: {details}"


def make_tetragon_request():
    return events_pb2.GetEventsRequest(
        allow_list=[
            events_pb2.Filter(
                event_set=[events_pb2.PROCESS_EXEC, events_pb2.PROCESS_EXIT],
            )
        ]
    )


def details_should_manage(details):
    labels = details.get("labels") or {}
    if labels:
        return matches_selector(labels)
    pod = get_pod_if_current(details["namespace"], details["name"], details["uid"])
    if not pod:
        return False
    metadata = pod.get("metadata") or {}
    if metadata.get("deletionTimestamp"):
        return False
    return matches_selector(metadata.get("labels") or {})


def details_should_release(details):
    labels = details.get("labels") or {}
    if labels and matches_selector(labels):
        return True
    if is_known_managed_uid(details["uid"]):
        return True
    pod = get_pod_if_current(details["namespace"], details["name"], details["uid"])
    if not pod:
        return False
    return matches_selector(((pod.get("metadata") or {}).get("labels")) or {})


def parse_runtime_delete_exec(response):
    if not response.HasField("process_exec"):
        return None
    envelope = response.process_exec
    process = envelope.process
    if process.HasField("pod"):
        return None
    binary = process.binary or ""
    arguments = process.arguments or ""
    if not binary.endswith("containerd-shim-runc-v2"):
        return None
    if " delete" not in f" {arguments} ":
        return None
    match = re.search(r"(?:^|\s)-id ([0-9a-f]{64})(?:\s|$)", arguments)
    if not match:
        return None
    return runtime_delete_details(match.group(1), wrapper_value(process, "pid"), binary, arguments)


def handle_tetragon_response(response):
    details = parse_exec_event(response)
    if details:
        if not details_should_manage(details):
            return
        log(
            f"tetragon-exec-match {details['namespace']}/{details['name']} uid={details['uid']} "
            f"pid={details['pid']} container-pid={details.get('container_pid') or 'none'} "
            f"binary={details['binary'] or 'process'}"
        )
        schedule_manage_retry(details)
        return
    details = parse_runtime_delete_exec(response)
    if details:
        if not details_should_release(details):
            return
        log(
            f"tetragon-runtime-delete-match {details['namespace']}/{details['name']} uid={details['uid']} "
            f"runtime-id={details.get('runtime_id') or 'none'} binary={details['binary'] or 'process'}"
        )
        schedule_release_check(details)
        return
    details = parse_exit_event(response)
    if details:
        if not details_should_release(details):
            return
        log(
            f"tetragon-exit-match {details['namespace']}/{details['name']} uid={details['uid']} "
            f"pid={details['pid']} container-pid={details.get('container_pid') or 'none'} "
            f"binary={details['binary'] or 'process'}"
        )
        schedule_release_check(details)


def tetragon_event_loop():
    options = [
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]
    while True:
        try:
            log(f"connecting to Tetragon gRPC stream address={TETRAGON_GRPC_ADDRESS}")
            channel = grpc.insecure_channel(TETRAGON_GRPC_ADDRESS, options=options)
            try:
                stub = sensors_pb2_grpc.FineGuidanceSensorsStub(channel)
                for response in stub.GetEvents(make_tetragon_request()):
                    handle_tetragon_response(response)
            finally:
                channel.close()
            log("tetragon gRPC stream ended")
        except grpc.RpcError as exc:
            log(f"tetragon gRPC stream error: {grpc_error_text(exc)}")
        except Exception as exc:
            log(f"tetragon gRPC stream error: {exc}")
        time.sleep(STREAM_RESTART_SECONDS)


class AgentHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def send_json(self, status, body):
        encoded = json.dumps(body, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError("Request body must be valid JSON.") from exc
        if not isinstance(payload, dict):
            raise ValueError("Request body must be a JSON object.")
        return payload

    def do_GET(self):
        if self.path == "/healthz":
            self.send_json(HTTPStatus.OK, {"status": "ok", "node_name": NODE_NAME})
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})

    def do_POST(self):
        try:
            payload = self.read_json()
            trace_context = extract_trace_context(payload)
            trace_fields = trace_log_fields(trace_context)
            if self.path == "/explicit-ipv6/clear":
                self.send_json(
                    HTTPStatus.OK,
                    clear_explicit_ipv6_runtime(payload.get("entries")),
                )
                return
            if self.path == "/explicit-ipv6/apply":
                namespace = str(payload.get("namespace") or "").strip()
                pod_name = str(payload.get("pod_name") or "").strip()
                pod_uid = str(payload.get("pod_uid") or "").strip()
                requested_ipv6 = payload.get("ipv6_address") or payload.get("requested_ipv6")
                if not namespace or not pod_name or not pod_uid or not requested_ipv6:
                    raise ValueError("namespace, pod_name, pod_uid, and ipv6_address are required.")
                allocator_sent_at_ms = trace_context.get("trace_allocator_sent_at_ms")
                client_started_at_ms = trace_context.get("trace_client_started_at_ms")
                allocator_to_agent_ms = (
                    round(current_epoch_ms() - allocator_sent_at_ms, 2)
                    if allocator_sent_at_ms is not None
                    else None
                )
                client_to_agent_ms = (
                    round(current_epoch_ms() - client_started_at_ms, 2)
                    if client_started_at_ms is not None
                    else None
                )
                log_explicit_trace(
                    source="node-agent-handler",
                    path=self.path,
                    requested_ipv6=requested_ipv6,
                    pod_uid=pod_uid,
                    allocator_to_agent_ms=allocator_to_agent_ms,
                    client_to_agent_ms=client_to_agent_ms,
                    **trace_fields,
                )
                self.send_json(
                    HTTPStatus.OK,
                    apply_explicit_ipv6_request(
                        payload,
                        requested_ipv6,
                        previous_owner=payload.get("previous_owner"),
                        source="agent-http",
                    ),
                )
                return
            if self.path == "/explicit-ipv6/bulk-apply":
                target = payload.get("target")
                entries = payload.get("entries")
                if not isinstance(target, dict):
                    raise ValueError("target must be an object.")
                if not isinstance(entries, list) or not entries:
                    raise ValueError("entries must be a non-empty list.")
                namespace = str(target.get("namespace") or "").strip()
                pod_name = str(target.get("pod_name") or "").strip()
                pod_uid = str(target.get("pod_uid") or "").strip()
                if not namespace or not pod_name or not pod_uid:
                    raise ValueError("target namespace, pod_name, and pod_uid are required.")
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_trace_context = extract_trace_context(entry)
                    allocator_sent_at_ms = entry_trace_context.get("trace_allocator_sent_at_ms")
                    client_started_at_ms = entry_trace_context.get("trace_client_started_at_ms")
                    allocator_to_agent_ms = (
                        round(current_epoch_ms() - allocator_sent_at_ms, 2)
                        if allocator_sent_at_ms is not None
                        else None
                    )
                    client_to_agent_ms = (
                        round(current_epoch_ms() - client_started_at_ms, 2)
                        if client_started_at_ms is not None
                        else None
                    )
                    log_explicit_trace(
                        source="node-agent-handler",
                        path=self.path,
                        requested_ipv6=entry.get("requested_ipv6") or entry.get("ipv6_address"),
                        pod_uid=pod_uid,
                        allocator_to_agent_ms=allocator_to_agent_ms,
                        client_to_agent_ms=client_to_agent_ms,
                        batch_size=len(entries),
                        **trace_log_fields(entry_trace_context),
                    )
                self.send_json(
                    HTTPStatus.OK,
                    apply_explicit_ipv6_requests_bulk(
                        target,
                        entries,
                        source="agent-http-bulk",
                    ),
                )
                return
            if self.path == "/explicit-ipv6/bulk-move":
                entries = payload.get("entries")
                if not isinstance(entries, list) or not entries:
                    raise ValueError("entries must be a non-empty list.")
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_trace_context = extract_trace_context(entry)
                    target = entry.get("target") if isinstance(entry.get("target"), dict) else {}
                    allocator_sent_at_ms = entry_trace_context.get("trace_allocator_sent_at_ms")
                    client_started_at_ms = entry_trace_context.get("trace_client_started_at_ms")
                    allocator_to_agent_ms = (
                        round(current_epoch_ms() - allocator_sent_at_ms, 2)
                        if allocator_sent_at_ms is not None
                        else None
                    )
                    client_to_agent_ms = (
                        round(current_epoch_ms() - client_started_at_ms, 2)
                        if client_started_at_ms is not None
                        else None
                    )
                    log_explicit_trace(
                        source="node-agent-handler",
                        path=self.path,
                        requested_ipv6=entry.get("requested_ipv6") or entry.get("ipv6_address"),
                        pod_uid=target.get("pod_uid"),
                        allocator_to_agent_ms=allocator_to_agent_ms,
                        client_to_agent_ms=client_to_agent_ms,
                        batch_size=len(entries),
                        **trace_log_fields(entry_trace_context),
                    )
                self.send_json(
                    HTTPStatus.OK,
                    apply_explicit_ipv6_move_requests_bulk(
                        entries,
                        source="agent-http-bulk-move",
                    ),
                )
                return
        except ValueError as exc:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except Exception as exc:
            self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"Unexpected server error: {exc}"})
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})


def serve_http():
    class AgentHTTPServer(ThreadingHTTPServer):
        request_queue_size = AGENT_REQUEST_QUEUE_SIZE

    server = AgentHTTPServer(("0.0.0.0", AGENT_HTTP_PORT), AgentHandler)
    log(f"node-agent http listening on 0.0.0.0:{AGENT_HTTP_PORT}")
    server.serve_forever()


if __name__ == "__main__":
    log(
        f"starting MAC event agent on node={NODE_NAME} gw-iface={GW_IFACE} "
        f"managed-iface={MANAGED_IFACE} explicit-iface={EXPLICIT_IFACE} "
        f"tetragon-grpc={TETRAGON_GRPC_ADDRESS} "
        f"ipv6-prefix={MANAGED_IPV6_PREFIX or 'disabled'} "
        f"safety-reconcile={SAFETY_RECONCILE_SECONDS}s"
    )
    while True:
        try:
            reconcile_existing_pods()
            break
        except Exception as exc:
            log(f"startup reconcile error: {exc}")
            time.sleep(STARTUP_RETRY_SECONDS)
    http_thread = threading.Thread(target=serve_http, daemon=True)
    http_thread.start()
    if SAFETY_RECONCILE_SECONDS > 0:
        reconcile_thread = threading.Thread(target=safety_reconcile_loop, daemon=True)
        reconcile_thread.start()
    tetragon_event_loop()
