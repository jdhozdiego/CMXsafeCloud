# Generated mirror of net-identity-allocator/app.py for MkDocs reference.
import hashlib
import ipaddress
import json
import os
import queue
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg import IntegrityError as PsycopgIntegrityError
    from psycopg_pool import ConnectionPool
except ImportError:
    psycopg = None
    dict_row = None
    PsycopgIntegrityError = None
    ConnectionPool = None

HOST = os.environ.get("MAC_ALLOCATOR_HOST", "0.0.0.0")
PORT = int(os.environ.get("ALLOCATOR_HTTP_PORT", "8080"))
ALLOCATOR_REQUEST_QUEUE_SIZE = max(1, int(os.environ.get("ALLOCATOR_REQUEST_QUEUE_SIZE", "1024")))
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "net-identity-allocator-postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "cmxsafemac_ipv6")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "allocator")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "")
POSTGRES_SSLMODE = os.environ.get("POSTGRES_SSLMODE", "disable")
DATABASE_URL = os.environ.get("DATABASE_URL")
NODE_AGENT_NAMESPACE = os.environ.get("NODE_AGENT_NAMESPACE", "mac-allocator")
NODE_AGENT_HTTP_PORT = int(os.environ.get("NODE_AGENT_HTTP_PORT", "8081"))
NODE_AGENT_LABEL_SELECTOR = os.environ.get("NODE_AGENT_LABEL_SELECTOR", "app=cmxsafemac-ipv6-node-agent")
KUBE_API_HOST = os.environ.get("KUBERNETES_SERVICE_HOST")
KUBE_API_PORT = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
CA_FILE = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
TOKEN = TOKEN_PATH.read_text(encoding="utf-8").strip() if KUBE_API_HOST and TOKEN_PATH.exists() else None
SSL_CONTEXT = ssl.create_default_context(cafile=CA_FILE) if KUBE_API_HOST and Path(CA_FILE).exists() else None
EMBEDDED_TAG_BYTES = 2
CANONICAL_EXPLICIT_COUNTER = 0
AUTO_MANAGED_EXPLICIT_TAG = str(os.environ.get("AUTO_MANAGED_EXPLICIT_TAG", "") or "").strip()
AUTO_MANAGED_EXPLICIT_MAC_DEV = "00:00:00:00:00:00"
MANAGED_CONTAINER_IFACE = str(os.environ.get("MANAGED_IFACE", "eth0") or "eth0").strip()
EXPLICIT_CONTAINER_IFACE = str(os.environ.get("EXPLICIT_IFACE", "net1") or "net1").strip()
CANONICAL_GATEWAY_MAC_SETTING = "canonical_gateway_mac"
DEFAULT_CANONICAL_GATEWAY_MAC = str(os.environ.get("CANONICAL_GATEWAY_MAC", "") or "").strip()

if psycopg is None or ConnectionPool is None:
    raise RuntimeError("psycopg and psycopg_pool must be installed for the PostgreSQL allocator backend.")

DB_INTEGRITY_ERRORS = (PsycopgIntegrityError,)
EXPLICIT_WRITE_RETRY_ATTEMPTS = int(os.environ.get("EXPLICIT_WRITE_RETRY_ATTEMPTS", "8"))
EXPLICIT_WRITE_RETRY_BASE_SECONDS = float(os.environ.get("EXPLICIT_WRITE_RETRY_BASE_SECONDS", "0.01"))
ASYNC_EXPLICIT_APPLY_ENABLED = str(os.environ.get("ASYNC_EXPLICIT_APPLY_ENABLED", "true")).strip().lower() not in {
    "0",
    "false",
    "no",
}
EXPLICIT_APPLY_WORKERS = max(1, int(os.environ.get("EXPLICIT_APPLY_WORKERS", "32")))
EXPLICIT_APPLY_BATCH_WINDOW_MS = max(0.0, float(os.environ.get("EXPLICIT_APPLY_BATCH_WINDOW_MS", "5")))
EXPLICIT_APPLY_BATCH_MAX_ITEMS = max(1, int(os.environ.get("EXPLICIT_APPLY_BATCH_MAX_ITEMS", "128")))
EXPLICIT_MOVE_BATCH_MAX_ITEMS = max(1, int(os.environ.get("EXPLICIT_MOVE_BATCH_MAX_ITEMS", str(max(128, EXPLICIT_APPLY_BATCH_MAX_ITEMS)))))
EXPLICIT_MOVE_MIN_BATCH_ITEMS = max(1, int(os.environ.get("EXPLICIT_MOVE_MIN_BATCH_ITEMS", "32")))
EXPLICIT_MOVE_BATCH_WINDOW_MS = max(0.0, float(os.environ.get("EXPLICIT_MOVE_BATCH_WINDOW_MS", "5")))
EXPLICIT_MOVE_DISPATCH_SHARDS = max(1, int(os.environ.get("EXPLICIT_MOVE_DISPATCH_SHARDS", "4")))
DB_POOL_MIN_SIZE = max(1, int(os.environ.get("DB_POOL_MIN_SIZE", "8")))
DB_POOL_MAX_SIZE = max(DB_POOL_MIN_SIZE, int(os.environ.get("DB_POOL_MAX_SIZE", str(max(32, EXPLICIT_APPLY_WORKERS)))))
DB_POOL_TIMEOUT_SECONDS = float(os.environ.get("DB_POOL_TIMEOUT_SECONDS", "30"))
DB_POOL_MAX_IDLE_SECONDS = float(os.environ.get("DB_POOL_MAX_IDLE_SECONDS", "300"))
EXPLICIT_APPLY_EXECUTOR = ThreadPoolExecutor(max_workers=EXPLICIT_APPLY_WORKERS, thread_name_prefix="explicit-apply")
EXPLICIT_APPLY_LOG_LOCK = threading.Lock()
EXPLICIT_APPLY_JOBS = {}
EXPLICIT_APPLY_JOBS_LOCK = threading.Lock()
EXPLICIT_APPLY_BATCHERS = {}
EXPLICIT_APPLY_BATCHERS_LOCK = threading.Lock()
EXPLICIT_MOVE_BATCHERS = {}
EXPLICIT_MOVE_BATCHERS_LOCK = threading.Lock()
RUNTIME_SNAPSHOT_FIELDS = (
    "sandbox_id",
    "sandbox_pid",
    "sandbox_pid_start_time",
    "netns_inode",
    "runtime_observed_at",
)


def now_utc():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def log(message):
    with EXPLICIT_APPLY_LOG_LOCK:
        print(message, flush=True)


def elapsed_ms(start_time):
    return round((time.perf_counter() - start_time) * 1000.0, 2)


def effective_move_batch_max_items(backlog_items):
    backlog = max(1, int(backlog_items or 1))
    if backlog <= EXPLICIT_MOVE_BATCH_MAX_ITEMS:
        return EXPLICIT_MOVE_BATCH_MAX_ITEMS
    reduced = max(EXPLICIT_MOVE_MIN_BATCH_ITEMS, EXPLICIT_MOVE_BATCH_MAX_ITEMS // 2)
    if backlog >= EXPLICIT_MOVE_BATCH_MAX_ITEMS * 4:
        return EXPLICIT_MOVE_MIN_BATCH_ITEMS
    return reduced


class ExplicitApplyQueueItem:
    def __init__(self, assignment, previous_owner=None, trace_context=None):
        self.assignment = dict(assignment)
        self.previous_owner = dict(previous_owner) if isinstance(previous_owner, dict) else previous_owner
        self.trace_context = dict(trace_context or {})


class ExplicitApplyPodBatcher:
    def __init__(self, batch_key):
        self.batch_key = tuple(batch_key)
        self.items = queue.Queue()
        self.thread = threading.Thread(
            target=self._run,
            name=f"explicit-apply-batch-{self.batch_key[1] or 'unknown'}",
            daemon=True,
        )
        self.thread.start()

    def submit(self, item):
        self.items.put(item)

    def _run(self):
        while True:
            first = self.items.get()
            batch = [first]
            if EXPLICIT_APPLY_BATCH_WINDOW_MS > 0:
                deadline = time.perf_counter() + (EXPLICIT_APPLY_BATCH_WINDOW_MS / 1000.0)
                while len(batch) < EXPLICIT_APPLY_BATCH_MAX_ITEMS:
                    timeout = deadline - time.perf_counter()
                    if timeout <= 0:
                        break
                    try:
                        batch.append(self.items.get(timeout=timeout))
                    except queue.Empty:
                        break
            deduped = {}
            for item in batch:
                requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
                previous = deduped.get(requested_ipv6)
                if previous is not None:
                    trace_context = previous.trace_context
                    log_explicit_trace(
                        source="allocator-worker-skip",
                        requested_ipv6=requested_ipv6,
                        pod_uid=previous.assignment.get("pod_uid"),
                        queue_wait_ms=round((time.perf_counter() - previous.assignment.get("_trace_enqueued_at", time.perf_counter())) * 1000.0, 2)
                        if previous.assignment.get("_trace_enqueued_at")
                        else None,
                        total_ms=0.0,
                        reason="superseded-in-batch",
                        **trace_log_fields(trace_context),
                    )
                deduped[requested_ipv6] = item
            EXPLICIT_APPLY_EXECUTOR.submit(run_explicit_ipv6_apply_batch_task, list(deduped.values()))


class ExplicitMoveBatcher:
    def __init__(self, batch_key):
        self.batch_key = tuple(batch_key)
        self.items = queue.Queue()
        self.thread = threading.Thread(
            target=self._run,
            name=f"explicit-move-batch-{self.batch_key[0] or 'unknown'}",
            daemon=True,
        )
        self.thread.start()

    def submit(self, item):
        self.items.put(item)

    def _run(self):
        while True:
            first = self.items.get()
            batch = [first]
            backlog_items = self.items.qsize() + 1
            target_batch_max = effective_move_batch_max_items(backlog_items)
            collect_window_ms = 0.0 if backlog_items >= target_batch_max else EXPLICIT_MOVE_BATCH_WINDOW_MS
            if collect_window_ms > 0:
                deadline = time.perf_counter() + (collect_window_ms / 1000.0)
                while len(batch) < target_batch_max:
                    timeout = deadline - time.perf_counter()
                    if timeout <= 0:
                        break
                    try:
                        batch.append(self.items.get(timeout=timeout))
                    except queue.Empty:
                        break
            else:
                while len(batch) < target_batch_max:
                    try:
                        batch.append(self.items.get_nowait())
                    except queue.Empty:
                        break
            deduped = {}
            for item in batch:
                requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
                previous = deduped.get(requested_ipv6)
                if previous is not None:
                    trace_context = previous.trace_context
                    log_explicit_trace(
                        source="allocator-worker-skip",
                        requested_ipv6=requested_ipv6,
                        pod_uid=previous.assignment.get("pod_uid"),
                        queue_wait_ms=round((time.perf_counter() - previous.assignment.get("_trace_enqueued_at", time.perf_counter())) * 1000.0, 2)
                        if previous.assignment.get("_trace_enqueued_at")
                        else None,
                        total_ms=0.0,
                        reason="superseded-in-move-batch",
                        **trace_log_fields(trace_context),
                    )
                deduped[requested_ipv6] = item
            try:
                run_explicit_ipv6_move_batch_task(list(deduped.values()))
            except Exception as exc:
                log(f"explicit-move-batch-error key={self.batch_key}: {exc}")


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


def qmark_to_postgres_sql(statement):
    result = []
    in_single = False
    in_double = False
    index = 0
    while index < len(statement):
        char = statement[index]
        if char == "'" and not in_double:
            result.append(char)
            if in_single and index + 1 < len(statement) and statement[index + 1] == "'":
                result.append(statement[index + 1])
                index += 2
                continue
            in_single = not in_single
            index += 1
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            result.append(char)
            index += 1
            continue
        if char == "?" and not in_single and not in_double:
            result.append("%s")
        else:
            result.append(char)
        index += 1
    return "".join(result)


def split_sql_statements(script):
    statements = []
    current = []
    in_single = False
    in_double = False
    index = 0
    while index < len(script):
        char = script[index]
        current.append(char)
        if char == "'" and not in_double:
            if in_single and index + 1 < len(script) and script[index + 1] == "'":
                current.append(script[index + 1])
                index += 2
                continue
            in_single = not in_single
        elif char == '"' and not in_single:
            in_double = not in_double
        elif char == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
        index += 1
    remainder = "".join(current).strip()
    if remainder:
        statements.append(remainder)
    return statements


class ResultWrapper:
    def __init__(self, cursor=None, rows=None):
        self.cursor = cursor
        self.rows = rows

    def fetchone(self):
        if self.rows is not None:
            return self.rows[0] if self.rows else None
        return self.cursor.fetchone()

    def fetchall(self):
        if self.rows is not None:
            return list(self.rows)
        return self.cursor.fetchall()


class ConnectionWrapper:
    def __init__(self, conn):
        self.conn = conn
        self.total_changes = 0
        self.last_insert_id = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type:
                self.conn.rollback()
            else:
                self.conn.commit()
        finally:
            self.conn.close()
        return False

    def _track_rowcount(self, statement, cursor):
        if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE")) and cursor.rowcount and cursor.rowcount > 0:
            self.total_changes += cursor.rowcount

    def execute(self, statement, params=()):
        if statement.strip().upper() == "SELECT LAST_INSERT_ROWID() AS ID":
            return ResultWrapper(rows=[{"id": self.last_insert_id}])
        sql = qmark_to_postgres_sql(statement)
        cursor = self.conn.cursor()
        if statement.lstrip().upper().startswith("INSERT") and "RETURNING" not in statement.upper():
            cursor.execute(f"{sql.rstrip()} RETURNING id", params)
            inserted = cursor.fetchone()
            self.last_insert_id = inserted["id"] if inserted else None
            self._track_rowcount(statement, cursor)
            return ResultWrapper(rows=[inserted] if inserted else [])
        cursor.execute(sql, params)
        self._track_rowcount(statement, cursor)
        return ResultWrapper(cursor=cursor)

    def executemany(self, statement, seq_of_params):
        sql = qmark_to_postgres_sql(statement)
        cursor = self.conn.cursor()
        cursor.executemany(sql, seq_of_params)
        self._track_rowcount(statement, cursor)
        return ResultWrapper(cursor=cursor)

    def commit(self):
        self.conn.commit()


def normalize_mac(value):
    parts = str(value).strip().lower().split(":")
    if len(parts) != 6:
        raise ValueError(f"Invalid MAC address: {value}")
    try:
        return ":".join(f"{int(part, 16):02x}" for part in parts)
    except ValueError as exc:
        raise ValueError(f"Invalid MAC address: {value}") from exc


def normalize_optional_mac(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    return normalize_mac(raw)


def normalize_tag_hex(value):
    raw = str(value or "").strip().lower()
    if raw.startswith("0x"):
        raw = raw[2:]
    if len(raw) != 4:
        raise ValueError(f"Tag must be exactly 2 bytes (4 hex chars): {value}")
    try:
        int(raw, 16)
    except ValueError as exc:
        raise ValueError(f"Invalid tag value: {value}") from exc
    return raw


def normalize_ipv6_address(value):
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("ipv6_address is required.")
    try:
        return ipaddress.IPv6Address(raw).compressed.lower()
    except ipaddress.AddressValueError as exc:
        raise ValueError(f"Invalid IPv6 address: {value}") from exc


def derive_device_byte(mac_dev):
    if isinstance(mac_dev, int):
        return mac_dev & 0xFF
    raw = str(mac_dev).strip().lower()
    if not raw:
        raise ValueError("mac_dev is required.")
    try:
        if raw.startswith("0x"):
            return int(raw, 16) & 0xFF
        if raw.isdigit():
            return int(raw, 10) & 0xFF
        if raw.count(":") == 5:
            return int(hashlib.sha256(normalize_mac(raw).encode("utf-8")).hexdigest()[:2], 16)
    except ValueError:
        pass
    return hashlib.sha256(raw.encode("utf-8")).digest()[0]


def format_mac(gw_mac, counter):
    gw = normalize_mac(gw_mac).split(":")
    high = (counter >> 8) & 0xFF
    low = counter & 0xFF
    return ":".join([gw[0], gw[1], gw[2], gw[3], f"{high:02x}", f"{low:02x}"])


def normalize_ipv6_prefix(value):
    raw = str(value or "").strip()
    if not raw:
        return None
    network = ipaddress.IPv6Network(raw, strict=False)
    if network.prefixlen != 64:
        raise ValueError("ipv6_prefix must be a /64 IPv6 network.")
    return network.compressed


def format_ipv6(ipv6_prefix, counter):
    if not ipv6_prefix:
        return None
    network = ipaddress.IPv6Network(ipv6_prefix, strict=False)
    address = ipaddress.IPv6Address(int(network.network_address) + counter + 1)
    return address.compressed.lower()


def build_explicit_ipv6(gw_tag_hex, gw_mac, mac_dev, counter=CANONICAL_EXPLICIT_COUNTER):
    tag = bytes.fromhex(normalize_tag_hex(gw_tag_hex))
    gw_bytes = bytes.fromhex(normalize_mac(gw_mac).replace(":", ""))
    if int(counter) < 0 or int(counter) > 0xFFFF:
        raise ValueError(f"counter must fit in 2 bytes: {counter}")
    counter_bytes = int(counter).to_bytes(2, byteorder="big")
    dev_bytes = bytes.fromhex(normalize_mac(mac_dev).replace(":", ""))
    address = ipaddress.IPv6Address(tag + gw_bytes + counter_bytes + dev_bytes)
    return address.compressed.lower()


def build_auto_managed_explicit_ipv6(gw_mac, counter, gw_tag_hex=AUTO_MANAGED_EXPLICIT_TAG):
    if not gw_tag_hex:
        return None
    return build_explicit_ipv6(
        gw_tag_hex,
        gw_mac,
        AUTO_MANAGED_EXPLICIT_MAC_DEV,
        counter=int(counter) + 1,
    )


def parse_embedded_ipv6(value):
    address = ipaddress.IPv6Address(normalize_ipv6_address(value))
    packed = address.packed
    return {
        "requested_ipv6": address.compressed.lower(),
        "gw_tag_hex": packed[:EMBEDDED_TAG_BYTES].hex(),
        "target_gw_mac": normalize_mac(":".join(f"{byte:02x}" for byte in packed[2:8])),
        "encoded_counter": int.from_bytes(packed[8:10], byteorder="big"),
        "mac_dev": normalize_mac(":".join(f"{byte:02x}" for byte in packed[10:16])),
    }


def json_request(method, url, body=None, content_type="application/json"):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = content_type
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = response.read()
        if not payload:
            return None
        return json.loads(payload.decode("utf-8"))


def kube_request(method, path, body=None, content_type="application/json"):
    if not KUBE_API_HOST or not TOKEN or not SSL_CONTEXT:
        raise RuntimeError("Kubernetes API access is not available in this environment.")
    url = f"https://{KUBE_API_HOST}:{KUBE_API_PORT}{path}"
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


def find_node_agent(node_name):
    namespace = urllib.parse.quote(NODE_AGENT_NAMESPACE, safe="")
    label_selector = urllib.parse.quote(NODE_AGENT_LABEL_SELECTOR, safe="")
    field_selector = urllib.parse.quote(f"spec.nodeName={node_name}", safe="")
    payload = kube_request(
        "GET",
        f"/api/v1/namespaces/{namespace}/pods?labelSelector={label_selector}&fieldSelector={field_selector}",
    )
    for item in payload.get("items", []):
        status = item.get("status") or {}
        if status.get("phase") != "Running":
            continue
        pod_ip = status.get("podIP")
        if pod_ip:
            return {
                "name": (item.get("metadata") or {}).get("name"),
                "uid": (item.get("metadata") or {}).get("uid"),
                "namespace": NODE_AGENT_NAMESPACE,
                "pod_ip": pod_ip,
                "node_name": node_name,
            }
    raise RuntimeError(f"No running CMXsafeMAC-IPv6-node-agent pod found on node {node_name}.")


def assignment_node_agent(assignment):
    pod_ip = str(assignment.get("node_agent_pod_ip") or "").strip()
    if not pod_ip:
        return None
    return {
        "name": str(assignment.get("node_agent_pod_name") or "").strip() or None,
        "uid": str(assignment.get("node_agent_pod_uid") or "").strip() or None,
        "namespace": NODE_AGENT_NAMESPACE,
        "pod_ip": pod_ip,
        "node_name": str(assignment.get("node_name") or "").strip() or None,
    }


def assignment_owner_details(assignment):
    if not assignment:
        return None
    owner = {
        "namespace": str(assignment.get("namespace") or "").strip() or None,
        "pod_name": str(assignment.get("pod_name") or "").strip() or None,
        "pod_uid": str(assignment.get("pod_uid") or "").strip() or None,
        "node_name": str(assignment.get("node_name") or "").strip() or None,
        "target_assigned_mac": str(assignment.get("target_assigned_mac") or "").strip() or None,
        "target_counter": assignment.get("target_counter"),
        "node_agent_pod_name": str(assignment.get("node_agent_pod_name") or "").strip() or None,
        "node_agent_pod_uid": str(assignment.get("node_agent_pod_uid") or "").strip() or None,
        "node_agent_pod_ip": str(assignment.get("node_agent_pod_ip") or "").strip() or None,
    }
    owner.update(normalize_runtime_snapshot(assignment))
    return owner


def explicit_runtime_cleanup_entry(assignment):
    entry = {
        "namespace": str(assignment.get("namespace") or "").strip(),
        "pod_name": str(assignment.get("pod_name") or "").strip(),
        "pod_uid": str(assignment.get("pod_uid") or "").strip(),
        "requested_ipv6": normalize_ipv6_address(assignment["requested_ipv6"]),
    }
    entry.update(normalize_runtime_snapshot(assignment))
    return entry


def owners_match(left, right):
    if not left or not right:
        return False
    return (
        str(left.get("pod_uid") or "").strip() == str(right.get("pod_uid") or "").strip()
        and str(left.get("target_assigned_mac") or "").strip().lower()
        == str(right.get("target_assigned_mac") or "").strip().lower()
    )


def normalize_optional_int(value):
    if value in (None, ""):
        return None
    return int(value)


def normalize_runtime_snapshot(source, observed_at=None):
    if not source:
        return {field: None for field in RUNTIME_SNAPSHOT_FIELDS}
    snapshot = {
        "sandbox_id": str(source.get("sandbox_id") or "").strip() or None,
        "sandbox_pid": normalize_optional_int(source.get("sandbox_pid") or source.get("pid")),
        "sandbox_pid_start_time": normalize_optional_int(source.get("sandbox_pid_start_time") or source.get("pid_start_time")),
        "netns_inode": normalize_optional_int(source.get("netns_inode")),
        "runtime_observed_at": str(source.get("runtime_observed_at") or "").strip() or None,
    }
    if any(snapshot[field] is not None for field in RUNTIME_SNAPSHOT_FIELDS[:-1]) and not snapshot["runtime_observed_at"]:
        snapshot["runtime_observed_at"] = observed_at or now_utc()
    return snapshot


def assignment_job_token(assignment):
    return (
        str(assignment.get("updated_at") or "").strip(),
        str(assignment.get("pod_uid") or "").strip(),
        str(assignment.get("target_assigned_mac") or "").strip().lower(),
    )


def register_explicit_apply_job(assignment):
    requested_ipv6 = normalize_ipv6_address(assignment["requested_ipv6"])
    token = assignment_job_token(assignment)
    with EXPLICIT_APPLY_JOBS_LOCK:
        EXPLICIT_APPLY_JOBS[requested_ipv6] = token
    return token


def is_latest_explicit_apply_job(assignment):
    requested_ipv6 = normalize_ipv6_address(assignment["requested_ipv6"])
    token = assignment_job_token(assignment)
    with EXPLICIT_APPLY_JOBS_LOCK:
        current = EXPLICIT_APPLY_JOBS.get(requested_ipv6)
    return current == token


def explicit_apply_batch_key(assignment):
    agent = assignment_node_agent(assignment) or {}
    return (
        str(assignment.get("node_name") or "").strip(),
        str(assignment.get("pod_uid") or "").strip(),
        str(agent.get("pod_ip") or "").strip(),
        str(assignment.get("container_iface") or "").strip(),
    )


def explicit_apply_batcher(assignment):
    key = explicit_apply_batch_key(assignment)
    with EXPLICIT_APPLY_BATCHERS_LOCK:
        batcher = EXPLICIT_APPLY_BATCHERS.get(key)
        if batcher is None:
            batcher = ExplicitApplyPodBatcher(key)
            EXPLICIT_APPLY_BATCHERS[key] = batcher
        return batcher


def explicit_move_batch_key(assignment):
    agent = assignment_node_agent(assignment) or {}
    requested_ipv6 = normalize_ipv6_address(assignment.get("requested_ipv6"))
    shard = 0
    if requested_ipv6 and EXPLICIT_MOVE_DISPATCH_SHARDS > 1:
        shard = hash(requested_ipv6) % EXPLICIT_MOVE_DISPATCH_SHARDS
    return (
        str(assignment.get("node_name") or "").strip(),
        str(agent.get("pod_ip") or "").strip(),
        str(assignment.get("container_iface") or "").strip(),
        shard,
    )


def explicit_move_batcher(assignment, previous_owner=None):
    queued_assignment = dict(assignment)
    queued_assignment["_previous_owner"] = dict(previous_owner) if isinstance(previous_owner, dict) else previous_owner
    key = explicit_move_batch_key(queued_assignment)
    with EXPLICIT_MOVE_BATCHERS_LOCK:
        batcher = EXPLICIT_MOVE_BATCHERS.get(key)
        if batcher is None:
            batcher = ExplicitMoveBatcher(key)
            EXPLICIT_MOVE_BATCHERS[key] = batcher
        return batcher


def explicit_apply_payload(assignment, previous_owner=None, trace_context=None):
    body = {
        "ipv6_address": assignment["requested_ipv6"],
        "namespace": assignment["namespace"],
        "pod_name": assignment["pod_name"],
        "pod_uid": assignment["pod_uid"],
        "container_iface": assignment["container_iface"],
        "target_assigned_mac": assignment["target_assigned_mac"],
        "skip_allocator_applied_callback": True,
    }
    body.update(normalize_runtime_snapshot(assignment))
    if trace_context:
        body.update(trace_context)
    if previous_owner:
        body["previous_owner"] = previous_owner
    return body


def explicit_applied_payload(assignment, trace_context=None):
    payload = {
        "requested_ipv6": assignment.get("requested_ipv6"),
        "namespace": assignment.get("namespace"),
        "pod_name": assignment.get("pod_name"),
        "pod_uid": assignment.get("pod_uid"),
        "node_name": assignment.get("node_name"),
        "container_iface": assignment.get("container_iface"),
        "target_assigned_mac": assignment.get("target_assigned_mac"),
    }
    if trace_context:
        payload.update(trace_context)
    return payload


def apply_explicit_ipv6_on_node(assignment, previous_owner=None, trace_context=None):
    agent = assignment_node_agent(assignment)
    body = explicit_apply_payload(assignment, previous_owner=previous_owner, trace_context=trace_context)
    if agent:
        url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/apply"
        try:
            body["trace_allocator_sent_at_ms"] = current_epoch_ms()
            result = json_request("POST", url, body)
            result["agent_lookup"] = "db-row"
            result["agent"] = agent
            return result
        except Exception:
            pass
    agent = find_node_agent(assignment["node_name"])
    url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/apply"
    body["trace_allocator_sent_at_ms"] = current_epoch_ms()
    result = json_request("POST", url, body)
    result["agent_lookup"] = "kubernetes-lookup"
    result["agent"] = agent
    return result


def apply_explicit_ipv6_batch_on_node(items):
    first = items[0]
    assignment = first.assignment
    agent = assignment_node_agent(assignment)
    target = {
        "namespace": assignment["namespace"],
        "pod_name": assignment["pod_name"],
        "pod_uid": assignment["pod_uid"],
        "container_iface": assignment["container_iface"],
        "target_assigned_mac": assignment["target_assigned_mac"],
        "skip_allocator_applied_callback": True,
    }
    target.update(normalize_runtime_snapshot(assignment))
    entries = []
    sent_at_ms = current_epoch_ms()
    for item in items:
        entry = {
            "requested_ipv6": item.assignment["requested_ipv6"],
        }
        if item.previous_owner:
            entry["previous_owner"] = item.previous_owner
        if item.trace_context:
            entry.update(item.trace_context)
        entry["trace_allocator_sent_at_ms"] = sent_at_ms
        entries.append(entry)
    body = {
        "target": target,
        "entries": entries,
    }
    if agent:
        url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/bulk-apply"
        try:
            result = json_request("POST", url, body)
            result["agent_lookup"] = "db-row"
            result["agent"] = agent
            return result
        except Exception:
            pass
    agent = find_node_agent(assignment["node_name"])
    url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/bulk-apply"
    result = json_request("POST", url, body)
    result["agent_lookup"] = "kubernetes-lookup"
    result["agent"] = agent
    return result


def apply_explicit_ipv6_move_batch_on_node(items):
    first = items[0]
    assignment = first.assignment
    agent = assignment_node_agent(assignment)
    sent_at_ms = current_epoch_ms()
    entries = []
    for item in items:
        target = {
            "namespace": item.assignment["namespace"],
            "pod_name": item.assignment["pod_name"],
            "pod_uid": item.assignment["pod_uid"],
            "container_iface": item.assignment["container_iface"],
            "target_assigned_mac": item.assignment["target_assigned_mac"],
            "skip_allocator_applied_callback": True,
        }
        target.update(normalize_runtime_snapshot(item.assignment))
        entry = {
            "requested_ipv6": item.assignment["requested_ipv6"],
            "target": target,
            "trace_allocator_sent_at_ms": sent_at_ms,
        }
        if item.previous_owner:
            entry["previous_owner"] = item.previous_owner
        if item.trace_context:
            entry.update(item.trace_context)
        entries.append(entry)
    body = {"entries": entries}
    if agent:
        url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/bulk-move"
        try:
            result = json_request("POST", url, body)
            result["agent_lookup"] = "db-row"
            result["agent"] = agent
            return result
        except Exception:
            pass
    agent = find_node_agent(assignment["node_name"])
    url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/bulk-move"
    result = json_request("POST", url, body)
    result["agent_lookup"] = "kubernetes-lookup"
    result["agent"] = agent
    return result


def dispatch_explicit_ipv6_apply(assignment, previous_owner=None, trace_context=None):
    if not ASYNC_EXPLICIT_APPLY_ENABLED:
        started = time.perf_counter()
        try:
            result = apply_explicit_ipv6_on_node(assignment, previous_owner=previous_owner, trace_context=trace_context)
            log_explicit_trace(
                source="allocator-sync",
                requested_ipv6=assignment.get("requested_ipv6"),
                pod_uid=assignment.get("pod_uid"),
                node_call_ms=elapsed_ms(started),
                total_ms=elapsed_ms(started),
                lookup=result.get("agent_lookup"),
                status=result.get("status"),
                **trace_log_fields(trace_context),
            )
            return HTTPStatus.CREATED, result
        except Exception as exc:
            log_explicit_trace(
                source="allocator-sync-error",
                requested_ipv6=assignment.get("requested_ipv6"),
                pod_uid=assignment.get("pod_uid"),
                total_ms=elapsed_ms(started),
                error=type(exc).__name__,
                **trace_log_fields(trace_context),
            )
            return HTTPStatus.ACCEPTED, {"status": "pending", "error": str(exc)}
    register_explicit_apply_job(assignment)
    queued_assignment = dict(assignment)
    queued_assignment["_trace_enqueued_at"] = time.perf_counter()
    if previous_owner:
        explicit_move_batcher(queued_assignment, previous_owner=previous_owner).submit(
            ExplicitApplyQueueItem(queued_assignment, previous_owner=previous_owner, trace_context=trace_context)
        )
    else:
        explicit_apply_batcher(queued_assignment).submit(
            ExplicitApplyQueueItem(queued_assignment, previous_owner=previous_owner, trace_context=trace_context)
        )
    return HTTPStatus.ACCEPTED, {
        "status": "queued",
        "mode": "async",
        "requested_ipv6": assignment["requested_ipv6"],
    }


def clear_explicit_ipv6_runtime(assignments):
    grouped = {}
    for assignment in assignments:
        if not assignment.get("requested_ipv6"):
            continue
        agent = assignment_node_agent(assignment)
        key = (str(assignment.get("node_name") or "").strip(), str((agent or {}).get("pod_ip") or "").strip())
        bucket = grouped.setdefault(
            key,
            {
                "node_name": str(assignment.get("node_name") or "").strip(),
                "agent": agent,
                "entries": [],
            },
        )
        bucket["entries"].append(explicit_runtime_cleanup_entry(assignment))

    results = []
    for bucket in grouped.values():
        node_name = bucket["node_name"]
        entries = bucket["entries"]
        agent = bucket["agent"]
        agent_lookup = "db-row"
        if not agent or not agent.get("pod_ip"):
            agent = find_node_agent(node_name)
            agent_lookup = "kubernetes-lookup"
        url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/clear"
        body = {"entries": entries}
        try:
            response = json_request("POST", url, body)
        except Exception:
            if agent_lookup == "kubernetes-lookup":
                raise
            agent = find_node_agent(node_name)
            url = f"http://{agent['pod_ip']}:{NODE_AGENT_HTTP_PORT}/explicit-ipv6/clear"
            response = json_request("POST", url, body)
            agent_lookup = "kubernetes-lookup"
        results.append(
            {
                "node_name": node_name,
                "agent_lookup": agent_lookup,
                "agent": agent,
                "entries": len(entries),
                "response": response,
            }
        )
    return results


def run_explicit_ipv6_apply_task(assignment, previous_owner=None, trace_context=None):
    requested_ipv6 = normalize_ipv6_address(assignment["requested_ipv6"])
    worker_started = time.perf_counter()
    enqueued_at = assignment.pop("_trace_enqueued_at", None)
    queue_wait_ms = round((worker_started - enqueued_at) * 1000.0, 2) if enqueued_at else 0.0
    try:
        if not is_latest_explicit_apply_job(assignment):
            queued_owner = assignment_owner_details(assignment)
            log(
                f"explicit-apply-skip ipv6={requested_ipv6} reason=stale-job "
                f"queued_uid={queued_owner.get('pod_uid')}"
            )
            log_explicit_trace(
                source="allocator-worker-skip",
                requested_ipv6=requested_ipv6,
                pod_uid=queued_owner.get("pod_uid"),
                queue_wait_ms=queue_wait_ms,
                total_ms=elapsed_ms(worker_started),
                reason="stale-job",
                **trace_log_fields(trace_context),
            )
            return
        node_call_started = time.perf_counter()
        result = apply_explicit_ipv6_on_node(assignment, previous_owner=previous_owner, trace_context=trace_context)
        applied_db_started = time.perf_counter()
        Handler.store.mark_explicit_ipv6_applied(
            {
                "requested_ipv6": assignment.get("requested_ipv6"),
                "namespace": assignment.get("namespace"),
                "pod_name": assignment.get("pod_name"),
                "pod_uid": assignment.get("pod_uid"),
                "node_name": assignment.get("node_name"),
                "container_iface": assignment.get("container_iface"),
                "target_assigned_mac": assignment.get("target_assigned_mac"),
                **dict(trace_context or {}),
            }
        )
        log(
            f"explicit-apply-dispatched ipv6={requested_ipv6} status={result.get('status')} "
            f"lookup={result.get('agent_lookup')} pod_uid={assignment.get('pod_uid')}"
        )
        log_explicit_trace(
            source="allocator-worker",
            requested_ipv6=requested_ipv6,
            pod_uid=assignment.get("pod_uid"),
            queue_wait_ms=queue_wait_ms,
            node_call_ms=elapsed_ms(node_call_started),
            applied_db_ms=elapsed_ms(applied_db_started),
            total_ms=elapsed_ms(worker_started),
            lookup=result.get("agent_lookup"),
            status=result.get("status"),
            **trace_log_fields(trace_context),
        )
    except Exception as exc:
        log(f"explicit-apply-error ipv6={requested_ipv6}: {exc}")
        log_explicit_trace(
            source="allocator-worker-error",
            requested_ipv6=requested_ipv6,
            pod_uid=assignment.get("pod_uid"),
            queue_wait_ms=queue_wait_ms,
            total_ms=elapsed_ms(worker_started),
            error=type(exc).__name__,
            **trace_log_fields(trace_context),
        )


def run_explicit_ipv6_apply_batch_task(items):
    if not items:
        return
    worker_started = time.perf_counter()
    active_items = []
    for item in items:
        assignment = item.assignment
        requested_ipv6 = normalize_ipv6_address(assignment["requested_ipv6"])
        enqueued_at = assignment.get("_trace_enqueued_at")
        queue_wait_ms = round((worker_started - enqueued_at) * 1000.0, 2) if enqueued_at else 0.0
        if not is_latest_explicit_apply_job(assignment):
            queued_owner = assignment_owner_details(assignment)
            log(
                f"explicit-apply-skip ipv6={requested_ipv6} reason=stale-job "
                f"queued_uid={queued_owner.get('pod_uid')}"
            )
            log_explicit_trace(
                source="allocator-worker-skip",
                requested_ipv6=requested_ipv6,
                pod_uid=queued_owner.get("pod_uid"),
                queue_wait_ms=queue_wait_ms,
                total_ms=elapsed_ms(worker_started),
                reason="stale-job",
                **trace_log_fields(item.trace_context),
            )
            continue
        active_items.append((item, queue_wait_ms))
    if not active_items:
        return
    node_call_started = time.perf_counter()
    try:
        if len(active_items) == 1:
            item, queue_wait_ms = active_items[0]
            result = apply_explicit_ipv6_on_node(
                item.assignment,
                previous_owner=item.previous_owner,
                trace_context=item.trace_context,
            )
            applied_db_started = time.perf_counter()
            Handler.store.mark_explicit_ipv6_applied(explicit_applied_payload(item.assignment, item.trace_context))
            applied_db_ms = elapsed_ms(applied_db_started)
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            log(
                f"explicit-apply-dispatched ipv6={requested_ipv6} status={result.get('status')} "
                f"lookup={result.get('agent_lookup')} pod_uid={item.assignment.get('pod_uid')}"
            )
            log_explicit_trace(
                source="allocator-worker",
                requested_ipv6=requested_ipv6,
                pod_uid=item.assignment.get("pod_uid"),
                queue_wait_ms=queue_wait_ms,
                node_call_ms=elapsed_ms(node_call_started),
                applied_db_ms=applied_db_ms,
                total_ms=elapsed_ms(worker_started),
                lookup=result.get("agent_lookup"),
                status=result.get("status"),
                batch_size=1,
                **trace_log_fields(item.trace_context),
            )
            return

        result = apply_explicit_ipv6_batch_on_node([item for item, _ in active_items])
        result_entries = result.get("results") if isinstance(result.get("results"), list) else []
        result_by_ipv6 = {}
        for entry in result_entries:
            ipv6 = normalize_ipv6_address(entry.get("requested_ipv6") or entry.get("ipv6_address"))
            if ipv6:
                result_by_ipv6[ipv6] = entry
        successful_payloads = []
        failed_entries = []
        for item, _ in active_items:
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            entry_result = result_by_ipv6.get(requested_ipv6)
            if entry_result and str(entry_result.get("status") or "").strip().lower() == "applied":
                successful_payloads.append(explicit_applied_payload(item.assignment, item.trace_context))
            else:
                failed_entries.append((item, entry_result))
        applied_db_ms = 0.0
        if successful_payloads:
            applied_db_started = time.perf_counter()
            Handler.store.mark_explicit_ipv6_applied_batch({"entries": successful_payloads})
            applied_db_ms = elapsed_ms(applied_db_started)
        node_call_ms = elapsed_ms(node_call_started)
        total_ms = elapsed_ms(worker_started)
        batch_size = len(active_items)
        for item, queue_wait_ms in active_items:
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            entry_result = result_by_ipv6.get(requested_ipv6)
            if entry_result and str(entry_result.get("status") or "").strip().lower() == "applied":
                log(
                    f"explicit-apply-dispatched ipv6={requested_ipv6} status=applied "
                    f"lookup={result.get('agent_lookup')} pod_uid={item.assignment.get('pod_uid')} batch={batch_size}"
                )
                log_explicit_trace(
                    source="allocator-worker",
                    requested_ipv6=requested_ipv6,
                    pod_uid=item.assignment.get("pod_uid"),
                    queue_wait_ms=queue_wait_ms,
                    node_call_ms=node_call_ms,
                    applied_db_ms=applied_db_ms,
                    total_ms=total_ms,
                    lookup=result.get("agent_lookup"),
                    status="applied",
                    batch_size=batch_size,
                    **trace_log_fields(item.trace_context),
                )
            else:
                error = str((entry_result or {}).get("error") or "batch-entry-failed")
                log(f"explicit-apply-error ipv6={requested_ipv6}: {error}")
                log_explicit_trace(
                    source="allocator-worker-error",
                    requested_ipv6=requested_ipv6,
                    pod_uid=item.assignment.get("pod_uid"),
                    queue_wait_ms=queue_wait_ms,
                    total_ms=total_ms,
                    error=error,
                    batch_size=batch_size,
                    **trace_log_fields(item.trace_context),
                )
    except Exception as exc:
        total_ms = elapsed_ms(worker_started)
        for item, queue_wait_ms in active_items:
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            log(f"explicit-apply-error ipv6={requested_ipv6}: {exc}")
            log_explicit_trace(
                source="allocator-worker-error",
                requested_ipv6=requested_ipv6,
                pod_uid=item.assignment.get("pod_uid"),
                queue_wait_ms=queue_wait_ms,
                total_ms=total_ms,
                error=type(exc).__name__,
                batch_size=len(active_items),
                **trace_log_fields(item.trace_context),
            )


def run_explicit_ipv6_move_batch_task(items):
    if not items:
        return
    worker_started = time.perf_counter()
    active_items = []
    for item in items:
        assignment = item.assignment
        requested_ipv6 = normalize_ipv6_address(assignment["requested_ipv6"])
        enqueued_at = assignment.get("_trace_enqueued_at")
        queue_wait_ms = round((worker_started - enqueued_at) * 1000.0, 2) if enqueued_at else 0.0
        if not is_latest_explicit_apply_job(assignment):
            queued_owner = assignment_owner_details(assignment)
            log(
                f"explicit-apply-skip ipv6={requested_ipv6} reason=stale-job "
                f"queued_uid={queued_owner.get('pod_uid')}"
            )
            log_explicit_trace(
                source="allocator-worker-skip",
                requested_ipv6=requested_ipv6,
                pod_uid=queued_owner.get("pod_uid"),
                queue_wait_ms=queue_wait_ms,
                total_ms=elapsed_ms(worker_started),
                reason="stale-job",
                **trace_log_fields(item.trace_context),
            )
            continue
        active_items.append((item, queue_wait_ms))
    if not active_items:
        return
    node_call_started = time.perf_counter()
    try:
        result = apply_explicit_ipv6_move_batch_on_node([item for item, _ in active_items])
        result_entries = result.get("results") if isinstance(result.get("results"), list) else []
        result_by_ipv6 = {}
        for entry in result_entries:
            ipv6 = normalize_ipv6_address(entry.get("requested_ipv6") or entry.get("ipv6_address"))
            if ipv6:
                result_by_ipv6[ipv6] = entry
        successful_payloads = []
        for item, _ in active_items:
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            entry_result = result_by_ipv6.get(requested_ipv6)
            if entry_result and str(entry_result.get("status") or "").strip().lower() == "applied":
                successful_payloads.append(explicit_applied_payload(item.assignment, item.trace_context))
        applied_db_ms = 0.0
        if successful_payloads:
            applied_db_started = time.perf_counter()
            Handler.store.mark_explicit_ipv6_applied_batch({"entries": successful_payloads})
            applied_db_ms = elapsed_ms(applied_db_started)
        node_call_ms = elapsed_ms(node_call_started)
        total_ms = elapsed_ms(worker_started)
        batch_size = len(active_items)
        for item, queue_wait_ms in active_items:
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            entry_result = result_by_ipv6.get(requested_ipv6)
            if entry_result and str(entry_result.get("status") or "").strip().lower() == "applied":
                log(
                    f"explicit-move-dispatched ipv6={requested_ipv6} status=applied "
                    f"lookup={result.get('agent_lookup')} pod_uid={item.assignment.get('pod_uid')} batch={batch_size}"
                )
                log_explicit_trace(
                    source="allocator-worker",
                    requested_ipv6=requested_ipv6,
                    pod_uid=item.assignment.get("pod_uid"),
                    queue_wait_ms=queue_wait_ms,
                    node_call_ms=node_call_ms,
                    applied_db_ms=applied_db_ms,
                    total_ms=total_ms,
                    lookup=result.get("agent_lookup"),
                    status="applied",
                    batch_size=batch_size,
                    **trace_log_fields(item.trace_context),
                )
            else:
                error = str((entry_result or {}).get("error") or "move-batch-entry-failed")
                log(f"explicit-move-error ipv6={requested_ipv6}: {error}")
                log_explicit_trace(
                    source="allocator-worker-error",
                    requested_ipv6=requested_ipv6,
                    pod_uid=item.assignment.get("pod_uid"),
                    queue_wait_ms=queue_wait_ms,
                    total_ms=total_ms,
                    error=error,
                    batch_size=batch_size,
                    **trace_log_fields(item.trace_context),
                )
    except Exception as exc:
        total_ms = elapsed_ms(worker_started)
        for item, queue_wait_ms in active_items:
            requested_ipv6 = normalize_ipv6_address(item.assignment["requested_ipv6"])
            log(f"explicit-move-error ipv6={requested_ipv6}: {exc}")
            log_explicit_trace(
                source="allocator-worker-error",
                requested_ipv6=requested_ipv6,
                pod_uid=item.assignment.get("pod_uid"),
                queue_wait_ms=queue_wait_ms,
                total_ms=total_ms,
                error=type(exc).__name__,
                batch_size=len(active_items),
                **trace_log_fields(item.trace_context),
            )


class Store:
    def __init__(self):
        self._pool = ConnectionPool(
            conninfo=DATABASE_URL or "",
            kwargs=self._connect_kwargs(),
            min_size=DB_POOL_MIN_SIZE,
            max_size=DB_POOL_MAX_SIZE,
            timeout=DB_POOL_TIMEOUT_SECONDS,
            max_idle=DB_POOL_MAX_IDLE_SECONDS,
            open=True,
            name="allocator-postgres",
        )
        self._pool.wait()
        self._init_db()

    def _run_write(self, operation, retries=1):
        attempts = max(1, retries)
        last_error = None
        for attempt in range(attempts):
            try:
                with self._conn() as conn:
                    conn.execute("BEGIN")
                    result = operation(conn)
                    conn.commit()
                    return result
            except DB_INTEGRITY_ERRORS as exc:
                last_error = exc
                if attempt >= attempts - 1:
                    raise
                time.sleep(min(EXPLICIT_WRITE_RETRY_BASE_SECONDS * (2 ** attempt), 0.25))
        if last_error:
            raise last_error

    def _connect_kwargs(self):
        kwargs = {
            "row_factory": dict_row,
            "autocommit": True,
        }
        if not DATABASE_URL:
            kwargs.update(
                {
                    "host": POSTGRES_HOST,
                    "port": POSTGRES_PORT,
                    "dbname": POSTGRES_DB,
                    "user": POSTGRES_USER,
                    "password": POSTGRES_PASSWORD,
                    "sslmode": POSTGRES_SSLMODE,
                }
            )
        return kwargs

    @contextmanager
    def _conn(self):
        with self._pool.connection() as conn:
            yield ConnectionWrapper(conn)

    def _allocations_table_sql(self):
        return """
            CREATE TABLE IF NOT EXISTS mac_allocations (
                id BIGSERIAL PRIMARY KEY,
                assigned_mac TEXT NOT NULL,
                gw_mac TEXT NOT NULL,
                gw_iface TEXT NOT NULL,
                node_name TEXT NOT NULL,
                mac_dev TEXT NOT NULL,
                mac_dev_byte INTEGER NOT NULL,
                counter INTEGER NOT NULL,
                ipv6_prefix TEXT,
                assigned_ipv6 TEXT,
                namespace TEXT NOT NULL,
                pod_name TEXT NOT NULL,
                pod_uid TEXT NOT NULL,
                container_iface TEXT NOT NULL DEFAULT 'eth0',
                owner_kind TEXT,
                owner_name TEXT,
                owner_uid TEXT,
                pod_ordinal INTEGER,
                node_agent_pod_name TEXT,
                node_agent_pod_uid TEXT,
                node_agent_pod_ip TEXT,
                sandbox_id TEXT,
                sandbox_pid BIGINT,
                sandbox_pid_start_time BIGINT,
                netns_inode BIGINT,
                runtime_observed_at TEXT,
                status TEXT NOT NULL CHECK (status IN ('ALLOCATED', 'RELEASED', 'STALE')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                released_at TEXT
            );
        """

    def _explicit_assignments_table_sql(self):
        return """
            CREATE TABLE IF NOT EXISTS explicit_ipv6_assignments (
                id BIGSERIAL PRIMARY KEY,
                requested_ipv6 TEXT NOT NULL,
                gw_tag_hex TEXT NOT NULL,
                target_gw_mac TEXT NOT NULL,
                target_counter INTEGER NOT NULL,
                target_assigned_mac TEXT NOT NULL,
                mac_dev TEXT NOT NULL,
                namespace TEXT NOT NULL,
                pod_name TEXT NOT NULL,
                pod_uid TEXT NOT NULL,
                node_name TEXT NOT NULL,
                container_iface TEXT NOT NULL DEFAULT 'eth0',
                node_agent_pod_name TEXT,
                node_agent_pod_uid TEXT,
                node_agent_pod_ip TEXT,
                sandbox_id TEXT,
                sandbox_pid BIGINT,
                sandbox_pid_start_time BIGINT,
                netns_inode BIGINT,
                runtime_observed_at TEXT,
                status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'STALE')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_applied_at TEXT,
                released_at TEXT
            );
        """

    @staticmethod
    def _system_settings_table_sql():
        return """
            CREATE TABLE IF NOT EXISTS cmxsafe_system_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            );
        """

    @staticmethod
    def _index_sql():
        return """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_allocated_mac
                ON mac_allocations(assigned_mac) WHERE status = 'ALLOCATED';
            CREATE UNIQUE INDEX IF NOT EXISTS uq_allocated_ipv6
                ON mac_allocations(assigned_ipv6)
                WHERE status = 'ALLOCATED' AND assigned_ipv6 IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS uq_allocated_pod_iface
                ON mac_allocations(pod_uid, container_iface) WHERE status = 'ALLOCATED';
            CREATE INDEX IF NOT EXISTS ix_status_updated
                ON mac_allocations(status, updated_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS uq_active_explicit_ipv6
                ON explicit_ipv6_assignments(requested_ipv6) WHERE status = 'ACTIVE';
            CREATE INDEX IF NOT EXISTS ix_explicit_target_identity
                ON explicit_ipv6_assignments(target_gw_mac, target_counter, status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS ix_explicit_target_mac
                ON explicit_ipv6_assignments(target_assigned_mac, status, updated_at DESC);
        """

    @staticmethod
    def _migration_sql():
        return """
            DROP INDEX IF EXISTS uq_allocated_stable_key;
            DROP INDEX IF EXISTS ix_explicit_stable_key;
            ALTER TABLE mac_allocations DROP COLUMN IF EXISTS stable_key;
            ALTER TABLE explicit_ipv6_assignments DROP COLUMN IF EXISTS stable_key;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS node_agent_pod_name TEXT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS node_agent_pod_uid TEXT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS node_agent_pod_ip TEXT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS sandbox_id TEXT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS sandbox_pid BIGINT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS sandbox_pid_start_time BIGINT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS netns_inode BIGINT;
            ALTER TABLE mac_allocations ADD COLUMN IF NOT EXISTS runtime_observed_at TEXT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS node_agent_pod_name TEXT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS node_agent_pod_uid TEXT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS node_agent_pod_ip TEXT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS sandbox_id TEXT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS sandbox_pid BIGINT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS sandbox_pid_start_time BIGINT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS netns_inode BIGINT;
            ALTER TABLE explicit_ipv6_assignments ADD COLUMN IF NOT EXISTS runtime_observed_at TEXT;
        """

    def _init_db(self):
        with self._conn() as conn:
            statements = split_sql_statements(
                f"{self._allocations_table_sql()}{self._explicit_assignments_table_sql()}{self._system_settings_table_sql()}{self._migration_sql()}{self._index_sql()}"
            )
            for statement in statements:
                conn.execute(statement)
            if DEFAULT_CANONICAL_GATEWAY_MAC:
                conn.execute(
                    """
                    INSERT INTO cmxsafe_system_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT (key) DO NOTHING
                    RETURNING key
                    """,
                    (
                        CANONICAL_GATEWAY_MAC_SETTING,
                        normalize_mac(DEFAULT_CANONICAL_GATEWAY_MAC),
                        now_utc(),
                    ),
                )
            conn.commit()

    @staticmethod
    def _row(row):
        result = {key: row[key] for key in row.keys()}
        gw_mac = result.get("gw_mac") or result.get("target_gw_mac")
        counter = result.get("counter")
        if counter is None:
            counter = result.get("target_counter")
        try:
            result["auto_managed_explicit_ipv6"] = build_auto_managed_explicit_ipv6(gw_mac, counter)
        except Exception:
            result["auto_managed_explicit_ipv6"] = None
        return result

    def configured_canonical_gateway_mac(self, conn):
        row = conn.execute(
            "SELECT value FROM cmxsafe_system_settings WHERE key = ?",
            (CANONICAL_GATEWAY_MAC_SETTING,),
        ).fetchone()
        configured = normalize_optional_mac(row["value"] if row else None)
        if configured:
            return configured
        return normalize_optional_mac(DEFAULT_CANONICAL_GATEWAY_MAC)

    def list_allocations(self, filters):
        clauses = []
        values = []
        for key in ("status", "node_name", "namespace", "pod_uid", "assigned_mac"):
            if filters.get(key):
                clauses.append(f"{key} = ?")
                values.append(filters[key])
        query = "SELECT * FROM mac_allocations"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY CASE status WHEN 'ALLOCATED' THEN 0 WHEN 'STALE' THEN 1 ELSE 2 END, updated_at DESC, id DESC"
        with self._conn() as conn:
            return [self._row(row) for row in conn.execute(query, values).fetchall()]

    def list_explicit_ipv6_assignments(self, filters):
        clauses = []
        values = []
        for key in (
            "status",
            "node_name",
            "namespace",
            "pod_uid",
            "target_assigned_mac",
            "target_gw_mac",
            "target_counter",
            "requested_ipv6",
        ):
            if filters.get(key):
                clauses.append(f"{key} = ?")
                values.append(filters[key])
        query = "SELECT * FROM explicit_ipv6_assignments"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY CASE status WHEN 'ACTIVE' THEN 0 WHEN 'STALE' THEN 1 ELSE 2 END, updated_at DESC, id DESC"
        with self._conn() as conn:
            return [self._row(row) for row in conn.execute(query, values).fetchall()]

    def stats(self):
        with self._conn() as conn:
            rows = conn.execute("SELECT status, COUNT(*) AS count FROM mac_allocations GROUP BY status").fetchall()
            explicit_rows = conn.execute(
                "SELECT status, COUNT(*) AS count FROM explicit_ipv6_assignments GROUP BY status"
            ).fetchall()
        return {
            "generated_at": now_utc(),
            "allocations": {row["status"]: row["count"] for row in rows},
            "explicit_ipv6": {row["status"]: row["count"] for row in explicit_rows},
        }

    def _find_active_allocation(self, conn, iface, pod_uid=None, assigned_mac=None, gw_mac=None, counter=None):
        if pod_uid:
            return conn.execute(
                """
                SELECT * FROM mac_allocations
                WHERE pod_uid = ? AND container_iface = ? AND status = 'ALLOCATED'
                ORDER BY updated_at DESC, id DESC LIMIT 1
                """,
                (str(pod_uid).strip(), iface),
            ).fetchone()
        if assigned_mac:
            return conn.execute(
                """
                SELECT * FROM mac_allocations
                WHERE assigned_mac = ? AND container_iface = ? AND status = 'ALLOCATED'
                ORDER BY updated_at DESC, id DESC LIMIT 1
                """,
                (normalize_mac(assigned_mac), iface),
            ).fetchone()
        return conn.execute(
            """
            SELECT * FROM mac_allocations
            WHERE gw_mac = ? AND counter = ? AND container_iface = ? AND status = 'ALLOCATED'
            ORDER BY updated_at DESC, id DESC LIMIT 1
            """,
            (normalize_mac(gw_mac), int(counter), iface),
        ).fetchone()

    @staticmethod
    def _normalize_node_agent(agent):
        return {
            "node_agent_pod_name": str(agent.get("name") or "").strip() or None,
            "node_agent_pod_uid": str(agent.get("uid") or "").strip() or None,
            "node_agent_pod_ip": str(agent.get("pod_ip") or "").strip() or None,
        }

    def _upsert_explicit_assignment(self, conn, record, now):
        active = conn.execute(
            """
            SELECT * FROM explicit_ipv6_assignments
            WHERE requested_ipv6 = ?
              AND status = 'ACTIVE'
            ORDER BY updated_at DESC, id DESC LIMIT 1
            """,
            (record["requested_ipv6"],),
        ).fetchone()
        previous_owner = assignment_owner_details(active)
        if owners_match(previous_owner, assignment_owner_details(record)):
            previous_owner = None
        carry_last_applied_at = None if previous_owner else (active["last_applied_at"] if active else None)
        if active:
            conn.execute(
                """
                UPDATE explicit_ipv6_assignments
                SET gw_tag_hex = ?, target_gw_mac = ?, target_counter = ?, target_assigned_mac = ?, mac_dev = ?,
                    namespace = ?, pod_name = ?, pod_uid = ?, node_name = ?, container_iface = ?,
                    node_agent_pod_name = ?, node_agent_pod_uid = ?, node_agent_pod_ip = ?,
                    sandbox_id = ?, sandbox_pid = ?, sandbox_pid_start_time = ?, netns_inode = ?, runtime_observed_at = ?,
                    status = 'ACTIVE', updated_at = ?, last_applied_at = ?, released_at = NULL
                WHERE id = ?
                RETURNING *
                """,
                (
                    record["gw_tag_hex"],
                    record["target_gw_mac"],
                    record["target_counter"],
                    record["target_assigned_mac"],
                    record["mac_dev"],
                    record["namespace"],
                    record["pod_name"],
                    record["pod_uid"],
                    record["node_name"],
                    record["container_iface"],
                    record["node_agent_pod_name"],
                    record["node_agent_pod_uid"],
                    record["node_agent_pod_ip"],
                    record["sandbox_id"],
                    record["sandbox_pid"],
                    record["sandbox_pid_start_time"],
                    record["netns_inode"],
                    record["runtime_observed_at"],
                    now,
                    carry_last_applied_at,
                    active["id"],
                ),
            ).fetchone(), previous_owner
        return conn.execute(
            """
            INSERT INTO explicit_ipv6_assignments (
                requested_ipv6, gw_tag_hex, target_gw_mac, target_counter, target_assigned_mac, mac_dev,
                namespace, pod_name, pod_uid, node_name, container_iface,
                node_agent_pod_name, node_agent_pod_uid, node_agent_pod_ip,
                sandbox_id, sandbox_pid, sandbox_pid_start_time, netns_inode, runtime_observed_at,
                status, created_at, updated_at, last_applied_at, released_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, NULL)
            ON CONFLICT (requested_ipv6) WHERE status = 'ACTIVE'
            DO UPDATE SET
                gw_tag_hex = EXCLUDED.gw_tag_hex,
                target_gw_mac = EXCLUDED.target_gw_mac,
                target_counter = EXCLUDED.target_counter,
                target_assigned_mac = EXCLUDED.target_assigned_mac,
                mac_dev = EXCLUDED.mac_dev,
                namespace = EXCLUDED.namespace,
                pod_name = EXCLUDED.pod_name,
                pod_uid = EXCLUDED.pod_uid,
                node_name = EXCLUDED.node_name,
                container_iface = EXCLUDED.container_iface,
                node_agent_pod_name = EXCLUDED.node_agent_pod_name,
                node_agent_pod_uid = EXCLUDED.node_agent_pod_uid,
                node_agent_pod_ip = EXCLUDED.node_agent_pod_ip,
                sandbox_id = EXCLUDED.sandbox_id,
                sandbox_pid = EXCLUDED.sandbox_pid,
                sandbox_pid_start_time = EXCLUDED.sandbox_pid_start_time,
                netns_inode = EXCLUDED.netns_inode,
                runtime_observed_at = EXCLUDED.runtime_observed_at,
                status = 'ACTIVE',
                updated_at = EXCLUDED.updated_at,
                last_applied_at = EXCLUDED.last_applied_at,
                released_at = NULL
            RETURNING *
            """,
            (
                record["requested_ipv6"],
                record["gw_tag_hex"],
                record["target_gw_mac"],
                record["target_counter"],
                record["target_assigned_mac"],
                record["mac_dev"],
                record["namespace"],
                record["pod_name"],
                record["pod_uid"],
                record["node_name"],
                record["container_iface"],
                record["node_agent_pod_name"],
                record["node_agent_pod_uid"],
                record["node_agent_pod_ip"],
                record["sandbox_id"],
                record["sandbox_pid"],
                record["sandbox_pid_start_time"],
                record["netns_inode"],
                record["runtime_observed_at"],
                now,
                now,
                carry_last_applied_at,
            ),
        ).fetchone(), previous_owner

    def ensure(self, payload):
        required = ("gw_mac", "gw_iface", "node_name", "namespace", "pod_name", "pod_uid")
        missing = [field for field in required if not payload.get(field)]
        if missing:
            raise ValueError(f"Missing required fields: {', '.join(missing)}")
        mac_dev = str(payload.get("mac_dev") or "").strip()
        gw_mac = normalize_mac(payload["gw_mac"])
        record = {
            "gw_mac": gw_mac,
            "gw_iface": str(payload["gw_iface"]).strip(),
            "node_name": str(payload["node_name"]).strip(),
            "mac_dev": mac_dev,
            "mac_dev_byte": derive_device_byte(mac_dev) if mac_dev else 0,
            "ipv6_prefix": normalize_ipv6_prefix(payload.get("ipv6_prefix")),
            "namespace": str(payload["namespace"]).strip(),
            "pod_name": str(payload["pod_name"]).strip(),
            "pod_uid": str(payload["pod_uid"]).strip(),
            "container_iface": str(payload.get("container_iface") or "eth0").strip(),
            "owner_kind": payload.get("owner_kind"),
            "owner_name": payload.get("owner_name"),
            "owner_uid": payload.get("owner_uid"),
            "pod_ordinal": payload.get("pod_ordinal"),
        }
        node_agent = self._normalize_node_agent(find_node_agent(record["node_name"]))
        record.update(node_agent)
        record.update(normalize_runtime_snapshot(payload))
        now = now_utc()
        def operation(conn):
            effective_gw_mac = self.configured_canonical_gateway_mac(conn) or record["gw_mac"]
            record["gw_mac"] = effective_gw_mac
            existing = conn.execute(
                "SELECT * FROM mac_allocations WHERE pod_uid = ? AND container_iface = ? AND status = 'ALLOCATED'",
                (record["pod_uid"], record["container_iface"]),
            ).fetchone()
            if existing:
                assigned_mac = format_mac(record["gw_mac"], existing["counter"])
                assigned_ipv6 = format_ipv6(record["ipv6_prefix"], existing["counter"])
                conn.execute(
                    """
                    UPDATE mac_allocations
                    SET assigned_mac = ?, gw_mac = ?, gw_iface = ?, node_name = ?, mac_dev = ?, mac_dev_byte = ?, ipv6_prefix = ?, assigned_ipv6 = ?,
                        namespace = ?, pod_name = ?, owner_kind = ?, owner_name = ?, owner_uid = ?,
                        pod_ordinal = ?, node_agent_pod_name = ?, node_agent_pod_uid = ?, node_agent_pod_ip = ?,
                        sandbox_id = ?, sandbox_pid = ?, sandbox_pid_start_time = ?, netns_inode = ?, runtime_observed_at = ?,
                        updated_at = ?, last_seen_at = ?
                    WHERE id = ?
                    """,
                    (
                        assigned_mac,
                        record["gw_mac"],
                        record["gw_iface"],
                        record["node_name"],
                        record["mac_dev"],
                        record["mac_dev_byte"],
                        record["ipv6_prefix"],
                        assigned_ipv6,
                        record["namespace"],
                        record["pod_name"],
                        record["owner_kind"],
                        record["owner_name"],
                        record["owner_uid"],
                        record["pod_ordinal"],
                        record["node_agent_pod_name"],
                        record["node_agent_pod_uid"],
                        record["node_agent_pod_ip"],
                        record["sandbox_id"],
                        record["sandbox_pid"],
                        record["sandbox_pid_start_time"],
                        record["netns_inode"],
                        record["runtime_observed_at"],
                        now,
                        now,
                        existing["id"],
                    ),
                )
                row = conn.execute("SELECT * FROM mac_allocations WHERE id = ?", (existing["id"],)).fetchone()
                return self._row(row)

            for counter in range(65536):
                candidate = format_mac(record["gw_mac"], counter)
                candidate_ipv6 = format_ipv6(record["ipv6_prefix"], counter)
                if candidate_ipv6 is not None:
                    conflict = conn.execute(
                        """
                        SELECT id FROM mac_allocations
                        WHERE status = 'ALLOCATED'
                          AND (assigned_mac = ? OR assigned_ipv6 = ?)
                        """,
                        (candidate, candidate_ipv6),
                    ).fetchone()
                else:
                    conflict = conn.execute(
                        """
                        SELECT id FROM mac_allocations
                        WHERE status = 'ALLOCATED' AND assigned_mac = ?
                        """,
                        (candidate,),
                    ).fetchone()
                if conflict:
                    continue
                conn.execute(
                    """
                    INSERT INTO mac_allocations (
                        assigned_mac, gw_mac, gw_iface, node_name, mac_dev, mac_dev_byte, counter, ipv6_prefix, assigned_ipv6,
                        namespace, pod_name, pod_uid, container_iface, owner_kind, owner_name, owner_uid,
                        pod_ordinal, node_agent_pod_name, node_agent_pod_uid, node_agent_pod_ip,
                        sandbox_id, sandbox_pid, sandbox_pid_start_time, netns_inode, runtime_observed_at,
                        status, created_at, updated_at, last_seen_at, released_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ALLOCATED', ?, ?, ?, NULL)
                    """,
                    (
                        candidate,
                        record["gw_mac"],
                        record["gw_iface"],
                        record["node_name"],
                        record["mac_dev"],
                        record["mac_dev_byte"],
                        counter,
                        record["ipv6_prefix"],
                        candidate_ipv6,
                        record["namespace"],
                        record["pod_name"],
                        record["pod_uid"],
                        record["container_iface"],
                        record["owner_kind"],
                        record["owner_name"],
                        record["owner_uid"],
                        record["pod_ordinal"],
                        record["node_agent_pod_name"],
                        record["node_agent_pod_uid"],
                        record["node_agent_pod_ip"],
                        record["sandbox_id"],
                        record["sandbox_pid"],
                        record["sandbox_pid_start_time"],
                        record["netns_inode"],
                        record["runtime_observed_at"],
                        now,
                        now,
                        now,
                    ),
                )
                new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                row = conn.execute("SELECT * FROM mac_allocations WHERE id = ?", (new_id,)).fetchone()
                return self._row(row)

        result = self._run_write(operation, retries=1)
        if result:
            return result

        raise ValueError("No MAC slots are available for this gateway MAC / collision-index space.")

    def release(self, payload):
        pod_uid = payload.get("pod_uid")
        assigned_mac = payload.get("assigned_mac")
        status = str(payload.get("status") or "RELEASED").strip().upper()
        if status not in {"RELEASED", "STALE"}:
            raise ValueError("status must be RELEASED or STALE")
        if not any((pod_uid, assigned_mac)):
            raise ValueError("Provide pod_uid or assigned_mac.")

        clauses = ["status = 'ALLOCATED'"]
        values = []
        if pod_uid:
            clauses.append("pod_uid = ?")
            values.append(str(pod_uid).strip())
        if assigned_mac:
            clauses.append("assigned_mac = ?")
            values.append(normalize_mac(assigned_mac))
        where = " AND ".join(clauses)
        now = now_utc()

        def operation(conn):
            rows = conn.execute(f"SELECT * FROM mac_allocations WHERE {where}", values).fetchall()
            conn.execute(
                f"UPDATE mac_allocations SET status = ?, updated_at = ?, released_at = ? WHERE {where}",
                [status, now, now, *values],
            )
            return rows

        rows = self._run_write(operation, retries=1)
        return {"released": len(rows), "status": status, "released_at": now, "rows": [self._row(row) for row in rows]}

    def touch(self, payload):
        pod_uid = payload.get("pod_uid")
        if not pod_uid:
            raise ValueError("pod_uid is required")
        iface = str(payload.get("container_iface") or "eth0").strip()
        now = now_utc()
        def operation(conn):
            conn.execute(
                """
                UPDATE mac_allocations
                SET updated_at = ?, last_seen_at = ?
                WHERE pod_uid = ? AND container_iface = ? AND status = 'ALLOCATED'
                """,
                (now, now, str(pod_uid).strip(), iface),
            )
            row = conn.execute(
                """
                SELECT * FROM mac_allocations
                WHERE pod_uid = ? AND container_iface = ? AND status = 'ALLOCATED'
                """,
                (str(pod_uid).strip(), iface),
            ).fetchone()
            return {"row": row, "changed": conn.total_changes}

        result = self._run_write(operation, retries=1)
        row = result["row"]
        changed = result["changed"]
        return {"updated": changed, "row": self._row(row) if row else None}

    def reconcile_live_pods(self, payload):
        live_pod_uids = payload.get("live_pod_uids")
        if not isinstance(live_pod_uids, list):
            raise ValueError("live_pod_uids must be a list")
        target_status = str(payload.get("status") or "STALE").strip().upper()
        if target_status not in {"RELEASED", "STALE"}:
            raise ValueError("status must be RELEASED or STALE")
        live = {str(uid).strip() for uid in live_pod_uids if str(uid).strip()}
        clauses = ["status = 'ALLOCATED'"]
        values = []
        if payload.get("node_name"):
            clauses.append("node_name = ?")
            values.append(str(payload["node_name"]).strip())
        if payload.get("namespace"):
            clauses.append("namespace = ?")
            values.append(str(payload["namespace"]).strip())
        where = " AND ".join(clauses)
        now = now_utc()

        def operation(conn):
            rows = conn.execute(f"SELECT * FROM mac_allocations WHERE {where}", values).fetchall()
            stale_rows = [row for row in rows if row["pod_uid"] not in live]
            if stale_rows:
                conn.executemany(
                    "UPDATE mac_allocations SET status = ?, updated_at = ?, released_at = ? WHERE id = ?",
                    [(target_status, now, now, row["id"]) for row in stale_rows],
                )
            return {"rows": rows, "stale_rows": stale_rows}

        result = self._run_write(operation, retries=1)
        rows = result["rows"]
        stale_rows = result["stale_rows"]
        return {
            "checked": len(rows),
            "changed": len(stale_rows),
            "status": target_status,
            "reconciled_at": now,
            "rows": [self._row(row) for row in stale_rows],
        }

    def clear_stale_allocations(self):
        now = now_utc()
        def operation(conn):
            rows = conn.execute(
                """
                SELECT * FROM mac_allocations
                WHERE status = 'STALE'
                ORDER BY updated_at DESC, id DESC
                """
            ).fetchall()
            if rows:
                conn.execute("DELETE FROM mac_allocations WHERE status = 'STALE'")
            return rows

        rows = self._run_write(operation, retries=1)
        return {
            "deleted": len(rows),
            "cleared_at": now,
            "rows": [self._row(row) for row in rows],
        }

    def reset_all_state(self):
        now = now_utc()
        def operation(conn):
            allocations = conn.execute("SELECT COUNT(*) AS count FROM mac_allocations").fetchone()
            explicit = conn.execute("SELECT COUNT(*) AS count FROM explicit_ipv6_assignments").fetchone()
            conn.execute("DELETE FROM explicit_ipv6_assignments")
            conn.execute("DELETE FROM mac_allocations")
            return {"allocations": allocations, "explicit": explicit}

        result = self._run_write(operation, retries=1)
        allocations = result["allocations"]
        explicit = result["explicit"]
        return {
            "reset_at": now,
            "deleted_allocations": allocations["count"] if allocations else 0,
            "deleted_explicit_ipv6_assignments": explicit["count"] if explicit else 0,
        }

    def reset_explicit_state(self, payload=None):
        now = now_utc()
        payload = payload or {}
        namespace = str(payload.get("namespace") or "").strip()
        where_clause = "status = 'ACTIVE'"
        values = []
        if namespace:
            where_clause += " AND namespace = ?"
            values.append(namespace)

        def operation(conn):
            rows = conn.execute(
                f"SELECT * FROM explicit_ipv6_assignments WHERE {where_clause}",
                tuple(values),
            ).fetchall()
            active = conn.execute(
                f"SELECT COUNT(*) AS count FROM explicit_ipv6_assignments WHERE {where_clause}",
                tuple(values),
            ).fetchone()
            conn.execute(
                """
                UPDATE explicit_ipv6_assignments
                SET status = 'RELEASED', updated_at = ?, released_at = ?, last_applied_at = NULL
                WHERE """ + where_clause,
                (now, now, *values),
            )
            remaining = conn.execute(
                f"SELECT COUNT(*) AS count FROM explicit_ipv6_assignments WHERE {where_clause}",
                tuple(values),
            ).fetchone()
            return {"released": active, "remaining": remaining, "rows": rows}

        result = self._run_write(operation, retries=1)
        released = result["released"]
        remaining = result["remaining"]
        return {
            "reset_at": now,
            "namespace": namespace or None,
            "released_explicit_ipv6_assignments": released["count"] if released else 0,
            "active_explicit_ipv6_assignments": remaining["count"] if remaining else 0,
            "rows": [self._row(row) for row in result["rows"]],
        }

    def ensure_explicit_ipv6(self, payload):
        allocation_iface = str(payload.get("managed_container_iface") or MANAGED_CONTAINER_IFACE).strip()
        explicit_iface = str(payload.get("container_iface") or EXPLICIT_CONTAINER_IFACE).strip()
        trace_context = extract_trace_context(payload)
        parsed = parse_embedded_ipv6(payload.get("ipv6_address"))
        if parsed["encoded_counter"] != CANONICAL_EXPLICIT_COUNTER:
            raise ValueError("Canonical explicit IPv6 addresses must encode counter 0000.")
        now = now_utc()
        request_started = time.perf_counter()

        def operation(conn):
            allocation = None
            pod_uid = str(payload.get("pod_uid") or "").strip()
            target_assigned_mac = str(payload.get("target_assigned_mac") or "").strip()
            if pod_uid:
                allocation = self._find_active_allocation(conn, allocation_iface, pod_uid=pod_uid)
            elif target_assigned_mac:
                allocation = self._find_active_allocation(conn, allocation_iface, assigned_mac=target_assigned_mac)
            else:
                existing = conn.execute(
                    """
                    SELECT * FROM explicit_ipv6_assignments
                    WHERE requested_ipv6 = ? AND status = 'ACTIVE'
                    ORDER BY updated_at DESC, id DESC LIMIT 1
                    """,
                    (parsed["requested_ipv6"],),
                ).fetchone()
                if existing:
                    allocation = self._find_active_allocation(conn, allocation_iface, pod_uid=existing["pod_uid"])
                    if not allocation:
                        allocation = self._find_active_allocation(
                            conn,
                            allocation_iface,
                            assigned_mac=existing["target_assigned_mac"],
                        )
            if not allocation:
                raise ValueError(
                    "Canonical explicit IPv6 cannot identify a target pod by itself. "
                    "Provide pod_uid or target_assigned_mac, or use "
                    "/explicit-ipv6-assignments/ensure-by-pod."
                )
            requested_ipv6 = parsed["requested_ipv6"]
            record = {
                **parsed,
                "requested_ipv6": requested_ipv6,
                "target_assigned_mac": allocation["assigned_mac"],
                "target_gw_mac": parsed["target_gw_mac"],
                "target_counter": allocation["counter"],
                "namespace": allocation["namespace"],
                "pod_name": allocation["pod_name"],
                "pod_uid": allocation["pod_uid"],
                "node_name": allocation["node_name"],
                "container_iface": explicit_iface,
                "node_agent_pod_name": allocation["node_agent_pod_name"],
                "node_agent_pod_uid": allocation["node_agent_pod_uid"],
                "node_agent_pod_ip": allocation["node_agent_pod_ip"],
            }
            record.update(normalize_runtime_snapshot(allocation))
            assignment, previous_owner = self._upsert_explicit_assignment(conn, record, now)
            return {
                "assignment": self._row(assignment),
                "allocation": self._row(allocation),
                "previous_owner": previous_owner,
            }

        result = self._run_write(operation, retries=EXPLICIT_WRITE_RETRY_ATTEMPTS)
        log_explicit_trace(
            source="allocator-request",
            requested_ipv6=result["assignment"]["requested_ipv6"],
            pod_uid=result["assignment"]["pod_uid"],
            total_ms=elapsed_ms(request_started),
            had_previous_owner=result.get("previous_owner") is not None,
            **trace_log_fields(trace_context),
        )
        return {
            "assignment": result["assignment"],
            "allocation": result["allocation"],
            "previous_owner": result["previous_owner"],
        }

    def ensure_explicit_ipv6_by_pod(self, payload):
        pod_uid = str(payload.get("pod_uid") or "").strip()
        gw_tag = payload.get("gw_tag")
        mac_dev = payload.get("mac_dev")
        allocation_iface = str(payload.get("managed_container_iface") or MANAGED_CONTAINER_IFACE).strip()
        explicit_iface = str(payload.get("container_iface") or EXPLICIT_CONTAINER_IFACE).strip()
        trace_context = extract_trace_context(payload)
        if not pod_uid:
            raise ValueError("pod_uid is required.")
        if not gw_tag:
            raise ValueError("gw_tag is required.")
        if not mac_dev:
            raise ValueError("mac_dev is required.")
        now = now_utc()
        request_started = time.perf_counter()

        def operation(conn):
            allocation_lookup_started = time.perf_counter()
            allocation = self._find_active_allocation(conn, allocation_iface, pod_uid=pod_uid)
            if not allocation:
                raise ValueError(f"No active allocation exists for pod_uid={pod_uid} on interface {allocation_iface}.")
            allocation_lookup_ms = elapsed_ms(allocation_lookup_started)
            upsert_started = time.perf_counter()
            identity_gw_mac = (
                normalize_optional_mac(payload.get("canonical_gateway_mac"))
                or self.configured_canonical_gateway_mac(conn)
                or allocation["gw_mac"]
            )
            requested_ipv6 = build_explicit_ipv6(gw_tag, identity_gw_mac, mac_dev)
            parsed = parse_embedded_ipv6(requested_ipv6)
            record = {
                **parsed,
                "target_gw_mac": parsed["target_gw_mac"],
                "target_counter": allocation["counter"],
                "target_assigned_mac": allocation["assigned_mac"],
                "namespace": allocation["namespace"],
                "pod_name": allocation["pod_name"],
                "pod_uid": allocation["pod_uid"],
                "node_name": allocation["node_name"],
                "container_iface": explicit_iface,
                "node_agent_pod_name": allocation["node_agent_pod_name"],
                "node_agent_pod_uid": allocation["node_agent_pod_uid"],
                "node_agent_pod_ip": allocation["node_agent_pod_ip"],
            }
            record.update(normalize_runtime_snapshot(allocation))
            assignment, previous_owner = self._upsert_explicit_assignment(conn, record, now)
            return {
                "requested_ipv6": requested_ipv6,
                "assignment": self._row(assignment),
                "allocation": self._row(allocation),
                "previous_owner": previous_owner,
                "_trace": {
                    "allocation_lookup_ms": allocation_lookup_ms,
                    "upsert_ms": elapsed_ms(upsert_started),
                },
            }

        result = self._run_write(operation, retries=EXPLICIT_WRITE_RETRY_ATTEMPTS)
        trace = result.pop("_trace", {})
        log_explicit_trace(
            source="allocator-request",
            requested_ipv6=result["requested_ipv6"],
            pod_uid=pod_uid,
            allocation_lookup_ms=trace.get("allocation_lookup_ms"),
            upsert_ms=trace.get("upsert_ms"),
            total_ms=elapsed_ms(request_started),
            had_previous_owner=result.get("previous_owner") is not None,
            **trace_log_fields(trace_context),
        )
        return {
            "requested_ipv6": result["requested_ipv6"],
            "assignment": result["assignment"],
            "allocation": result["allocation"],
            "previous_owner": result["previous_owner"],
        }

    def _mark_explicit_ipv6_applied_row(self, conn, payload, now, strict=True):
        requested_ipv6 = normalize_ipv6_address(payload.get("ipv6_address") or payload.get("requested_ipv6"))
        expected_pod_uid = str(payload.get("pod_uid") or "").strip() or None
        expected_target_assigned_mac = str(payload.get("target_assigned_mac") or "").strip().lower() or None
        where_clauses = ["requested_ipv6 = ?", "status = 'ACTIVE'"]
        values = [
            str(payload.get("namespace") or "").strip() or None,
            str(payload.get("pod_name") or "").strip() or None,
            str(payload.get("pod_uid") or "").strip() or None,
            str(payload.get("node_name") or "").strip() or None,
            str(payload.get("container_iface") or "").strip() or None,
            now,
            now,
            requested_ipv6,
        ]
        if expected_pod_uid:
            where_clauses.append("pod_uid = ?")
            values.append(expected_pod_uid)
        if expected_target_assigned_mac:
            where_clauses.append("lower(target_assigned_mac) = ?")
            values.append(expected_target_assigned_mac)
        updated = conn.execute(
            """
            UPDATE explicit_ipv6_assignments
            SET namespace = ?, pod_name = ?, pod_uid = ?, node_name = ?, container_iface = ?,
                updated_at = ?, last_applied_at = ?, status = 'ACTIVE'
            WHERE """ + " AND ".join(where_clauses) + """
            RETURNING *
            """,
            values,
        ).fetchone()
        if not updated and strict:
            raise ValueError(f"No matching active explicit IPv6 assignment exists for {requested_ipv6}.")
        return self._row(updated) if updated else None

    def _mark_explicit_ipv6_applied_rows_batch(self, conn, entries, now):
        prepared_entries = []
        for index, entry in enumerate(entries):
            prepared_entries.append(
                {
                    "entry_index": index,
                    "entry": entry,
                    "requested_ipv6": normalize_ipv6_address(entry.get("ipv6_address") or entry.get("requested_ipv6")),
                    "namespace": str(entry.get("namespace") or "").strip() or None,
                    "pod_name": str(entry.get("pod_name") or "").strip() or None,
                    "pod_uid": str(entry.get("pod_uid") or "").strip() or None,
                    "node_name": str(entry.get("node_name") or "").strip() or None,
                    "container_iface": str(entry.get("container_iface") or "").strip() or None,
                    "expected_pod_uid": str(entry.get("pod_uid") or "").strip() or None,
                    "expected_target_assigned_mac": str(entry.get("target_assigned_mac") or "").strip().lower() or None,
                }
            )
        if not prepared_entries:
            return {"updated_rows": [], "skipped": []}
        value_placeholders = []
        values = [now, now]
        for prepared in prepared_entries:
            value_placeholders.append("(?, ?, ?, ?, ?, ?, ?, ?, ?)")
            values.extend(
                [
                    prepared["entry_index"],
                    prepared["requested_ipv6"],
                    prepared["namespace"],
                    prepared["pod_name"],
                    prepared["pod_uid"],
                    prepared["node_name"],
                    prepared["container_iface"],
                    prepared["expected_pod_uid"],
                    prepared["expected_target_assigned_mac"],
                ]
            )
        rows = conn.execute(
            """
            UPDATE explicit_ipv6_assignments AS assignments
            SET namespace = applied.namespace,
                pod_name = applied.pod_name,
                pod_uid = applied.pod_uid,
                node_name = applied.node_name,
                container_iface = applied.container_iface,
                updated_at = ?,
                last_applied_at = ?,
                status = 'ACTIVE'
            FROM (
                VALUES """
            + ", ".join(value_placeholders)
            + """
            ) AS applied(
                entry_index,
                requested_ipv6,
                namespace,
                pod_name,
                pod_uid,
                node_name,
                container_iface,
                expected_pod_uid,
                expected_target_assigned_mac
            )
            WHERE assignments.requested_ipv6 = applied.requested_ipv6
              AND assignments.status = 'ACTIVE'
              AND (applied.expected_pod_uid IS NULL OR assignments.pod_uid = applied.expected_pod_uid)
              AND (
                    applied.expected_target_assigned_mac IS NULL
                    OR lower(assignments.target_assigned_mac) = applied.expected_target_assigned_mac
              )
            RETURNING applied.entry_index AS entry_index, assignments.*
            """,
            values,
        ).fetchall()
        rows_by_index = {}
        for row in rows:
            normalized = self._row(row)
            entry_index = int(normalized.pop("entry_index"))
            rows_by_index[entry_index] = normalized
        updated_rows = []
        skipped = []
        for prepared in prepared_entries:
            matched = rows_by_index.get(prepared["entry_index"])
            if matched:
                updated_rows.append((prepared["entry"], matched))
            else:
                skipped.append(prepared["entry"])
        return {"updated_rows": updated_rows, "skipped": skipped}

    def mark_explicit_ipv6_applied(self, payload):
        requested_ipv6 = normalize_ipv6_address(payload.get("ipv6_address") or payload.get("requested_ipv6"))
        trace_context = extract_trace_context(payload)
        applied_started = time.perf_counter()
        now = now_utc()

        def operation(conn):
            return self._mark_explicit_ipv6_applied_row(conn, payload, now, strict=True)

        updated = self._run_write(operation, retries=EXPLICIT_WRITE_RETRY_ATTEMPTS)
        log_explicit_trace(
            source="allocator-applied",
            requested_ipv6=requested_ipv6,
            pod_uid=str(payload.get("pod_uid") or "").strip() or None,
            db_update_ms=elapsed_ms(applied_started),
            **trace_log_fields(trace_context),
        )
        return updated

    def mark_explicit_ipv6_applied_batch(self, payload):
        entries = payload.get("entries")
        if not isinstance(entries, list):
            raise ValueError("entries must be a list.")
        normalized_entries = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError("each applied entry must be an object.")
            normalized_entries.append(entry)
        batch_started = time.perf_counter()
        now = now_utc()

        def operation(conn):
            return self._mark_explicit_ipv6_applied_rows_batch(conn, normalized_entries, now)

        result = self._run_write(operation, retries=EXPLICIT_WRITE_RETRY_ATTEMPTS)
        db_update_ms = elapsed_ms(batch_started)
        for entry, row in result["updated_rows"]:
            trace_context = extract_trace_context(entry)
            log_explicit_trace(
                source="allocator-applied",
                requested_ipv6=row.get("requested_ipv6"),
                pod_uid=row.get("pod_uid"),
                db_update_ms=db_update_ms,
                **trace_log_fields(trace_context),
            )
        for entry in result["skipped"]:
            trace_context = extract_trace_context(entry)
            log_explicit_trace(
                source="allocator-applied-skip",
                requested_ipv6=entry.get("requested_ipv6") or entry.get("ipv6_address"),
                pod_uid=str(entry.get("pod_uid") or "").strip() or None,
                db_update_ms=db_update_ms,
                reason="no-match",
                **trace_log_fields(trace_context),
            )
        return {
            "updated": len(result["updated_rows"]),
            "skipped": len(result["skipped"]),
            "requested": len(normalized_entries),
        }

class Handler(BaseHTTPRequestHandler):
    store = None

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
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            self.send_json(HTTPStatus.OK, {"status": "ok", "time": now_utc()})
            return
        if parsed.path == "/stats":
            self.send_json(HTTPStatus.OK, self.store.stats())
            return
        if parsed.path == "/allocations":
            filters = {key: values[0] for key, values in parse_qs(parsed.query).items() if values}
            self.send_json(HTTPStatus.OK, self.store.list_allocations(filters))
            return
        if parsed.path == "/explicit-ipv6-assignments":
            filters = {key: values[0] for key, values in parse_qs(parsed.query).items() if values}
            self.send_json(HTTPStatus.OK, self.store.list_explicit_ipv6_assignments(filters))
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            payload = self.read_json()
            trace_context = extract_trace_context(payload)
            trace_fields = trace_log_fields(trace_context)
            if parsed.path in ("/explicit-ipv6-assignments/ensure", "/explicit-ipv6-assignments/ensure-by-pod"):
                client_started_at_ms = trace_context.get("trace_client_started_at_ms")
                client_to_allocator_ms = (
                    round(current_epoch_ms() - client_started_at_ms, 2)
                    if client_started_at_ms is not None
                    else None
                )
                log_explicit_trace(
                    source="allocator-handler",
                    path=parsed.path,
                    pod_uid=str(payload.get("pod_uid") or "").strip() or None,
                    target_assigned_mac=str(payload.get("target_assigned_mac") or "").strip().lower() or None,
                    client_to_allocator_ms=client_to_allocator_ms,
                    **trace_fields,
                )
            elif parsed.path == "/explicit-ipv6-assignments/applied":
                callback_sent_at_ms = normalize_optional_trace_int(payload.get("trace_node_callback_sent_at_ms"))
                node_callback_to_allocator_ms = (
                    round(current_epoch_ms() - callback_sent_at_ms, 2)
                    if callback_sent_at_ms is not None
                    else None
                )
                log_explicit_trace(
                    source="allocator-applied-handler",
                    path=parsed.path,
                    requested_ipv6=payload.get("requested_ipv6") or payload.get("ipv6_address"),
                    pod_uid=str(payload.get("pod_uid") or "").strip() or None,
                    node_callback_to_allocator_ms=node_callback_to_allocator_ms,
                    **trace_fields,
                )
            elif parsed.path == "/explicit-ipv6-assignments/applied-batch":
                entries = payload.get("entries") if isinstance(payload.get("entries"), list) else []
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    entry_trace_context = extract_trace_context(entry)
                    entry_trace_fields = trace_log_fields(entry_trace_context)
                    callback_sent_at_ms = normalize_optional_trace_int(entry.get("trace_node_callback_sent_at_ms"))
                    node_callback_to_allocator_ms = (
                        round(current_epoch_ms() - callback_sent_at_ms, 2)
                        if callback_sent_at_ms is not None
                        else None
                    )
                    log_explicit_trace(
                        source="allocator-applied-handler",
                        path=parsed.path,
                        requested_ipv6=entry.get("requested_ipv6") or entry.get("ipv6_address"),
                        pod_uid=str(entry.get("pod_uid") or "").strip() or None,
                        node_callback_to_allocator_ms=node_callback_to_allocator_ms,
                        **entry_trace_fields,
                    )
            if parsed.path == "/allocations/ensure":
                self.send_json(HTTPStatus.CREATED, self.store.ensure(payload))
                return
            if parsed.path == "/allocations/release":
                self.send_json(HTTPStatus.OK, self.store.release(payload))
                return
            if parsed.path == "/allocations/touch":
                self.send_json(HTTPStatus.OK, self.store.touch(payload))
                return
            if parsed.path == "/reconcile/live-pods":
                self.send_json(HTTPStatus.OK, self.store.reconcile_live_pods(payload))
                return
            if parsed.path == "/allocations/clear-stale":
                self.send_json(HTTPStatus.OK, self.store.clear_stale_allocations())
                return
            if parsed.path == "/admin/reset":
                self.send_json(HTTPStatus.OK, self.store.reset_all_state())
                return
            if parsed.path == "/admin/reset-explicit":
                result = self.store.reset_explicit_state(payload)
                if payload.get("clear_runtime"):
                    result["runtime_cleanup"] = clear_explicit_ipv6_runtime(result.get("rows", []))
                self.send_json(HTTPStatus.OK, result)
                return
            if parsed.path == "/explicit-ipv6-assignments/ensure":
                result = self.store.ensure_explicit_ipv6(payload)
                status_code, apply_result = dispatch_explicit_ipv6_apply(
                    result["assignment"],
                    previous_owner=result.get("previous_owner"),
                    trace_context=trace_context,
                )
                result["applied"] = apply_result
                self.send_json(status_code, result)
                return
            if parsed.path == "/explicit-ipv6-assignments/ensure-by-pod":
                result = self.store.ensure_explicit_ipv6_by_pod(payload)
                status_code, apply_result = dispatch_explicit_ipv6_apply(
                    result["assignment"],
                    previous_owner=result.get("previous_owner"),
                    trace_context=trace_context,
                )
                result["applied"] = apply_result
                self.send_json(status_code, result)
                return
            if parsed.path == "/explicit-ipv6-assignments/applied":
                self.send_json(HTTPStatus.OK, self.store.mark_explicit_ipv6_applied(payload))
                return
            if parsed.path == "/explicit-ipv6-assignments/applied-batch":
                self.send_json(HTTPStatus.OK, self.store.mark_explicit_ipv6_applied_batch(payload))
                return
        except ValueError as exc:
            self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except DB_INTEGRITY_ERRORS as exc:
            self.send_json(HTTPStatus.CONFLICT, {"error": f"Database constraint error: {exc}"})
            return
        except Exception as exc:
            self.send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": f"Unexpected server error: {exc}"})
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})


class AllocatorHTTPServer(ThreadingHTTPServer):
    request_queue_size = ALLOCATOR_REQUEST_QUEUE_SIZE


def main():
    Handler.store = Store()
    server = AllocatorHTTPServer((HOST, PORT), Handler)
    print(f"net-identity-allocator listening on http://{HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()

