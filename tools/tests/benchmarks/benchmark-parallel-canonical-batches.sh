#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export CMXSAFE_SCRIPT_DIR="$SCRIPT_DIR"

python3 - "$@" <<'PY'
import argparse
import json
import math
import os
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Barrier, BrokenBarrierError, Event
from uuid import uuid4


ALLOCATOR_URL = os.environ.get(
    "ALLOCATOR_URL",
    "http://net-identity-allocator.mac-allocator.svc.cluster.local:8080",
)
DEPLOYMENT_NAMESPACE = os.environ.get("DEPLOYMENT_NAMESPACE", "mac-deployment-demo")
DEPLOYMENT_SELECTOR = os.environ.get("DEPLOYMENT_SELECTOR", "app=demo-deployment")
DEPLOYMENT_REPLICAS = int(os.environ.get("DEPLOYMENT_REPLICAS", "4"))
DEFAULT_BATCH_SIZES = [10, 30, 60, 100]
MULTUS_NET1_PREFIX = "fd42:4242:ff:"
MULTUS_NET1_ROUTE = "fd42:4242:ff::/64"


def write_step(message):
    print(f"\n==> {message}", flush=True)


def write_info(message):
    print(f"    {message}", flush=True)


def detect_project_root():
    script_dir = os.environ.get("CMXSAFE_SCRIPT_DIR", "").strip()
    candidates = []
    if script_dir:
        script_path = Path(script_dir).resolve()
        candidates.extend([script_path, *script_path.parents])
    candidates.extend([
        Path("/tmp/cmxsafe"),
        Path("/workspace/CMXsafeMAC-IPv6"),
        Path("/workspaces/CMXsafeMAC-IPv6"),
        Path.cwd(),
    ])
    for candidate in candidates:
        if candidate is None:
            continue
        if (candidate / "k8s" / "demo-deployment.yaml").exists():
            return str(candidate)
    return str(Path.cwd())


def run(command, *, check=True, capture_output=True):
    result = subprocess.run(
        command,
        text=True,
        capture_output=capture_output,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result


def kubectl(*args, capture_output=True, check=True):
    return run(["kubectl", *args], capture_output=capture_output, check=check)


def kubectl_json(*args):
    output = kubectl(*args).stdout
    return json.loads(output) if output else {}


def http_json(method, url, body=None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = response.read()
            return json.loads(payload.decode("utf-8")) if payload else None
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            payload = exc.read()
        except Exception:
            payload = b""
        if payload:
            try:
                detail = payload.decode("utf-8", errors="replace")
            except Exception:
                detail = repr(payload)
        raise RuntimeError(
            f"HTTP {exc.code} {exc.reason} for {method} {url}"
            + (f": {detail}" if detail else "")
        ) from exc


def ensure_explicit_request(base_url, payload, attempts=3):
    last_error = None
    for attempt in range(max(1, attempts)):
        try:
            return http_json("POST", f"{base_url}/explicit-ipv6-assignments/ensure-by-pod", payload)
        except (urllib.error.URLError, ConnectionResetError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt >= attempts - 1:
                raise
            time.sleep(min(0.05 * (2 ** attempt), 0.2))
    if last_error is not None:
        raise last_error


def cleanup_samples():
    kubectl("delete", "statefulset", "demo", "-n", "mac-demo", "--ignore-not-found=true", check=False)
    kubectl("delete", "service", "demo", "-n", "mac-demo", "--ignore-not-found=true", check=False)
    kubectl(
        "delete",
        "deployment",
        "demo-deployment",
        "-n",
        DEPLOYMENT_NAMESPACE,
        "--ignore-not-found=true",
        check=False,
    )


def wait_until(description, condition, timeout_seconds=180, interval_seconds=2):
    deadline = time.time() + timeout_seconds
    started = time.time()
    next_progress = started + 15
    last_error = None
    while time.time() < deadline:
        try:
            result = condition()
            if result:
                return result
        except Exception as exc:
            last_error = exc
        now = time.time()
        if now >= next_progress:
            progress = getattr(condition, "_progress", None)
            if progress:
                cpu_snapshot = sample_cpu_snapshot()
                progress_text = progress()
                if cpu_snapshot:
                    write_info(
                        f"{description}: elapsed={int(now - started)}s progress={progress_text} cpu={cpu_snapshot}"
                    )
                else:
                    write_info(
                        f"{description}: elapsed={int(now - started)}s progress={progress_text}"
                    )
            next_progress = now + 15
        time.sleep(interval_seconds)
    if last_error is not None:
        raise RuntimeError(f"Timed out waiting for {description}. Last error: {last_error}")
    raise RuntimeError(f"Timed out waiting for {description}")


def scaled_create_timeout_seconds(batch_size):
    return min(3600, max(240, 240 + math.ceil(max(0, batch_size) / 50.0)))


def scaled_move_timeout_seconds(batch_size):
    return min(3600, max(300, 300 + math.ceil(max(0, batch_size) / 20.0)))


def scaled_reachability_timeout_seconds(batch_size):
    return min(600, max(30, 30 + math.ceil(max(0, batch_size) / 10.0)))


def sample_cpu_snapshot():
    try:
        output = kubectl("top", "pod", "-n", "mac-allocator", "--no-headers").stdout.strip()
    except Exception:
        return None
    if not output:
        return None
    buckets = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        name = parts[0]
        cpu = parts[1]
        if name.startswith("net-identity-allocator-"):
            label = "allocator"
        elif name.startswith("cmxsafemac-ipv6-node-agent-"):
            label = "node-agent"
        elif name.startswith("cmxsafemac-ipv6-toolbox-"):
            label = "toolbox"
        elif name.startswith("net-identity-allocator-postgres-"):
            label = "postgres"
        else:
            continue
        buckets.append(f"{label}={cpu}")
    return ",".join(buckets) if buckets else None


def wait_no_deployment_pods():
    wait_until(
        "deployment sample pods to disappear",
        lambda: len(kubectl_json("get", "pods", "-n", DEPLOYMENT_NAMESPACE, "-l", DEPLOYMENT_SELECTOR, "-o", "json").get("items", [])) == 0,
        timeout_seconds=180,
        interval_seconds=3,
    )


def reset_allocator_state(base_url):
    http_json("POST", f"{base_url}/admin/reset", {})


def reset_allocator_explicit_state(base_url, namespace=None, clear_runtime=False):
    body = {"namespace": namespace} if namespace else {}
    if clear_runtime:
        body["clear_runtime"] = True
    http_json("POST", f"{base_url}/admin/reset-explicit", body)

def get_pod_ipv6_routes(namespace, pod_name, interface):
    command = f"ip -6 route show dev {interface} table main type unicast 2>/dev/null | awk '{{print $1}}'"
    output = kubectl("exec", "-n", namespace, pod_name, "--", "sh", "-lc", command).stdout
    return [line.strip().lower() for line in output.splitlines() if line.strip()]


def normalize_value_set(values):
    return sorted({str(value).strip().lower() for value in values if str(value).strip()})


def value_sets_match(expected, actual):
    return normalize_value_set(expected) == normalize_value_set(actual)


def remove_pod_explicit_routes(namespace, pod_name, allowed_routes):
    allowed = set(normalize_value_set(allowed_routes))
    routes = get_pod_ipv6_routes(namespace, pod_name, "net1")
    for route in routes:
        if route in allowed:
            continue
        kubectl(
            "exec",
            "-n",
            namespace,
            pod_name,
            "--",
            "sh",
            "-lc",
            f"ip -6 route del {route} dev net1 >/dev/null 2>&1 || true",
        )


def clear_deployment_explicit_state(base_url, pods, baseline_addresses, baseline_routes):
    reset_allocator_explicit_state(base_url, DEPLOYMENT_NAMESPACE, clear_runtime=True)

    def condition():
        rows = (
            http_json(
                "GET",
                f"{base_url}/explicit-ipv6-assignments?namespace={urllib.parse.quote(DEPLOYMENT_NAMESPACE)}&status=ACTIVE",
            )
            or []
        )
        if rows:
            return None
        for pod in pods:
            pod_uid = pod["metadata"]["uid"]
            addresses = get_pod_ipv6_addresses(pod["metadata"]["namespace"], pod["metadata"]["name"], "net1")
            if not value_sets_match(baseline_addresses.get(pod_uid, []), addresses):
                return None
        return True

    wait_until(
        "deployment explicit IPv6 state to clear",
        condition,
        timeout_seconds=120,
        interval_seconds=2,
    )
    for pod in pods:
        remove_pod_explicit_routes(
            pod["metadata"]["namespace"],
            pod["metadata"]["name"],
            baseline_routes.get(pod["metadata"]["uid"], []),
        )

    def route_condition():
        for pod in pods:
            pod_uid = pod["metadata"]["uid"]
            routes = get_pod_ipv6_routes(pod["metadata"]["namespace"], pod["metadata"]["name"], "net1")
            if not value_sets_match(baseline_routes.get(pod_uid, []), routes):
                return None
        return True

    wait_until(
        "deployment explicit IPv6 routes to clear",
        route_condition,
        timeout_seconds=60,
        interval_seconds=2,
    )


def allocator_backend():
    env_dump = kubectl(
        "exec",
        "-n",
        "mac-allocator",
        "deploy/net-identity-allocator",
        "--",
        "sh",
        "-lc",
        "env | sort | grep -E 'POSTGRES_HOST|POSTGRES_DB|POSTGRES_USER'",
    ).stdout
    if "POSTGRES_HOST=" not in env_dump or "POSTGRES_DB=" not in env_dump or "POSTGRES_USER=" not in env_dump:
        raise RuntimeError("Allocator PostgreSQL env is incomplete.")
    return "postgres"


def verify_postgres():
    result = kubectl(
        "exec",
        "-n",
        "mac-allocator",
        "net-identity-allocator-postgres-0",
        "--",
        "sh",
        "-lc",
        'PGPASSWORD="change-me-in-production" psql -t -A -U allocator -d cmxsafemac_ipv6 -c "select current_database(), current_user"',
    ).stdout.replace("\r", "").strip()
    return result == "cmxsafemac_ipv6|allocator"


def list_managed_pods(namespace, selector):
    payload = kubectl_json("get", "pods", "-n", namespace, "-l", selector, "-o", "json")
    items = payload.get("items", [])
    items.sort(key=lambda item: item.get("metadata", {}).get("name", ""))
    return items


def wait_pods_ready(namespace, selector, expected_count, timeout_seconds=420):
    def condition():
        pods = list_managed_pods(namespace, selector)
        if len(pods) != expected_count:
            return None
        for pod in pods:
            if (pod.get("status") or {}).get("phase") != "Running":
                return None
            statuses = (pod.get("status") or {}).get("containerStatuses") or []
            if any(not status.get("ready") for status in statuses):
                return None
        return pods

    return wait_until(
        f"ready pods for {selector} in {namespace}",
        condition,
        timeout_seconds=timeout_seconds,
        interval_seconds=3,
    )


def wait_allocation_for_pod(base_url, pod_uid, timeout_seconds=180):
    def condition():
        rows = http_json("GET", f"{base_url}/allocations?pod_uid={urllib.parse.quote(pod_uid)}&status=ALLOCATED") or []
        rows = [row for row in rows if row.get("container_iface") == "eth0"]
        return rows[0] if rows else None

    return wait_until(
        f"managed allocation for pod {pod_uid}",
        condition,
        timeout_seconds=timeout_seconds,
        interval_seconds=3,
    )


def get_pod_mac(namespace, pod_name, interface="eth0"):
    return kubectl(
        "exec",
        "-n",
        namespace,
        pod_name,
        "--",
        "cat",
        f"/sys/class/net/{interface}/address",
    ).stdout.strip().lower()


def get_pod_ipv6_addresses(namespace, pod_name, interface):
    command = f"ip -6 -o addr show dev {interface} scope global 2>/dev/null | awk '{{print $4}}' | cut -d/ -f1"
    output = kubectl("exec", "-n", namespace, pod_name, "--", "sh", "-lc", command).stdout
    return [line.strip().lower() for line in output.splitlines() if line.strip()]


def wait_pod_has_ipv6(namespace, pod_name, interface, ipv6, timeout_seconds=120):
    target = ipv6.lower()
    wait_until(
        f"{pod_name} to have {target} on {interface}",
        lambda: target in get_pod_ipv6_addresses(namespace, pod_name, interface),
        timeout_seconds=timeout_seconds,
        interval_seconds=2,
    )


def invoke_pod_ping6(namespace, pod_name, target_ipv6, count=1, timeout_seconds=2, source_ipv6=None):
    source_clause = f"-I {source_ipv6} " if source_ipv6 else ""
    kubectl(
        "exec",
        "-n",
        namespace,
        pod_name,
        "--",
        "sh",
        "-lc",
        f"ping -6 {source_clause}-c {count} -W {timeout_seconds} {target_ipv6}",
    )


def get_batch_gw_tag(size):
    return f"{size & 0xFFFF:04x}"


def get_latency_metrics(durations_ms):
    if not durations_ms:
        return {
            "Count": 0,
            "TotalMs": 0.0,
            "AvgMs": 0.0,
            "P50Ms": 0.0,
            "P95Ms": 0.0,
            "MaxMs": 0.0,
            "OpsPerSec": 0.0,
        }
    values = sorted(float(value) for value in durations_ms)
    total = sum(values)

    def percentile(percent):
        if len(values) == 1:
            return values[0]
        position = (percent / 100.0) * (len(values) - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return values[lower]
        weight = position - lower
        return values[lower] + ((values[upper] - values[lower]) * weight)

    max_ms = values[-1]
    return {
        "Count": len(values),
        "TotalMs": round(total, 2),
        "AvgMs": round(total / len(values), 2),
        "P50Ms": round(percentile(50), 2),
        "P95Ms": round(percentile(95), 2),
        "MaxMs": round(max_ms, 2),
        "OpsPerSec": round((len(values) / (max_ms / 1000.0)) if max_ms > 0 else 0.0, 2),
    }


def current_epoch_ms():
    return int(time.time() * 1000)


def get_trace_pod_targets():
    allocator_items = kubectl_json(
        "get", "pods", "-n", "mac-allocator", "-l", "app=net-identity-allocator", "-o", "json"
    ).get("items", [])
    node_agent_items = kubectl_json(
        "get", "pods", "-n", "mac-allocator", "-l", "app=cmxsafemac-ipv6-node-agent", "-o", "json"
    ).get("items", [])
    allocator_pods = [
        item.get("metadata", {}).get("name")
        for item in allocator_items
        if (item.get("status") or {}).get("phase") == "Running"
    ]
    node_agent_pods = [
        item.get("metadata", {}).get("name")
        for item in node_agent_items
        if (item.get("status") or {}).get("phase") == "Running"
    ]
    return {
        "namespace": "mac-allocator",
        "allocator_pods": [name for name in allocator_pods if name],
        "node_agent_pods": [name for name in node_agent_pods if name],
    }


def parse_trace_timestamp(value):
    raw = str(value).strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    return datetime.fromisoformat(raw).astimezone(timezone.utc)


def parse_trace_field(raw_value):
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return str(raw_value)


def get_trace_records(trace_pods, requested_ipv6s, trace_ids, phase_start_utc, phase_end_utc):
    requested_set = {str(ip).strip().lower() for ip in requested_ipv6s if str(ip).strip()}
    trace_id_set = {str(trace_id).strip() for trace_id in trace_ids if str(trace_id).strip()}
    if not requested_set and not trace_id_set:
        return []
    records = []
    since_time = (
        phase_start_utc.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    for pod_name in trace_pods["allocator_pods"] + trace_pods["node_agent_pods"]:
        try:
            output = kubectl(
                "logs",
                "--timestamps",
                "-n",
                trace_pods["namespace"],
                pod_name,
                "--since-time",
                since_time,
            ).stdout
        except RuntimeError as exc:
            lowered = str(exc).lower()
            if "not found" in lowered or "notfound" in lowered:
                continue
            raise
        for line in output.splitlines():
            if not line.strip() or "explicit-trace" not in line:
                continue
            parts = line.split(None, 1)
            if len(parts) < 2:
                continue
            try:
                timestamp = parse_trace_timestamp(parts[0])
            except ValueError:
                continue
            if timestamp < phase_start_utc or timestamp > phase_end_utc:
                continue
            fields = dict(re.findall(r"([A-Za-z0-9_]+)=([^\s]+)", parts[1]))
            trace_id = str(fields.get("trace_id") or "").strip()
            requested_ipv6 = str(fields.get("requested_ipv6") or "").strip().lower()
            matches_trace_id = trace_id in trace_id_set if trace_id else False
            matches_ipv6 = requested_ipv6 in requested_set if requested_ipv6 else False
            if not matches_trace_id and not matches_ipv6:
                continue
            record = {
                "timestamp": timestamp,
                "pod": pod_name,
                "requested_ipv6": requested_ipv6 or None,
                "source": str(fields.get("source") or "").strip(),
            }
            for key, raw_value in fields.items():
                if key in record:
                    continue
                record[key] = parse_trace_field(raw_value)
            records.append(record)
    return records


def get_dominant_trace_stage(summary_row):
    best_name = None
    best_value = -1.0
    for key, value in summary_row.items():
        if not key.endswith("_ms") or key == "total_ms":
            continue
        if float(value) > best_value:
            best_name = key
            best_value = float(value)
    if not best_name:
        return None
    return {"name": best_name, "value": round(best_value, 2)}


def get_trace_summary(records):
    numeric_fields = [
        "client_to_allocator_ms",
        "allocator_to_agent_ms",
        "client_to_agent_ms",
        "node_callback_to_allocator_ms",
        "allocation_lookup_ms",
        "upsert_ms",
        "db_update_ms",
        "queue_wait_ms",
        "node_call_ms",
        "applied_db_ms",
        "lock_wait_ms",
        "resolve_runtime_ms",
        "evict_ms",
        "set_ms",
        "flush_ms",
        "mark_applied_ms",
        "prefix_sync_ms",
        "total_ms",
    ]
    if not records:
        return []
    groups = {}
    for record in records:
        groups.setdefault(record.get("source") or "", []).append(record)
    summary = []
    for source, group in sorted(groups.items()):
        row = {"source": source, "count": len(group)}
        for field in numeric_fields:
            values = [float(record[field]) for record in group if field in record]
            if values:
                row[field] = round(sum(values) / len(values), 2)
        summary.append(row)
    return summary


def write_trace_summary(label, summary):
    write_step(label)
    if not summary:
        write_info("No explicit trace records were found.")
        return
    ordered_fields = [
        "client_to_allocator_ms",
        "allocator_to_agent_ms",
        "client_to_agent_ms",
        "node_callback_to_allocator_ms",
        "allocation_lookup_ms",
        "upsert_ms",
        "queue_wait_ms",
        "node_call_ms",
        "applied_db_ms",
        "lock_wait_ms",
        "resolve_runtime_ms",
        "evict_ms",
        "set_ms",
        "flush_ms",
        "mark_applied_ms",
        "prefix_sync_ms",
        "db_update_ms",
        "total_ms",
    ]
    for row in summary:
        dominant = get_dominant_trace_stage(row)
        dominant_text = (
            f"{dominant['name']}={dominant['value']}ms"
            if dominant
            else "n/a"
        )
        details = [f"{field}={row[field]}ms" for field in ordered_fields if field in row]
        write_info(
            f"{row['source']} count={row['count']} dominant={dominant_text} {' '.join(details)}"
        )


def prepare_deployment_pods(project_root, base_url, replicas):
    kubectl("apply", "-f", str(Path(project_root) / "k8s" / "explicit-v6-network.yaml"))
    kubectl("apply", "-f", str(Path(project_root) / "k8s" / "demo-deployment.yaml"))
    kubectl("scale", "deployment", "demo-deployment", "-n", DEPLOYMENT_NAMESPACE, f"--replicas={replicas}")

    pods = wait_pods_ready(DEPLOYMENT_NAMESPACE, DEPLOYMENT_SELECTOR, replicas)
    for pod in pods:
        metadata = pod.get("metadata") or {}
        allocation = wait_allocation_for_pod(base_url, metadata.get("uid"))
        managed_mac = get_pod_mac(metadata.get("namespace"), metadata.get("name"), "eth0")
        if managed_mac != allocation.get("assigned_mac"):
            raise RuntimeError(
                f"Managed MAC mismatch for {metadata.get('name')}: "
                f"expected {allocation.get('assigned_mac')} got {managed_mac}"
            )
        wait_pod_has_ipv6(
            metadata.get("namespace"),
            metadata.get("name"),
            "eth0",
            allocation.get("assigned_ipv6"),
        )
    return pods


def new_canonical_batch_plan(pods, count, gw_tag):
    plan = []
    for i in range(count):
        initial_index = i % len(pods)
        move_index = (initial_index + 1) % len(pods)
        entry_number = i + 1
        scenario_byte = count & 0xFF
        index_byte = entry_number & 0xFF
        index_high = (entry_number >> 8) & 0xFF
        mac_dev = f"aa:bb:{scenario_byte:02x}:{index_high:02x}:{index_byte:02x}:01"
        source = pods[initial_index]
        target = pods[move_index]
        source_meta = source.get("metadata") or {}
        target_meta = target.get("metadata") or {}
        plan.append(
            {
                "index": entry_number,
                "gw_tag": gw_tag,
                "mac_dev": mac_dev,
                "source_pod": {
                    "uid": source_meta.get("uid"),
                    "namespace": source_meta.get("namespace"),
                    "name": source_meta.get("name"),
                },
                "target_pod": {
                    "uid": target_meta.get("uid"),
                    "namespace": target_meta.get("namespace"),
                    "name": target_meta.get("name"),
                },
                "requested_ipv6": None,
            }
        )
    return plan


def invoke_parallel_explicit_batch(base_url, plan, pod_property, batch_size, trace_phase):
    launcher_start = time.perf_counter()
    completion_offsets = []
    request_latencies = []
    ready_offsets = []
    start_skews = []
    trace_ids = []
    ready_barrier = Barrier(len(plan) + 1)
    start_event = Event()
    release_time_holder = {"value": None}

    def submit(entry):
        ready_offset_ms = (time.perf_counter() - launcher_start) * 1000.0
        try:
            ready_barrier.wait()
        except BrokenBarrierError as exc:
            raise RuntimeError("Parallel benchmark start barrier broke before release") from exc
        start_event.wait()
        release_time = release_time_holder["value"]
        request_started = time.perf_counter()
        start_skew_ms = (request_started - release_time) * 1000.0
        pod = entry[pod_property]
        trace_id = f"{trace_phase}-b{batch_size}-i{entry['index']}-{uuid4().hex[:8]}"
        trace_client_started_at_ms = current_epoch_ms()
        payload = {
            "pod_uid": pod["uid"],
            "gw_tag": entry["gw_tag"],
            "mac_dev": entry["mac_dev"],
            "trace_id": trace_id,
            "trace_phase": trace_phase,
            "trace_batch_size": batch_size,
            "trace_request_index": entry["index"],
            "trace_client_started_at_ms": trace_client_started_at_ms,
        }
        response = ensure_explicit_request(base_url, payload)
        response_finished = time.perf_counter()
        request_latency_ms = (response_finished - request_started) * 1000.0
        completion_offset_ms = (response_finished - release_time) * 1000.0
        requested_ipv6 = (response or {}).get("requested_ipv6", "").lower()
        if not requested_ipv6:
            raise RuntimeError(
                f"Parallel canonical request response was incomplete for {pod['name']} mac_dev={entry['mac_dev']}"
            )
        entry.setdefault("phase_info", {})[trace_phase] = {
            "trace_id": trace_id,
            "client_started_at_ms": trace_client_started_at_ms,
            "start_skew_ms": start_skew_ms,
            "request_latency_ms": request_latency_ms,
            "completion_offset_ms": completion_offset_ms,
        }
        return (
            entry,
            requested_ipv6,
            completion_offset_ms,
            request_latency_ms,
            ready_offset_ms,
            start_skew_ms,
            trace_id,
        )

    with ThreadPoolExecutor(max_workers=len(plan)) as executor:
        futures = [executor.submit(submit, entry) for entry in plan]
        try:
            ready_barrier.wait()
        except BrokenBarrierError as exc:
            raise RuntimeError("Parallel benchmark start barrier broke while arming workers") from exc
        release_time_holder["value"] = time.perf_counter()
        start_event.set()
        for future in as_completed(futures):
            (
                entry,
                requested_ipv6,
                completion_offset_ms,
                request_latency_ms,
                ready_offset_ms,
                start_skew_ms,
                trace_id,
            ) = future.result()
            if not entry["requested_ipv6"]:
                entry["requested_ipv6"] = requested_ipv6
            elif entry["requested_ipv6"] != requested_ipv6:
                raise RuntimeError(
                    f"Canonical IPv6 changed unexpectedly for mac_dev={entry['mac_dev']}: "
                    f"expected {entry['requested_ipv6']} got {requested_ipv6}"
                )
            completion_offsets.append(completion_offset_ms)
            request_latencies.append(request_latency_ms)
            ready_offsets.append(ready_offset_ms)
            start_skews.append(start_skew_ms)
            trace_ids.append(trace_id)

    return {
        "completion_offset_metrics": get_latency_metrics(completion_offsets),
        "request_latency_metrics": get_latency_metrics(request_latencies),
        "ready_offset_metrics": get_latency_metrics(ready_offsets),
        "start_skew_metrics": get_latency_metrics(start_skews),
        "trace_ids": trace_ids,
    }


def unique_pods_from_plan(plan):
    unique = {}
    for entry in plan:
        for key in ("source_pod", "target_pod"):
            pod = entry[key]
            unique[pod["uid"]] = pod
    return unique


def unique_pods(pods):
    unique = {}
    for pod in pods:
        metadata = pod.get("metadata", {})
        uid = metadata.get("uid")
        if uid:
            unique[uid] = {
                "namespace": metadata.get("namespace"),
                "name": metadata.get("name"),
                "uid": uid,
            }
    return unique


def get_explicit_assignment(base_url, requested_ipv6):
    rows = (
        http_json(
            "GET",
            f"{base_url}/explicit-ipv6-assignments?requested_ipv6={urllib.parse.quote(requested_ipv6)}&status=ACTIVE",
        )
        or []
    )
    return rows[0] if rows else None


def get_net1_snapshot(unique_pods):
    snapshot = {}
    for uid, pod in unique_pods.items():
        snapshot[uid] = get_pod_ipv6_addresses(pod["namespace"], pod["name"], "net1")
    return snapshot


def get_net1_route_snapshot(unique_pods):
    snapshot = {}
    for uid, pod in unique_pods.items():
        snapshot[uid] = get_pod_ipv6_routes(pod["namespace"], pod["name"], "net1")
    return snapshot


def wait_parallel_apply_completion(base_url, plan, owner_property, trace_phase, timeout_seconds=300):
    state = {"done": 0}
    apply_completion_offsets = []
    apply_latencies = []
    completed = set()

    def condition():
        done = len(completed)
        for entry in plan:
            if entry["index"] in completed:
                continue
            phase_info = (entry.get("phase_info") or {}).get(trace_phase) or {}
            client_started_at_ms = phase_info.get("client_started_at_ms")
            start_skew_ms = phase_info.get("start_skew_ms")
            if client_started_at_ms is None or start_skew_ms is None:
                continue
            assignment = get_explicit_assignment(base_url, entry["requested_ipv6"])
            if not assignment:
                continue
            if assignment.get("pod_uid") != entry[owner_property]["uid"]:
                continue
            if str(assignment.get("container_iface") or "").strip() != "net1":
                continue
            if not assignment.get("last_applied_at"):
                continue
            observed_applied_ms = current_epoch_ms()
            apply_latency_ms = max(0.0, float(observed_applied_ms) - float(client_started_at_ms))
            apply_completion_offset_ms = max(0.0, float(start_skew_ms) + apply_latency_ms)
            completed.add(entry["index"])
            apply_latencies.append(apply_latency_ms)
            apply_completion_offsets.append(apply_completion_offset_ms)
            done += 1
        state["done"] = done
        return done == len(plan)

    condition._progress = lambda: f"{state['done']}/{len(plan)}"
    phase_name = "create" if trace_phase == "create" else "move"
    wait_until(
        f"parallel canonical {phase_name} completion",
        condition,
        timeout_seconds=timeout_seconds,
        interval_seconds=0.2,
    )
    return {
        "apply_latency_metrics": get_latency_metrics(apply_latencies),
        "apply_completion_metrics": get_latency_metrics(apply_completion_offsets),
    }


def select_reachability_pairs(plan, owner_property, seed_text):
    rng = random.Random(seed_text)
    pairs = []
    entries = list(plan)
    for target_entry in entries:
        owner_uid = target_entry[owner_property]["uid"]
        candidates = [candidate for candidate in entries if candidate[owner_property]["uid"] != owner_uid]
        if not candidates:
            raise RuntimeError("Reachability validation requires at least two distinct owner pods.")
        pairs.append((rng.choice(candidates), target_entry))
    return pairs


def test_canonical_reachability_all(plan, owner_property, batch_size, trace_phase, max_workers=64):
    pairs = select_reachability_pairs(plan, owner_property, f"{trace_phase}-{batch_size}-reachability")
    started = time.perf_counter()
    reachability_deadline_seconds = scaled_reachability_timeout_seconds(batch_size)
    reachability_latencies = []
    reachability_completion_offsets = []

    def ping_pair(pair):
        source_entry, target_entry = pair
        source_owner = source_entry[owner_property]
        target_phase_info = (target_entry.get("phase_info") or {}).get(trace_phase) or {}
        client_started_at_ms = target_phase_info.get("client_started_at_ms")
        start_skew_ms = target_phase_info.get("start_skew_ms")
        if client_started_at_ms is None or start_skew_ms is None:
            raise RuntimeError("Reachability validation is missing phase timing metadata.")
        deadline = time.time() + reachability_deadline_seconds
        while time.time() < deadline:
            try:
                invoke_pod_ping6(
                    source_owner["namespace"],
                    source_owner["name"],
                    target_entry["requested_ipv6"],
                    1,
                    2,
                )
                observed_reachable_ms = current_epoch_ms()
                reachability_latency_ms = max(0.0, float(observed_reachable_ms) - float(client_started_at_ms))
                reachability_completion_offset_ms = max(0.0, float(start_skew_ms) + reachability_latency_ms)
                return reachability_latency_ms, reachability_completion_offset_ms
            except RuntimeError:
                time.sleep(0.5)
        raise RuntimeError(
            f"Timed out waiting for {target_entry['requested_ipv6']} to become reachable from "
            f"{source_owner['namespace']}/{source_owner['name']}"
        )

    with ThreadPoolExecutor(max_workers=min(max_workers, len(pairs))) as executor:
        futures = [executor.submit(ping_pair, pair) for pair in pairs]
        for future in as_completed(futures):
            reachability_latency_ms, reachability_completion_offset_ms = future.result()
            reachability_latencies.append(reachability_latency_ms)
            reachability_completion_offsets.append(reachability_completion_offset_ms)

    duration_ms = (time.perf_counter() - started) * 1000.0
    return {
        "count": len(pairs),
        "duration_ms": round(duration_ms, 2),
        "ops_per_sec": round((len(pairs) / (duration_ms / 1000.0)) if duration_ms > 0 else 0.0, 2),
        "reach_latency_metrics": get_latency_metrics(reachability_latencies),
        "reach_completion_metrics": get_latency_metrics(reachability_completion_offsets),
    }


def run_batch_scenario(base_url, pods, batch_size, trace_pods):
    gw_tag = get_batch_gw_tag(batch_size)
    plan = new_canonical_batch_plan(pods, batch_size, gw_tag)
    create_timeout_seconds = scaled_create_timeout_seconds(batch_size)
    move_timeout_seconds = scaled_move_timeout_seconds(batch_size)

    write_step(f"Scenario {batch_size}: creating {batch_size} canonical IPv6 addresses in parallel")
    write_info(f"Create completion timeout: {create_timeout_seconds}s")
    create_start = datetime.now(timezone.utc)
    create_result = invoke_parallel_explicit_batch(base_url, plan, "source_pod", batch_size, "create")
    create_apply = wait_parallel_apply_completion(
        base_url,
        plan,
        "source_pod",
        "create",
        timeout_seconds=create_timeout_seconds,
    )
    create_end = datetime.now(timezone.utc)
    create_reachability = test_canonical_reachability_all(plan, "source_pod", batch_size, "create")
    requested_ipv6s = [entry["requested_ipv6"] for entry in plan if entry.get("requested_ipv6")]
    create_trace = get_trace_summary(
        get_trace_records(
            trace_pods,
            requested_ipv6s,
            create_result["trace_ids"],
            create_start,
            create_end,
        )
    )
    write_trace_summary(f"Scenario {batch_size}: create trace summary", create_trace)

    write_step(f"Scenario {batch_size}: moving {batch_size} canonical IPv6 addresses in parallel")
    write_info(f"Move completion timeout: {move_timeout_seconds}s")
    move_start = datetime.now(timezone.utc)
    move_result = invoke_parallel_explicit_batch(base_url, plan, "target_pod", batch_size, "move")
    move_apply = wait_parallel_apply_completion(
        base_url,
        plan,
        "target_pod",
        "move",
        timeout_seconds=move_timeout_seconds,
    )
    move_end = datetime.now(timezone.utc)
    move_reachability = test_canonical_reachability_all(plan, "target_pod", batch_size, "move")
    move_trace = get_trace_summary(
        get_trace_records(
            trace_pods,
            requested_ipv6s,
            move_result["trace_ids"],
            move_start,
            move_end,
        )
    )
    write_trace_summary(f"Scenario {batch_size}: move trace summary", move_trace)

    return {
        "BatchSize": batch_size,
        "GwTag": gw_tag,
        "CreateCompletionMetrics": create_result["completion_offset_metrics"],
        "CreateRequestMetrics": create_result["request_latency_metrics"],
        "CreateApplyLatencyMetrics": create_apply["apply_latency_metrics"],
        "CreateApplyCompletionMetrics": create_apply["apply_completion_metrics"],
        "CreateReadyMetrics": create_result["ready_offset_metrics"],
        "CreateStartSkewMetrics": create_result["start_skew_metrics"],
        "MoveCompletionMetrics": move_result["completion_offset_metrics"],
        "MoveRequestMetrics": move_result["request_latency_metrics"],
        "MoveApplyLatencyMetrics": move_apply["apply_latency_metrics"],
        "MoveApplyCompletionMetrics": move_apply["apply_completion_metrics"],
        "MoveReadyMetrics": move_result["ready_offset_metrics"],
        "MoveStartSkewMetrics": move_result["start_skew_metrics"],
        "CreateTrace": create_trace,
        "MoveTrace": move_trace,
        "CreateReachability": create_reachability,
        "MoveReachability": move_reachability,
    }


def print_summary(scenarios):
    write_step("Parallel canonical batch summary")
    header = (
        f"{'BatchSize':<10} "
        f"{'CreateReqAvg':<12} {'CreateApplyMax':<15} {'CreateApplyOps':<15} "
        f"{'MoveReqAvg':<12} {'MoveApplyMax':<14} {'MoveApplyOps':<14} "
        f"{'CreatePings':<12} {'MovePings':<12}"
    )
    print(header)
    for scenario in scenarios:
        create_request = scenario["CreateRequestMetrics"]
        move_request = scenario["MoveRequestMetrics"]
        create_apply = scenario["CreateApplyCompletionMetrics"]
        move_apply = scenario["MoveApplyCompletionMetrics"]
        create_reachability = scenario["CreateReachability"]
        move_reachability = scenario["MoveReachability"]
        print(
            f"{scenario['BatchSize']:<10} "
            f"{create_request['AvgMs']:<12} {create_apply['MaxMs']:<15} {create_apply['OpsPerSec']:<15} "
            f"{move_request['AvgMs']:<12} {move_apply['MaxMs']:<14} {move_apply['OpsPerSec']:<14} "
            f"{create_reachability['count']:<12} {move_reachability['count']:<12}"
        )
        write_info(
            "client fanout "
            f"create_ready_avg={scenario['CreateReadyMetrics']['AvgMs']}ms "
            f"create_ready_max={scenario['CreateReadyMetrics']['MaxMs']}ms "
            f"create_start_skew_avg={scenario['CreateStartSkewMetrics']['AvgMs']}ms "
            f"create_start_skew_max={scenario['CreateStartSkewMetrics']['MaxMs']}ms "
            f"move_ready_avg={scenario['MoveReadyMetrics']['AvgMs']}ms "
            f"move_ready_max={scenario['MoveReadyMetrics']['MaxMs']}ms "
            f"move_start_skew_avg={scenario['MoveStartSkewMetrics']['AvgMs']}ms "
            f"move_start_skew_max={scenario['MoveStartSkewMetrics']['MaxMs']}ms"
        )
        write_info(
            "fully applied "
            f"create_apply_avg={scenario['CreateApplyLatencyMetrics']['AvgMs']}ms "
            f"create_apply_p95={scenario['CreateApplyLatencyMetrics']['P95Ms']}ms "
            f"create_apply_batch_avg={scenario['CreateApplyCompletionMetrics']['AvgMs']}ms "
            f"create_apply_batch_max={scenario['CreateApplyCompletionMetrics']['MaxMs']}ms "
            f"create_apply_ops={scenario['CreateApplyCompletionMetrics']['OpsPerSec']}ops/s "
            f"move_apply_avg={scenario['MoveApplyLatencyMetrics']['AvgMs']}ms "
            f"move_apply_p95={scenario['MoveApplyLatencyMetrics']['P95Ms']}ms "
            f"move_apply_batch_avg={scenario['MoveApplyCompletionMetrics']['AvgMs']}ms "
            f"move_apply_batch_max={scenario['MoveApplyCompletionMetrics']['MaxMs']}ms "
            f"move_apply_ops={scenario['MoveApplyCompletionMetrics']['OpsPerSec']}ops/s"
        )
        write_info(
            "reachability "
            f"create_count={create_reachability['count']} "
            f"create_duration={create_reachability['duration_ms']}ms "
            f"create_ops={create_reachability['ops_per_sec']}ops/s "
            f"create_reach_avg={create_reachability['reach_latency_metrics']['AvgMs']}ms "
            f"create_reach_batch_max={create_reachability['reach_completion_metrics']['MaxMs']}ms "
            f"create_reach_batch_ops={create_reachability['reach_completion_metrics']['OpsPerSec']}ops/s "
            f"move_count={move_reachability['count']} "
            f"move_duration={move_reachability['duration_ms']}ms "
            f"move_ops={move_reachability['ops_per_sec']}ops/s "
            f"move_reach_avg={move_reachability['reach_latency_metrics']['AvgMs']}ms "
            f"move_reach_batch_max={move_reachability['reach_completion_metrics']['MaxMs']}ms "
            f"move_reach_batch_ops={move_reachability['reach_completion_metrics']['OpsPerSec']}ops/s"
        )


def main():
    parser = argparse.ArgumentParser(description="Linux in-cluster parallel canonical explicit IPv6 benchmark")
    parser.add_argument("--cleanup-samples-after", action="store_true")
    parser.add_argument("--deployment-replicas", type=int, default=DEPLOYMENT_REPLICAS)
    parser.add_argument("--batch-sizes", nargs="+", type=int, default=DEFAULT_BATCH_SIZES)
    parser.add_argument("--allocator-url", default=ALLOCATOR_URL)
    parser.add_argument("--project-root", default=detect_project_root())
    args = parser.parse_args()

    write_step("Checking the core stack")
    kubectl("rollout", "status", "statefulset/net-identity-allocator-postgres", "-n", "mac-allocator", "--timeout=180s")
    kubectl("rollout", "status", "deployment/net-identity-allocator", "-n", "mac-allocator", "--timeout=180s")
    kubectl("rollout", "status", "daemonset/cmxsafemac-ipv6-node-agent", "-n", "mac-allocator", "--timeout=180s")

    write_step("Checking allocator backend")
    backend = allocator_backend()
    write_info(f"Allocator backend: {backend}")
    if not verify_postgres():
        raise RuntimeError("PostgreSQL verification failed")
    trace_pods = get_trace_pod_targets()
    write_info(f"Allocator trace pod(s): {', '.join(trace_pods['allocator_pods'])}")
    write_info(f"Node-agent trace pod(s): {', '.join(trace_pods['node_agent_pods'])}")

    write_step("Preparing the shared 4-replica benchmark workload")
    cleanup_samples()
    try:
        wait_no_deployment_pods()
    except Exception:
        pass
    reset_allocator_state(args.allocator_url)
    pods = prepare_deployment_pods(args.project_root, args.allocator_url, args.deployment_replicas)
    baseline_addresses = get_net1_snapshot(unique_pods(pods))
    baseline_routes = get_net1_route_snapshot(unique_pods(pods))

    scenarios = []
    try:
      for index, batch_size in enumerate(args.batch_sizes):
          if index > 0:
              write_step(f"Scenario {batch_size}: clearing explicit IPv6 state from the existing deployment")
              clear_deployment_explicit_state(args.allocator_url, pods, baseline_addresses, baseline_routes)
              pods = wait_pods_ready(DEPLOYMENT_NAMESPACE, DEPLOYMENT_SELECTOR, args.deployment_replicas)
          scenarios.append(run_batch_scenario(args.allocator_url, pods, batch_size, trace_pods))
      print_summary(scenarios)
    finally:
      if args.cleanup_samples_after:
          write_step("Cleaning sample workloads")
          cleanup_samples()
          try:
              wait_no_deployment_pods()
          except Exception:
              pass


if __name__ == "__main__":
    main()
PY
