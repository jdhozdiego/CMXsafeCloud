from __future__ import annotations

import html
import ipaddress
import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8086"))
DASHBOARD_BASE_URL = os.getenv(
    "SSH_DASHBOARD_BASE_URL",
    "http://portable-openssh-dashboard.mac-ssh-demo.svc.cluster.local:8084",
).rstrip("/")
ALLOCATOR_BASE_URL = os.getenv(
    "MAC_ALLOCATOR_BASE_URL",
    "http://net-identity-allocator.mac-allocator.svc.cluster.local:8080",
).rstrip("/")
BRIDGE_COLLECTOR_BASE_URL = os.getenv(
    "TRAFFIC_COLLECTOR_BASE_URL",
    "http://cmxsafemac-ipv6-traffic-collector.mac-allocator.svc.cluster.local:8082",
).rstrip("/")
GATEWAY_NAMESPACE = os.getenv("GATEWAY_NAMESPACE", "mac-ssh-demo")
GATEWAY_LABEL_SELECTOR = os.getenv("GATEWAY_LABEL_SELECTOR", "app=portable-openssh-busybox")
REPLICA_COLLECTOR_PORT = int(os.getenv("REPLICA_COLLECTOR_PORT", "8083"))
FLOW_WINDOW_SECONDS = int(os.getenv("FLOW_WINDOW_SECONDS", "60"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "3"))
APPLICATION_EVENT_NAMESPACE = os.getenv("APPLICATION_EVENT_NAMESPACE", "mac-ssh-demo")
APPLICATION_EVENT_LABEL_SELECTOR = os.getenv("APPLICATION_EVENT_LABEL_SELECTOR", "cmxsafe-role=platform")
APPLICATION_EVENT_PORT = int(os.getenv("APPLICATION_EVENT_PORT", "9000"))
APPLICATION_EVENT_LIMIT = int(os.getenv("APPLICATION_EVENT_LIMIT", "160"))

KUBE_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
KUBE_CA_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lower_text(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_ipv6(value: Any) -> str:
    try:
        return ipaddress.IPv6Address(str(value).strip()).exploded.lower()
    except Exception:
        return lower_text(value)


def compressed_ipv6(value: Any) -> str:
    try:
        return ipaddress.IPv6Address(str(value).strip()).compressed.lower()
    except Exception:
        return lower_text(value)


def decode_mac_from_canonical(value: Any) -> str:
    try:
        packed = ipaddress.IPv6Address(str(value).strip()).packed
    except Exception:
        return ""
    return ":".join(f"{byte:02x}" for byte in packed[10:16])


def is_canonical_username(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return len(text) == 32 and all(ch in "0123456789abcdef" for ch in text)


def short_endpoint_alias(identity: dict[str, Any], role: str = "endpoint") -> str:
    for key in ("alias", "display_name"):
        value = str(identity.get(key) or "").strip()
        if value and not is_canonical_username(value):
            return value[:24]
    mac = str(identity.get("mac") or decode_mac_from_canonical(identity.get("canonical_ipv6")) or "").strip()
    if mac:
        suffix = mac.split(":")[-1].lower()
        if role == "device":
            return f"dev-{suffix}"
        if role == "platform":
            return f"platform-{suffix}"
    username = str(identity.get("username") or "").strip()
    if is_canonical_username(username):
        return f"{role[:3]}-{username[-2:]}"
    return (username or role)[:24]


def short_service_alias(service: dict[str, Any]) -> str:
    alias = str(service.get("alias") or service.get("name") or "service").strip()
    replacements = (
        ("Live current ", ""),
        ("current ", ""),
        ("IoT ", "IoT "),
    )
    for old, new in replacements:
        alias = alias.replace(old, new)
    return alias[:26] if alias else "service"


def endpoint_summary(identity: dict[str, Any], username: str = "") -> dict[str, Any]:
    resolved_username = identity.get("username") or username
    canonical_ipv6 = normalize_ipv6(identity.get("canonical_ipv6"))
    alias = identity.get("alias") or identity.get("display_name") or resolved_username
    display_name = identity.get("display_name") or identity.get("alias") or resolved_username
    return {
        "username": resolved_username,
        "alias": alias,
        "display_name": display_name,
        "canonical_ipv6": canonical_ipv6,
        "canonical_ipv6_compact": compressed_ipv6(canonical_ipv6),
        "mac": decode_mac_from_canonical(canonical_ipv6),
        "is_iot_device": bool(identity.get("is_iot_device")),
        "is_iot_platform": bool(identity.get("is_iot_platform")),
        "role_labels": identity.get("role_labels") or [],
        "published_service_count": 0,
        "accessible_service_count": 0,
    }


def merge_endpoint(collection: dict[str, dict[str, Any]], username: str, identity: dict[str, Any]) -> dict[str, Any]:
    summary = endpoint_summary(identity, username)
    row = collection.setdefault(username, summary)
    for key in ("alias", "display_name", "canonical_ipv6", "mac", "role_labels"):
        if summary.get(key):
            row[key] = summary[key]
    row["is_iot_device"] = bool(row.get("is_iot_device") or summary.get("is_iot_device"))
    row["is_iot_platform"] = bool(row.get("is_iot_platform") or summary.get("is_iot_platform"))
    return row


def request_json(url: str, timeout: float = HTTP_TIMEOUT_SECONDS) -> Any:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")
    return json.loads(payload)


def safe_request_json(url: str, errors: list[dict[str, str]], source: str, timeout: float = HTTP_TIMEOUT_SECONDS) -> Any:
    try:
        return request_json(url, timeout=timeout)
    except Exception as exc:
        errors.append({"source": source, "message": str(exc), "timestamp": now_iso()})
        return {}


def rows(payload: Any, key: str | None = None) -> list[dict[str, Any]]:
    value = payload.get(key, []) if key else payload
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def kube_list_pods(namespace: str, label_selector: str, errors: list[dict[str, str]], source: str) -> list[dict[str, Any]]:
    if not KUBE_TOKEN_PATH.exists():
        errors.append(
            {
                "source": source,
                "message": "service account token is unavailable; Kubernetes pod discovery disabled",
                "timestamp": now_iso(),
            }
        )
        return []

    selector = urllib.parse.quote(label_selector, safe="")
    url = (
        "https://kubernetes.default.svc/api/v1/namespaces/"
        f"{urllib.parse.quote(namespace)}/pods?labelSelector={selector}"
    )
    token = KUBE_TOKEN_PATH.read_text(encoding="utf-8").strip()
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    context = ssl.create_default_context(cafile=str(KUBE_CA_PATH)) if KUBE_CA_PATH.exists() else None
    try:
        with urllib.request.urlopen(request, context=context, timeout=HTTP_TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        errors.append(
            {
                "source": source,
                "message": f"Kubernetes pod discovery failed with HTTP {exc.code}",
                "timestamp": now_iso(),
            }
        )
        return []
    except Exception as exc:
        errors.append({"source": source, "message": str(exc), "timestamp": now_iso()})
        return []

    pods = []
    for item in rows(payload, "items"):
        metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
        status = item.get("status", {}) if isinstance(item.get("status"), dict) else {}
        spec = item.get("spec", {}) if isinstance(item.get("spec"), dict) else {}
        pods.append(
            {
                "name": metadata.get("name", ""),
                "namespace": metadata.get("namespace", namespace),
                "uid": metadata.get("uid", ""),
                "pod_ip": status.get("podIP", ""),
                "node_name": spec.get("nodeName", ""),
                "phase": status.get("phase", ""),
                "labels": metadata.get("labels", {}) if isinstance(metadata.get("labels"), dict) else {},
                "ready": any(
                    condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in status.get("conditions", [])
                    if isinstance(condition, dict)
                ),
            }
        )
    return pods


def kube_list_gateway_pods(errors: list[dict[str, str]]) -> list[dict[str, Any]]:
    return kube_list_pods(GATEWAY_NAMESPACE, GATEWAY_LABEL_SELECTOR, errors, "kubernetes.gateway-pods")


def load_flow_source(base_url: str, source: str, errors: list[dict[str, str]], window_seconds: int) -> dict[str, Any]:
    query = urllib.parse.urlencode({"window_seconds": window_seconds, "limit": 500})
    payload = safe_request_json(f"{base_url.rstrip('/')}/flows?{query}", errors, source)
    payload.setdefault("flows", [])
    return payload


def normalize_flow(flow: dict[str, Any], source_scope: str, source_name: str, capture_interface: str) -> dict[str, Any]:
    return {
        "source_scope": source_scope,
        "source_name": source_name,
        "capture_interface": capture_interface,
        "src_address": normalize_ipv6(flow.get("src_address")),
        "dst_address": normalize_ipv6(flow.get("dst_address")),
        "src_port": flow.get("src_port"),
        "dst_port": flow.get("dst_port"),
        "protocol": lower_text(flow.get("protocol")),
        "packets": int(flow.get("packets") or 0),
        "bytes": int(flow.get("bytes") or 0),
        "first_seen": str(flow.get("first_seen") or ""),
        "last_seen": str(flow.get("last_seen") or ""),
    }


def flow_matches_path(flow: dict[str, Any], device_ipv6: str, service_ipv6: str, service_port: int) -> str:
    src = flow["src_address"]
    dst = flow["dst_address"]
    src_port = int(flow.get("src_port") or 0)
    dst_port = int(flow.get("dst_port") or 0)
    if src == device_ipv6 and dst == service_ipv6 and dst_port == service_port:
        return "request"
    if src == service_ipv6 and dst == device_ipv6 and src_port == service_port:
        return "response"
    return ""


def identity_port_from_flow(flow: dict[str, Any], direction: str) -> Any:
    if direction == "request":
        return flow.get("src_port")
    if direction == "response":
        return flow.get("dst_port")
    return None


def pretty_payload_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        try:
            return json.dumps(json.loads(text), indent=2, sort_keys=True)
        except Exception:
            return value
    try:
        return json.dumps(value, indent=2, sort_keys=True)
    except Exception:
        return str(value)


def request_payload_text_from_event(event: dict[str, Any]) -> str:
    raw_body = str(event.get("raw_body") or "")
    if raw_body.strip():
        return pretty_payload_text(raw_body)
    body = event.get("body")
    if body not in (None, "", {}):
        return pretty_payload_text(body)
    content = str(event.get("content") or "")
    return content


def response_payload_text_from_event(event: dict[str, Any]) -> str:
    response_payload = {
        key: value
        for key, value in event.items()
        if key not in {"source_pod", "source_role"}
    }
    return pretty_payload_text(response_payload)


def load_application_events(errors: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    pods = kube_list_pods(
        APPLICATION_EVENT_NAMESPACE,
        APPLICATION_EVENT_LABEL_SELECTOR,
        errors,
        "kubernetes.application-event-pods",
    )
    events: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    limit = max(1, min(500, APPLICATION_EVENT_LIMIT))
    query = urllib.parse.urlencode({"limit": limit})
    for pod in pods:
        if not pod.get("pod_ip"):
            continue
        url = f"http://{pod['pod_ip']}:{APPLICATION_EVENT_PORT}/monitor/recent?{query}"
        payload = safe_request_json(url, errors, f"application-events:{pod['name']}")
        source_events = []
        for event in rows(payload, "events"):
            client = normalize_ipv6(event.get("client"))
            if not client:
                continue
            try:
                source_port = int(event.get("port") or 0)
            except Exception:
                source_port = 0
            try:
                service_port = int(event.get("service_port") or APPLICATION_EVENT_PORT)
            except Exception:
                service_port = APPLICATION_EVENT_PORT
            normalized = {
                **event,
                "client": client,
                "port": source_port,
                "service_port": service_port,
                "source_pod": pod["name"],
                "source_role": str((pod.get("labels") or {}).get("cmxsafe-role") or ""),
                "request_payload_text": request_payload_text_from_event(event),
                "response_payload_text": response_payload_text_from_event(event),
            }
            source_events.append(normalized)
        sources.append(
            {
                "pod": pod["name"],
                "pod_ip": pod["pod_ip"],
                "ready": bool(pod.get("ready")),
                "role": str((pod.get("labels") or {}).get("cmxsafe-role") or ""),
                "event_count": len(source_events),
            }
        )
        events.extend(source_events)
    return events, sources


def build_application_event_index(events: list[dict[str, Any]]) -> dict[tuple[str, int, int], dict[str, Any]]:
    index: dict[tuple[str, int, int], dict[str, Any]] = {}
    for event in events:
        key = (
            normalize_ipv6(event.get("client")),
            int(event.get("port") or 0),
            int(event.get("service_port") or 0),
        )
        current = index.get(key)
        if current is None or str(event.get("received_at") or "") >= str(current.get("received_at") or ""):
            index[key] = event
    return index


def flow_payload_details(
    flow: dict[str, Any],
    direction: str,
    device_ipv6: str,
    service_port: int,
    event_index: dict[tuple[str, int, int], dict[str, Any]],
) -> dict[str, Any]:
    identity_port = identity_port_from_flow(flow, direction)
    if not identity_port:
        return {"available": False}
    event = event_index.get((normalize_ipv6(device_ipv6), int(identity_port), int(service_port or 0)))
    if not event:
        return {"available": False}
    primary_label = "Request payload" if direction == "request" else "Response payload"
    primary_text = event.get("request_payload_text") if direction == "request" else event.get("response_payload_text")
    return {
        "available": bool(primary_text),
        "event_id": event.get("event_id"),
        "source_pod": event.get("source_pod"),
        "source_role": event.get("source_role"),
        "received_at": event.get("received_at"),
        "content_type": event.get("content_type") or "",
        "request_path": event.get("path") or "",
        "primary_label": primary_label,
        "primary_text": primary_text or "",
        "request_text": event.get("request_payload_text") or "",
        "response_text": event.get("response_payload_text") or "",
        "request_content": event.get("content") or "",
    }


def build_snapshot(window_seconds: int) -> dict[str, Any]:
    errors: list[dict[str, str]] = []
    topology = safe_request_json(f"{DASHBOARD_BASE_URL}/api/topology", errors, "dashboard")
    allocations = rows(safe_request_json(f"{ALLOCATOR_BASE_URL}/allocations", errors, "allocator.allocations"))
    explicit_assignments = rows(
        safe_request_json(
            f"{ALLOCATOR_BASE_URL}/explicit-ipv6-assignments",
            errors,
            "allocator.explicit-ipv6-assignments",
        )
    )
    gateway_pods = kube_list_gateway_pods(errors)
    application_events, application_event_sources = load_application_events(errors)
    application_event_index = build_application_event_index(application_events)

    explicit_by_ipv6: dict[str, dict[str, Any]] = {}
    active_explicit = []
    for row in explicit_assignments:
        if lower_text(row.get("status")) != "active":
            continue
        requested = normalize_ipv6(row.get("requested_ipv6"))
        if not requested:
            continue
        active_explicit.append(row)
        explicit_by_ipv6[requested] = row

    pod_by_uid = {pod["uid"]: pod for pod in gateway_pods if pod.get("uid")}
    pod_by_name = {pod["name"]: pod for pod in gateway_pods if pod.get("name")}

    def current_replica_for_assignment(assignment: dict[str, Any]) -> dict[str, Any]:
        return pod_by_uid.get(str(assignment.get("pod_uid") or ""), {})

    def assignment_details(assignment: dict[str, Any]) -> dict[str, Any]:
        if not assignment:
            return {}
        current = current_replica_for_assignment(assignment)
        return {
            "requested_ipv6": normalize_ipv6(assignment.get("requested_ipv6")),
            "pod_name": assignment.get("pod_name"),
            "pod_uid": assignment.get("pod_uid"),
            "current_replica": current.get("name", ""),
            "current_replica_uid": current.get("uid", ""),
            "current": bool(current),
            "target_assigned_mac": assignment.get("target_assigned_mac"),
            "target_gw_mac": assignment.get("target_gw_mac"),
            "mac_dev": assignment.get("mac_dev"),
            "last_applied_at": assignment.get("last_applied_at"),
            "runtime_observed_at": assignment.get("runtime_observed_at"),
        }

    bridge_payload = load_flow_source(BRIDGE_COLLECTOR_BASE_URL, "bridge", errors, window_seconds)
    flows: list[dict[str, Any]] = [
        normalize_flow(flow, "bridge", "br-explicit-v6", bridge_payload.get("capture_interface", "br-explicit-v6"))
        for flow in rows(bridge_payload, "flows")
    ]

    replica_collector_status = []
    for pod in gateway_pods:
        if not pod.get("pod_ip"):
            continue
        source_name = pod["name"]
        base = f"http://{pod['pod_ip']}:{REPLICA_COLLECTOR_PORT}"
        payload = load_flow_source(base, f"replica:{source_name}", errors, window_seconds)
        collector_errors = rows(payload, "errors")
        replica_collector_status.append(
            {
                "replica": source_name,
                "pod_ip": pod["pod_ip"],
                "capture_interface": payload.get("capture_interface"),
                "capture_active": bool(payload.get("capture_active")),
                "flow_count": len(rows(payload, "flows")),
                "errors": collector_errors[-3:],
            }
        )
        flows.extend(
            normalize_flow(
                flow,
                "replica",
                source_name,
                str(payload.get("capture_interface") or "any"),
            )
            for flow in rows(payload, "flows")
        )

    identities_by_username: dict[str, dict[str, Any]] = {}
    services_by_id: dict[int, dict[str, Any]] = {}
    devices: dict[str, dict[str, Any]] = {}
    platforms: dict[str, dict[str, Any]] = {}
    services: list[dict[str, Any]] = []
    registered_paths = []
    payload_records: dict[str, dict[str, Any]] = {}

    for target in rows(topology, "targets"):
        for identity in rows(target, "identities"):
            username = identity.get("username", "")
            if not username:
                continue
            identity["canonical_ipv6"] = normalize_ipv6(identity.get("canonical_ipv6"))
            identities_by_username[username] = identity
        for service in rows(target, "published_services"):
            service_id = int(service.get("id") or 0)
            service["canonical_ipv6"] = normalize_ipv6(service.get("canonical_ipv6"))
            service["short_alias"] = short_service_alias(service)
            services_by_id[service_id] = service
            services.append(service)
            owner = service.get("owner_username", "")
            if owner:
                owner_identity = identities_by_username.get(owner, {})
                fallback_identity = {
                    **owner_identity,
                    "username": owner,
                    "alias": service.get("owner_alias") or owner_identity.get("alias") or service.get("owner_display_name") or owner,
                    "display_name": service.get("owner_display_name") or owner_identity.get("display_name") or owner,
                    "canonical_ipv6": owner_identity.get("canonical_ipv6") or service.get("canonical_ipv6"),
                }
                if owner_identity.get("is_iot_device") and not owner_identity.get("is_iot_platform"):
                    endpoint = merge_endpoint(devices, owner, fallback_identity)
                else:
                    # Backward compatibility: older rows had no role flag, and service owners were treated as platforms.
                    endpoint = merge_endpoint(platforms, owner, fallback_identity)
                    endpoint["is_iot_platform"] = True
                endpoint["published_service_count"] += 1
        for path in rows(target, "registered_paths"):
            registered_paths.append(path)
            consumer = path.get("consumer", {}) if isinstance(path.get("consumer"), dict) else {}
            username = consumer.get("username", "")
            if username:
                consumer_identity = identities_by_username.get(username, consumer)
                endpoint = merge_endpoint(devices, username, {**consumer, **consumer_identity})
                if not endpoint.get("is_iot_platform"):
                    endpoint["is_iot_device"] = True
                endpoint["accessible_service_count"] += 1

    replica_sockets: dict[str, dict[str, Any]] = {
        pod["uid"] or pod["name"]: {
            **pod,
            "published_sockets": [],
            "_published_socket_map": {},
            "_sps_map": {},
            "identity_sockets": [],
            "canonical_addresses": [],
            "flow_count": 0,
        }
        for pod in gateway_pods
    }
    for address, assignment in explicit_by_ipv6.items():
        pod_uid = str(assignment.get("pod_uid") or "")
        replica = replica_sockets.get(pod_uid)
        if replica is None:
            continue
        replica["canonical_addresses"].append(
            {
                "ipv6": address,
                "compressed": compressed_ipv6(address),
                "mac_dev": assignment.get("mac_dev") or decode_mac_from_canonical(address),
                "gw_tag_hex": assignment.get("gw_tag_hex"),
            }
        )

    path_rows = []
    for index, path in enumerate(registered_paths):
        consumer = path.get("consumer", {}) if isinstance(path.get("consumer"), dict) else {}
        publisher = path.get("publisher", {}) if isinstance(path.get("publisher"), dict) else {}
        service = path.get("service", {}) if isinstance(path.get("service"), dict) else {}
        consumer_identity = identities_by_username.get(consumer.get("username", ""), consumer)
        publisher_identity = identities_by_username.get(publisher.get("username", ""), publisher)
        device_ipv6 = normalize_ipv6(consumer.get("canonical_ipv6"))
        service_ipv6 = normalize_ipv6(service.get("canonical_ipv6"))
        service_port = int(service.get("port") or 0)
        device_assignment = explicit_by_ipv6.get(device_ipv6, {})
        service_assignment = explicit_by_ipv6.get(service_ipv6, {})
        device_assignment_details = assignment_details(device_assignment)
        service_assignment_details = assignment_details(service_assignment)
        device_replica_uid = device_assignment_details.get("current_replica_uid", "")
        service_replica_uid = service_assignment_details.get("current_replica_uid", "")
        device_replica = device_assignment_details.get("current_replica", "")
        service_replica = service_assignment_details.get("current_replica", "")

        path_id = path.get("id") or f"path-{index}"
        device_label = short_endpoint_alias(
            {
                **consumer,
                **consumer_identity,
                "username": consumer.get("username"),
                "canonical_ipv6": device_ipv6,
                "mac": decode_mac_from_canonical(device_ipv6),
            },
            "device",
        )
        platform_label = short_endpoint_alias(
            {
                **publisher,
                **publisher_identity,
                "username": publisher.get("username"),
                "canonical_ipv6": normalize_ipv6(publisher.get("canonical_ipv6")),
                "mac": decode_mac_from_canonical(publisher.get("canonical_ipv6")),
            },
            "platform",
        )
        service_label = short_service_alias(service)
        service_socket_key = f"{service.get('id') or 'svc'}:{service_ipv6}:{service_port}"

        matched = []
        bridge_seen = False
        replica_seen = False
        packet_count = 0
        byte_count = 0
        last_seen = ""
        communications_by_port: dict[str, dict[str, Any]] = {}
        latest_identity_port = None
        latest_identity_port_seen = ""
        for flow in flows:
            direction = flow_matches_path(flow, device_ipv6, service_ipv6, service_port)
            if not direction:
                continue
            payload = flow_payload_details(
                flow,
                direction,
                device_ipv6,
                service_port,
                application_event_index,
            )
            payload_ref = ""
            payload_meta = {"available": False}
            if payload.get("available"):
                payload_ref = f"event:{payload.get('event_id')}:{direction}"
                payload_records[payload_ref] = payload
                payload_meta = {
                    "available": True,
                    "event_id": payload.get("event_id"),
                    "source_pod": payload.get("source_pod"),
                    "source_role": payload.get("source_role"),
                    "received_at": payload.get("received_at"),
                    "content_type": payload.get("content_type"),
                    "request_path": payload.get("request_path"),
                    "primary_label": payload.get("primary_label"),
                    "payload_ref": payload_ref,
                }
            observed = {
                **flow,
                "direction": direction,
                "payload_available": bool(payload_ref),
                "payload": payload_meta,
            }
            matched.append(observed)
            packet_count += int(flow.get("packets") or 0)
            byte_count += int(flow.get("bytes") or 0)
            if flow["source_scope"] == "bridge":
                bridge_seen = True
            if flow["source_scope"] == "replica":
                replica_seen = True
            if flow.get("last_seen", "") > last_seen:
                last_seen = flow.get("last_seen", "")
            candidate_identity_port = identity_port_from_flow(flow, direction)
            if candidate_identity_port and flow.get("last_seen", "") >= latest_identity_port_seen:
                latest_identity_port = candidate_identity_port
                latest_identity_port_seen = flow.get("last_seen", "")
            if candidate_identity_port:
                port_key = str(candidate_identity_port)
                communication = communications_by_port.setdefault(
                    port_key,
                    {
                        "source_port": candidate_identity_port,
                        "directions": [],
                        "source_scopes": [],
                        "packet_count": 0,
                        "byte_count": 0,
                        "first_seen": flow.get("first_seen", ""),
                        "last_seen": flow.get("last_seen", ""),
                    },
                )
                if direction not in communication["directions"]:
                    communication["directions"].append(direction)
                if flow["source_scope"] not in communication["source_scopes"]:
                    communication["source_scopes"].append(flow["source_scope"])
                communication["packet_count"] += int(flow.get("packets") or 0)
                communication["byte_count"] += int(flow.get("bytes") or 0)
                if flow.get("first_seen") and (
                    not communication["first_seen"] or flow.get("first_seen", "") < communication["first_seen"]
                ):
                    communication["first_seen"] = flow.get("first_seen", "")
                if flow.get("last_seen", "") > communication["last_seen"]:
                    communication["last_seen"] = flow.get("last_seen", "")

            if flow["source_scope"] == "replica":
                replica = pod_by_name.get(flow["source_name"])
                replica_key = (replica or {}).get("uid") or flow["source_name"]
                if replica_key in replica_sockets:
                    replica_sockets[replica_key]["flow_count"] += int(flow.get("packets") or 0)

        communications = sorted(
            communications_by_port.values(),
            key=lambda communication: str(communication.get("last_seen") or ""),
            reverse=True,
        )

        if device_replica_uid in replica_sockets:
            sps_key = f"{device_replica_uid}:{consumer.get('username') or device_ipv6}"
            sps_map = replica_sockets[device_replica_uid]["_sps_map"]
            sps_row = sps_map.setdefault(
                sps_key,
                {
                    "key": sps_key,
                    "device_username": consumer.get("username"),
                    "device_alias": device_label,
                    "sps_label": f"SPS {device_label}",
                    "source": compressed_ipv6(device_ipv6),
                    "mac": decode_mac_from_canonical(device_ipv6),
                    "grants": [],
                    "path_ids": [],
                    "live": False,
                },
            )
            sps_row["path_ids"].append(path_id)
            sps_row["live"] = bool(sps_row["live"] or latest_identity_port)
            sps_row["grants"].append(
                {
                    "path_id": path_id,
                    "service_socket_key": service_socket_key,
                    "service_alias": service_label,
                    "publisher_alias": platform_label,
                    "sc_label": f"{platform_label}:{service_label} ({service_port})",
                    "direction": "request",
                    "source": compressed_ipv6(device_ipv6),
                    "source_port": latest_identity_port,
                    "source_port_observed_at": latest_identity_port_seen,
                    "source_port_semantics": "latest observed ephemeral identity port in the selected flow window",
                    "communications": communications,
                    "destination": compressed_ipv6(service_ipv6),
                    "destination_port": service_port,
                    "service_port": service_port,
                    "service_replica": service_replica,
                    "same_replica": bool(device_replica_uid and device_replica_uid == service_replica_uid),
                    "live": bool(latest_identity_port),
                }
            )

        if service_replica_uid in replica_sockets:
            published_map = replica_sockets[service_replica_uid]["_published_socket_map"]
            published_socket = published_map.setdefault(
                service_socket_key,
                {
                    "key": service_socket_key,
                    "alias": service_label,
                    "full_alias": service.get("alias"),
                    "publisher_alias": platform_label,
                    "ipv6": service_ipv6,
                    "port": service_port,
                    "consumer_count": 0,
                    "live": False,
                },
            )
            published_socket["consumer_count"] += 1
            published_socket["live"] = bool(published_socket["live"] or latest_identity_port)

        assignments_known = bool(device_assignment) and bool(service_assignment)
        stale_assignment = assignments_known and not (device_replica and service_replica)
        materialized = assignments_known and not stale_assignment
        state = "registered"
        if stale_assignment:
            state = "stale-assignment"
        if materialized:
            state = "materialized"
        if bridge_seen:
            state = "bridge-observed"
        if replica_seen:
            state = "replica-observed"
        if bridge_seen and replica_seen:
            state = "fully-observed"

        path_rows.append(
            {
                "id": path_id,
                "context_alias": path.get("context_alias") or service.get("alias") or f"path-{index}",
                "state": state,
                "enabled": bool(path.get("enabled", True)),
                "color": "#0f766e",
                "materialized": materialized,
                "assignments_known": assignments_known,
                "stale_assignment": stale_assignment,
                "same_replica": bool(device_replica_uid and device_replica_uid == service_replica_uid),
                "bridge_seen": bridge_seen,
                "replica_seen": replica_seen,
                "live": bridge_seen or replica_seen,
                "observed_packet_count": packet_count,
                "observed_byte_count": byte_count,
                "last_seen": last_seen,
                "communications": communications,
                "device": {
                    "username": consumer.get("username"),
                    "alias": consumer.get("alias") or consumer.get("display_name") or consumer.get("username"),
                    "short_alias": device_label,
                    "display_name": consumer.get("display_name") or consumer.get("alias") or consumer.get("username"),
                    "canonical_ipv6": device_ipv6,
                    "mac": decode_mac_from_canonical(device_ipv6),
                    "is_iot_device": True if not consumer_identity.get("is_iot_platform") else bool(consumer_identity.get("is_iot_device")),
                    "is_iot_platform": bool(consumer_identity.get("is_iot_platform")),
                    "role_labels": consumer_identity.get("role_labels") or [],
                    "replica": device_replica,
                    "replica_uid": device_replica_uid,
                    "assignment": device_assignment_details,
                },
                "platform": {
                    "username": publisher.get("username"),
                    "alias": publisher.get("alias") or publisher.get("display_name") or publisher.get("username"),
                    "short_alias": platform_label,
                    "display_name": publisher.get("display_name") or publisher.get("alias") or publisher.get("username"),
                    "canonical_ipv6": normalize_ipv6(publisher.get("canonical_ipv6")),
                    "mac": decode_mac_from_canonical(publisher.get("canonical_ipv6")),
                    "is_iot_device": bool(publisher_identity.get("is_iot_device")),
                    "is_iot_platform": True,
                    "role_labels": publisher_identity.get("role_labels") or [],
                    "replica": service_replica,
                    "replica_uid": service_replica_uid,
                    "assignment": service_assignment_details,
                },
                "service": {
                    "id": service.get("id"),
                    "alias": service.get("alias"),
                    "short_alias": service_label,
                    "socket_key": service_socket_key,
                    "canonical_ipv6": service_ipv6,
                    "port": service_port,
                    "protocol": service.get("protocol") or "tcp",
                },
                "flows": matched[:24],
            }
        )

    for endpoint in devices.values():
        endpoint["short_alias"] = short_endpoint_alias(endpoint, "device")
        endpoint["canonical_ipv6_compact"] = compressed_ipv6(endpoint.get("canonical_ipv6") or endpoint.get("username"))
        endpoint["is_iot_device"] = True if not endpoint.get("is_iot_platform") else bool(endpoint.get("is_iot_device"))
    for endpoint in platforms.values():
        endpoint["short_alias"] = short_endpoint_alias(endpoint, "platform")
        endpoint["canonical_ipv6_compact"] = compressed_ipv6(endpoint.get("canonical_ipv6") or endpoint.get("username"))
        endpoint["is_iot_platform"] = True
    for service in services:
        service["short_alias"] = short_service_alias(service)
    for replica in replica_sockets.values():
        published_map = replica.pop("_published_socket_map", {})
        sps_map = replica.pop("_sps_map", {})
        replica["published_sockets"] = sorted(
            published_map.values(),
            key=lambda socket: (str(socket.get("alias") or ""), int(socket.get("port") or 0)),
        )
        replica["identity_sockets"] = sorted(
            sps_map.values(),
            key=lambda socket: str(socket.get("device_alias") or ""),
        )

    summary = {
        "device_count": len(devices),
        "platform_count": len(platforms),
        "service_count": len(services),
        "replica_count": len(gateway_pods),
        "registered_path_count": len(path_rows),
        "materialized_path_count": sum(1 for path in path_rows if path["materialized"]),
        "stale_assignment_path_count": sum(1 for path in path_rows if path["stale_assignment"]),
        "replica_observed_path_count": sum(1 for path in path_rows if path["replica_seen"]),
        "bridge_observed_path_count": sum(1 for path in path_rows if path["bridge_seen"]),
        "flow_count": len(flows),
    }

    return {
        "generated_at": now_iso(),
        "window_seconds": window_seconds,
        "summary": summary,
        "devices": sorted(devices.values(), key=lambda row: row["display_name"]),
        "platforms": sorted(platforms.values(), key=lambda row: row["display_name"]),
        "services": sorted(services, key=lambda row: str(row.get("alias") or "")),
        "replicas": sorted(replica_sockets.values(), key=lambda row: row.get("name", "")),
        "paths": path_rows,
        "payloads": payload_records,
        "flows": flows[:300],
        "collector_status": {
            "bridge": {
                "capture_interface": bridge_payload.get("capture_interface"),
                "capture_active": bool(bridge_payload.get("capture_active")),
                "flow_count": len(rows(bridge_payload, "flows")),
                "errors": rows(bridge_payload, "errors")[-3:],
            },
            "replicas": replica_collector_status,
            "application_events": application_event_sources,
        },
        "sources": {
            "dashboard": DASHBOARD_BASE_URL,
            "allocator": ALLOCATOR_BASE_URL,
            "bridge_collector": BRIDGE_COLLECTOR_BASE_URL,
            "replica_collector_port": REPLICA_COLLECTOR_PORT,
            "application_event_namespace": APPLICATION_EVENT_NAMESPACE,
            "application_event_label_selector": APPLICATION_EVENT_LABEL_SELECTOR,
            "application_event_port": APPLICATION_EVENT_PORT,
        },
        "errors": errors,
        "notes": [
            "Registered paths come from dashboard services and access grants.",
            "Materialized paths come from allocator explicit IPv6 placement.",
            "Stale assignments mean allocator state still names a retired replica, so the path is registered but not currently placed on a live gateway pod.",
            "Replica-observed traffic comes from gateway sidecar collectors.",
            "Bridge-observed traffic comes from br-explicit-v6 and may be empty for same-replica local forwarding.",
            "When recent platform monitor events are available, request and response payloads can be opened from the traffic dump rows.",
        ],
    }


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CMXsafe Secure Path Observer</title>
  <style>
    :root {
      --bg: #0f1c18;
      --bg2: #162a23;
      --panel: rgba(248, 243, 229, 0.96);
      --ink: #14201c;
      --muted: #66736d;
      --line: #d5c8ae;
      --device: #0f766e;
      --replica: #a16207;
      --platform: #1d4ed8;
      --live: #22c55e;
      --bridge: #7c3aed;
      --danger: #b91c1c;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at 15% 10%, rgba(34, 197, 94, 0.16), transparent 28rem),
        radial-gradient(circle at 82% 20%, rgba(37, 99, 235, 0.18), transparent 26rem),
        linear-gradient(135deg, var(--bg), var(--bg2));
      font-family: "Segoe UI", "Aptos", sans-serif;
      min-height: 100vh;
    }
    header {
      color: #f8f3e5;
      padding: 28px 34px 16px;
    }
    h1 {
      margin: 0;
      font-size: clamp(28px, 4vw, 46px);
      letter-spacing: -0.04em;
    }
    .subtitle {
      max-width: 1050px;
      margin: 10px 0 0;
      color: rgba(248, 243, 229, 0.78);
      line-height: 1.45;
      font-size: 16px;
    }
    main { padding: 0 24px 32px; }
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }
    .metric, .panel, .legend, .detail-drawer {
      background: var(--panel);
      border: 1px solid rgba(248, 243, 229, 0.45);
      border-radius: 24px;
      box-shadow: 0 26px 70px rgba(0, 0, 0, 0.24);
    }
    .metric {
      padding: 15px 16px;
      overflow: hidden;
      position: relative;
    }
    .metric .label {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-weight: 800;
    }
    .metric .value {
      font-size: 31px;
      font-weight: 900;
      margin-top: 5px;
      letter-spacing: -0.05em;
    }
    .panel { padding: 18px; }
    .toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 10px;
      flex-wrap: wrap;
    }
    .toolbar h2 { margin: 0; font-size: 20px; }
    .toolbar .hint { color: var(--muted); font-size: 13px; }
    .legend {
      display: flex;
      flex-wrap: wrap;
      gap: 9px;
      padding: 10px 12px;
      margin-bottom: 12px;
      color: var(--muted);
      font-size: 13px;
    }
    .legend span { display: inline-flex; align-items: center; gap: 7px; }
    .dot { width: 12px; height: 12px; border-radius: 99px; display: inline-block; }
    .stage-labels {
      display: grid;
      grid-template-columns: 28% 38% 28%;
      justify-content: space-between;
      color: #f8f3e5;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.15em;
      font-weight: 900;
      margin: 0 10px 9px;
    }
    .stage-labels span:nth-child(2) { text-align: center; }
    .stage-labels span:nth-child(3) { text-align: right; }
    .canvas-wrap {
      position: relative;
      overflow: auto;
      border-radius: 22px;
      border: 1px solid rgba(20, 32, 28, 0.18);
      background:
        linear-gradient(rgba(20, 32, 28, 0.05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(20, 32, 28, 0.05) 1px, transparent 1px),
        #f8f3e5;
      background-size: 26px 26px;
      min-height: 620px;
    }
    svg { min-width: 1260px; width: 100%; display: block; }
    .node { cursor: pointer; }
    .node rect {
      filter: drop-shadow(0 12px 18px rgba(15, 28, 24, 0.18));
    }
    .node-title { font-weight: 900; font-size: 15px; fill: #17231f; }
    .node-sub { font-size: 11px; fill: #66736d; }
    .socket-text { font-size: 10px; fill: #33413c; }
    .socket-title { font-size: 10px; fill: #17231f; font-weight: 900; }
    .socket-sub { font-size: 9px; fill: #66736d; }
    .socket-box.identity { fill: #dcfce7; stroke: #0f766e; }
    .socket-box.service { fill: #dbeafe; stroke: #1d4ed8; }
    .bridge-band {
      fill: rgba(124, 58, 237, 0.08);
      stroke: rgba(124, 58, 237, 0.16);
      stroke-width: 1;
      pointer-events: none;
    }
    .path-line {
      fill: none;
      stroke-width: 3;
      stroke-linecap: round;
      opacity: 0.62;
      cursor: pointer;
    }
    .path-line.registered { stroke-dasharray: 7 9; opacity: 0.45; }
    .path-line.stale { stroke-dasharray: 3 11; opacity: 0.32; }
    .path-line.materialized { stroke-dasharray: none; opacity: 0.58; }
    .path-line.live {
      stroke-dasharray: 18 12;
      animation: dash 1.1s linear infinite;
      filter: drop-shadow(0 0 8px rgba(34, 197, 94, 0.75));
      opacity: 0.95;
    }
    .path-line.bridge-leg { stroke: #7c3aed; }
    .path-line.device-leg { stroke: #0f766e; }
    .path-line.service-leg { stroke: #1d4ed8; }
    @keyframes dash { to { stroke-dashoffset: -30; } }
    .packet-pulse {
      animation: pulse 1.2s ease-in-out infinite alternate;
    }
    @keyframes pulse { from { opacity: 0.35; } to { opacity: 1; } }
    .detail-drawer {
      margin-top: 14px;
      padding: 16px;
      display: none;
    }
    .detail-drawer.open { display: block; }
    .payload-row.clickable {
      cursor: pointer;
      transition: background 120ms ease;
    }
    .payload-row.clickable:hover {
      background: rgba(29, 78, 216, 0.08);
    }
    .payload-row.clickable td:last-child {
      color: #1d4ed8;
      font-weight: 700;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
      gap: 12px;
    }
    .detail-card {
      border-radius: 18px;
      border: 1px solid #ddd2bb;
      background: #fffdf7;
      padding: 12px;
    }
    .detail-card h3 { margin: 0 0 8px; font-size: 16px; }
    code {
      background: #edf6f2;
      color: #0f513f;
      border-radius: 8px;
      padding: 2px 5px;
      overflow-wrap: anywhere;
      font-size: 12px;
    }
    table { width: 100%; border-collapse: collapse; }
    th, td {
      text-align: left;
      padding: 7px 8px;
      border-bottom: 1px solid #eadfc9;
      vertical-align: top;
      font-size: 13px;
    }
    th {
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
      font-size: 11px;
    }
    .payload-modal {
      position: fixed;
      inset: 0;
      display: none;
      align-items: center;
      justify-content: center;
      background: rgba(15, 28, 24, 0.66);
      backdrop-filter: blur(8px);
      z-index: 20;
      padding: 24px;
    }
    .payload-modal.open { display: flex; }
    .payload-dialog {
      width: min(980px, 100%);
      max-height: min(88vh, 980px);
      overflow: auto;
      background: rgba(248, 243, 229, 0.98);
      border: 1px solid rgba(248, 243, 229, 0.65);
      border-radius: 24px;
      box-shadow: 0 28px 70px rgba(0, 0, 0, 0.36);
      padding: 22px;
    }
    .payload-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 14px;
    }
    .payload-toolbar h3 {
      margin: 0;
      font-size: 24px;
    }
    .payload-close {
      appearance: none;
      border: 0;
      background: #14201c;
      color: #f8f3e5;
      border-radius: 999px;
      padding: 10px 14px;
      font: inherit;
      cursor: pointer;
    }
    .payload-meta {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .payload-card {
      border: 1px solid rgba(20, 32, 28, 0.12);
      border-radius: 18px;
      background: #fffdf8;
      padding: 14px;
    }
    .payload-card h4 {
      margin: 0 0 8px;
      font-size: 15px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--muted);
    }
    .payload-text {
      margin: 0;
      background: #121c19;
      color: #eef9f2;
      border-radius: 18px;
      padding: 16px;
      overflow: auto;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: Consolas, "Courier New", monospace;
      line-height: 1.45;
      min-height: 120px;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 4px 8px;
      background: #e7efe9;
      color: #315144;
      font-size: 11px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }
    .pill.live { background: #dcfce7; color: #166534; }
    .pill.bridge { background: #ede9fe; color: #5b21b6; }
    .pill.replica { background: #fef3c7; color: #92400e; }
    .errors { color: #fca5a5; margin: 10px 4px 0; font-size: 13px; }
    @media (max-width: 900px) {
      main { padding: 0 12px 24px; }
      .stage-labels { grid-template-columns: 1fr; gap: 4px; }
      .stage-labels span { text-align: left !important; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Secure Path Observer</h1>
    <p class="subtitle">
      Registered services and permissions define the intended secure path. Allocator placement materializes canonical IPv6 identities on gateway replicas.
      Bridge and replica collectors animate packet evidence, and recent platform monitor events let us open request and response payloads from the traffic dump when available.
    </p>
    <div id="errors" class="errors"></div>
  </header>
  <main>
    <section id="metrics" class="cards"></section>
    <section class="legend">
      <span><i class="dot" style="background: var(--device)"></i> IoT device identity</span>
      <span><i class="dot" style="background: var(--replica)"></i> OpenSSH gateway replica</span>
      <span><i class="dot" style="background: var(--platform)"></i> IoT platform and published service</span>
      <span><i class="dot" style="background: var(--live)"></i> Replica-observed traffic</span>
      <span><i class="dot" style="background: var(--bridge)"></i> Bridge-observed traffic</span>
    </section>
    <section class="panel">
      <div class="toolbar">
        <h2>[IoT Devices] ----- [Replicas] ----- [Platforms]</h2>
        <div class="hint" id="freshness">Loading...</div>
      </div>
      <div class="stage-labels">
        <span>IoT devices</span>
        <span>Gateway replicas</span>
        <span>Platforms</span>
      </div>
      <div class="canvas-wrap">
        <svg id="graph" viewBox="0 0 1260 700" preserveAspectRatio="xMinYMin meet"></svg>
      </div>
    </section>
    <section id="details" class="detail-drawer"></section>
    <section id="payload-modal" class="payload-modal" aria-hidden="true">
      <div class="payload-dialog">
        <div class="payload-toolbar">
          <h3 id="payload-title">Payload</h3>
          <button id="payload-close" class="payload-close" type="button">Close</button>
        </div>
        <div id="payload-body"></div>
      </div>
    </section>
  </main>
  <script>
    const graph = document.getElementById("graph");
    const metrics = document.getElementById("metrics");
    const details = document.getElementById("details");
    const freshness = document.getElementById("freshness");
    const errors = document.getElementById("errors");
    const payloadModal = document.getElementById("payload-modal");
    const payloadTitle = document.getElementById("payload-title");
    const payloadBody = document.getElementById("payload-body");
    const payloadClose = document.getElementById("payload-close");
    const SPS_PREVIEW_TUPLE_LIMIT = 3;
    let snapshot = null;

    function h(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function compact(value, size = 32) {
      const text = String(value || "");
      return text.length <= size ? text : `${text.slice(0, Math.floor(size / 2))}...${text.slice(-Math.floor(size / 3))}`;
    }

    function statusClass(path) {
      if (path.live) return "live";
      if (path.stale_assignment) return "stale";
      if (path.materialized) return "materialized";
      return "registered";
    }

    function setMetrics(data) {
      const s = data.summary || {};
      metrics.innerHTML = [
        ["Devices", s.device_count],
        ["Replicas", s.replica_count],
        ["Platforms", s.platform_count],
        ["Services", s.service_count],
        ["Registered Paths", s.registered_path_count],
        ["Materialized", s.materialized_path_count],
        ["Stale Assignments", s.stale_assignment_path_count],
        ["Replica Observed", s.replica_observed_path_count],
        ["Bridge Observed", s.bridge_observed_path_count],
      ].map(([label, value]) => `<article class="metric"><div class="label">${h(label)}</div><div class="value">${h(value ?? 0)}</div></article>`).join("");
    }

    function nodeBox(x, y, width, height, kind, title, subtitle, lines, dataType, dataId) {
      const colors = { device: "#e1f4ef", replica: "#fff1d6", platform: "#e8efff" };
      const strokes = { device: "#0f766e", replica: "#a16207", platform: "#1d4ed8" };
      const lineText = (lines || []).slice(0, 5).map((line, index) => (
        `<text x="${x + 18}" y="${y + 72 + index * 17}" class="socket-text">${h(compact(line, 44))}</text>`
      )).join("");
      return `
        <g class="node" data-type="${h(dataType)}" data-id="${h(dataId)}">
          <rect x="${x}" y="${y}" width="${width}" height="${height}" rx="20" fill="${colors[kind]}" stroke="${strokes[kind]}" stroke-width="2"></rect>
          <text x="${x + 18}" y="${y + 30}" class="node-title">${h(compact(title, 34))}</text>
          <text x="${x + 18}" y="${y + 50}" class="node-sub">${h(compact(subtitle, 46))}</text>
          ${lineText}
        </g>
      `;
    }

    function socketGlyph(x, y, width, height, kind, title, subtitle, dataType, dataId) {
      return `
        <g class="node" data-type="${h(dataType)}" data-id="${h(dataId)}">
          <rect class="socket-box ${kind}" x="${x}" y="${y}" width="${width}" height="${height}" rx="10" stroke-width="1.4"></rect>
          <text x="${x + 9}" y="${y + 14}" class="socket-title">${h(compact(title, 25))}</text>
          <text x="${x + 9}" y="${y + 26}" class="socket-sub">${h(compact(subtitle, 30))}</text>
        </g>
      `;
    }

    function scLabel(grant) {
      return `SC: ${grant.publisher_alias || "platform"}:${grant.service_alias || "service"} (${grant.service_port || "-"})`;
    }

    function tupleLabel(socket, grant, communication) {
      return `${socket.device_alias || "device"}:${portText(communication.source_port, "no active flow")} -> ${grant.publisher_alias || "platform"}:${grant.service_alias || "service"} (${grant.service_port || "-"})`;
    }

    function spsRows(socket) {
      const rows = [];
      (socket.grants || []).forEach((grant) => {
        const communications = grant.communications || [];
        rows.push({
          pathId: grant.path_id,
          anchor: true,
          kind: "sc",
          live: communications.length > 0,
          text: scLabel(grant),
        });
        const previewCommunications = communications.slice(0, SPS_PREVIEW_TUPLE_LIMIT);
        if (!previewCommunications.length) {
          rows.push({
            pathId: grant.path_id,
            anchor: false,
            kind: "tuple",
            live: false,
            text: "no active flow",
          });
          return;
        }
        previewCommunications.forEach((communication) => {
          rows.push({
            pathId: grant.path_id,
            anchor: false,
            kind: "tuple",
            live: true,
            text: tupleLabel(socket, grant, communication),
          });
        });
      });
      if (!rows.length) return [{ pathId: socket.path_ids?.[0] || socket.key, anchor: true, live: false, text: "No SC registered" }];
      return rows;
    }

    function spsHeight(socket) {
      return Math.max(50, 28 + spsRows(socket).length * 17);
    }

    function spsGlyph(x, y, width, socket) {
      const rows = spsRows(socket);
      const height = spsHeight(socket);
      const rowText = rows.map((row, index) => (
        `<text x="${x + 10}" y="${y + 34 + index * 17}" class="${row.kind === "sc" ? "socket-title" : "socket-sub"}">${h(compact(row.text, 56))}</text>`
      )).join("");
      return `
        <g class="node" data-type="sps" data-id="${h(socket.key)}">
          <rect class="socket-box identity" x="${x}" y="${y}" width="${width}" height="${height}" rx="12" stroke-width="1.5"></rect>
          <text x="${x + 10}" y="${y + 18}" class="socket-title">${h(compact(socket.sps_label || `SPS ${socket.device_alias || "device"}`, 34))}</text>
          ${rowText}
        </g>
      `;
    }

    function pathSegment(path, from, to, leg) {
      const cls = statusClass(path);
      const color = leg === "bridge" ? "#7c3aed" : (leg === "service" ? "#1d4ed8" : "#0f766e");
      const stroke = path.stale_assignment ? "#a8a29e" : color;
      const dx = Math.max(60, Math.abs(to.x - from.x) * 0.42);
      const d = `M ${from.x} ${from.y} C ${from.x + dx} ${from.y}, ${to.x - dx} ${to.y}, ${to.x} ${to.y}`;
      const pulse = path.live ? `<circle class="packet-pulse" r="6" fill="${stroke}"><animateMotion dur="1.15s" repeatCount="indefinite" path="${d}"></animateMotion></circle>` : "";
      return `
        <path class="path-line ${cls} ${leg}-leg" data-type="path" data-id="${h(path.id)}" d="${d}" stroke="${stroke}"></path>
        ${pulse}
      `;
    }

    function renderGraph(data) {
      const devices = data.devices || [];
      const replicas = data.replicas || [];
      const platforms = data.platforms || [];
      const paths = data.paths || [];
      const deviceRow = 108;
      const platformRow = 142;
      const replicaHeights = replicas.map((replica) => {
        const spsHeightTotal = (replica.identity_sockets || []).reduce((total, socket) => total + spsHeight(socket) + 10, 0);
        const publishedHeightTotal = Math.max((replica.published_sockets || []).length, 1) * 38;
        return Math.max(190, 86 + Math.max(spsHeightTotal, publishedHeightTotal));
      });
      const replicaTotalHeight = 50 + replicaHeights.reduce((total, value) => total + value + 28, 0);
      const height = Math.max(700, 90 + devices.length * deviceRow, 90 + platforms.length * platformRow, replicaTotalHeight);
      graph.setAttribute("viewBox", `0 0 1360 ${height}`);

      const devicePos = new Map();
      const replicaPos = new Map();
      const platformPos = new Map();
      const identityPos = new Map();
      const publishedPos = new Map();
      const shapes = [];
      const lines = [];
      const serviceLegs = new Set();

      devices.forEach((device, index) => {
        const y = 48 + index * deviceRow;
        devicePos.set(device.username, { x: 326, y: y + 48 });
        shapes.push(nodeBox(40, y, 286, 94, "device", device.short_alias || device.display_name || device.alias, device.mac || device.username, [
          device.canonical_ipv6,
          device.accessible_service_count ? `access grants ${device.accessible_service_count}` : "",
          device.published_service_count ? `publishes ${device.published_service_count} service(s)` : "",
        ].filter(Boolean), "device", device.username));
      });

      shapes.push(`<rect class="bridge-band" x="360" y="30" width="630" height="${height - 60}" rx="30"></rect>`);

      let replicaY = 44;
      replicas.forEach((replica, index) => {
        const y = replicaY;
        const boxHeight = replicaHeights[index];
        const label = replica.ready ? "Ready" : replica.phase || "Unknown";
        const x = 380;
        const width = 580;
        const spsWidth = 340;
        replicaPos.set(replica.name, { x: x + width / 2, y: y + boxHeight / 2, leftX: x, rightX: x + width });
        shapes.push(`
          <g class="node" data-type="replica" data-id="${h(replica.name)}">
            <rect x="${x}" y="${y}" width="${width}" height="${boxHeight}" rx="24" fill="#fff1d6" stroke="#a16207" stroke-width="2" filter="drop-shadow(0 12px 18px rgba(15, 28, 24, 0.18))"></rect>
            <text x="${x + 18}" y="${y + 30}" class="node-title">${h(compact(replica.name, 42))}</text>
            <text x="${x + 18}" y="${y + 50}" class="node-sub">${h(label)} - ${h(replica.identity_sockets?.length || 0)} IoT dev SPS - ${h(replica.published_sockets?.length || 0)} RSP</text>
          </g>
        `);
        let identityY = y + 66;
        (replica.identity_sockets || []).forEach((socket) => {
          const sy = identityY;
          const sx = x + 14;
          spsRows(socket).forEach((row, rowIndex) => {
            if (row.anchor) {
              const rowY = sy + 34 + rowIndex * 17 - 5;
              identityPos.set(row.pathId, { input: { x: sx, y: rowY }, output: { x: sx + spsWidth, y: rowY } });
            }
          });
          shapes.push(spsGlyph(sx, sy, spsWidth, socket));
          identityY += spsHeight(socket) + 10;
        });
        (replica.published_sockets || []).forEach((socket, socketIndex) => {
          const sy = y + 66 + socketIndex * 34;
          const sx = x + width - 190 - 14;
          publishedPos.set(socket.key, { input: { x: sx, y: sy + 15 }, output: { x: sx + 190, y: sy + 15 } });
          shapes.push(socketGlyph(sx, sy, 190, 28, "service", `${socket.publisher_alias || "platform"}:${socket.alias || "service"}`, `(${socket.port || "-"})`, "replica", replica.name));
        });
        replicaY += boxHeight + 28;
      });

      platforms.forEach((platform, index) => {
        const y = 48 + index * platformRow;
        platformPos.set(platform.username, { x: 1020, y: y + 58 });
        const services = (data.services || []).filter((svc) => svc.owner_username === platform.username);
        shapes.push(nodeBox(1020, y, 300, 118, "platform", platform.short_alias || platform.display_name || platform.alias, `${services.length} published service(s)`, [
          platform.canonical_ipv6,
          ...services.map((svc) => `${svc.short_alias || svc.alias} [${svc.port}]`),
        ].filter(Boolean), "platform", platform.username));
      });

      paths.forEach((path, index) => {
        const from = devicePos.get(path.device.username) || { x: 326, y: 80 + index * deviceRow };
        const identity = identityPos.get(path.id);
        const published = publishedPos.get(path.service.socket_key);
        const to = platformPos.get(path.platform.username) || { x: 1020, y: 80 + index * platformRow };
        const deviceReplicaFallback = replicaPos.get(path.device.replica || "") || { x: 560, y: from.y };
        const serviceReplicaFallback = replicaPos.get(path.platform.replica || "") || { x: 820, y: to.y };
        const identityIn = identity?.input || { x: deviceReplicaFallback.leftX || 380, y: deviceReplicaFallback.y };
        const identityOut = identity?.output || { x: deviceReplicaFallback.x, y: deviceReplicaFallback.y };
        const publishedIn = published?.input || { x: serviceReplicaFallback.x, y: serviceReplicaFallback.y };
        const publishedOut = published?.output || { x: serviceReplicaFallback.rightX || 850, y: serviceReplicaFallback.y };
        lines.push(pathSegment(path, from, identityIn, "device"));
        lines.push(pathSegment(path, identityOut, publishedIn, path.same_replica ? "device" : "bridge"));
        const serviceKey = path.service.socket_key || `${path.service.canonical_ipv6}:${path.service.port}`;
        if (!serviceLegs.has(serviceKey)) {
          serviceLegs.add(serviceKey);
          lines.push(pathSegment(path, publishedOut, to, "service"));
        }
      });

      if (!paths.length) {
        shapes.push(`<text x="60" y="90" fill="#66736d" font-size="18">No registered secure paths found.</text>`);
      }
      graph.innerHTML = `${lines.join("")}${shapes.join("")}`;
      graph.querySelectorAll("[data-type]").forEach((node) => {
        node.addEventListener("click", () => showDetails(node.dataset.type, node.dataset.id));
      });
    }

    function flowActionText(flow) {
      return flow?.payload_available ? "Click to inspect" : "Not captured";
    }

    function flowTable(flows, pathId) {
      if (!flows || !flows.length) return `<p>No packet flow has been observed for this path in the current window.</p>`;
      return `
        <table>
          <thead><tr><th>Source</th><th>Direction</th><th>Tuple</th><th>Packets</th><th>Last Seen</th><th>Payload</th></tr></thead>
          <tbody>
            ${flows.map((flow, index) => `
              <tr class="payload-row ${flow.payload_available ? "clickable" : ""}" ${flow.payload_available ? `onclick='showPayload(${JSON.stringify(pathId)}, ${index})'` : ""}>
                <td><span class="pill ${flow.source_scope === "bridge" ? "bridge" : "replica"}">${h(flow.source_scope)}</span><br>${h(flow.source_name)}</td>
                <td>${h(flow.direction)}</td>
                <td><code>${h(tupleText(flow.src_address, flow.src_port, "-"))}</code><br><code>${h(tupleText(flow.dst_address, flow.dst_port, "-"))}</code></td>
                <td>${h(flow.packets)} / ${h(flow.bytes)} bytes</td>
                <td>${h(flow.last_seen || "-")}</td>
                <td>${h(flowActionText(flow))}</td>
              </tr>
            `).join("")}
          </tbody>
        </table>
      `;
    }

    function portText(port, fallback = "ephemeral") {
      return port === undefined || port === null || port === "" ? fallback : String(port);
    }

    function tupleText(address, port, fallback = "ephemeral") {
      const host = address || "-";
      const bracketedHost = String(host).includes(":") ? `[${host}]` : host;
      return `${bracketedHost}:${portText(port, fallback)}`;
    }

    function roleText(item) {
      const labels = item.role_labels || [];
      if (labels.length) return labels.join(", ");
      const inferred = [];
      if (item.is_iot_device) inferred.push("IoT device");
      if (item.is_iot_platform) inferred.push("IoT platform");
      return inferred.length ? inferred.join(", ") : "not explicitly flagged";
    }

    function isCanonicalUsername(value) {
      return /^[0-9a-f]{32}$/i.test(String(value || "").trim());
    }

    function meaningfulAlias(item) {
      const username = String(item.username || "").trim();
      for (const key of ["alias", "display_name"]) {
        const value = String(item[key] || "").trim();
        if (value && value !== username && !isCanonicalUsername(value)) return value;
      }
      return "";
    }

    function endpointTitle(item) {
      return item.short_alias || meaningfulAlias(item) || item.username || item.canonical_ipv6 || "Identity";
    }

    function compactCanonicalIpv6(item) {
      return item.canonical_ipv6_compact || item.canonical_ipv6 || item.username || "-";
    }

    function identitySocketLabel(socket) {
      return socket.sps_label || `SPS ${socket.device_alias || "IoT device"}`;
    }

    function communicationLine(socket, grant, communication) {
      return tupleLabel(socket, grant, communication);
    }

    function grantLines(socket, grant) {
      const communications = grant.communications || [];
      const lines = [{ text: scLabel(grant), kind: "sc" }];
      if (!communications.length) {
        lines.push({ text: "no active flow", kind: "tuple" });
        return lines;
      }
      return lines.concat(communications.map((communication) => ({ text: communicationLine(socket, grant, communication), kind: "tuple" })));
    }

    function grantNote(grant) {
      if (grant.source_port) {
        return `Latest observed ephemeral port ${grant.source_port}${grant.source_port_observed_at ? `, last seen ${grant.source_port_observed_at}` : ""}. It may change on the next TCP connection.`;
      }
      return "No ephemeral TCP port observed in the current flow window. The SC is registered, but no current communication is visible.";
    }

    function spsGrantHtml(socket) {
      return (socket.grants || []).map((grant) => `
        <p>
          ${grantLines(socket, grant).map((line) => `<code class="${line.kind === "sc" ? "socket-title" : "socket-sub"}">${h(line.text)}</code>`).join("<br>")}
          <br><span class="node-sub">${h(grantNote(grant))}</span>
        </p>
      `).join("") || "<p>No granted access registered for this SPS.</p>";
    }

    function detailValue(label, value) {
      return `<div class="payload-card"><h4>${h(label)}</h4><div>${h(value || "-")}</div></div>`;
    }

    function closePayloadModal() {
      payloadModal.classList.remove("open");
      payloadModal.setAttribute("aria-hidden", "true");
      payloadBody.innerHTML = "";
    }

    function showPayload(pathId, flowIndex) {
      if (!snapshot) return;
      const path = (snapshot.paths || []).find((row) => String(row.id) === String(pathId));
      if (!path) return;
      const flow = (path.flows || [])[flowIndex];
      if (!flow || !flow.payload_available || !flow.payload) return;
      const payload = snapshot.payloads?.[flow.payload.payload_ref] || flow.payload;
      const requestText = payload.request_text || "(request payload unavailable)";
      const responseText = payload.response_text || "(response payload unavailable)";
      const primaryText = payload.primary_text || (flow.direction === "request" ? requestText : responseText);
      payloadTitle.textContent = `${payload.primary_label || "Payload"} - ${path.device.short_alias || path.device.alias || "device"} -> ${path.platform.short_alias || path.platform.alias || "platform"}`;
      payloadBody.innerHTML = `
        <div class="payload-meta">
          ${detailValue("Direction", flow.direction)}
          ${detailValue("Observed", payload.received_at || flow.last_seen || "-")}
          ${detailValue("Source Pod", payload.source_pod || "-")}
          ${detailValue("Service Path", payload.request_path || "-")}
          ${detailValue("Content Type", payload.content_type || "-")}
          ${detailValue("Tuple", `${tupleText(flow.src_address, flow.src_port, "-")} -> ${tupleText(flow.dst_address, flow.dst_port, "-")}`)}
        </div>
        <div class="detail-grid">
          <div class="payload-card">
            <h4>${h(payload.primary_label || "Payload")}</h4>
            <pre class="payload-text">${h(primaryText)}</pre>
          </div>
          <div class="payload-card">
            <h4>Request Payload</h4>
            <pre class="payload-text">${h(requestText)}</pre>
          </div>
          <div class="payload-card">
            <h4>Response Payload</h4>
            <pre class="payload-text">${h(responseText)}</pre>
          </div>
        </div>
      `;
      payloadModal.classList.add("open");
      payloadModal.setAttribute("aria-hidden", "false");
    }

    function showDetails(type, id) {
      if (!snapshot) return;
      let title = "";
      let body = "";
      if (type === "path") {
        const path = (snapshot.paths || []).find((row) => String(row.id) === String(id));
        if (!path) return;
        title = path.context_alias;
        body = `
          <div class="detail-grid">
            <div class="detail-card"><h3>Consumer Identity</h3><p>${h(path.device.display_name || path.device.alias)}</p><code>${h(path.device.canonical_ipv6)}</code><p>Role ${h(roleText(path.device))}<br>MAC ${h(path.device.mac || "-")}<br>Live replica ${h(path.device.replica || "not materialized")}<br>Allocator row ${h((path.device.assignment || {}).pod_name || "-")}</p></div>
            <div class="detail-card"><h3>Published Service</h3><p>${h(path.service.alias)}</p><code>${h(path.service.canonical_ipv6)}:${h(path.service.port)}</code><p>Publisher ${h(path.platform.display_name || path.platform.alias)}<br>Role ${h(roleText(path.platform))}<br>Live replica ${h(path.platform.replica || "not materialized")}<br>Allocator row ${h((path.platform.assignment || {}).pod_name || "-")}</p></div>
            <div class="detail-card"><h3>State</h3><p><span class="pill ${path.live ? "live" : ""}">${h(path.state)}</span></p><p>${h(path.observed_packet_count)} packets, ${h(path.observed_byte_count)} bytes<br>Last seen ${h(path.last_seen || "no traffic yet")}<br>${path.stale_assignment ? "Allocator has active rows for retired gateway pods." : ""}</p></div>
          </div>
          <div class="detail-card" style="margin-top:12px;"><h3>Traffic Dump</h3>${flowTable(path.flows, path.id)}</div>
        `;
      } else if (type === "sps") {
        const socket = (snapshot.replicas || []).flatMap((replica) => replica.identity_sockets || []).find((row) => row.key === id);
        if (!socket) return;
        title = identitySocketLabel(socket);
        body = `<div class="detail-grid">
          <div class="detail-card"><h3>IoT Device SPS</h3><p>${h(socket.device_alias || "-")}</p><code>${h(socket.source || "-")}</code><p>MAC ${h(socket.mac || "-")}<br>Granted SC rows ${h((socket.grants || []).length)}<br>${socket.live ? "Traffic observed in the selected window." : "No current TCP flow observed."}</p></div>
          <div class="detail-card"><h3>Granted Secure Communications</h3>${spsGrantHtml(socket)}</div>
        </div>`;
      } else if (type === "replica") {
        const replica = (snapshot.replicas || []).find((row) => row.name === id);
        if (!replica) return;
        title = replica.name;
        body = `<div class="detail-grid">
          <div class="detail-card"><h3>Canonical Addresses</h3>${(replica.canonical_addresses || []).map((row) => `<p><code>${h(row.ipv6)}</code><br>MAC ${h(row.mac_dev || "-")}</p>`).join("") || "<p>No active canonical address assignment.</p>"}</div>
          <div class="detail-card"><h3>Published Sockets</h3>${(replica.published_sockets || []).map((socket) => `<p>${h(socket.publisher_alias || "platform")}:${h(socket.alias || "service")} (${h(socket.port || "-")})<br><code>${h(socket.ipv6)}:${h(socket.port)}</code></p>`).join("") || "<p>No published sockets mapped here.</p>"}</div>
          <div class="detail-card"><h3>IoT Device SPS</h3>${(replica.identity_sockets || []).slice(-12).map((socket) => `<p><strong>${h(identitySocketLabel(socket))}</strong><br><code>${h(socket.source || "-")}</code></p>${spsGrantHtml(socket)}`).join("") || "<p>No IoT device SPS mapped here.</p>"}</div>
        </div>`;
      } else {
        const collection = type === "device" ? snapshot.devices : snapshot.platforms;
        const item = (collection || []).find((row) => row.username === id);
        if (!item) return;
        const alias = meaningfulAlias(item);
        title = endpointTitle(item);
        body = `<div class="detail-grid"><div class="detail-card"><h3>Identity</h3><p><span class="label">Canonical IPv6</span><br><code>${h(compactCanonicalIpv6(item))}</code></p><p>Display label ${h(item.short_alias || "-")}<br>Username <code>${h(item.username || "-")}</code>${alias ? `<br>Alias ${h(alias)}` : ""}<br>Role ${h(roleText(item))}<br>MAC ${h(item.mac || "-")}<br>Access grants ${h(item.accessible_service_count || 0)}<br>Published services ${h(item.published_service_count || 0)}</p></div></div>`;
      }
      details.classList.add("open");
      details.innerHTML = `<h2>${h(title)}</h2>${body}`;
      details.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }

    async function load() {
      const response = await fetch("api/snapshot", { cache: "no-store" });
      const data = await response.json();
      snapshot = data;
      setMetrics(data);
      renderGraph(data);
      freshness.textContent = `Updated ${data.generated_at} | window ${data.window_seconds}s`;
      errors.textContent = (data.errors || []).slice(0, 3).map((error) => `${error.source}: ${error.message}`).join(" | ");
    }

    payloadClose.addEventListener("click", closePayloadModal);
    payloadModal.addEventListener("click", (event) => {
      if (event.target === payloadModal) closePayloadModal();
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") closePayloadModal();
    });

    load().catch((error) => { errors.textContent = error.message; });
    setInterval(() => load().catch((error) => { errors.textContent = error.message; }), 2500);
  </script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    server_version = "CMXsafeSecurePathObserver/1.0"

    def log_message(self, format_: str, *args: Any) -> None:
        return

    def send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, status: HTTPStatus, body: str) -> None:
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/healthz":
            self.send_json(HTTPStatus.OK, {"ok": True, "generated_at": now_iso()})
            return
        if parsed.path == "/api/snapshot":
            query = urllib.parse.parse_qs(parsed.query)
            window = int((query.get("window_seconds") or [str(FLOW_WINDOW_SECONDS)])[0])
            self.send_json(HTTPStatus.OK, build_snapshot(window_seconds=max(10, min(600, window))))
            return
        if parsed.path == "/":
            self.send_html(HTTPStatus.OK, INDEX_HTML)
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"secure path observer listening on {HOST}:{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
