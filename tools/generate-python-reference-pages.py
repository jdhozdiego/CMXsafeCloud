#!/usr/bin/env python3
"""Generate Markdown reference pages for the main Python services.

The generated pages are meant to stay useful in two places:

1. In the MkDocs site, where mkdocstrings renders the full API reference.
2. In the GitHub source view, where only the Markdown source is visible.

Because GitHub does not render mkdocstrings directives, each generated page
includes endpoint inventories, key symbols, and a top-level function/class map
before the mkdocstrings block.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_ROOT = REPO_ROOT / "docs" / "reference" / "python"
GITHUB_BLOB_BASE = "https://github.com/cmxsafe/CMXsafeMAC-IPv6/blob/main"


@dataclass(frozen=True)
class ModuleSpec:
    slug: str
    title: str
    mirror_module: str
    source_rel_path: str
    summary: str
    companion_docs: tuple[str, ...]
    key_symbols: tuple[tuple[str, str], ...]
    concern_groups: tuple["ConcernGroup", ...]
    handler_classes: tuple[str, ...] = ("Handler",)


@dataclass(frozen=True)
class ConcernGroup:
    title: str
    description: str
    exact: tuple[str, ...] = ()
    prefixes: tuple[str, ...] = ()
    contains: tuple[str, ...] = ()
    kinds: tuple[str, ...] = ("function", "class")


MODULE_SPECS: tuple[ModuleSpec, ...] = (
    ModuleSpec(
        slug="allocator",
        title="Allocator API Module",
        mirror_module="docs_api.allocator_app",
        source_rel_path="net-identity-allocator/app.py",
        summary=(
            "The allocator is the control-plane service that stores managed MAC "
            "and explicit IPv6 ownership in PostgreSQL, exposes the external "
            "HTTP API, and forwards apply and move work into the node-agent "
            "batching pipeline."
        ),
        companion_docs=(
            "net-identity-allocator.md",
            "explicit-ipv6-apply-move-pipeline.md",
            "explicit-ipv6-parallelism.md",
        ),
        key_symbols=(
            ("dispatch_explicit_ipv6_apply", "Decides whether an explicit request enters the apply or move batching path."),
            ("explicit_apply_batcher", "Returns the per-node apply batcher used to collapse bursts of explicit apply work."),
            ("explicit_move_batcher", "Returns the per-node move batcher used to collapse bursts of explicit move work."),
            ("apply_explicit_ipv6_batch_on_node", "Builds and sends allocator-side bulk-apply requests to the node agent."),
            ("apply_explicit_ipv6_move_batch_on_node", "Builds and sends allocator-side bulk-move requests to the node agent."),
            ("run_explicit_ipv6_apply_batch_task", "Worker entry point for asynchronous explicit apply batches."),
            ("run_explicit_ipv6_move_batch_task", "Worker entry point for asynchronous explicit move batches."),
            ("Store", "Owns PostgreSQL access and the persistent managed-MAC / explicit-IPv6 records."),
            ("Handler", "Implements the allocator HTTP endpoints."),
            ("main", "Starts the allocator HTTP service."),
        ),
        concern_groups=(
            ConcernGroup(
                title="Trace, Timing, And SQL Helpers",
                description="Small helpers used across the allocator for timestamps, trace extraction, and SQL preparation.",
                exact=("now_utc", "log", "elapsed_ms", "effective_move_batch_max_items", "log_explicit_trace", "current_epoch_ms", "normalize_optional_trace_int", "extract_trace_context", "trace_log_fields", "qmark_to_postgres_sql", "split_sql_statements"),
            ),
            ConcernGroup(
                title="Identity Derivation And Normalization",
                description="Canonical helpers that normalize MAC and IPv6 fields and derive the deterministic explicit identities used by the rest of the system.",
                exact=("normalize_mac", "normalize_tag_hex", "normalize_ipv6_address", "derive_device_byte", "format_mac", "normalize_ipv6_prefix", "format_ipv6", "build_explicit_ipv6", "build_auto_managed_explicit_ipv6", "parse_embedded_ipv6"),
            ),
            ConcernGroup(
                title="Kubernetes And Node-Agent Coordination",
                description="Boundary helpers that call Kubernetes or node-local services and convert stored assignment rows into runtime ownership details.",
                exact=("json_request", "kube_request", "find_node_agent", "assignment_node_agent", "assignment_owner_details", "explicit_runtime_cleanup_entry", "owners_match", "normalize_optional_int", "normalize_runtime_snapshot"),
            ),
            ConcernGroup(
                title="Allocator-Side Apply And Move Batching",
                description="The queue and batcher classes that collapse bursts of explicit IPv6 work before forwarding it to the node agent.",
                exact=("ExplicitApplyQueueItem", "ExplicitApplyPodBatcher", "ExplicitMoveBatcher", "assignment_job_token", "register_explicit_apply_job", "is_latest_explicit_apply_job", "explicit_apply_batch_key", "explicit_apply_batcher", "explicit_move_batch_key", "explicit_move_batcher"),
            ),
            ConcernGroup(
                title="Node-Agent Dispatch And Async Workers",
                description="Functions that build the payloads, call the node agent, and run the asynchronous apply and move workers behind the public API.",
                exact=("explicit_apply_payload", "explicit_applied_payload", "apply_explicit_ipv6_on_node", "apply_explicit_ipv6_batch_on_node", "apply_explicit_ipv6_move_batch_on_node", "dispatch_explicit_ipv6_apply", "clear_explicit_ipv6_runtime", "run_explicit_ipv6_apply_task", "run_explicit_ipv6_apply_batch_task", "run_explicit_ipv6_move_batch_task"),
            ),
            ConcernGroup(
                title="Persistence And HTTP Surface",
                description="The PostgreSQL store and HTTP server layer that expose the allocator API to the rest of the stack.",
                exact=("ResultWrapper", "ConnectionWrapper", "Store", "Handler", "AllocatorHTTPServer", "main"),
            ),
        ),
    ),
    ModuleSpec(
        slug="node-agent",
        title="Node Agent Module",
        mirror_module="docs_api.node_agent",
        source_rel_path="CMXsafeMAC-IPv6-node-agent/agent.py",
        summary=(
            "The node agent lives on each Kubernetes node, watches managed pods, "
            "applies MAC and IPv6 state inside pod network namespaces, and turns "
            "allocator bulk requests into per-pod batched netlink operations."
        ),
        companion_docs=(
            "CMXsafeMAC-IPv6-node-agent.md",
            "explicit-ipv6-apply-move-pipeline.md",
            "explicit-ipv6-parallelism.md",
        ),
        key_symbols=(
            ("ExplicitPodCommandBatcher", "Collects short bursts of pod-local explicit operations before one batched netlink execution."),
            ("submit_explicit_pod_commands", "Queues explicit pod commands and waits for their batched completion."),
            ("apply_explicit_netlink_batch", "Executes grouped addr-add, addr-del, and neighbor flush operations through pyroute2."),
            ("apply_explicit_ipv6_request", "Handles the single explicit-IPv6 apply path for one target pod."),
            ("apply_explicit_ipv6_requests_bulk", "Handles allocator bulk-apply requests."),
            ("apply_explicit_ipv6_move_requests_bulk", "Regroups move entries by old owner, new owner, and observers before execution."),
            ("manage_pod", "Applies the managed MAC and managed IPv6 state for a pod."),
            ("tetragon_event_loop", "Consumes Tetragon runtime events and turns them into manage and release work."),
            ("AgentHandler", "Implements the node-agent HTTP endpoints."),
            ("serve_http", "Starts the node-agent HTTP server."),
        ),
        concern_groups=(
            ConcernGroup(
                title="Trace And Identity Helpers",
                description="Cross-cutting helpers for trace timestamps, IPv6 normalization, and canonical explicit identity construction.",
                exact=("log", "elapsed_ms", "log_explicit_trace", "current_epoch_ms", "normalize_optional_trace_int", "extract_trace_context", "trace_log_fields", "normalize_ipv6_address", "normalize_tag_hex", "build_explicit_ipv6", "auto_managed_explicit_ipv6"),
            ),
            ConcernGroup(
                title="Per-Pod Explicit Command Batching",
                description="The queue objects and submission helpers that batch short bursts of explicit pod work before one netlink execution.",
                exact=("explicit_state_lock", "ExplicitPodBatchItem", "ExplicitPodCommandBatcher", "explicit_pod_shard", "explicit_pod_batcher", "wait_batched_explicit_commands", "explicit_state_locks", "submit_explicit_pod_commands", "queue_explicit_pod_commands", "chunked_entries"),
            ),
            ConcernGroup(
                title="Allocator And Kubernetes Coordination",
                description="API calls to the allocator and Kubernetes, plus the applied-callback path that closes the loop after explicit work succeeds.",
                exact=("json_request", "kube_request", "get_pod", "get_pod_if_current", "host_gateway_mac", "matches_selector", "label_selector", "field_selector", "list_target_pods", "patch_pod_annotations", "runtime_snapshot_from_network_info", "explicit_identity_details", "allocator_ensure", "allocator_release", "allocator_reconcile", "allocator_list_explicit_ipv6", "allocator_list_all_explicit_ipv6", "allocator_list_allocations", "allocator_get_explicit_ipv6", "active_explicit_prefixes", "explicit_applied_payload", "allocator_mark_explicit_ipv6_applied", "allocator_mark_explicit_ipv6_applied_batch", "AppliedCallbackQueueItem", "ensure_applied_callback_workers", "queue_explicit_ipv6_applied", "requeue_applied_callback_items", "run_applied_callback_worker", "replace_known_explicit_prefixes", "refresh_known_explicit_prefixes_from_allocator", "seed_known_explicit_prefixes_from_allocator", "remember_known_explicit_prefix"),
            ),
            ConcernGroup(
                title="Runtime Discovery And Registry State",
                description="Helpers that discover pod runtimes, cache network-namespace details, and keep the managed-pod registry in sync with reality.",
                exact=("pid_start_time", "netns_inode", "runtime_cache_key", "invalidate_pod_runtime_cache", "cache_runtime_snapshot", "cached_runtime_snapshot", "cached_pod_network_info", "owner_pod_details", "managed_registry_entry", "managed_registry_entries", "managed_runtime_identities", "register_managed_pod", "retain_managed_registry_uids", "inspect_runtime_by_sandbox_id", "crictl_json", "sandbox_id_for_pod", "pod_network_info", "resolve_runtime_for_identity", "can_enter_pid", "interface_exists", "read_int", "container_peer_ifindex", "host_interface_name_by_index", "host_peer_for_container_iface", "host_link_local", "ensure_host_ipv6_forwarding"),
            ),
            ConcernGroup(
                title="Netlink And Network Mutation",
                description="The low-level functions that actually set MACs, add and remove IPv6s, synchronize routes, and flush stale neighbor state.",
                exact=("set_mac", "current_global_ipv6s", "netlink_ignore_missing", "apply_explicit_netlink_batch", "current_managed_ipv6s", "apply_managed_ipv6_address", "ensure_onlink_route", "apply_explicit_ipv6_address", "remove_explicit_ipv6_address", "delete_explicit_route", "flush_all_explicit_neighbors", "set_ipv6", "explicit_route_network", "sync_explicit_prefix_routes_for_pod", "sync_explicit_prefix_route_to_all_pods", "clear_explicit_prefix_route_from_all_pods", "sync_explicit_prefix_routes_all_pods", "set_explicit_ipv6", "evict_explicit_ipv6_from_previous_owner", "evict_explicit_ipv6_from_other_pods", "flush_explicit_ipv6_neighbors"),
            ),
            ConcernGroup(
                title="Explicit Request Pipeline",
                description="The entry points that take one explicit request or a bulk move batch and turn them into grouped pod-local mutations.",
                exact=("begin_inflight", "end_inflight", "desired_state_matches", "annotations_show_assignment", "apply_tracked_explicit_ipv6s", "apply_explicit_ipv6_request", "apply_explicit_ipv6_move_requests_bulk", "apply_explicit_ipv6_requests_bulk", "clear_explicit_ipv6_runtime"),
            ),
            ConcernGroup(
                title="Pod Management And Tetragon Event Flow",
                description="The higher-level reconcile and runtime-event logic that keeps managed pods aligned with the desired allocator state.",
                exact=("manage_pod", "reconcile_existing_pods", "safety_reconcile_loop", "wrapper_value", "parse_process_event", "parse_exec_event", "parse_exit_event", "pod_stub_from_event", "sandbox_record_by_id", "runtime_delete_details", "begin_manage_retry", "end_manage_retry", "manage_retry_worker", "schedule_manage_retry", "begin_release_check", "end_release_check", "replace_known_managed_uids", "mark_uid_managed", "mark_uid_unmanaged", "is_known_managed_uid", "release_check_worker", "schedule_release_check", "grpc_error_text", "make_tetragon_request", "details_should_manage", "details_should_release", "parse_runtime_delete_exec", "handle_tetragon_response", "tetragon_event_loop"),
            ),
            ConcernGroup(
                title="HTTP Surface",
                description="The HTTP handler that exposes health and explicit apply, clear, bulk-apply, and bulk-move endpoints.",
                exact=("AgentHandler", "serve_http"),
            ),
        ),
        handler_classes=("AgentHandler",),
    ),
    ModuleSpec(
        slug="ssh-dashboard",
        title="SSH Dashboard Module",
        mirror_module="docs_api.ssh_dashboard_app",
        source_rel_path="CMXsafeMAC-IPv6-ssh-dashboard/app.py",
        summary=(
            "The SSH dashboard stores the desired SSH target, user, group, key, "
            "and policy state in PostgreSQL, renders passwd/group/authorized_keys "
            "artifacts onto mounted storage, and provides the browser UI and "
            "reconcile worker in one service."
        ),
        companion_docs=(
            "portable-openssh-dashboard.md",
            "portable-openssh-canonical-routing.md",
            "busybox-portable-openssh.md",
        ),
        key_symbols=(
            ("generate_ed25519_keypair", "Creates default keypairs for newly created dashboard users."),
            ("storage_view", "Builds the browseable PVC file view shown in the dashboard."),
            ("render_authorized_key_line", "Renders one authorized_keys line from a key and policy row."),
            ("render_sshd_config", "Builds sshd_config from dashboard-managed server settings."),
            ("write_atomic", "Ensures reconciled files are updated atomically on mounted storage."),
            ("Reconciler", "Applies the desired dashboard state onto PVCs and SSH configuration sources."),
            ("Worker", "Background thread that drains reconcile jobs."),
            ("Handler", "Implements the dashboard HTTP UI and API endpoints."),
            ("main", "Starts the dashboard web service and worker threads."),
        ),
        concern_groups=(
            ConcernGroup(
                title="Input, Sanitization, And Display Helpers",
                description="Small helpers that normalize form input, sanitize passwd and group content, and format values for the UI.",
                exact=("now_iso", "sha256_text", "normalize_json_list", "parse_json_list", "to_int_or_none", "int_from_query", "bool_from_form", "required", "escape_authorized_value", "sanitize_passwd_field", "sanitize_group_field", "dashboard_path", "safe_redirect_path", "format_size", "format_mode", "is_probably_text"),
            ),
            ConcernGroup(
                title="Key Generation And File Rendering",
                description="Functions that generate default keypairs and render the concrete SSH files written onto mounted storage.",
                exact=("generate_ed25519_keypair", "storage_view", "render_authorized_key_line", "render_sshd_config", "write_atomic"),
            ),
            ConcernGroup(
                title="Persistence And Kubernetes Helpers",
                description="The storage and cluster integration layer that the reconcile worker uses to apply desired state.",
                exact=("KubernetesHelper", "Store"),
            ),
            ConcernGroup(
                title="Reconcile Pipeline",
                description="The renderer and worker thread that take desired state from PostgreSQL and turn it into PVC and SSH configuration artifacts.",
                exact=("Reconciler", "Worker"),
            ),
            ConcernGroup(
                title="HTTP And HTML Surface",
                description="The browser UI, API entry points, and server startup path.",
                exact=("html_page", "render_dashboard_html", "Handler", "main"),
            ),
        ),
    ),
    ModuleSpec(
        slug="traffic-collector",
        title="Traffic Collector Module",
        mirror_module="docs_api.traffic_collector",
        source_rel_path="CMXsafeMAC-IPv6-traffic-collector/collector.py",
        summary=(
            "The traffic collector runs a packet capture loop, parses tshark output "
            "into structured flow samples, and exposes a small health endpoint for "
            "the surrounding stack."
        ),
        companion_docs=(
            "deployment-and-samples.md",
            "system-overview.md",
        ),
        key_symbols=(
            ("protocol_from_next_header", "Normalizes tshark transport fields into the collector's protocol label."),
            ("parse_tshark_line", "Turns one tshark line into a structured flow sample."),
            ("tshark_command", "Builds the tshark invocation used by the collector."),
            ("capture_loop", "Owns the long-running tshark capture and ingestion loop."),
            ("Handler", "Implements the collector health endpoint."),
            ("main", "Starts the capture worker and HTTP service."),
        ),
        concern_groups=(
            ConcernGroup(
                title="Parsing And Normalization Helpers",
                description="Helpers that normalize tshark fields and turn raw lines into structured flow data.",
                exact=("utc_now", "lower_or_none", "safe_int", "safe_float", "protocol_from_next_header", "parse_tshark_line"),
            ),
            ConcernGroup(
                title="Capture Pipeline",
                description="The state object and capture loop that drive tshark and accumulate collector state.",
                exact=("CollectorState", "tshark_command", "capture_loop"),
            ),
            ConcernGroup(
                title="HTTP Surface",
                description="The small health and startup layer used to expose the collector to the rest of the stack.",
                exact=("Handler", "main"),
            ),
        ),
    ),
)


def github_link(source_rel_path: str, line: int | None = None) -> str:
    base = f"{GITHUB_BLOB_BASE}/{source_rel_path}"
    if line:
        return f"{base}#L{line}"
    return base


def is_path_compare(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "path"
        and isinstance(node.value, ast.Name)
        and node.value.id in {"self", "parsed"}
    )


def extract_compare_paths(compare: ast.Compare) -> list[str]:
    paths: list[str] = []
    if not is_path_compare(compare.left):
        return paths
    for op, comparator in zip(compare.ops, compare.comparators):
        if isinstance(op, ast.Eq) and isinstance(comparator, ast.Constant) and isinstance(comparator.value, str):
            paths.append(comparator.value)
        elif isinstance(op, ast.In) and isinstance(comparator, (ast.Tuple, ast.List, ast.Set)):
            for item in comparator.elts:
                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                    paths.append(item.value)
    return paths


def format_expr(node: ast.AST | None) -> str:
    if node is None:
        return ""
    if isinstance(node, ast.Constant):
        return repr(node.value)
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return f"{format_expr(node.value)}.{node.attr}".strip(".")
    if isinstance(node, ast.List):
        return "[" + ", ".join(format_expr(item) for item in node.elts) + "]"
    if isinstance(node, ast.Tuple):
        return "(" + ", ".join(format_expr(item) for item in node.elts) + ")"
    if isinstance(node, ast.Dict):
        pairs = []
        for key, value in zip(node.keys, node.values):
            pairs.append(f"{format_expr(key)}: {format_expr(value)}")
        return "{" + ", ".join(pairs) + "}"
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        return "-" + format_expr(node.operand)
    return "..."


def format_signature(args: ast.arguments) -> str:
    parts: list[str] = []
    positional = list(args.posonlyargs) + list(args.args)
    positional_defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)

    for index, (arg, default) in enumerate(zip(positional, positional_defaults)):
        if args.posonlyargs and index == len(args.posonlyargs):
            parts.append("/")
        item = arg.arg
        if default is not None:
            item += f"={format_expr(default)}"
        parts.append(item)

    if args.posonlyargs and len(positional) == len(args.posonlyargs):
        parts.append("/")

    if args.vararg:
        parts.append(f"*{args.vararg.arg}")
    elif args.kwonlyargs:
        parts.append("*")

    for arg, default in zip(args.kwonlyargs, args.kw_defaults):
        item = arg.arg
        if default is not None:
            item += f"={format_expr(default)}"
        parts.append(item)

    if args.kwarg:
        parts.append(f"**{args.kwarg.arg}")

    return "(" + ", ".join(parts) + ")"


def collect_top_level_members(module: ast.Module) -> tuple[list[dict], list[dict], dict[str, ast.ClassDef]]:
    functions: list[dict] = []
    classes: list[dict] = []
    class_lookup: dict[str, ast.ClassDef] = {}
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.append(
                {
                    "name": node.name,
                    "signature": format_signature(node.args),
                    "line": node.lineno,
                }
            )
        elif isinstance(node, ast.ClassDef):
            bases = []
            for base in node.bases:
                bases.append(format_expr(base))
            classes.append(
                {
                    "name": node.name,
                    "line": node.lineno,
                    "bases": ", ".join(base for base in bases if base) or "-",
                }
            )
            class_lookup[node.name] = node
    return functions, classes, class_lookup


def collect_endpoints(class_node: ast.ClassDef) -> list[dict]:
    endpoints: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    for node in class_node.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not node.name.startswith("do_"):
            continue
        method = node.name[3:]
        for child in ast.walk(node):
            if isinstance(child, ast.Compare):
                for path in extract_compare_paths(child):
                    key = (method, path, node.name)
                    if key not in seen:
                        seen.add(key)
                        endpoints.append(
                            {
                                "method": method,
                                "path": path,
                                "handler": node.name,
                                "line": child.lineno,
                            }
                        )
    endpoints.sort(key=lambda item: (item["path"], item["method"]))
    return endpoints


def render_table(headers: Iterable[str], rows: Iterable[Iterable[str]]) -> str:
    headers = list(headers)
    rows = [list(row) for row in rows]
    if not rows:
        return "_None found._"
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return "\n".join(lines)


def render_key_symbols(spec: ModuleSpec, functions: list[dict], classes: list[dict]) -> str:
    line_lookup = {item["name"]: item["line"] for item in functions}
    line_lookup.update({item["name"]: item["line"] for item in classes})
    kind_lookup = {item["name"]: "function" for item in functions}
    kind_lookup.update({item["name"]: "class" for item in classes})
    signature_lookup = {item["name"]: item["signature"] for item in functions}

    rows: list[list[str]] = []
    for name, why in spec.key_symbols:
        line = line_lookup.get(name)
        kind = kind_lookup.get(name, "symbol")
        link_target = github_link(spec.source_rel_path, line)
        label = f"[`{name}`]({link_target})"
        if kind == "function" and name in signature_lookup:
            label += f" `{signature_lookup[name]}`"
        rows.append(
            [
                label,
                kind,
                str(line or "-"),
                why,
            ]
        )
    return render_table(("Symbol", "Kind", "Line", "Why It Matters"), rows)


def render_function_inventory(spec: ModuleSpec, functions: list[dict]) -> str:
    rows = [
        [
            f"[`{item['name']}`]({github_link(spec.source_rel_path, item['line'])})",
            f"`{item['signature']}`",
            str(item["line"]),
        ]
        for item in functions
    ]
    return render_table(("Function", "Signature", "Line"), rows)


def render_class_inventory(spec: ModuleSpec, classes: list[dict]) -> str:
    rows = [
        [
            f"[`{item['name']}`]({github_link(spec.source_rel_path, item['line'])})",
            item["bases"],
            str(item["line"]),
        ]
        for item in classes
    ]
    return render_table(("Class", "Base Classes", "Line"), rows)


def render_endpoint_inventory(spec: ModuleSpec, endpoints: list[dict]) -> str:
    rows = [
        [
            item["method"],
            f"`{item['path']}`",
            f"[`{item['handler']}`]({github_link(spec.source_rel_path, item['line'])})",
            str(item["line"]),
        ]
        for item in endpoints
    ]
    return render_table(("Method", "Path", "Handler Method", "Line"), rows)


def render_companion_docs(companion_docs: tuple[str, ...]) -> str:
    return "\n".join(f"- [{doc}](../../{doc})" for doc in companion_docs)


def symbol_table_row(spec: ModuleSpec, item: dict) -> list[str]:
    symbol = f"[`{item['name']}`]({github_link(spec.source_rel_path, item['line'])})"
    detail = f"`{item['signature']}`" if item["kind"] == "function" else item.get("bases", "-")
    return [symbol, item["kind"], detail, str(item["line"])]


def matches_concern_group(item: dict, group: ConcernGroup) -> bool:
    if item["kind"] not in group.kinds:
        return False
    name = item["name"]
    if name in group.exact:
        return True
    if any(name.startswith(prefix) for prefix in group.prefixes):
        return True
    if any(fragment in name for fragment in group.contains):
        return True
    return False


def render_concern_groups(spec: ModuleSpec, functions: list[dict], classes: list[dict]) -> str:
    symbols = [
        {"kind": "class", **item}
        for item in classes
    ] + [
        {"kind": "function", **item}
        for item in functions
    ]
    matched_names: set[str] = set()
    sections: list[str] = [
        "## Mental Model By Concern",
        "",
        "These groups are ordered to make the module easier to learn from left to right: input and helpers first, state and batching next, then the outer HTTP or runtime surface.",
        "",
    ]
    for group in spec.concern_groups:
        members = [item for item in symbols if matches_concern_group(item, group)]
        if not members:
            continue
        members.sort(key=lambda item: item["line"])
        matched_names.update(item["name"] for item in members)
        sections.extend(
            [
                f"### {group.title}",
                "",
                group.description,
                "",
                render_table(
                    ("Symbol", "Kind", "Signature / Base", "Line"),
                    [symbol_table_row(spec, item) for item in members],
                ),
                "",
            ]
        )

    remaining = [item for item in symbols if item["name"] not in matched_names]
    if remaining:
        remaining.sort(key=lambda item: item["line"])
        sections.extend(
            [
                "### Remaining Helpers",
                "",
                "These symbols were not assigned to one of the main concerns above, but they are still part of the module's public top-level structure.",
                "",
                render_table(
                    ("Symbol", "Kind", "Signature / Base", "Line"),
                    [symbol_table_row(spec, item) for item in remaining],
                ),
                "",
            ]
        )
    return "\n".join(sections)


def build_page(spec: ModuleSpec) -> str:
    source_path = REPO_ROOT / spec.source_rel_path
    module = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    functions, classes, class_lookup = collect_top_level_members(module)

    endpoints: list[dict] = []
    for handler_name in spec.handler_classes:
        class_node = class_lookup.get(handler_name)
        if class_node:
            endpoints.extend(collect_endpoints(class_node))
    endpoints.sort(key=lambda item: (item["path"], item["method"], item["handler"]))

    parts: list[str] = [
        f"# {spec.title}",
        "",
        "<!-- Generated by tools/generate-python-reference-pages.py -->",
        "",
        spec.summary,
        "",
        "## Source Module",
        "",
        f"- Source file: [`{spec.source_rel_path}`]({github_link(spec.source_rel_path)})",
        f"- Mirrored module for MkDocs: `{spec.mirror_module}`",
        "Companion deep-dive docs:",
        "",
        render_companion_docs(spec.companion_docs),
        "",
        "## HTTP Endpoints",
        "",
        "These are the route checks implemented by the module's HTTP handler class.",
        "",
        render_endpoint_inventory(spec, endpoints),
        "",
        "## Key Architecture Entry Points",
        "",
        render_key_symbols(spec, functions, classes),
        "",
        render_concern_groups(spec, functions, classes),
        "",
        "## Top-Level Classes",
        "",
        render_class_inventory(spec, classes),
        "",
        "## Top-Level Functions",
        "",
        f"The module defines **{len(functions)}** top-level functions.",
        "",
        "<details>",
        f"<summary>Show the full function inventory for {spec.title}</summary>",
        "",
        render_function_inventory(spec, functions),
        "",
        "</details>",
        "",
        "## Rendered API Reference",
        "",
        "The section below is rendered by `mkdocstrings` in the built docs site.",
        "",
        f"::: {spec.mirror_module}",
        "",
    ]
    return "\n".join(parts)


def main() -> None:
    DOCS_ROOT.mkdir(parents=True, exist_ok=True)
    for spec in MODULE_SPECS:
        output_path = DOCS_ROOT / f"{spec.slug}.md"
        output_path.write_text(build_page(spec), encoding="utf-8")
        print(f"Generated {output_path}")


if __name__ == "__main__":
    main()
