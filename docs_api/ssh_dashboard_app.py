# Generated mirror of CMXsafeMAC-IPv6-ssh-dashboard/app.py for MkDocs reference.
import hashlib
import html
import io
import ipaddress
import json
import os
import posixpath
import cgi
import re
import tarfile
import threading
import time
import traceback
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

try:
    from kubernetes import client as k8s_client
    from kubernetes import config as k8s_config
    from kubernetes.stream import stream as k8s_stream
except Exception:
    k8s_client = None
    k8s_config = None
    k8s_stream = None


HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "8084"))
RECONCILE_POLL_INTERVAL = float(os.environ.get("RECONCILE_POLL_INTERVAL", "2"))
DB_DSN = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_DSN")
HEX32_RE = re.compile(r"^[0-9a-fA-F]{32}$")
BUNDLE_HELPER_ROOT = Path(os.environ.get("SSH_DASHBOARD_BUNDLE_HELPER_ROOT", "/app/bundle_assets/endpoint-helper"))

if not DB_DSN:
    db_host = os.environ.get("DB_HOST", "net-identity-allocator-postgres.mac-allocator.svc.cluster.local")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "mac_allocator")
    db_user = os.environ.get("DB_USER", "mac_allocator")
    db_password = os.environ.get("DB_PASSWORD", "")
    DB_DSN = f"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_password}"


SSH_SCHEMA_SQL = [
    "CREATE SCHEMA IF NOT EXISTS ssh_admin",
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.targets (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        namespace TEXT NOT NULL,
        workload_kind TEXT NOT NULL DEFAULT 'Deployment',
        workload_name TEXT NOT NULL,
        workload_selector TEXT NOT NULL,
        account_root_path TEXT NOT NULL,
        home_root_path TEXT NOT NULL,
        runtime_root_path TEXT,
        ssh_configmap_name TEXT,
        ssh_configmap_key TEXT NOT NULL DEFAULT 'sshd_config',
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "ALTER TABLE ssh_admin.targets ADD COLUMN IF NOT EXISTS runtime_root_path TEXT",
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.policy_profiles (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        description TEXT,
        force_command TEXT,
        allow_port_forwarding BOOLEAN NOT NULL DEFAULT TRUE,
        allow_pty BOOLEAN NOT NULL DEFAULT FALSE,
        allow_agent_forwarding BOOLEAN NOT NULL DEFAULT FALSE,
        allow_x11_forwarding BOOLEAN NOT NULL DEFAULT FALSE,
        permit_open_json TEXT NOT NULL DEFAULT '[]',
        permit_listen_json TEXT NOT NULL DEFAULT '[]',
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(target_id, name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.groups (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        gid INTEGER NOT NULL,
        comment TEXT,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(target_id, name),
        UNIQUE(target_id, gid)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.users (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        username TEXT NOT NULL,
        alias TEXT,
        uid INTEGER NOT NULL,
        gid INTEGER NOT NULL,
        home_dir TEXT NOT NULL,
        shell TEXT NOT NULL,
        comment TEXT,
        is_iot_device BOOLEAN NOT NULL DEFAULT FALSE,
        is_iot_platform BOOLEAN NOT NULL DEFAULT FALSE,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        default_policy_profile_id BIGINT REFERENCES ssh_admin.policy_profiles(id) ON DELETE SET NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(target_id, username),
        UNIQUE(target_id, uid)
    )
    """,
    "ALTER TABLE ssh_admin.users ADD COLUMN IF NOT EXISTS alias TEXT",
    "ALTER TABLE ssh_admin.users ADD COLUMN IF NOT EXISTS is_iot_device BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE ssh_admin.users ADD COLUMN IF NOT EXISTS is_iot_platform BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE ssh_admin.users DROP CONSTRAINT IF EXISTS users_target_id_gid_key",
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.public_keys (
        id BIGSERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES ssh_admin.users(id) ON DELETE CASCADE,
        label TEXT,
        public_key TEXT NOT NULL,
        private_key TEXT,
        policy_profile_id BIGINT REFERENCES ssh_admin.policy_profiles(id) ON DELETE SET NULL,
        generated BOOLEAN NOT NULL DEFAULT FALSE,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "ALTER TABLE ssh_admin.public_keys ADD COLUMN IF NOT EXISTS private_key TEXT",
    "ALTER TABLE ssh_admin.public_keys ADD COLUMN IF NOT EXISTS generated BOOLEAN NOT NULL DEFAULT FALSE",
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.published_services (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        owner_user_id BIGINT NOT NULL REFERENCES ssh_admin.users(id) ON DELETE CASCADE,
        alias TEXT NOT NULL,
        protocol TEXT NOT NULL DEFAULT 'http',
        canonical_ipv6 TEXT NOT NULL,
        port INTEGER NOT NULL CHECK (port > 0 AND port < 65536),
        description TEXT,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(target_id, alias)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS published_services_target_owner_endpoint_idx
    ON ssh_admin.published_services (target_id, owner_user_id, canonical_ipv6, port)
    """,
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.service_access_grants (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        service_id BIGINT NOT NULL REFERENCES ssh_admin.published_services(id) ON DELETE CASCADE,
        grantee_user_id BIGINT NOT NULL REFERENCES ssh_admin.users(id) ON DELETE CASCADE,
        context_alias TEXT,
        description TEXT,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(service_id, grantee_user_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.server_settings (
        target_id BIGINT PRIMARY KEY REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        canonical_gateway_mac TEXT,
        listen_port INTEGER NOT NULL DEFAULT 2222,
        allow_tcp_forwarding BOOLEAN NOT NULL DEFAULT TRUE,
        gateway_ports BOOLEAN NOT NULL DEFAULT FALSE,
        permit_tunnel BOOLEAN NOT NULL DEFAULT FALSE,
        x11_forwarding BOOLEAN NOT NULL DEFAULT FALSE,
        log_level TEXT NOT NULL DEFAULT 'VERBOSE',
        updated_at TEXT NOT NULL
    )
    """,
    "ALTER TABLE ssh_admin.server_settings ADD COLUMN IF NOT EXISTS canonical_gateway_mac TEXT",
    """
    CREATE TABLE IF NOT EXISTS cmxsafe_system_settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.render_revisions (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        revision INTEGER NOT NULL,
        passwd_sha256 TEXT,
        group_sha256 TEXT,
        authorized_keys_sha256 TEXT,
        sshd_config_sha256 TEXT,
        status TEXT NOT NULL,
        needs_reload BOOLEAN NOT NULL DEFAULT FALSE,
        needs_restart BOOLEAN NOT NULL DEFAULT FALSE,
        details_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        applied_at TEXT,
        UNIQUE(target_id, revision)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS ssh_admin.reconcile_runs (
        id BIGSERIAL PRIMARY KEY,
        target_id BIGINT NOT NULL REFERENCES ssh_admin.targets(id) ON DELETE CASCADE,
        requested_action TEXT NOT NULL CHECK (requested_action IN ('INITIALIZE', 'RENDER_ONLY', 'RENDER_AND_RELOAD', 'RENDER_AND_RESTART')),
        requested_by TEXT,
        status TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED')) DEFAULT 'QUEUED',
        render_revision_id BIGINT REFERENCES ssh_admin.render_revisions(id) ON DELETE SET NULL,
        details_json TEXT NOT NULL DEFAULT '{}',
        error_text TEXT,
        created_at TEXT NOT NULL,
        started_at TEXT,
        finished_at TEXT
    )
    """,
]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def sha256_text(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_json_list(raw_value):
    if raw_value is None:
        return "[]"
    if isinstance(raw_value, str):
        stripped = raw_value.strip()
        if not stripped:
            return "[]"
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            parsed = [item.strip() for item in stripped.splitlines() if item.strip()]
        return json.dumps(list(parsed), sort_keys=True)
    if isinstance(raw_value, (list, tuple)):
        return json.dumps(list(raw_value), sort_keys=True)
    return "[]"


def parse_json_list(raw_value):
    if not raw_value:
        return []
    if isinstance(raw_value, list):
        return raw_value
    return json.loads(raw_value)


def to_int_or_none(value):
    if value in (None, "", "none", "null"):
        return None
    return int(value)


def int_from_query(value, default=None):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def bool_from_form(mapping, name, default=False):
    if name not in mapping:
        return default
    value = mapping.get(name)
    if isinstance(value, list):
        value = value[-1]
    return bool_value(value)


def bool_value(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "on", "yes"}


def username_to_ipv6_candidate(username):
    normalized = (username or "").strip().lower()
    if not HEX32_RE.match(normalized):
        return ""
    return ":".join(normalized[index:index + 4] for index in range(0, 32, 4))


def maybe_normalize_ipv6_text(value):
    try:
        return normalize_ipv6_text(value)
    except Exception:
        return ""


def user_canonical_ipv6(user):
    candidate = username_to_ipv6_candidate(user.get("username"))
    if candidate:
        return maybe_normalize_ipv6_text(candidate)
    comment = (user.get("comment") or "").strip()
    if comment:
        return maybe_normalize_ipv6_text(comment)
    return ""


def user_display_alias(user):
    alias = (user.get("alias") or "").strip()
    if alias:
        return alias
    comment = (user.get("comment") or "").strip()
    if comment and not username_to_ipv6_candidate(comment) and not maybe_normalize_ipv6_text(comment):
        return comment
    return ""


def user_role_labels(user):
    labels = []
    if user.get("is_iot_device"):
        labels.append("IoT device")
    if user.get("is_iot_platform"):
        labels.append("IoT platform")
    return labels


def user_role_badges(user, empty="-"):
    labels = user_role_labels(user)
    if not labels:
        return f'<span class="muted">{html.escape(empty)}</span>'
    return "".join(f'<span class="pill secondary">{html.escape(label)}</span>' for label in labels)


def normalize_ipv6_text(value):
    return ipaddress.IPv6Address(str(value).strip()).exploded.lower()


def normalize_mac_text(value):
    raw = str(value or "").strip().lower()
    if not raw:
        return None
    parts = raw.split(":")
    if len(parts) != 6:
        raise ValueError(f"invalid MAC address: {value}")
    try:
        return ":".join(f"{int(part, 16):02x}" for part in parts)
    except ValueError as exc:
        raise ValueError(f"invalid MAC address: {value}") from exc


def required(mapping, key):
    value = mapping.get(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"missing required field: {key}")
    return value


def escape_authorized_value(value):
    return value.replace("\\", "\\\\").replace('"', '\\"')


def sanitize_passwd_field(value, fallback=""):
    if value is None:
        value = fallback
    value = str(value).replace(":", " ").replace("\r", " ").replace("\n", " ").strip()
    return value or fallback


def sanitize_group_field(value, fallback=""):
    if value is None:
        value = fallback
    value = str(value).replace(":", "_").replace("\r", "").replace("\n", "").strip()
    return value or fallback


DEFAULT_ENDPOINT_BUNDLE_FORMAT = "runtime-image"
ENDPOINT_BUNDLE_FORMAT_ALIASES = {
    "runtime": DEFAULT_ENDPOINT_BUNDLE_FORMAT,
    "image": DEFAULT_ENDPOINT_BUNDLE_FORMAT,
    "runtime-image": DEFAULT_ENDPOINT_BUNDLE_FORMAT,
    "thin": DEFAULT_ENDPOINT_BUNDLE_FORMAT,
    "self-contained": "self-contained",
    "selfcontained": "self-contained",
    "full": "self-contained",
}


def safe_bundle_filename_part(value):
    return re.sub(r"[^A-Za-z0-9._-]+", "-", str(value)).strip("-") or "identity"


def normalize_endpoint_bundle_format(value):
    key = (value or DEFAULT_ENDPOINT_BUNDLE_FORMAT).strip().lower()
    format_name = ENDPOINT_BUNDLE_FORMAT_ALIASES.get(key)
    if not format_name:
        supported = ", ".join(sorted(set(ENDPOINT_BUNDLE_FORMAT_ALIASES.values())))
        raise ValueError(f"unsupported endpoint bundle format: {value!r}; expected one of {supported}")
    return format_name


def read_bundle_helper_asset(name):
    path = BUNDLE_HELPER_ROOT / name
    if not path.is_file():
        raise RuntimeError(f"bundle helper asset is missing: {path}")
    return path.read_bytes()


def read_bundle_helper_text(name):
    return read_bundle_helper_asset(name).decode("utf-8")


def add_tar_bytes(tar, path, data, mode=0o644):
    info = tarfile.TarInfo(path)
    info.size = len(data)
    info.mode = mode
    info.mtime = int(time.time())
    tar.addfile(info, io.BytesIO(data))


def add_tar_text(tar, path, text, mode=0o644):
    add_tar_bytes(tar, path, text.encode("utf-8"), mode=mode)


def dashboard_path(target_id=None, section=None, user_id=None, storage=None, storage_path=None, extra=None):
    params = {}
    if target_id:
        params["target_id"] = str(target_id)
    if section:
        params["section"] = str(section)
    if user_id:
        params["user_id"] = str(user_id)
    if storage:
        params["storage"] = str(storage)
    if storage_path:
        params["storage_path"] = str(storage_path)
    if extra:
        for key, value in extra.items():
            if value not in (None, ""):
                params[key] = str(value)
    if not params:
        return "/"
    return "/?" + urlencode(params)


def safe_redirect_path(value):
    if not value:
        return "/"
    parsed = urlparse(value)
    if parsed.scheme or parsed.netloc:
        return "/"
    path = parsed.path or "/"
    if not path.startswith("/"):
        return "/"
    if parsed.query:
        return f"{path}?{parsed.query}"
    return path


def generate_ed25519_keypair(comment=""):
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    public_text = public_key.public_bytes(
        encoding=serialization.Encoding.OpenSSH,
        format=serialization.PublicFormat.OpenSSH,
    ).decode("utf-8")
    if comment:
        public_text = f"{public_text} {comment}"
    private_text = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.OpenSSH,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    return {"public_key": public_text, "private_key": private_text}


def format_size(num_bytes):
    size = float(num_bytes)
    for unit in ["B", "KiB", "MiB", "GiB"]:
        if size < 1024.0 or unit == "GiB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{int(num_bytes)} B"


def format_mode(mode):
    return oct(mode & 0o777)


def is_probably_text(data):
    if not data:
        return True
    sample = data[:2048]
    if b"\x00" in sample:
        return False
    control_count = sum(1 for byte in sample if byte < 9 or (13 < byte < 32))
    return control_count < max(8, len(sample) // 20)


def storage_view(root_path, requested_relative=""):
    if not root_path:
        return {"configured": False, "exists": False, "error": "Not configured."}

    root = Path(root_path).resolve()
    if not root.exists():
        return {
            "configured": True,
            "exists": False,
            "root_path": str(root),
            "error": "Mounted path does not exist in the dashboard pod.",
        }

    relative = requested_relative.strip().lstrip("/") if requested_relative else ""
    current = (root / relative).resolve()
    try:
        if os.path.commonpath([str(root), str(current)]) != str(root):
            raise ValueError("Requested path escapes the configured root.")
    except ValueError:
        return {
            "configured": True,
            "exists": False,
            "root_path": str(root),
            "error": "Requested path is outside the configured root.",
        }

    if not current.exists():
        return {
            "configured": True,
            "exists": False,
            "root_path": str(root),
            "requested_relative": relative,
            "error": "Requested file or directory does not exist.",
        }

    stat_result = current.stat()
    info = {
        "configured": True,
        "exists": True,
        "root_path": str(root),
        "requested_relative": relative,
        "absolute_path": str(current),
        "name": current.name or str(current),
        "is_dir": current.is_dir(),
        "mode": format_mode(stat_result.st_mode),
        "size": stat_result.st_size,
        "size_label": format_size(stat_result.st_size),
        "modified_at": datetime.fromtimestamp(stat_result.st_mtime, timezone.utc).isoformat(),
        "entries": [],
        "preview_text": None,
        "preview_kind": None,
    }

    if current.is_dir():
        entries = []
        for child in sorted(current.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower()))[:200]:
            child_stat = child.stat()
            child_relative = child.relative_to(root).as_posix()
            entries.append(
                {
                    "name": child.name,
                    "relative_path": child_relative,
                    "is_dir": child.is_dir(),
                    "mode": format_mode(child_stat.st_mode),
                    "size": child_stat.st_size,
                    "size_label": format_size(child_stat.st_size),
                    "modified_at": datetime.fromtimestamp(child_stat.st_mtime, timezone.utc).isoformat(),
                }
            )
        info["entries"] = entries
        return info

    with open(current, "rb") as handle:
        blob = handle.read(32768)
    if is_probably_text(blob):
        info["preview_kind"] = "text"
        info["preview_text"] = blob.decode("utf-8", errors="replace")
    else:
        info["preview_kind"] = "binary"
        info["preview_text"] = blob[:128].hex(" ", 1)
    return info


def render_authorized_key_line(key_row, policy_row):
    public_key = key_row["public_key"].strip()
    options = []
    if policy_row:
        force_command = policy_row.get("force_command")
        if force_command:
            options.append(f'command="{escape_authorized_value(force_command)}"')
        if not policy_row.get("allow_pty"):
            options.append("no-pty")
        if not policy_row.get("allow_agent_forwarding"):
            options.append("no-agent-forwarding")
        if not policy_row.get("allow_x11_forwarding"):
            options.append("no-X11-forwarding")
        if not policy_row.get("allow_port_forwarding"):
            options.append("no-port-forwarding")
        for item in parse_json_list(policy_row.get("permit_open_json")):
            options.append(f'permitopen="{escape_authorized_value(str(item))}"')
        for item in parse_json_list(policy_row.get("permit_listen_json")):
            options.append(f'permitlisten="{escape_authorized_value(str(item))}"')
    if options:
        return ",".join(options) + " " + public_key
    return public_key


def render_sshd_config(settings):
    return "\n".join(
        [
            f"Port {settings['listen_port']}",
            "ListenAddress 0.0.0.0",
            "Protocol 2",
            "PasswordAuthentication no",
            "KbdInteractiveAuthentication no",
            "ChallengeResponseAuthentication no",
            "PubkeyAuthentication yes",
            "PermitRootLogin no",
            "PermitEmptyPasswords no",
            "HostKey /etc/ssh/ssh_host_ed25519_key",
            "AuthorizedKeysFile .ssh/authorized_keys",
            "PidFile /var/run/sshd.pid",
            "UseDNS no",
            f"X11Forwarding {'yes' if settings['x11_forwarding'] else 'no'}",
            f"AllowTcpForwarding {'yes' if settings['allow_tcp_forwarding'] else 'no'}",
            f"GatewayPorts {'clientspecified' if settings['gateway_ports'] else 'no'}",
            f"PermitTunnel {'yes' if settings['permit_tunnel'] else 'no'}",
            "PrintMotd no",
            f"LogLevel {settings['log_level']}",
            "",
        ]
    )


def write_atomic(path, contents, mode=0o644):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(contents)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)
    os.chmod(path, mode)


class KubernetesHelper:
    def __init__(self):
        self._core = None
        self._apps = None
        self._lock = threading.Lock()

    def _ensure(self):
        if self._core is not None and self._apps is not None:
            return
        if k8s_client is None or k8s_config is None:
            raise RuntimeError("kubernetes client is not installed")
        with self._lock:
            if self._core is not None and self._apps is not None:
                return
            try:
                k8s_config.load_incluster_config()
            except Exception:
                k8s_config.load_kube_config()
            self._core = k8s_client.CoreV1Api()
            self._apps = k8s_client.AppsV1Api()

    def patch_config_map(self, namespace, name, key, value):
        self._ensure()
        self._core.patch_namespaced_config_map(
            name=name,
            namespace=namespace,
            body={"data": {key: value}},
        )

    def reload_pods(self, namespace, selector):
        self._ensure()
        pods = self._core.list_namespaced_pod(namespace=namespace, label_selector=selector).items
        outputs = []
        for pod in pods:
            if pod.status.phase != "Running":
                continue
            container_names = [container.name for container in (pod.spec.containers or [])]
            container_name = None
            if "sshd" in container_names:
                container_name = "sshd"
            elif len(container_names) == 1:
                container_name = container_names[0]
            else:
                raise RuntimeError(
                    f"pod {pod.metadata.name} has multiple containers; expected one named sshd, "
                    f"found: {', '.join(container_names)}"
                )
            output = k8s_stream(
                self._core.connect_get_namespaced_pod_exec,
                pod.metadata.name,
                namespace,
                command=["/bin/sh", "-lc", "kill -HUP $(cat /var/run/sshd.pid)"],
                container=container_name,
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
            )
            outputs.append({"pod": pod.metadata.name, "container": container_name, "output": output})
        return outputs

    def restart_deployment(self, namespace, name):
        self._ensure()
        self._apps.patch_namespaced_deployment(
            name=name,
            namespace=namespace,
            body={
                "spec": {
                    "template": {
                        "metadata": {
                            "annotations": {
                                "ssh-dashboard/restarted-at": now_iso()
                            }
                        }
                    }
                }
            },
        )


class Store:
    def __init__(self, dsn):
        self.pool = ConnectionPool(conninfo=dsn, min_size=1, max_size=6, kwargs={"row_factory": dict_row, "autocommit": True})
        self.init_db()

    def conn(self):
        return self.pool.connection()

    def init_db(self):
        with self.conn() as conn:
            for statement in SSH_SCHEMA_SQL:
                conn.execute(statement)
        self.sync_groups_from_users()

    def bootstrap_sample(self):
        if os.environ.get("SSH_DASHBOARD_BOOTSTRAP_SAMPLE", "true").lower() not in {"1", "true", "yes", "on"}:
            return
        target = self.upsert_target(
            {
                "name": os.environ.get("SSH_DASHBOARD_SAMPLE_TARGET_NAME", "portable-openssh-busybox"),
                "namespace": os.environ.get("SSH_DASHBOARD_SAMPLE_NAMESPACE", "mac-ssh-demo"),
                "workload_kind": os.environ.get("SSH_DASHBOARD_SAMPLE_WORKLOAD_KIND", "Deployment"),
                "workload_name": os.environ.get("SSH_DASHBOARD_SAMPLE_WORKLOAD_NAME", "portable-openssh-busybox"),
                "workload_selector": os.environ.get("SSH_DASHBOARD_SAMPLE_WORKLOAD_SELECTOR", "app=portable-openssh-busybox"),
                "account_root_path": os.environ.get("SSH_DASHBOARD_SAMPLE_ACCOUNT_ROOT", "/mnt/targets/portable-openssh/etc"),
                "home_root_path": os.environ.get("SSH_DASHBOARD_SAMPLE_HOME_ROOT", "/mnt/targets/portable-openssh/home"),
                "runtime_root_path": os.environ.get("SSH_DASHBOARD_SAMPLE_RUNTIME_ROOT", "/mnt/targets/portable-openssh/runtime"),
                "ssh_configmap_name": os.environ.get("SSH_DASHBOARD_SAMPLE_SSH_CONFIGMAP", "portable-openssh-etc"),
                "ssh_configmap_key": os.environ.get("SSH_DASHBOARD_SAMPLE_SSH_CONFIGMAP_KEY", "sshd_config"),
            }
        )
        self.upsert_server_settings(
            target["id"],
            {
                "canonical_gateway_mac": os.environ.get("SSH_DASHBOARD_SAMPLE_CANONICAL_GATEWAY_MAC"),
                "listen_port": int(os.environ.get("SSH_DASHBOARD_SAMPLE_LISTEN_PORT", "2222")),
                "allow_tcp_forwarding": True,
                "gateway_ports": False,
                "permit_tunnel": False,
                "x11_forwarding": False,
                "log_level": "VERBOSE",
            },
        )
        self.upsert_policy_profile(
            target["id"],
            {
                "name": os.environ.get("SSH_DASHBOARD_DEFAULT_POLICY_NAME", "forwarding-default"),
                "description": "Forwarding enabled by default, no PTY, forced keepalive command",
                "force_command": os.environ.get("SSH_DASHBOARD_DEFAULT_FORCE_COMMAND", "/opt/ssh-policy/forward-only.sh"),
                "allow_port_forwarding": True,
                "allow_pty": False,
                "allow_agent_forwarding": False,
                "allow_x11_forwarding": False,
                "permit_open_json": "[]",
                "permit_listen_json": "[]",
            },
        )

    def list_targets(self):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    t.*,
                    COALESCE(user_counts.user_count, 0) AS user_count,
                    COALESCE(key_counts.key_count, 0) AS key_count,
                    COALESCE(group_counts.group_count, 0) AS group_count
                FROM ssh_admin.targets AS t
                LEFT JOIN (
                    SELECT target_id, COUNT(*) AS user_count
                    FROM ssh_admin.users
                    GROUP BY target_id
                ) AS user_counts ON user_counts.target_id = t.id
                LEFT JOIN (
                    SELECT u.target_id, COUNT(*) AS key_count
                    FROM ssh_admin.public_keys AS k
                    JOIN ssh_admin.users AS u ON u.id = k.user_id
                    GROUP BY u.target_id
                ) AS key_counts ON key_counts.target_id = t.id
                LEFT JOIN (
                    SELECT target_id, COUNT(*) AS group_count
                    FROM ssh_admin.groups
                    GROUP BY target_id
                ) AS group_counts ON group_counts.target_id = t.id
                ORDER BY t.name
                """
            ).fetchall()

    def sync_groups_from_users(self):
        with self.conn() as conn:
            rows = conn.execute(
                """
                SELECT u.target_id, u.username, u.gid
                FROM ssh_admin.users AS u
                LEFT JOIN ssh_admin.groups AS g
                  ON g.target_id = u.target_id AND g.gid = u.gid
                WHERE g.id IS NULL
                ORDER BY u.target_id, u.username
                """
            ).fetchall()
            for row in rows:
                proposed_name = row["username"]
                name_in_use = conn.execute(
                    "SELECT 1 FROM ssh_admin.groups WHERE target_id = %s AND name = %s",
                    (row["target_id"], proposed_name),
                ).fetchone()
                if name_in_use:
                    proposed_name = f"group-{row['gid']}"
                now = now_iso()
                conn.execute(
                    """
                    INSERT INTO ssh_admin.groups (target_id, name, gid, comment, enabled, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, TRUE, %s, %s)
                    ON CONFLICT (target_id, gid) DO NOTHING
                    """,
                    (
                        row["target_id"],
                        proposed_name,
                        row["gid"],
                        f"Imported group for gid {row['gid']}",
                        now,
                        now,
                    ),
                )

    def list_groups(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    g.*,
                    COALESCE(member_counts.member_count, 0) AS member_count,
                    COALESCE(member_counts.members, '') AS members
                FROM ssh_admin.groups AS g
                LEFT JOIN (
                    SELECT
                        u.target_id,
                        u.gid,
                        COUNT(*) AS member_count,
                        string_agg(u.username, ', ' ORDER BY u.username) AS members
                    FROM ssh_admin.users AS u
                    GROUP BY u.target_id, u.gid
                ) AS member_counts
                  ON member_counts.target_id = g.target_id
                 AND member_counts.gid = g.gid
                WHERE g.target_id = %s
                ORDER BY g.name
                """,
                (target_id,),
            ).fetchall()

    def get_group(self, group_id):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.groups WHERE id = %s",
                (group_id,),
            ).fetchone()

    def get_group_by_name(self, target_id, name):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.groups WHERE target_id = %s AND name = %s",
                (target_id, name),
            ).fetchone()

    def get_group_by_gid(self, target_id, gid):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.groups WHERE target_id = %s AND gid = %s",
                (target_id, gid),
            ).fetchone()

    def next_available_uid(self, target_id, minimum=100000):
        with self.conn() as conn:
            rows = conn.execute(
                "SELECT uid FROM ssh_admin.users WHERE target_id = %s ORDER BY uid",
                (target_id,),
            ).fetchall()
        used = {row["uid"] for row in rows}
        candidate = minimum
        while candidate in used:
            candidate += 1
        return candidate

    def next_available_gid(self, target_id, minimum=100000):
        with self.conn() as conn:
            rows = conn.execute(
                "SELECT gid FROM ssh_admin.groups WHERE target_id = %s ORDER BY gid",
                (target_id,),
            ).fetchall()
        used = {row["gid"] for row in rows}
        candidate = minimum
        while candidate in used:
            candidate += 1
        return candidate

    def upsert_group(self, payload):
        now = now_iso()
        target_id = int(payload["target_id"])
        name = payload["name"].strip()
        gid = to_int_or_none(payload.get("gid"))
        if gid is None:
            existing = self.get_group_by_name(target_id, name)
            gid = existing["gid"] if existing else self.next_available_gid(target_id)
        with self.conn() as conn:
            group_id = to_int_or_none(payload.get("group_id"))
            if group_id:
                return conn.execute(
                    """
                    UPDATE ssh_admin.groups
                    SET name = %(name)s,
                        gid = %(gid)s,
                        comment = %(comment)s,
                        enabled = %(enabled)s,
                        updated_at = %(now)s
                    WHERE id = %(group_id)s
                    RETURNING *
                    """,
                    {
                        "group_id": group_id,
                        "name": name,
                        "gid": gid,
                        "comment": payload.get("comment"),
                        "enabled": payload.get("enabled", True) not in {False, "false", "0", "off"},
                        "now": now,
                    },
                ).fetchone()
            return conn.execute(
                """
                INSERT INTO ssh_admin.groups (
                    target_id, name, gid, comment, enabled, created_at, updated_at
                ) VALUES (
                    %(target_id)s, %(name)s, %(gid)s, %(comment)s, %(enabled)s, %(now)s, %(now)s
                )
                ON CONFLICT (target_id, name) DO UPDATE SET
                    gid = EXCLUDED.gid,
                    comment = EXCLUDED.comment,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                {
                    "target_id": target_id,
                    "name": name,
                    "gid": gid,
                    "comment": payload.get("comment"),
                    "enabled": payload.get("enabled", True) not in {False, "false", "0", "off"},
                    "now": now,
                },
            ).fetchone()

    def toggle_group(self, group_id):
        with self.conn() as conn:
            return conn.execute(
                "UPDATE ssh_admin.groups SET enabled = NOT enabled, updated_at = %s WHERE id = %s RETURNING *",
                (now_iso(), group_id),
            ).fetchone()

    def delete_group(self, group_id):
        group_row = self.get_group(group_id)
        if not group_row:
            return
        with self.conn() as conn:
            member = conn.execute(
                "SELECT 1 FROM ssh_admin.users WHERE target_id = %s AND gid = %s LIMIT 1",
                (group_row["target_id"], group_row["gid"]),
            ).fetchone()
            if member:
                raise ValueError("group is still used by one or more users")
            conn.execute("DELETE FROM ssh_admin.groups WHERE id = %s", (group_id,))

    def ensure_private_group(self, target_id, username):
        existing_by_name = self.get_group_by_name(target_id, username)
        if existing_by_name:
            return existing_by_name
        return self.upsert_group(
            {
                "target_id": target_id,
                "name": username,
                "gid": self.next_available_gid(target_id),
                "comment": f"Private group for {username}",
                "enabled": True,
            }
        )

    def get_target(self, target_id):
        with self.conn() as conn:
            return conn.execute("SELECT * FROM ssh_admin.targets WHERE id = %s", (target_id,)).fetchone()

    def upsert_target(self, payload):
        now = now_iso()
        with self.conn() as conn:
            return conn.execute(
                """
                INSERT INTO ssh_admin.targets (
                    name, namespace, workload_kind, workload_name, workload_selector,
                    account_root_path, home_root_path, runtime_root_path, ssh_configmap_name, ssh_configmap_key,
                    created_at, updated_at
                ) VALUES (
                    %(name)s, %(namespace)s, %(workload_kind)s, %(workload_name)s, %(workload_selector)s,
                    %(account_root_path)s, %(home_root_path)s, %(runtime_root_path)s, %(ssh_configmap_name)s, %(ssh_configmap_key)s,
                    %(now)s, %(now)s
                )
                ON CONFLICT (name) DO UPDATE SET
                    namespace = EXCLUDED.namespace,
                    workload_kind = EXCLUDED.workload_kind,
                    workload_name = EXCLUDED.workload_name,
                    workload_selector = EXCLUDED.workload_selector,
                    account_root_path = EXCLUDED.account_root_path,
                    home_root_path = EXCLUDED.home_root_path,
                    runtime_root_path = EXCLUDED.runtime_root_path,
                    ssh_configmap_name = EXCLUDED.ssh_configmap_name,
                    ssh_configmap_key = EXCLUDED.ssh_configmap_key,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                {
                    **payload,
                    "ssh_configmap_name": payload.get("ssh_configmap_name"),
                    "ssh_configmap_key": payload.get("ssh_configmap_key", "sshd_config"),
                    "runtime_root_path": payload.get("runtime_root_path"),
                    "workload_kind": payload.get("workload_kind", "Deployment"),
                    "now": now,
                },
            ).fetchone()

    def list_policy_profiles(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.policy_profiles WHERE target_id = %s ORDER BY name",
                (target_id,),
            ).fetchall()

    def upsert_policy_profile(self, target_id, payload):
        now = now_iso()
        with self.conn() as conn:
            return conn.execute(
                """
                INSERT INTO ssh_admin.policy_profiles (
                    target_id, name, description, force_command, allow_port_forwarding, allow_pty,
                    allow_agent_forwarding, allow_x11_forwarding, permit_open_json, permit_listen_json,
                    created_at, updated_at
                ) VALUES (
                    %(target_id)s, %(name)s, %(description)s, %(force_command)s, %(allow_port_forwarding)s,
                    %(allow_pty)s, %(allow_agent_forwarding)s, %(allow_x11_forwarding)s,
                    %(permit_open_json)s, %(permit_listen_json)s, %(now)s, %(now)s
                )
                ON CONFLICT (target_id, name) DO UPDATE SET
                    description = EXCLUDED.description,
                    force_command = EXCLUDED.force_command,
                    allow_port_forwarding = EXCLUDED.allow_port_forwarding,
                    allow_pty = EXCLUDED.allow_pty,
                    allow_agent_forwarding = EXCLUDED.allow_agent_forwarding,
                    allow_x11_forwarding = EXCLUDED.allow_x11_forwarding,
                    permit_open_json = EXCLUDED.permit_open_json,
                    permit_listen_json = EXCLUDED.permit_listen_json,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                {
                    "target_id": target_id,
                    "name": payload["name"],
                    "description": payload.get("description"),
                    "force_command": payload.get("force_command"),
                    "allow_port_forwarding": bool(payload.get("allow_port_forwarding", True)),
                    "allow_pty": bool(payload.get("allow_pty", False)),
                    "allow_agent_forwarding": bool(payload.get("allow_agent_forwarding", False)),
                    "allow_x11_forwarding": bool(payload.get("allow_x11_forwarding", False)),
                    "permit_open_json": normalize_json_list(payload.get("permit_open_json")),
                    "permit_listen_json": normalize_json_list(payload.get("permit_listen_json")),
                    "now": now,
                },
            ).fetchone()

    def list_users(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    u.*,
                    g.name AS group_name,
                    p.name AS default_policy_name,
                    COALESCE(k.key_count, 0) AS key_count
                FROM ssh_admin.users AS u
                LEFT JOIN ssh_admin.groups AS g
                  ON g.target_id = u.target_id
                 AND g.gid = u.gid
                LEFT JOIN ssh_admin.policy_profiles AS p ON p.id = u.default_policy_profile_id
                LEFT JOIN (
                    SELECT user_id, COUNT(*) AS key_count
                    FROM ssh_admin.public_keys
                    GROUP BY user_id
                ) AS k ON k.user_id = u.id
                WHERE u.target_id = %s
                ORDER BY u.username
                """,
                (target_id,),
            ).fetchall()

    def get_user(self, user_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    u.*,
                    g.name AS group_name,
                    p.name AS default_policy_name,
                    COALESCE(k.key_count, 0) AS key_count
                FROM ssh_admin.users AS u
                LEFT JOIN ssh_admin.groups AS g
                  ON g.target_id = u.target_id
                 AND g.gid = u.gid
                LEFT JOIN ssh_admin.policy_profiles AS p ON p.id = u.default_policy_profile_id
                LEFT JOIN (
                    SELECT user_id, COUNT(*) AS key_count
                    FROM ssh_admin.public_keys
                    GROUP BY user_id
                ) AS k ON k.user_id = u.id
                WHERE u.id = %s
                """,
                (user_id,),
            ).fetchone()

    def get_user_by_username(self, target_id, username):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    u.*,
                    g.name AS group_name
                FROM ssh_admin.users AS u
                LEFT JOIN ssh_admin.groups AS g
                  ON g.target_id = u.target_id
                 AND g.gid = u.gid
                WHERE u.target_id = %s AND u.username = %s
                """,
                (target_id, username),
            ).fetchone()

    def upsert_user(self, payload):
        now = now_iso()
        target_id = int(payload["target_id"])
        username = payload["username"].strip()
        existing = self.get_user_by_username(target_id, username)
        provided_uid = to_int_or_none(payload.get("uid"))
        provided_gid = to_int_or_none(payload.get("gid"))
        group_id = to_int_or_none(payload.get("group_id"))
        if group_id is not None:
            group_row = self.get_group(group_id)
            if not group_row:
                raise ValueError("selected group was not found")
            gid = group_row["gid"]
        elif provided_gid is not None:
            gid = provided_gid
            if not self.get_group_by_gid(target_id, gid):
                group_name = username if not self.get_group_by_name(target_id, username) else f"group-{gid}"
                self.upsert_group(
                    {
                        "target_id": target_id,
                        "name": group_name,
                        "gid": gid,
                        "comment": f"Imported group for gid {gid}",
                        "enabled": True,
                    }
                )
        elif existing:
            gid = existing["gid"]
        else:
            gid = self.ensure_private_group(target_id, username)["gid"]

        uid = provided_uid if provided_uid is not None else (existing["uid"] if existing else self.next_available_uid(target_id))
        home_dir = (payload.get("home_dir") or "").strip() or (existing["home_dir"] if existing else f"/home/{username}")
        shell = (payload.get("shell") or "").strip() or (existing["shell"] if existing else "/bin/sh")
        alias = (payload.get("alias") or "").strip() if "alias" in payload else (existing.get("alias") if existing else None)
        alias = alias or None
        is_iot_device = (
            payload.get("is_iot_device")
            if "is_iot_device" in payload
            else (existing.get("is_iot_device") if existing else False)
        )
        is_iot_platform = (
            payload.get("is_iot_platform")
            if "is_iot_platform" in payload
            else (existing.get("is_iot_platform") if existing else False)
        )
        with self.conn() as conn:
            return conn.execute(
                """
                INSERT INTO ssh_admin.users (
                    target_id, username, alias, uid, gid, home_dir, shell, comment,
                    is_iot_device, is_iot_platform,
                    enabled, default_policy_profile_id, created_at, updated_at
                ) VALUES (
                    %(target_id)s, %(username)s, %(alias)s, %(uid)s, %(gid)s, %(home_dir)s,
                    %(shell)s, %(comment)s, %(is_iot_device)s, %(is_iot_platform)s,
                    %(enabled)s, %(default_policy_profile_id)s, %(now)s, %(now)s
                )
                ON CONFLICT (target_id, username) DO UPDATE SET
                    alias = EXCLUDED.alias,
                    uid = EXCLUDED.uid,
                    gid = EXCLUDED.gid,
                    home_dir = EXCLUDED.home_dir,
                    shell = EXCLUDED.shell,
                    comment = EXCLUDED.comment,
                    is_iot_device = EXCLUDED.is_iot_device,
                    is_iot_platform = EXCLUDED.is_iot_platform,
                    enabled = EXCLUDED.enabled,
                    default_policy_profile_id = EXCLUDED.default_policy_profile_id,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                {
                    "target_id": target_id,
                    "username": username,
                    "alias": alias,
                    "uid": uid,
                    "gid": gid,
                    "home_dir": home_dir,
                    "shell": shell,
                    "comment": payload.get("comment"),
                    "is_iot_device": bool_value(is_iot_device),
                    "is_iot_platform": bool_value(is_iot_platform),
                    "enabled": payload.get("enabled", True) not in {False, "false", "0", "off"},
                    "default_policy_profile_id": to_int_or_none(payload.get("default_policy_profile_id")),
                    "now": now,
                },
            ).fetchone()

    def batch_create_users(
        self,
        target_id,
        usernames,
        default_policy_profile_id=None,
        shell="/bin/sh",
        is_iot_device=True,
        is_iot_platform=False,
    ):
        created = []
        skipped = []
        seen = set()
        for raw_name in usernames:
            username = raw_name.strip()
            if not username or username in seen:
                continue
            seen.add(username)
            if self.get_user_by_username(target_id, username):
                skipped.append(username)
                continue
            user_row = self.upsert_user(
                {
                    "target_id": target_id,
                    "username": username,
                    "home_dir": f"/home/{username}",
                    "shell": shell,
                    "comment": username,
                    "is_iot_device": is_iot_device,
                    "is_iot_platform": is_iot_platform,
                    "default_policy_profile_id": default_policy_profile_id,
                    "enabled": True,
                }
            )
            target_row = self.get_target(target_id)
            self.create_generated_keypair_for_user(user_row, target_row)
            created.append(user_row)
        return {"created": created, "skipped": skipped}

    def toggle_user(self, user_id):
        with self.conn() as conn:
            return conn.execute(
                "UPDATE ssh_admin.users SET enabled = NOT enabled, updated_at = %s WHERE id = %s RETURNING *",
                (now_iso(), user_id),
            ).fetchone()

    def delete_user(self, user_id):
        with self.conn() as conn:
            conn.execute("DELETE FROM ssh_admin.users WHERE id = %s", (user_id,))

    def list_published_services(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    s.*,
                    owner.username AS owner_username,
                    COALESCE(grants.enabled_grant_count, 0) AS enabled_grant_count,
                    COALESCE(grants.grantee_usernames, '') AS grantee_usernames
                FROM ssh_admin.published_services AS s
                JOIN ssh_admin.users AS owner ON owner.id = s.owner_user_id
                LEFT JOIN (
                    SELECT
                        g.service_id,
                        COUNT(*) FILTER (WHERE g.enabled) AS enabled_grant_count,
                        string_agg(grantee.username, ', ' ORDER BY grantee.username) FILTER (WHERE g.enabled) AS grantee_usernames
                    FROM ssh_admin.service_access_grants AS g
                    JOIN ssh_admin.users AS grantee ON grantee.id = g.grantee_user_id
                    GROUP BY g.service_id
                ) AS grants ON grants.service_id = s.id
                WHERE s.target_id = %s
                ORDER BY s.alias
                """,
                (target_id,),
            ).fetchall()

    def get_published_service(self, service_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT s.*, owner.username AS owner_username
                FROM ssh_admin.published_services AS s
                JOIN ssh_admin.users AS owner ON owner.id = s.owner_user_id
                WHERE s.id = %s
                """,
                (service_id,),
            ).fetchone()

    def upsert_published_service(self, payload):
        now = now_iso()
        target_id = int(payload["target_id"])
        owner_user_id = int(payload["owner_user_id"])
        owner = self.get_user(owner_user_id)
        if not owner or owner["target_id"] != target_id:
            raise ValueError("service owner must be a user in the selected target")
        port = int(required(payload, "port"))
        if port <= 0 or port >= 65536:
            raise ValueError("service port must be between 1 and 65535")
        canonical_ipv6 = (payload.get("canonical_ipv6") or username_to_ipv6_candidate(owner["username"])).strip().lower()
        if not canonical_ipv6:
            raise ValueError("canonical IPv6 is required unless the owner username is a 32-hex canonical identity")
        params = {
            "service_id": to_int_or_none(payload.get("service_id")),
            "target_id": target_id,
            "owner_user_id": owner_user_id,
            "alias": required(payload, "alias").strip(),
            "protocol": (payload.get("protocol") or "http").strip().lower(),
            "canonical_ipv6": canonical_ipv6,
            "port": port,
            "description": payload.get("description"),
            "enabled": payload.get("enabled", True) not in {False, "false", "0", "off"},
            "now": now,
        }
        with self.conn() as conn:
            if params["service_id"]:
                return conn.execute(
                    """
                    UPDATE ssh_admin.published_services
                    SET owner_user_id = %(owner_user_id)s,
                        alias = %(alias)s,
                        protocol = %(protocol)s,
                        canonical_ipv6 = %(canonical_ipv6)s,
                        port = %(port)s,
                        description = %(description)s,
                        enabled = %(enabled)s,
                        updated_at = %(now)s
                    WHERE id = %(service_id)s AND target_id = %(target_id)s
                    RETURNING *
                    """,
                    params,
                ).fetchone()
            return conn.execute(
                """
                INSERT INTO ssh_admin.published_services (
                    target_id, owner_user_id, alias, protocol, canonical_ipv6, port,
                    description, enabled, created_at, updated_at
                ) VALUES (
                    %(target_id)s, %(owner_user_id)s, %(alias)s, %(protocol)s, %(canonical_ipv6)s, %(port)s,
                    %(description)s, %(enabled)s, %(now)s, %(now)s
                )
                ON CONFLICT (target_id, alias) DO UPDATE SET
                    owner_user_id = EXCLUDED.owner_user_id,
                    protocol = EXCLUDED.protocol,
                    canonical_ipv6 = EXCLUDED.canonical_ipv6,
                    port = EXCLUDED.port,
                    description = EXCLUDED.description,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                params,
            ).fetchone()

    def toggle_published_service(self, service_id):
        with self.conn() as conn:
            return conn.execute(
                "UPDATE ssh_admin.published_services SET enabled = NOT enabled, updated_at = %s WHERE id = %s RETURNING *",
                (now_iso(), service_id),
            ).fetchone()

    def delete_published_service(self, service_id):
        with self.conn() as conn:
            conn.execute("DELETE FROM ssh_admin.published_services WHERE id = %s", (service_id,))

    def list_service_access_grants(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    g.*,
                    s.alias AS service_alias,
                    s.protocol AS service_protocol,
                    s.canonical_ipv6 AS service_canonical_ipv6,
                    s.port AS service_port,
                    owner.username AS owner_username,
                    grantee.username AS grantee_username
                FROM ssh_admin.service_access_grants AS g
                JOIN ssh_admin.published_services AS s ON s.id = g.service_id
                JOIN ssh_admin.users AS owner ON owner.id = s.owner_user_id
                JOIN ssh_admin.users AS grantee ON grantee.id = g.grantee_user_id
                WHERE g.target_id = %s
                ORDER BY grantee.username, s.alias
                """,
                (target_id,),
            ).fetchall()

    def get_service_access_grant(self, grant_id):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.service_access_grants WHERE id = %s",
                (grant_id,),
            ).fetchone()

    def upsert_service_access_grant(self, payload):
        now = now_iso()
        target_id = int(payload["target_id"])
        service = self.get_published_service(int(payload["service_id"]))
        grantee = self.get_user(int(payload["grantee_user_id"]))
        if not service or service["target_id"] != target_id:
            raise ValueError("selected service does not belong to the selected target")
        if not grantee or grantee["target_id"] != target_id:
            raise ValueError("service grantee must be a user in the selected target")
        default_context = f"{grantee['username']} -> {service['alias']}"
        params = {
            "grant_id": to_int_or_none(payload.get("grant_id")),
            "target_id": target_id,
            "service_id": service["id"],
            "grantee_user_id": grantee["id"],
            "context_alias": (payload.get("context_alias") or default_context).strip(),
            "description": payload.get("description"),
            "enabled": payload.get("enabled", True) not in {False, "false", "0", "off"},
            "now": now,
        }
        with self.conn() as conn:
            if params["grant_id"]:
                return conn.execute(
                    """
                    UPDATE ssh_admin.service_access_grants
                    SET service_id = %(service_id)s,
                        grantee_user_id = %(grantee_user_id)s,
                        context_alias = %(context_alias)s,
                        description = %(description)s,
                        enabled = %(enabled)s,
                        updated_at = %(now)s
                    WHERE id = %(grant_id)s AND target_id = %(target_id)s
                    RETURNING *
                    """,
                    params,
                ).fetchone()
            return conn.execute(
                """
                INSERT INTO ssh_admin.service_access_grants (
                    target_id, service_id, grantee_user_id, context_alias,
                    description, enabled, created_at, updated_at
                ) VALUES (
                    %(target_id)s, %(service_id)s, %(grantee_user_id)s, %(context_alias)s,
                    %(description)s, %(enabled)s, %(now)s, %(now)s
                )
                ON CONFLICT (service_id, grantee_user_id) DO UPDATE SET
                    context_alias = EXCLUDED.context_alias,
                    description = EXCLUDED.description,
                    enabled = EXCLUDED.enabled,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                params,
            ).fetchone()

    def toggle_service_access_grant(self, grant_id):
        with self.conn() as conn:
            return conn.execute(
                "UPDATE ssh_admin.service_access_grants SET enabled = NOT enabled, updated_at = %s WHERE id = %s RETURNING *",
                (now_iso(), grant_id),
            ).fetchone()

    def delete_service_access_grant(self, grant_id):
        with self.conn() as conn:
            conn.execute("DELETE FROM ssh_admin.service_access_grants WHERE id = %s", (grant_id,))

    def list_public_keys(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    k.*,
                    u.username,
                    COALESCE(p.name, dp.name) AS effective_policy_name
                FROM ssh_admin.public_keys AS k
                JOIN ssh_admin.users AS u ON u.id = k.user_id
                LEFT JOIN ssh_admin.policy_profiles AS p ON p.id = k.policy_profile_id
                LEFT JOIN ssh_admin.policy_profiles AS dp ON dp.id = u.default_policy_profile_id
                WHERE u.target_id = %s
                ORDER BY u.username, k.id
                """,
                (target_id,),
            ).fetchall()

    def list_public_keys_for_user(self, user_id):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.public_keys WHERE user_id = %s ORDER BY id",
                (user_id,),
            ).fetchall()

    def get_public_key(self, key_id):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.public_keys WHERE id = %s",
                (key_id,),
            ).fetchone()

    def upsert_public_key(self, payload):
        now = now_iso()
        with self.conn() as conn:
            params = {
                "user_id": int(payload["user_id"]),
                "label": payload.get("label"),
                "public_key": payload["public_key"].strip(),
                "private_key": (payload.get("private_key") or "").strip() or None,
                "policy_profile_id": to_int_or_none(payload.get("policy_profile_id")),
                "generated": payload.get("generated", False) in {True, "true", "1", "on"},
                "enabled": payload.get("enabled", True) not in {False, "false", "0", "off"},
                "now": now,
            }
            key_id = to_int_or_none(payload.get("key_id"))
            if key_id:
                params["key_id"] = key_id
                return conn.execute(
                    """
                    UPDATE ssh_admin.public_keys
                    SET label = %(label)s,
                        public_key = %(public_key)s,
                        private_key = %(private_key)s,
                        policy_profile_id = %(policy_profile_id)s,
                        generated = %(generated)s,
                        enabled = %(enabled)s,
                        updated_at = %(now)s
                    WHERE id = %(key_id)s
                    RETURNING *
                    """,
                    params,
                ).fetchone()
            return conn.execute(
                """
                INSERT INTO ssh_admin.public_keys (
                    user_id, label, public_key, private_key, policy_profile_id, generated, enabled, created_at, updated_at
                ) VALUES (
                    %(user_id)s, %(label)s, %(public_key)s, %(private_key)s, %(policy_profile_id)s, %(generated)s, %(enabled)s, %(now)s, %(now)s
                )
                RETURNING *
                """,
                params,
            ).fetchone()

    def create_generated_keypair_for_user(self, user_row, target_row, label=None):
        label = label or "generated-default"
        comment = f"{user_row['username']}@{target_row['name']}"
        pair = generate_ed25519_keypair(comment)
        return self.upsert_public_key(
            {
                "user_id": user_row["id"],
                "label": label,
                "public_key": pair["public_key"],
                "private_key": pair["private_key"],
                "policy_profile_id": user_row.get("default_policy_profile_id"),
                "generated": True,
                "enabled": True,
            }
        )

    def select_bundle_key_for_user(self, user_row, target_row):
        keys = [
            key
            for key in self.list_public_keys_for_user(user_row["id"])
            if key["enabled"] and key.get("private_key")
        ]
        if not keys:
            return self.create_generated_keypair_for_user(user_row, target_row, label="generated-bundle"), True
        generated = [key for key in keys if key.get("generated")]
        return (generated or keys)[0], False

    def list_accessible_services_for_user(self, user_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT
                    g.id AS grant_id,
                    g.context_alias,
                    g.description AS grant_description,
                    s.id AS service_id,
                    s.alias AS service_alias,
                    s.protocol AS service_protocol,
                    s.canonical_ipv6 AS service_canonical_ipv6,
                    s.port AS service_port,
                    s.description AS service_description,
                    owner.username AS owner_username
                FROM ssh_admin.service_access_grants AS g
                JOIN ssh_admin.published_services AS s ON s.id = g.service_id
                JOIN ssh_admin.users AS owner ON owner.id = s.owner_user_id
                WHERE g.grantee_user_id = %s
                  AND g.enabled
                  AND s.enabled
                ORDER BY s.alias
                """,
                (user_id,),
            ).fetchall()

    def list_publishable_services_for_user(self, user_id):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT *
                FROM ssh_admin.published_services
                WHERE owner_user_id = %s
                  AND enabled
                ORDER BY alias
                """,
                (user_id,),
            ).fetchall()

    def build_endpoint_bundle(
        self,
        user_id,
        gateway_host,
        gateway_port=None,
        ssh_bin="ssh",
        endpoint_iface="cmx0",
        bundle_format=DEFAULT_ENDPOINT_BUNDLE_FORMAT,
    ):
        user_row = self.get_user(user_id)
        if not user_row:
            raise ValueError("bundle user was not found")
        target_row = self.get_target(user_row["target_id"])
        settings = self.get_server_settings(user_row["target_id"]) or {}
        bundle_format = normalize_endpoint_bundle_format(bundle_format)
        key_row, generated_new_key = self.select_bundle_key_for_user(user_row, target_row)
        if generated_new_key:
            self.queue_reconcile(target_row["id"], "RENDER_ONLY", requested_by="bundle-generator")

        canonical_candidate = username_to_ipv6_candidate(user_row["username"])
        canonical_ipv6 = canonical_candidate
        if not canonical_ipv6 and user_row.get("comment") and ":" in user_row["comment"]:
            canonical_ipv6 = user_row["comment"]
        if not canonical_ipv6:
            raise ValueError("endpoint bundles require a 32-hex canonical username or an IPv6 address in the user comment")
        canonical_ipv6 = normalize_ipv6_text(canonical_ipv6)
        gateway_port = int(gateway_port or settings.get("listen_port") or 2222)

        accessible_services = []
        for row in self.list_accessible_services_for_user(user_row["id"]):
            accessible_services.append(
                {
                    "alias": row["service_alias"],
                    "context_alias": row.get("context_alias"),
                    "protocol": row.get("service_protocol") or "tcp",
                    "canonical_ipv6": normalize_ipv6_text(row["service_canonical_ipv6"]),
                    "port": int(row["service_port"]),
                    "remote_port": int(row["service_port"]),
                    "local_port": int(row["service_port"]),
                    "publisher_username": row["owner_username"],
                    "description": row.get("service_description") or row.get("grant_description") or "",
                }
            )

        publishable_services = []
        for row in self.list_publishable_services_for_user(user_row["id"]):
            service_ipv6 = normalize_ipv6_text(row["canonical_ipv6"])
            if ipaddress.IPv6Address(service_ipv6) != ipaddress.IPv6Address(canonical_ipv6):
                raise ValueError(
                    f"publishable service {row['alias']} uses {service_ipv6}, but the bundle identity is {canonical_ipv6}"
                )
            publishable_services.append(
                {
                    "alias": row["alias"],
                    "protocol": row.get("protocol") or "tcp",
                    "canonical_ipv6": service_ipv6,
                    "port": int(row["port"]),
                    "remote_port": int(row["port"]),
                    "local_port": int(row["port"]),
                    "local_host": "::1",
                    "description": row.get("description") or "",
                }
            )

        config = {
            "generated_at": now_iso(),
            "target": {
                "id": target_row["id"],
                "name": target_row["name"],
                "namespace": target_row["namespace"],
                "workload": f"{target_row['workload_kind']}/{target_row['workload_name']}",
                "canonical_gateway_mac": settings.get("canonical_gateway_mac"),
            },
            "gateway": {
                "host": gateway_host,
                "port": gateway_port,
            },
            "identity": {
                "username": user_row["username"],
                "alias": user_display_alias(user_row),
                "canonical_ipv6": canonical_ipv6,
                "uid": user_row["uid"],
                "gid": user_row["gid"],
                "home_dir": user_row["home_dir"],
            },
            "ssh": {
                "ssh_bin": ssh_bin or "ssh",
                "identity_file": "credentials/id_ed25519",
            },
            "runtime": {
                "endpoint_iface": endpoint_iface or "cmx0",
                "run_dir": f"/var/run/cmxsafe-bundle-{safe_bundle_filename_part(user_row['username'])}",
            },
            "accessible_services": accessible_services,
            "publishable_services": publishable_services,
            "security_context_note": "Registration-only: this bundle reflects dashboard service context records; OpenSSH enforcement is not generated yet.",
            "bundle_format": bundle_format,
        }

        root_name = f"cmxsafe-endpoint-{safe_bundle_filename_part(user_row['username'])}"
        if bundle_format == DEFAULT_ENDPOINT_BUNDLE_FORMAT:
            readme = f"""# CMXsafe Endpoint Bundle

Identity: `{user_row['username']}`
Canonical IPv6: `{canonical_ipv6}`
Gateway: `{gateway_host}:{gateway_port}`
Format: `runtime-image`

This bundle is meant to be mounted into the reusable CMXsafe external endpoint image. It contains the identity-specific material only:

- `config.json`
- endpoint SSH keys
- small wrapper scripts such as `run-forever`

The shared runtime image supplies:

- patched CMXsafe Portable OpenSSH client at `{ssh_bin or '/opt/openssh/bin/ssh'}`
- `python3`
- `iproute2`
- `endpointd.py`, `cmxsafe-ssh`, and `bundlectl.py`

## Requirements

- the CMXsafe endpoint runtime image, for example `cmxsafemac-ipv6-endpoint-base`
- `CAP_NET_ADMIN` or equivalent privileges so the helper can manage the local `cmx0` interface
- `{gateway_host}` must resolve to the stable CMXsafe gateway entrypoint. Use real DNS when available; otherwise add a local host entry such as `/etc/hosts`, Windows `hosts`, or Docker `--add-host`.

## Use With Docker

```sh
tar xzf {root_name}-runtime-image.tar.gz
docker run --rm -it \\
  --cap-add NET_ADMIN \\
  --add-host {gateway_host}:<gateway-ip> \\
  -v "$PWD/{root_name}:/bundle" \\
  cmxsafemac-ipv6-endpoint-base
```

The image entrypoint switches into `/bundle` and runs `./run-forever` by default.

To send one message manually:

```sh
docker run --rm -it \\
  --cap-add NET_ADMIN \\
  --add-host {gateway_host}:<gateway-ip> \\
  -v "$PWD/{root_name}:/bundle" \\
  cmxsafemac-ipv6-endpoint-base ./send-message "hello from this CMXsafe endpoint"
```

Accessible services: {len(accessible_services)}
Publishable services: {len(publishable_services)}
"""
        else:
            readme = f"""# CMXsafe Endpoint Bundle

Identity: `{user_row['username']}`
Canonical IPv6: `{canonical_ipv6}`
Gateway: `{gateway_host}:{gateway_port}`
Format: `self-contained`

This bundle was generated by the Portable OpenSSH dashboard from registered publishable services and service access grants.

## Requirements

- Linux with `python3`, `iproute2`, and permission to create a dummy interface. Run `connect-platform` or `run-forever` with root privileges or equivalent `CAP_NET_ADMIN`.
- A CMXsafe-compatible OpenSSH client. Set `CMXSAFE_SSH_BIN` or regenerate the bundle with the right SSH binary path if the default `{ssh_bin or 'ssh'}` is not correct.
- If this bundle runs outside Kubernetes, `{gateway_host}` must resolve to the stable CMXsafe gateway entrypoint. Use a real DNS record when available; otherwise add a local host entry such as `/etc/hosts`, Windows `hosts`, or Docker `--add-host`.

`iproute2` is currently required because the helper creates the local `cmx0` dummy interface and adds or removes canonical `/128` IPv6 addresses through the Linux `ip` command. The current helper is Python-based for portability and fast iteration. A future static helper binary can remove both Python and `iproute2` from the endpoint dependency set.

## Use

```sh
tar xzf {root_name}.tar.gz
cd {root_name}
sudo ./run-forever
```

In another terminal:

```sh
./send-message "hello from this CMXsafe endpoint"
```

Stop the session with `Ctrl+C`, or force a cleanup with:

```sh
sudo ./disconnect
```

`connect-platform` remains available for one-shot setup. `run-forever` is the preferred persistent mode because it reconnects after SSH master failure and reinstalls forwards when the session comes back.

Accessible services: {len(accessible_services)}
Publishable services: {len(publishable_services)}
"""

        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
            add_tar_text(tar, f"{root_name}/README.md", readme)
            add_tar_text(tar, f"{root_name}/config.json", json.dumps(config, indent=2, sort_keys=True) + "\n")
            add_tar_text(tar, f"{root_name}/connect-platform", read_bundle_helper_text("connect-platform"), mode=0o755)
            add_tar_text(tar, f"{root_name}/run-forever", read_bundle_helper_text("run-forever"), mode=0o755)
            add_tar_text(tar, f"{root_name}/send-message", read_bundle_helper_text("send-message"), mode=0o755)
            add_tar_text(tar, f"{root_name}/disconnect", read_bundle_helper_text("disconnect"), mode=0o755)
            if bundle_format == "self-contained":
                add_tar_text(tar, f"{root_name}/bin/bundlectl.py", read_bundle_helper_text("bundlectl.py"), mode=0o755)
                add_tar_bytes(tar, f"{root_name}/bin/endpointd.py", read_bundle_helper_asset("endpointd.py"), mode=0o755)
                add_tar_bytes(tar, f"{root_name}/bin/cmxsafe-ssh", read_bundle_helper_asset("cmxsafe-ssh"), mode=0o755)
            add_tar_text(tar, f"{root_name}/credentials/id_ed25519", key_row["private_key"].rstrip() + "\n", mode=0o600)
            add_tar_text(tar, f"{root_name}/credentials/id_ed25519.pub", key_row["public_key"].rstrip() + "\n")
            add_tar_text(tar, f"{root_name}/logs/.keep", "")

        filename = f"{root_name}-{safe_bundle_filename_part(bundle_format)}.tar.gz"
        return filename, buffer.getvalue()

    def toggle_public_key(self, key_id):
        with self.conn() as conn:
            return conn.execute(
                "UPDATE ssh_admin.public_keys SET enabled = NOT enabled, updated_at = %s WHERE id = %s RETURNING *",
                (now_iso(), key_id),
            ).fetchone()

    def delete_public_key(self, key_id):
        with self.conn() as conn:
            conn.execute("DELETE FROM ssh_admin.public_keys WHERE id = %s", (key_id,))

    def get_server_settings(self, target_id):
        with self.conn() as conn:
            return conn.execute(
                "SELECT * FROM ssh_admin.server_settings WHERE target_id = %s",
                (target_id,),
            ).fetchone()

    def upsert_server_settings(self, target_id, payload):
        now = now_iso()
        canonical_gateway_mac = normalize_mac_text(payload.get("canonical_gateway_mac"))
        with self.conn() as conn:
            settings = conn.execute(
                """
                INSERT INTO ssh_admin.server_settings (
                    target_id, canonical_gateway_mac, listen_port, allow_tcp_forwarding, gateway_ports,
                    permit_tunnel, x11_forwarding, log_level, updated_at
                ) VALUES (
                    %(target_id)s, %(canonical_gateway_mac)s, %(listen_port)s, %(allow_tcp_forwarding)s, %(gateway_ports)s,
                    %(permit_tunnel)s, %(x11_forwarding)s, %(log_level)s, %(now)s
                )
                ON CONFLICT (target_id) DO UPDATE SET
                    canonical_gateway_mac = EXCLUDED.canonical_gateway_mac,
                    listen_port = EXCLUDED.listen_port,
                    allow_tcp_forwarding = EXCLUDED.allow_tcp_forwarding,
                    gateway_ports = EXCLUDED.gateway_ports,
                    permit_tunnel = EXCLUDED.permit_tunnel,
                    x11_forwarding = EXCLUDED.x11_forwarding,
                    log_level = EXCLUDED.log_level,
                    updated_at = EXCLUDED.updated_at
                RETURNING *
                """,
                {
                    "target_id": int(target_id),
                    "canonical_gateway_mac": canonical_gateway_mac,
                    "listen_port": int(payload.get("listen_port", 2222)),
                    "allow_tcp_forwarding": payload.get("allow_tcp_forwarding", True) not in {False, "false", "0", "off"},
                    "gateway_ports": payload.get("gateway_ports", False) in {True, "true", "1", "on"},
                    "permit_tunnel": payload.get("permit_tunnel", False) in {True, "true", "1", "on"},
                    "x11_forwarding": payload.get("x11_forwarding", False) in {True, "true", "1", "on"},
                    "log_level": payload.get("log_level", "VERBOSE"),
                    "now": now,
                },
            ).fetchone()
            conn.execute(
                """
                INSERT INTO cmxsafe_system_settings (key, value, updated_at)
                VALUES ('canonical_gateway_mac', %s, %s)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = EXCLUDED.updated_at
                """,
                (canonical_gateway_mac, now),
            )
            return settings

    def queue_reconcile(self, target_id, action, requested_by="dashboard"):
        with self.conn() as conn:
            return conn.execute(
                """
                INSERT INTO ssh_admin.reconcile_runs (
                    target_id, requested_action, requested_by, status, details_json, created_at
                ) VALUES (%s, %s, %s, 'QUEUED', '{}', %s)
                RETURNING *
                """,
                (target_id, action, requested_by, now_iso()),
            ).fetchone()

    def list_reconcile_runs(self, limit=20):
        with self.conn() as conn:
            return conn.execute(
                """
                SELECT r.*, t.name AS target_name
                FROM ssh_admin.reconcile_runs AS r
                JOIN ssh_admin.targets AS t ON t.id = r.target_id
                ORDER BY r.id DESC
                LIMIT %s
                """,
                (limit,),
            ).fetchall()

    def claim_next_reconcile_run(self):
        with self.conn() as conn:
            return conn.execute(
                """
                WITH next_run AS (
                    SELECT id
                    FROM ssh_admin.reconcile_runs
                    WHERE status = 'QUEUED'
                    ORDER BY id
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE ssh_admin.reconcile_runs AS r
                SET status = 'RUNNING', started_at = %s
                FROM next_run
                WHERE r.id = next_run.id
                RETURNING r.*
                """,
                (now_iso(),),
            ).fetchone()

    def complete_reconcile_run(self, run_id, revision_id, details, success=True, error_text=None):
        with self.conn() as conn:
            conn.execute(
                """
                UPDATE ssh_admin.reconcile_runs
                SET status = %s,
                    render_revision_id = %s,
                    details_json = %s,
                    error_text = %s,
                    finished_at = %s
                WHERE id = %s
                """,
                (
                    "SUCCEEDED" if success else "FAILED",
                    revision_id,
                    json.dumps(details, sort_keys=True),
                    error_text,
                    now_iso(),
                    run_id,
                ),
            )

    def insert_render_revision(self, target_id, action, hashes, details):
        with self.conn() as conn:
            return conn.execute(
                """
                INSERT INTO ssh_admin.render_revisions (
                    target_id, revision, passwd_sha256, group_sha256, authorized_keys_sha256, sshd_config_sha256,
                    status, needs_reload, needs_restart, details_json, created_at, applied_at
                )
                SELECT
                    %s,
                    COALESCE(MAX(revision), 0) + 1,
                    %s, %s, %s, %s,
                    'APPLIED',
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                FROM ssh_admin.render_revisions
                WHERE target_id = %s
                RETURNING *
                """,
                (
                    target_id,
                    hashes.get("passwd_sha256"),
                    hashes.get("group_sha256"),
                    hashes.get("authorized_keys_sha256"),
                    hashes.get("sshd_config_sha256"),
                    action == "RENDER_AND_RELOAD",
                    action == "RENDER_AND_RESTART",
                    json.dumps(details, sort_keys=True),
                    now_iso(),
                    now_iso(),
                    target_id,
                ),
            ).fetchone()


class Reconciler:
    def __init__(self, store, kube):
        self.store = store
        self.kube = kube

    def load_target_bundle(self, target_id):
        target = self.store.get_target(target_id)
        if not target:
            raise RuntimeError(f"target {target_id} not found")
        users = self.store.list_users(target_id)
        groups = self.store.list_groups(target_id)
        policies = {row["id"]: row for row in self.store.list_policy_profiles(target_id)}
        keys_by_user = {user["id"]: self.store.list_public_keys_for_user(user["id"]) for user in users}
        settings = self.store.get_server_settings(target_id) or self.store.upsert_server_settings(target_id, {})
        return target, users, groups, policies, keys_by_user, settings

    def render(self, target_id, action):
        target, users, groups, policies, keys_by_user, settings = self.load_target_bundle(target_id)
        enabled_users = [row for row in users if row["enabled"]]
        enabled_groups = [row for row in groups if row["enabled"]]
        passwd_lines = [
            "root:x:0:0:root:/root:/bin/sh",
            "sshd:x:74:74:Privilege-separated SSH:/var/empty:/bin/false",
        ]
        group_lines = [
            "root:x:0:",
            "sshd:x:74:",
        ]
        managed_authorized = {}
        members_by_gid = {}

        for user in enabled_users:
            username = sanitize_passwd_field(user["username"], "user")
            gecos = sanitize_passwd_field(user.get("comment"), username)
            home_dir = sanitize_passwd_field(user["home_dir"], f"/home/{username}")
            shell = sanitize_passwd_field(user["shell"], "/bin/sh")
            passwd_lines.append(
                f"{username}:x:{user['uid']}:{user['gid']}:{gecos}:{home_dir}:{shell}"
            )
            members_by_gid.setdefault(user["gid"], []).append(username)
            default_policy = policies.get(user.get("default_policy_profile_id"))
            key_lines = []
            for key_row in keys_by_user.get(user["id"], []):
                if not key_row["enabled"]:
                    continue
                policy = policies.get(key_row.get("policy_profile_id")) or default_policy
                key_lines.append(render_authorized_key_line(key_row, policy))
            managed_authorized[username] = "\n".join(key_lines) + ("\n" if key_lines else "")

        rendered_gids = set()
        for group in enabled_groups:
            members = ",".join(sorted(members_by_gid.get(group["gid"], [])))
            group_name = sanitize_group_field(group["name"], f"group-{group['gid']}")
            group_lines.append(f"{group_name}:x:{group['gid']}:{members}")
            rendered_gids.add(group["gid"])
        for gid, members in sorted(members_by_gid.items()):
            if gid in rendered_gids:
                continue
            group_lines.append(f"group-{gid}:x:{gid}:{','.join(sorted(members))}")

        passwd_text = "\n".join(passwd_lines) + "\n"
        group_text = "\n".join(group_lines) + "\n"
        sshd_config_text = render_sshd_config(settings)
        account_root = target["account_root_path"]
        home_root = target["home_root_path"]
        if not os.path.isabs(account_root) or not os.path.isabs(home_root):
            raise RuntimeError("target paths must be absolute")

        write_atomic(os.path.join(account_root, "passwd"), passwd_text, 0o644)
        write_atomic(os.path.join(account_root, "group"), group_text, 0o644)

        for user in users:
            if not user["home_dir"].startswith("/home/"):
                raise RuntimeError(f"user {user['username']} has unsupported home path {user['home_dir']}")
            relative_home = posixpath.relpath(user["home_dir"], "/home")
            auth_path = os.path.join(home_root, relative_home, ".ssh", "authorized_keys")
            write_atomic(auth_path, managed_authorized.get(user["username"], ""), 0o600)
            ssh_dir = os.path.dirname(auth_path)
            home_dir = os.path.dirname(ssh_dir)
            try:
                os.chmod(home_dir, 0o755)
                os.chmod(ssh_dir, 0o700)
                os.chown(home_dir, user["uid"], user["gid"])
                os.chown(ssh_dir, user["uid"], user["gid"])
                os.chown(auth_path, user["uid"], user["gid"])
            except (PermissionError, OSError):
                pass

        kube_details = {}
        if target.get("ssh_configmap_name"):
            self.kube.patch_config_map(
                namespace=target["namespace"],
                name=target["ssh_configmap_name"],
                key=target.get("ssh_configmap_key") or "sshd_config",
                value=sshd_config_text,
            )
        if action == "RENDER_AND_RELOAD":
            kube_details["reload"] = self.kube.reload_pods(target["namespace"], target["workload_selector"])
        elif action == "RENDER_AND_RESTART":
            if target["workload_kind"].lower() != "deployment":
                raise RuntimeError("restart is currently implemented only for Deployment targets")
            self.kube.restart_deployment(target["namespace"], target["workload_name"])
            kube_details["restart"] = {"deployment": target["workload_name"]}

        hashes = {
            "passwd_sha256": sha256_text(passwd_text),
            "group_sha256": sha256_text(group_text),
            "authorized_keys_sha256": sha256_text(json.dumps(managed_authorized, sort_keys=True)),
            "sshd_config_sha256": sha256_text(sshd_config_text),
        }
        details = {
            "target_name": target["name"],
            "rendered_users": [user["username"] for user in users],
            "enabled_users": [user["username"] for user in enabled_users],
            "rendered_groups": [group["name"] for group in enabled_groups],
            "kube": kube_details,
        }
        revision = self.store.insert_render_revision(target["id"], action, hashes, details)
        return revision, details


class Worker(threading.Thread):
    daemon = True

    def __init__(self, store, reconciler):
        super().__init__(name="ssh-dashboard-worker")
        self.store = store
        self.reconciler = reconciler
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            try:
                claimed = self.store.claim_next_reconcile_run()
                if not claimed:
                    self.stop_event.wait(RECONCILE_POLL_INTERVAL)
                    continue
                revision = None
                details = {}
                try:
                    revision, details = self.reconciler.render(claimed["target_id"], claimed["requested_action"])
                    self.store.complete_reconcile_run(claimed["id"], revision["id"], details, success=True)
                except Exception as exc:
                    details["traceback"] = traceback.format_exc()
                    self.store.complete_reconcile_run(
                        claimed["id"],
                        revision["id"] if revision else None,
                        details,
                        success=False,
                        error_text=str(exc),
                    )
            except Exception:
                time.sleep(RECONCILE_POLL_INTERVAL)

    def stop(self):
        self.stop_event.set()


def html_page(title, body):
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 0; padding: 2rem; background: #f7f8fb; color: #1f2937; }}
    h1, h2, h3 {{ margin-top: 0; }}
    h4 {{ margin: 0 0 0.4rem; }}
    .grid {{ display: grid; gap: 1rem; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }}
      .layout {{ display: grid; gap: 1rem; grid-template-columns: minmax(400px, 540px) minmax(0, 1fr); align-items: start; }}
    .card {{ background: white; border-radius: 14px; padding: 1rem 1.25rem; box-shadow: 0 10px 28px rgba(15, 23, 42, 0.08); }}
    .subcard {{ background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 12px; padding: 0.9rem 1rem; margin-top: 0.9rem; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 0.45rem 0.4rem; border-bottom: 1px solid #e5e7eb; vertical-align: top; }}
    th {{ font-size: 0.88rem; color: #4b5563; }}
    code {{ background: #f3f4f6; padding: 0.1rem 0.3rem; border-radius: 4px; }}
    form {{ display: grid; gap: 0.55rem; margin-top: 0.75rem; }}
    input, select, textarea, button {{ font: inherit; padding: 0.55rem 0.65rem; border-radius: 8px; border: 1px solid #cbd5e1; }}
    textarea {{ min-height: 90px; }}
    button {{ background: #111827; color: white; cursor: pointer; }}
    button.secondary {{ background: #475569; }}
    .row-form {{ display: flex; gap: 0.5rem; flex-wrap: wrap; }}
    .muted {{ color: #6b7280; font-size: 0.92rem; }}
    .pill {{ display: inline-block; padding: 0.15rem 0.55rem; border-radius: 999px; background: #e0f2fe; color: #075985; font-size: 0.8rem; }}
    .pill.secondary {{ background: #e2e8f0; color: #334155; }}
    .danger {{ color: #991b1b; }}
    .menu {{ display: flex; gap: 0.65rem; flex-wrap: wrap; margin: 1.2rem 0 1rem; }}
    .menu a, .target-pills a, .user-list a {{ text-decoration: none; }}
    .menu a {{ padding: 0.55rem 0.85rem; border-radius: 999px; background: #e5e7eb; color: #334155; font-weight: 600; }}
    .menu a.active {{ background: #111827; color: white; }}
    .target-pills, .user-list {{ display: flex; gap: 0.55rem; flex-wrap: wrap; }}
    .target-pills a, .user-list a {{ padding: 0.45rem 0.75rem; border-radius: 999px; background: #f1f5f9; color: #334155; }}
    .target-pills a.active, .user-list a.active {{ background: #dbeafe; color: #1d4ed8; }}
    .label-row {{ display: flex; gap: 0.5rem; align-items: center; flex-wrap: wrap; }}
    .field-group {{ display: grid; gap: 0.3rem; }}
    .field-label {{ font-weight: 600; color: #334155; }}
    .field-help {{ color: #64748b; font-size: 0.88rem; line-height: 1.4; }}
    details > summary {{ cursor: pointer; color: #334155; font-weight: 600; }}
      @media (max-width: 1280px) {{
        .layout {{ grid-template-columns: minmax(360px, 460px) minmax(0, 1fr); }}
      }}
      @media (max-width: 980px) {{
        .layout {{ grid-template-columns: 1fr; }}
      }}
  </style>
</head>
<body>
{body}
</body>
</html>"""


def render_dashboard_html(payload, state):
    blocks = payload["targets"]
    block_by_target_id = {block["target"]["id"]: block for block in blocks}
    selected_target_id = int_from_query(state.get("target_id"))
    selected_block = block_by_target_id.get(selected_target_id)
    if not selected_block and blocks:
        selected_block = blocks[0]
        selected_target_id = selected_block["target"]["id"]

    allowed_sections = {"targets", "users", "groups", "services", "policies", "server", "reconcile", "storage"}
    section = state.get("section") or ("users" if selected_block else "targets")
    if section not in allowed_sections:
        section = "users" if selected_block else "targets"

    selected_target = selected_block["target"] if selected_block else None
    policies = selected_block["policies"] if selected_block else []
    users = selected_block["users"] if selected_block else []
    groups = selected_block["groups"] if selected_block else []
    published_services = selected_block["published_services"] if selected_block else []
    service_access_grants = selected_block["service_access_grants"] if selected_block else []
    keys_by_user = selected_block["keys_by_user"] if selected_block else {}
    settings = selected_block["server_settings"] if selected_block else {}
    user_query = (state.get("user_query") or "").strip()
    user_offset = max(int_from_query(state.get("user_offset"), 0) or 0, 0)
    filtered_users = [
        user for user in users
        if user_query.lower() in " ".join(
            [
                str(user.get("username") or ""),
                str(user.get("alias") or ""),
                str(user.get("comment") or ""),
                str(user.get("group_name") or ""),
            ]
        ).lower()
    ] if user_query else list(users)
    user_page_size = 25
    paged_users = filtered_users[user_offset:user_offset + user_page_size]
    selected_user = None
    selected_user_id = int_from_query(state.get("user_id"))
    if filtered_users:
        selected_user = next((user for user in filtered_users if user["id"] == selected_user_id), filtered_users[0])
        selected_user_id = selected_user["id"]
    else:
        selected_user_id = None

    selected_storage = state.get("storage") or "account"
    selected_storage_path = state.get("storage_path") or ""
    current_path = dashboard_path(
        selected_target_id,
        section,
        selected_user_id if section == "users" else None,
        storage=selected_storage if section == "storage" else None,
        storage_path=selected_storage_path if section == "storage" else None,
        extra={"user_query": user_query, "user_offset": user_offset} if section == "users" and (user_query or user_offset) else None,
    )

    def return_to_input():
        return f'<input type="hidden" name="return_to" value="{html.escape(current_path)}">'

    def policy_options(selected_policy_id=None, allow_empty=True):
        options = []
        if allow_empty:
            options.append('<option value="">Use user default policy</option>')
        for policy in policies:
            selected_attr = " selected" if selected_policy_id and policy["id"] == selected_policy_id else ""
            options.append(
                f'<option value="{policy["id"]}"{selected_attr}>{html.escape(policy["name"])}</option>'
            )
        return "".join(options)

    def group_options(selected_group_id=None, allow_private=True):
        options = []
        if allow_private:
            options.append('<option value="">Create or use a private group matching the username</option>')
        for group in groups:
            selected_attr = " selected" if selected_group_id and group["id"] == selected_group_id else ""
            options.append(
                f'<option value="{group["id"]}"{selected_attr}>{html.escape(group["name"])} (gid {group["gid"]})</option>'
            )
        return "".join(options)

    def user_options(selected_user_id=None):
        options = []
        for user in users:
            selected_attr = " selected" if selected_user_id and user["id"] == selected_user_id else ""
            ipv6_hint = username_to_ipv6_candidate(user["username"])
            label = user["username"] + (f" ({ipv6_hint})" if ipv6_hint else "")
            options.append(
                f'<option value="{user["id"]}"{selected_attr}>{html.escape(label)}</option>'
            )
        return "".join(options) or '<option value="">Create users first</option>'

    def service_options(selected_service_id=None):
        options = []
        for service in published_services:
            selected_attr = " selected" if selected_service_id and service["id"] == selected_service_id else ""
            label = (
                f"{service['alias']} - {service['owner_username']} "
                f"[{service['canonical_ipv6']}]:{service['port']}"
            )
            options.append(
                f'<option value="{service["id"]}"{selected_attr}>{html.escape(label)}</option>'
            )
        return "".join(options) or '<option value="">Register publishable services first</option>'

    def bool_checked(value):
        return "checked" if value else ""

    menu_html = "".join(
        f'<a class="{"active" if item == section else ""}" href="{html.escape(dashboard_path(selected_target_id, item, selected_user_id if item == "users" else None, storage=selected_storage if item == "storage" else None, storage_path=selected_storage_path if item == "storage" else None, extra={"user_query": user_query, "user_offset": user_offset} if item == "users" and (user_query or user_offset) else None))}">{html.escape(item.title())}</a>'
        for item in ["targets", "users", "groups", "services", "policies", "server", "reconcile", "storage"]
    )

    target_switcher = "".join(
        f'<a class="{"active" if block["target"]["id"] == selected_target_id else ""}" href="{html.escape(dashboard_path(block["target"]["id"], section if selected_block else "targets", None, extra={"user_query": user_query, "user_offset": 0} if section == "users" and user_query else None))}">{html.escape(block["target"]["name"])}</a>'
        for block in blocks
    ) or '<span class="muted">No targets configured yet.</span>'

    reconcile_runs = payload["reconcile_runs"]
    if selected_target:
        reconcile_runs = [run for run in reconcile_runs if run["target_name"] == selected_target["name"]]

    if section == "targets":
        selected_summary = ""
        if selected_target:
            selected_summary = f"""
            <section class="card">
              <h2>Selected Target</h2>
              <p class="muted">A target is one managed SSH service plus the mounted paths the dashboard reconciles for it. It tells the dashboard which Kubernetes workload owns the SSH server, where rendered account and home files live, and which SSH config source should be reloaded or restarted when settings change.</p>
              <table>
                <tbody>
                  <tr><th>Name</th><td>{html.escape(selected_target['name'])}</td></tr>
                  <tr><th>Namespace</th><td><code>{html.escape(selected_target['namespace'])}</code></td></tr>
                  <tr><th>Workload</th><td><code>{html.escape(selected_target['workload_kind'])}/{html.escape(selected_target['workload_name'])}</code></td></tr>
                  <tr><th>Selector</th><td><code>{html.escape(selected_target['workload_selector'])}</code></td></tr>
                  <tr><th>Account Root</th><td><code>{html.escape(selected_target['account_root_path'])}</code></td></tr>
                  <tr><th>Home Root</th><td><code>{html.escape(selected_target['home_root_path'])}</code></td></tr>
                  <tr><th>Users</th><td>{selected_target.get('user_count', 0)}</td></tr>
                  <tr><th>Groups</th><td>{selected_target.get('group_count', 0)}</td></tr>
                  <tr><th>Keys</th><td>{selected_target.get('key_count', 0)}</td></tr>
                  <tr><th>Runtime Root</th><td><code>{html.escape(selected_target.get('runtime_root_path') or '-')}</code></td></tr>
                </tbody>
              </table>
            </section>
            """
        section_body = f"""
        <div class="grid">
          {selected_summary}
          <section class="card">
            <h2>Add Or Update Target</h2>
            <p class="muted">These fields define one managed SSH service: which Kubernetes workload to act on, where rendered files are mounted, and which SSH configuration source belongs to that service.</p>
            <form method="post" action="/targets">
              {return_to_input()}
              <div class="field-group">
                <label class="field-label" for="target-name">Target name</label>
                <input id="target-name" name="name" value="{html.escape(selected_target['name'] if selected_target else '')}" placeholder="Target name">
                <div class="field-help">A friendly dashboard name for one SSH deployment.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-namespace">Namespace</label>
                <input id="target-namespace" name="namespace" value="{html.escape(selected_target['namespace'] if selected_target else 'mac-ssh-demo')}" placeholder="Namespace">
                <div class="field-help">The Kubernetes namespace that contains the SSH workload and its config.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-workload-kind">Workload kind</label>
                <input id="target-workload-kind" name="workload_kind" value="{html.escape(selected_target['workload_kind'] if selected_target else 'Deployment')}" placeholder="Workload kind">
                <div class="field-help">The workload object type to restart when a rollout is needed, such as <code>Deployment</code>.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-workload-name">Workload name</label>
                <input id="target-workload-name" name="workload_name" value="{html.escape(selected_target['workload_name'] if selected_target else 'portable-openssh-busybox')}" placeholder="Workload name">
                <div class="field-help">The exact Kubernetes workload name for this SSH service.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-selector">Label selector</label>
                <input id="target-selector" name="workload_selector" value="{html.escape(selected_target['workload_selector'] if selected_target else 'app=portable-openssh-busybox')}" placeholder="Label selector">
                <div class="field-help">Used to find the live pod when the dashboard reloads <code>sshd</code> without a rollout.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-account-root">Account root</label>
                <input id="target-account-root" name="account_root_path" value="{html.escape(selected_target['account_root_path'] if selected_target else '/mnt/targets/portable-openssh/etc')}" placeholder="Mounted account root">
                <div class="field-help">The mounted path where rendered account files such as <code>passwd</code> and <code>group</code> live.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-home-root">Home root</label>
                <input id="target-home-root" name="home_root_path" value="{html.escape(selected_target['home_root_path'] if selected_target else '/mnt/targets/portable-openssh/home')}" placeholder="Mounted home root">
                <div class="field-help">The mounted path where user home directories and <code>.ssh/authorized_keys</code> are rendered.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-runtime-root">Runtime root</label>
                <input id="target-runtime-root" name="runtime_root_path" value="{html.escape((selected_target.get('runtime_root_path') or '/mnt/targets/portable-openssh/runtime') if selected_target else '/mnt/targets/portable-openssh/runtime')}" placeholder="Mounted runtime root">
                <div class="field-help">An optional read-only mount for the seeded OpenSSH runtime bundle. It is mainly used by the Storage view.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-configmap-name">ConfigMap name</label>
                <input id="target-configmap-name" name="ssh_configmap_name" value="{html.escape((selected_target.get('ssh_configmap_name') or 'portable-openssh-etc') if selected_target else 'portable-openssh-etc')}" placeholder="ConfigMap name">
                <div class="field-help">The ConfigMap that stores this target's <code>sshd_config</code>.</div>
              </div>
              <div class="field-group">
                <label class="field-label" for="target-configmap-key">ConfigMap key</label>
                <input id="target-configmap-key" name="ssh_configmap_key" value="{html.escape((selected_target.get('ssh_configmap_key') or 'sshd_config') if selected_target else 'sshd_config')}" placeholder="ConfigMap key">
                <div class="field-help">The key inside that ConfigMap that contains the OpenSSH server configuration text.</div>
              </div>
              <button>Save target</button>
            </form>
          </section>
        </div>
        """
    elif section == "users":
        user_rows = "".join(
            f"""
            <tr>
              <td><a href="{html.escape(dashboard_path(selected_target_id, 'users', user['id'], extra={'user_query': user_query, 'user_offset': user_offset} if (user_query or user_offset) else None))}">{html.escape(user['username'])}</a></td>
              <td>{html.escape(user_display_alias(user) or '-')}</td>
              <td>{user_role_badges(user)}</td>
              <td>{user['uid']}</td>
              <td>{user['gid']}</td>
              <td>{html.escape(user.get('group_name') or '-')}</td>
              <td>{user['key_count']}</td>
              <td>{'enabled' if user['enabled'] else 'disabled'}</td>
            </tr>
            """
            for user in paged_users
        ) or '<tr><td colspan="8" class="muted">No users match the current search.</td></tr>'

        prev_offset = max(user_offset - user_page_size, 0)
        next_offset = user_offset + user_page_size
        showing_from = user_offset + 1 if filtered_users else 0
        showing_to = min(user_offset + user_page_size, len(filtered_users))
        user_pager = []
        if user_offset > 0:
            user_pager.append(
                f'<a href="{html.escape(dashboard_path(selected_target_id, "users", selected_user_id, extra={"user_query": user_query, "user_offset": prev_offset}))}">Previous</a>'
            )
        if next_offset < len(filtered_users):
            user_pager.append(
                f'<a href="{html.escape(dashboard_path(selected_target_id, "users", selected_user_id, extra={"user_query": user_query, "user_offset": next_offset}))}">Next</a>'
            )
        user_pager_html = " ".join(user_pager) or '<span class="muted">No more pages.</span>'

        selected_user_panel = '<section class="card"><h2>User Details</h2><p class="muted">Create a user first to manage their keypairs.</p></section>'
        if selected_user:
            key_forms = []
            for key_row in keys_by_user.get(selected_user["username"], []):
                key_forms.append(
                    f"""
                    <div class="subcard">
                      <div class="label-row">
                        <h4>{html.escape(key_row.get('label') or 'key')}</h4>
                        <span class="pill">{'enabled' if key_row['enabled'] else 'disabled'}</span>
                        {('<span class="pill secondary">generated</span>' if key_row.get('generated') else '')}
                      </div>
                      <form method="post" action="/keys">
                        {return_to_input()}
                        <input type="hidden" name="key_id" value="{key_row['id']}">
                        <input type="hidden" name="user_id" value="{selected_user['id']}">
                        <input name="label" value="{html.escape(key_row.get('label') or '')}" placeholder="Key label">
                        <select name="policy_profile_id">
                          {policy_options(key_row.get('policy_profile_id'))}
                        </select>
                        <textarea name="public_key" placeholder="ssh-ed25519 AAAA... comment">{html.escape(key_row['public_key'])}</textarea>
                        <details>
                          <summary>Stored private key</summary>
                          <textarea name="private_key" placeholder="Optional private key in OpenSSH format">{html.escape(key_row.get('private_key') or '')}</textarea>
                        </details>
                        <label><input type="checkbox" name="generated" {bool_checked(key_row.get('generated'))}> Generated keypair</label>
                        <label><input type="checkbox" name="enabled" {bool_checked(key_row['enabled'])}> Key enabled</label>
                        <button>Save key</button>
                      </form>
                      <div class="row-form">
                        <form method="post" action="/keys/toggle">
                          {return_to_input()}
                          <input type="hidden" name="key_id" value="{key_row['id']}">
                          <button class="secondary">Toggle</button>
                        </form>
                        <form method="post" action="/keys/delete">
                          {return_to_input()}
                          <input type="hidden" name="key_id" value="{key_row['id']}">
                          <button class="secondary">Delete</button>
                        </form>
                      </div>
                    </div>
                    """
                )

            bundle_access_grants = [
                grant for grant in service_access_grants
                if grant["grantee_user_id"] == selected_user["id"] and grant["enabled"]
            ]
            bundle_publishable_services = [
                service for service in published_services
                if service["owner_user_id"] == selected_user["id"] and service["enabled"]
            ]
            access_summary = "".join(
                f"<li><code>{html.escape(grant['service_alias'])}</code> at <code>[{html.escape(grant['service_canonical_ipv6'])}]:{grant['service_port']}</code></li>"
                for grant in bundle_access_grants
            ) or '<li class="muted">No accessible services registered for this identity.</li>'
            publish_summary = "".join(
                f"<li><code>{html.escape(service['alias'])}</code> at <code>[{html.escape(service['canonical_ipv6'])}]:{service['port']}</code></li>"
                for service in bundle_publishable_services
            ) or '<li class="muted">No publishable services registered for this identity.</li>'

            selected_user_panel = f"""
            <section class="card">
              <div class="label-row">
                <h2>{html.escape(selected_user['username'])}</h2>
                {f'<span class="pill secondary">{html.escape(user_display_alias(selected_user))}</span>' if user_display_alias(selected_user) else ''}
                {user_role_badges(selected_user, empty='no role flag')}
                <span class="pill">{'enabled' if selected_user['enabled'] else 'disabled'}</span>
                <span class="pill secondary">{selected_user['key_count']} keys</span>
              </div>
              <p class="muted">uid={selected_user['uid']} gid={selected_user['gid']} primary group=<code>{html.escape(selected_user.get('group_name') or '-')}</code> home=<code>{html.escape(selected_user['home_dir'])}</code> shell=<code>{html.escape(selected_user['shell'])}</code> default policy=<code>{html.escape(selected_user.get('default_policy_name') or '-')}</code></p>
              <div class="subcard">
                <h4>Identity Metadata</h4>
                <p class="muted">Aliases make canonical IPv6 usernames readable in dashboards and monitor views. They do not change the Linux username or the SSH identity.</p>
                <form method="post" action="/users">
                  {return_to_input()}
                  <input type="hidden" name="target_id" value="{selected_target_id or ''}">
                  <input type="hidden" name="username" value="{html.escape(selected_user['username'])}">
                  <input type="hidden" name="uid" value="{selected_user['uid']}">
                  <input type="hidden" name="gid" value="{selected_user['gid']}">
                  <input type="hidden" name="home_dir" value="{html.escape(selected_user['home_dir'])}">
                  <input type="hidden" name="shell" value="{html.escape(selected_user['shell'])}">
                  <input type="hidden" name="default_policy_profile_id" value="{html.escape(str(selected_user.get('default_policy_profile_id') or ''))}">
                  <input type="hidden" name="enabled" value="{'on' if selected_user['enabled'] else 'off'}">
                  <input name="alias" value="{html.escape(selected_user.get('alias') or '')}" placeholder="Friendly alias">
                  <input name="comment" value="{html.escape(selected_user.get('comment') or '')}" placeholder="Comment">
                  <label><input type="checkbox" name="is_iot_device" {bool_checked(selected_user.get('is_iot_device'))}> IoT device</label>
                  <label><input type="checkbox" name="is_iot_platform" {bool_checked(selected_user.get('is_iot_platform'))}> IoT platform</label>
                  <button>Save identity metadata</button>
                </form>
              </div>
              <div class="row-form">
                <form method="post" action="/users/toggle">
                  {return_to_input()}
                  <input type="hidden" name="user_id" value="{selected_user['id']}">
                  <button class="secondary">Toggle user</button>
                </form>
                <form method="post" action="/users/delete">
                  {return_to_input()}
                  <input type="hidden" name="user_id" value="{selected_user['id']}">
                  <button class="secondary">Delete user</button>
                </form>
                <form method="post" action="/keys/generate">
                  {return_to_input()}
                  <input type="hidden" name="user_id" value="{selected_user['id']}">
                  <button>Generate new Ed25519 keypair</button>
                </form>
              </div>
              <p class="muted">A default Ed25519 keypair is created automatically the first time a user is saved with no keys. Generated private keys stay stored in PostgreSQL so you can copy or replace them later.</p>
              {''.join(key_forms) or '<p class="muted">This user has no keys yet.</p>'}
              <div class="subcard">
                <h4>Add Manual Key</h4>
                <form method="post" action="/keys">
                  {return_to_input()}
                  <input type="hidden" name="user_id" value="{selected_user['id']}">
                  <input name="label" placeholder="Key label">
                  <select name="policy_profile_id">
                    {policy_options(None)}
                  </select>
                  <textarea name="public_key" placeholder="ssh-ed25519 AAAA... comment"></textarea>
                  <details>
                    <summary>Optional private key storage</summary>
                    <textarea name="private_key" placeholder="Optional private key in OpenSSH format"></textarea>
                  </details>
                  <label><input type="checkbox" name="enabled" checked> Key enabled</label>
                  <button>Add key</button>
                </form>
              </div>
              <div class="subcard">
                <h4>Endpoint Bundle</h4>
                <p class="muted">Generate either a runtime-image bundle for the shared external endpoint image, or a self-contained Linux bundle for direct extraction on a host. Both include the stored private key plus direct forwards for Service Access grants and reverse forwards for this identity's Publishable Services.</p>
                <div class="grid">
                  <div>
                    <h4>Service Access</h4>
                    <ul>{access_summary}</ul>
                  </div>
                  <div>
                    <h4>Publishable Services</h4>
                    <ul>{publish_summary}</ul>
                  </div>
                </div>
                <form method="get" action="/bundles/download">
                  <input type="hidden" name="user_id" value="{selected_user['id']}">
                  <div class="field-group">
                    <label class="field-label" for="bundle-format">Bundle format</label>
                    <select id="bundle-format" name="bundle_format">
                      <option value="runtime-image" {'selected' if (state.get('bundle_format') or DEFAULT_ENDPOINT_BUNDLE_FORMAT) == DEFAULT_ENDPOINT_BUNDLE_FORMAT else ''}>Runtime image bundle (Recommended)</option>
                      <option value="self-contained" {'selected' if (state.get('bundle_format') or DEFAULT_ENDPOINT_BUNDLE_FORMAT) == 'self-contained' else ''}>Self-contained Linux bundle</option>
                    </select>
                    <div class="field-help">Runtime image bundles are smaller and meant to be mounted into the reusable CMXsafe endpoint image. Self-contained bundles carry their own helper scripts under <code>bin/</code>.</div>
                  </div>
                  <div class="field-group">
                    <label class="field-label" for="bundle-gateway-host">Gateway host reachable by this endpoint</label>
                    <input id="bundle-gateway-host" name="gateway_host" value="127.0.0.1" placeholder="Gateway DNS name or IP">
                    <div class="field-help">For the local Docker Desktop test this can be the SSH port-forward host. For a real device, use the reachable CMXsafe gateway DNS name or IP. If you enter a DNS name without a real record, add a matching local hosts entry on the endpoint.</div>
                  </div>
                  <div class="field-group">
                    <label class="field-label" for="bundle-gateway-port">Gateway SSH port</label>
                    <input id="bundle-gateway-port" name="gateway_port" value="{html.escape(str(settings.get('listen_port', 2222)))}" placeholder="2222">
                  </div>
                  <div class="field-group">
                    <label class="field-label" for="bundle-ssh-bin">CMXsafe OpenSSH client path on endpoint</label>
                    <input id="bundle-ssh-bin" name="ssh_bin" value="{html.escape(state.get('ssh_bin') or ('/opt/openssh/bin/ssh' if (state.get('bundle_format') or DEFAULT_ENDPOINT_BUNDLE_FORMAT) == DEFAULT_ENDPOINT_BUNDLE_FORMAT else 'ssh'))}" placeholder="/opt/openssh/bin/ssh">
                    <div class="field-help">Use <code>/opt/openssh/bin/ssh</code> with the reusable endpoint image. For a self-contained bundle on a host, set this to the patched CMXsafe OpenSSH client path available on that endpoint.</div>
                  </div>
                  <button>Download endpoint bundle</button>
                </form>
              </div>
            </section>
            """

        section_body = f"""
        <div class="layout">
          <section class="card">
            <h2>User Explorer</h2>
            <p class="muted">Search and browse users here, then open one user at a time on the right to manage keys and account state.</p>
            <form method="get">
              <input type="hidden" name="target_id" value="{selected_target_id or ''}">
              <input type="hidden" name="section" value="users">
              <input name="user_query" value="{html.escape(user_query)}" placeholder="Search usernames, aliases, comments, or groups...">
              <button class="secondary">Search users</button>
            </form>
            <p class="muted">Showing {showing_from}-{showing_to} of {len(filtered_users)} matching users.</p>
            <table>
              <thead><tr><th>Username</th><th>Alias</th><th>Role</th><th>UID</th><th>GID</th><th>Group</th><th>Keys</th><th>Status</th></tr></thead>
              <tbody>{user_rows}</tbody>
            </table>
            <div class="row-form">{user_pager_html}</div>
            <div class="subcard">
              <h3>Add One User</h3>
              <form method="post" action="/users">
                {return_to_input()}
                <input type="hidden" name="target_id" value="{selected_target_id or ''}">
                <input name="username" placeholder="Username">
                <input name="alias" placeholder="Friendly alias, for example device-kitchen-01">
                <input name="uid" placeholder="UID (leave blank to auto-assign)">
                <select name="group_id">
                  {group_options(None, allow_private=True)}
                </select>
                <input name="home_dir" placeholder="/home/newuser (leave blank for /home/username)">
                <input name="shell" value="/bin/sh" placeholder="/bin/sh">
                <input name="comment" placeholder="Comment">
                <select name="default_policy_profile_id">
                  {policy_options(None, allow_empty=True)}
                </select>
                <label><input type="checkbox" name="is_iot_device" checked> IoT device</label>
                <label><input type="checkbox" name="is_iot_platform"> IoT platform</label>
                <label><input type="checkbox" name="enabled" checked> User enabled</label>
                <button>Save user and generate default keypair if needed</button>
              </form>
            </div>
            <div class="subcard">
              <h3>Batch Add Users</h3>
              <p class="muted">Upload a text file with one username per line. Each new user gets an automatic UID, a private group with an automatic GID, a home folder at <code>/home/&lt;username&gt;</code>, and a generated Ed25519 keypair.</p>
              <form method="post" action="/users/batch" enctype="multipart/form-data">
                {return_to_input()}
                <input type="hidden" name="target_id" value="{selected_target_id or ''}">
                <input type="file" name="user_file" accept=".txt,text/plain">
                <textarea name="user_batch_text" placeholder="Optional paste mode: one username per line"></textarea>
                <input name="shell" value="/bin/sh" placeholder="/bin/sh">
                <select name="default_policy_profile_id">
                  {policy_options(None, allow_empty=True)}
                </select>
                <label><input type="checkbox" name="is_iot_device" checked> Mark new users as IoT devices</label>
                <label><input type="checkbox" name="is_iot_platform"> Mark new users as IoT platforms</label>
                <button>Batch create users</button>
              </form>
            </div>
          </section>
          {selected_user_panel}
        </div>
        """
    elif section == "groups":
        group_rows = "".join(
            f"""
            <tr>
              <td>{html.escape(group['name'])}</td>
              <td>{group['gid']}</td>
              <td>{group['member_count']}</td>
              <td>{html.escape(group.get('members') or '-')}</td>
              <td>{'enabled' if group['enabled'] else 'disabled'}</td>
              <td>
                <div class="row-form">
                  <form method="post" action="/groups/toggle">
                    {return_to_input()}
                    <input type="hidden" name="group_id" value="{group['id']}">
                    <button class="secondary">Toggle</button>
                  </form>
                  <form method="post" action="/groups/delete">
                    {return_to_input()}
                    <input type="hidden" name="group_id" value="{group['id']}">
                    <button class="secondary">Delete</button>
                  </form>
                </div>
              </td>
            </tr>
            """
            for group in groups
        ) or '<tr><td colspan="6" class="muted">No groups yet.</td></tr>'
        section_body = f"""
        <div class="grid">
          <section class="card">
            <h2>Groups</h2>
            <p class="muted">Groups let several users share the same GID. Users can select one of these groups as their primary group instead of always getting a private group.</p>
            <table>
              <thead><tr><th>Name</th><th>GID</th><th>Members</th><th>Usernames</th><th>Status</th><th>Actions</th></tr></thead>
              <tbody>{group_rows}</tbody>
            </table>
          </section>
          <section class="card">
            <h2>Add Group</h2>
            <form method="post" action="/groups">
              {return_to_input()}
              <input type="hidden" name="target_id" value="{selected_target_id or ''}">
              <input name="name" placeholder="Group name">
              <input name="gid" placeholder="GID (leave blank to auto-assign)">
              <input name="comment" placeholder="Comment">
              <label><input type="checkbox" name="enabled" checked> Group enabled</label>
              <button>Save group</button>
            </form>
          </section>
        </div>
        """
    elif section == "services":
        service_rows = "".join(
            f"""
            <tr>
              <td>{html.escape(service['alias'])}</td>
              <td>{html.escape(service['owner_username'])}</td>
              <td><code>[{html.escape(service['canonical_ipv6'])}]:{service['port']}</code></td>
              <td>{html.escape(service.get('protocol') or 'tcp')}</td>
              <td>{service.get('enabled_grant_count', 0)}</td>
              <td>{html.escape(service.get('grantee_usernames') or '-')}</td>
              <td>{'enabled' if service['enabled'] else 'disabled'}</td>
              <td>
                <div class="row-form">
                  <form method="post" action="/published-services/toggle">
                    {return_to_input()}
                    <input type="hidden" name="service_id" value="{service['id']}">
                    <button class="secondary">Toggle</button>
                  </form>
                  <form method="post" action="/published-services/delete">
                    {return_to_input()}
                    <input type="hidden" name="service_id" value="{service['id']}">
                    <button class="secondary">Delete</button>
                  </form>
                </div>
              </td>
            </tr>
            """
            for service in published_services
        ) or '<tr><td colspan="8" class="muted">No publishable services registered yet.</td></tr>'

        grant_rows = "".join(
            f"""
            <tr>
              <td>{html.escape(grant.get('context_alias') or '-')}</td>
              <td>{html.escape(grant['grantee_username'])}</td>
              <td>{html.escape(grant['service_alias'])}</td>
              <td>{html.escape(grant['owner_username'])}</td>
              <td><code>[{html.escape(grant['service_canonical_ipv6'])}]:{grant['service_port']}</code></td>
              <td>{'enabled' if grant['enabled'] else 'disabled'}</td>
              <td>
                <div class="row-form">
                  <form method="post" action="/service-access/toggle">
                    {return_to_input()}
                    <input type="hidden" name="grant_id" value="{grant['id']}">
                    <button class="secondary">Toggle</button>
                  </form>
                  <form method="post" action="/service-access/delete">
                    {return_to_input()}
                    <input type="hidden" name="grant_id" value="{grant['id']}">
                    <button class="secondary">Delete</button>
                  </form>
                </div>
              </td>
            </tr>
            """
            for grant in service_access_grants
        ) or '<tr><td colspan="7" class="muted">No service access grants registered yet.</td></tr>'

        section_body = f"""
        <div class="grid">
          <section class="card">
            <h2>Publishable Services</h2>
            <p class="muted">A publishable service is a declared reverse-forwarding capability owned by any CMXsafe identity. This registers what an identity may expose; enforcement is intentionally not rendered into OpenSSH policy yet.</p>
            <table>
              <thead><tr><th>Alias</th><th>Owner</th><th>Canonical endpoint</th><th>Protocol</th><th>Access grants</th><th>Grantees</th><th>Status</th><th>Actions</th></tr></thead>
              <tbody>{service_rows}</tbody>
            </table>
            <div class="subcard">
              <h3>Register Publishable Service</h3>
              <form method="post" action="/published-services">
                {return_to_input()}
                <input type="hidden" name="target_id" value="{selected_target_id or ''}">
                <select name="owner_user_id">
                  {user_options(None)}
                </select>
                <input name="alias" placeholder="Service alias, for example iot-platform-monitor">
                <input name="canonical_ipv6" placeholder="Canonical IPv6 (blank uses 32-hex owner username when possible)">
                <input name="port" value="9000" placeholder="Service port">
                <input name="protocol" value="http" placeholder="Protocol label, for example http or tcp">
                <input name="description" placeholder="Description">
                <label><input type="checkbox" name="enabled" checked> Service declaration enabled</label>
                <button>Register publishable service</button>
              </form>
            </div>
          </section>
          <section class="card">
            <h2>Service Access</h2>
            <p class="muted">A service access grant declares that one identity may consume another identity's publishable service. For now this records the future Security Context; bundle generation can use it before OpenSSH enforcement is added.</p>
            <table>
              <thead><tr><th>Context</th><th>Consumer identity</th><th>Service</th><th>Publisher identity</th><th>Endpoint</th><th>Status</th><th>Actions</th></tr></thead>
              <tbody>{grant_rows}</tbody>
            </table>
            <div class="subcard">
              <h3>Register Service Access</h3>
              <form method="post" action="/service-access">
                {return_to_input()}
                <input type="hidden" name="target_id" value="{selected_target_id or ''}">
                <select name="grantee_user_id">
                  {user_options(None)}
                </select>
                <select name="service_id">
                  {service_options(None)}
                </select>
                <input name="context_alias" placeholder="Optional context alias, for example device-to-platform-monitor">
                <input name="description" placeholder="Description or future enforcement note">
                <label><input type="checkbox" name="enabled" checked> Access grant enabled</label>
                <button>Register service access</button>
              </form>
            </div>
          </section>
        </div>
        """
    elif section == "policies":
        policy_rows = "".join(
            f"<tr><td>{html.escape(policy['name'])}</td><td>{html.escape(policy.get('description') or '')}</td><td><code>{html.escape(policy.get('force_command') or '')}</code></td><td>{'yes' if policy['allow_port_forwarding'] else 'no'}</td><td>{'yes' if policy['allow_pty'] else 'no'}</td></tr>"
            for policy in policies
        ) or '<tr><td colspan="5" class="muted">No policies yet.</td></tr>'
        section_body = f"""
        <div class="grid">
          <section class="card">
            <h2>Policy Profiles</h2>
            <table>
              <thead><tr><th>Name</th><th>Description</th><th>Force command</th><th>Forwarding</th><th>PTY</th></tr></thead>
              <tbody>{policy_rows}</tbody>
            </table>
          </section>
          <section class="card">
            <h2>Add Policy Profile</h2>
            <form method="post" action="/policies">
              {return_to_input()}
              <input type="hidden" name="target_id" value="{selected_target_id or ''}">
              <input name="name" placeholder="Profile name">
              <input name="description" placeholder="Short description">
              <input name="force_command" value="/opt/ssh-policy/forward-only.sh" placeholder="Force command">
              <textarea name="permit_open_json" placeholder='["db.internal:5432"]'></textarea>
              <textarea name="permit_listen_json" placeholder='["127.0.0.1:8080"]'></textarea>
              <label><input type="checkbox" name="allow_port_forwarding" checked> Allow forwarding</label>
              <label><input type="checkbox" name="allow_pty"> Allow PTY</label>
              <label><input type="checkbox" name="allow_agent_forwarding"> Allow agent forwarding</label>
              <label><input type="checkbox" name="allow_x11_forwarding"> Allow X11 forwarding</label>
              <button>Save policy profile</button>
            </form>
          </section>
        </div>
        """
    elif section == "server":
        section_body = f"""
        <section class="card">
          <h2>Server Settings</h2>
          <p class="muted">These values render the target <code>sshd_config</code> and define the stable CMXsafe identity root. Use the reconcile menu afterward if you want the running pod to reload or restart.</p>
          <form method="post" action="/server-settings">
            {return_to_input()}
            <input type="hidden" name="target_id" value="{selected_target_id or ''}">
            <div class="field-group">
              <label class="field-label" for="server-canonical-gateway-mac">Canonical gateway MAC</label>
              <input id="server-canonical-gateway-mac" name="canonical_gateway_mac" value="{html.escape(settings.get('canonical_gateway_mac') or '')}" placeholder="f6:db:2b:39:78:94">
              <div class="field-help">Stable identity-root MAC used to craft canonical IPv6 usernames and explicit /128 identities. It is logical configuration, not the live MAC of whichever gateway replica accepted the SSH session.</div>
            </div>
            <input name="listen_port" value="{html.escape(str(settings.get('listen_port', 2222)))}" placeholder="Listen port">
            <input name="log_level" value="{html.escape(settings.get('log_level', 'VERBOSE'))}" placeholder="Log level">
            <label><input type="checkbox" name="allow_tcp_forwarding" {bool_checked(settings.get('allow_tcp_forwarding', True))}> Allow forwarding</label>
            <label><input type="checkbox" name="gateway_ports" {bool_checked(settings.get('gateway_ports', False))}> GatewayPorts clientspecified</label>
            <label><input type="checkbox" name="permit_tunnel" {bool_checked(settings.get('permit_tunnel', False))}> Permit tunnels</label>
            <label><input type="checkbox" name="x11_forwarding" {bool_checked(settings.get('x11_forwarding', False))}> X11 forwarding</label>
            <button>Save server settings</button>
          </form>
        </section>
        """
    elif section == "storage":
        storage_roots = {
            "account": ("Account PVC", selected_target.get("account_root_path") if selected_target else None),
            "home": ("Home PVC", selected_target.get("home_root_path") if selected_target else None),
            "runtime": ("Runtime PVC", selected_target.get("runtime_root_path") if selected_target else None),
        }
        if selected_storage not in storage_roots:
            selected_storage = "account"
        storage_label, storage_root = storage_roots[selected_storage]
        view = storage_view(storage_root, selected_storage_path)
        parent_relative = ""
        if view.get("requested_relative"):
            parent_relative = posixpath.dirname(view["requested_relative"])
            if parent_relative == ".":
                parent_relative = ""
        storage_tabs = "".join(
            f'<a class="{"active" if key == selected_storage else ""}" href="{html.escape(dashboard_path(selected_target_id, "storage", storage=key))}">{html.escape(label)}</a>'
            for key, (label, _root) in storage_roots.items()
            if _root
        ) or '<span class="muted">No mounted storage roots are configured for this target.</span>'

        entries_html = ""
        if view.get("exists") and view.get("is_dir"):
            if parent_relative != view.get("requested_relative", ""):
                entries_html += (
                    "<tr>"
                    f"<td><a href=\"{html.escape(dashboard_path(selected_target_id, 'storage', storage=selected_storage, storage_path=parent_relative))}\">..</a></td>"
                    "<td>directory</td><td>-</td><td>-</td><td>-</td>"
                    "</tr>"
                )
            for entry in view.get("entries", []):
                entry_link = dashboard_path(
                    selected_target_id,
                    "storage",
                    storage=selected_storage,
                    storage_path=entry["relative_path"],
                )
                entries_html += (
                    "<tr>"
                    f"<td><a href=\"{html.escape(entry_link)}\">{html.escape(entry['name'])}</a></td>"
                    f"<td>{'directory' if entry['is_dir'] else 'file'}</td>"
                    f"<td>{html.escape(entry['size_label'])}</td>"
                    f"<td><code>{html.escape(entry['mode'])}</code></td>"
                    f"<td>{html.escape(entry['modified_at'])}</td>"
                    "</tr>"
                )
            if not entries_html:
                entries_html = '<tr><td colspan="5" class="muted">Directory is empty.</td></tr>'

        preview_html = ""
        if view.get("exists") and not view.get("is_dir"):
            preview_label = "Text preview" if view.get("preview_kind") == "text" else "Binary preview (hex)"
            preview_html = f"""
            <div class="subcard">
              <h4>{preview_label}</h4>
              <p class="muted">Path <code>{html.escape(view['absolute_path'])}</code>, mode <code>{html.escape(view['mode'])}</code>, size {html.escape(view['size_label'])}.</p>
              <textarea readonly>{html.escape(view.get('preview_text') or '')}</textarea>
            </div>
            """

        error_html = ""
        if view.get("error"):
            error_html = f'<p class="danger">{html.escape(view["error"])}</p>'

        section_body = f"""
        <div class="grid">
          <section class="card">
            <h2>Mounted Storage</h2>
            <p class="muted">This is a read-only browser for the mounted PVC paths inside the dashboard pod, so you can verify what was really rendered and deployed.</p>
            <div class="target-pills">{storage_tabs}</div>
            <div class="subcard">
              <h4>{html.escape(storage_label)}</h4>
              <p class="muted">Configured root <code>{html.escape(storage_root or '-')}</code></p>
              {error_html}
              <table>
                <tbody>
                  <tr><th>Root</th><td><code>{html.escape(view.get('root_path') or storage_root or '-')}</code></td></tr>
                  <tr><th>Selected Path</th><td><code>{html.escape(view.get('absolute_path') or storage_root or '-')}</code></td></tr>
                  <tr><th>Relative Path</th><td><code>{html.escape(view.get('requested_relative') or '.')}</code></td></tr>
                  <tr><th>Kind</th><td>{'directory' if view.get('is_dir') else 'file' if view.get('exists') else '-'}</td></tr>
                  <tr><th>Mode</th><td><code>{html.escape(view.get('mode') or '-')}</code></td></tr>
                  <tr><th>Size</th><td>{html.escape(view.get('size_label') or '-')}</td></tr>
                </tbody>
              </table>
              {preview_html}
            </div>
          </section>
          <section class="card">
            <h2>Directory Listing</h2>
            <table>
              <thead><tr><th>Name</th><th>Kind</th><th>Size</th><th>Mode</th><th>Modified</th></tr></thead>
              <tbody>{entries_html if entries_html else '<tr><td colspan="5" class="muted">Select a directory to browse.</td></tr>'}</tbody>
            </table>
          </section>
        </div>
        """
    else:
        reconcile_rows = "".join(
            f"<tr><td>{run['id']}</td><td>{html.escape(run['requested_action'])}</td><td>{html.escape(run['status'])}</td><td>{html.escape(run.get('started_at') or '-')}</td><td>{html.escape(run.get('finished_at') or '-')}</td><td class='danger'>{html.escape(run.get('error_text') or '')}</td></tr>"
            for run in reconcile_runs
        ) or "<tr><td colspan='6' class='muted'>No reconcile runs yet.</td></tr>"
        section_body = f"""
        <div class="grid">
          <section class="card">
            <h2>Queue Reconcile</h2>
            <p class="muted">Use <code>RENDER_ONLY</code> for live user and key changes. Reload for <code>sshd_config</code> changes. Restart for bigger rotations.</p>
            <form method="post" action="/reconcile">
              {return_to_input()}
              <input type="hidden" name="target_id" value="{selected_target_id or ''}">
              <select name="requested_action">
                <option value="INITIALIZE">INITIALIZE</option>
                <option value="RENDER_ONLY">RENDER_ONLY</option>
                <option value="RENDER_AND_RELOAD">RENDER_AND_RELOAD</option>
                <option value="RENDER_AND_RESTART">RENDER_AND_RESTART</option>
              </select>
              <button>Queue reconcile run</button>
            </form>
          </section>
          <section class="card">
            <h2>Recent Reconcile Runs</h2>
            <table>
              <thead><tr><th>ID</th><th>Action</th><th>Status</th><th>Started</th><th>Finished</th><th>Error</th></tr></thead>
              <tbody>{reconcile_rows}</tbody>
            </table>
          </section>
        </div>
        """

    target_context = ""
    if selected_target:
        target_context = f"""
        <section class="card">
          <div class="label-row">
            <h2>{html.escape(selected_target['name'])}</h2>
            <span class="pill secondary">{selected_target.get('user_count', 0)} users</span>
            <span class="pill secondary">{selected_target.get('group_count', 0)} groups</span>
            <span class="pill secondary">{selected_target.get('key_count', 0)} keys</span>
            <span class="pill secondary">{len(published_services)} services</span>
            <span class="pill secondary">{len(service_access_grants)} access grants</span>
            {f'<span class="pill secondary">canonical gateway MAC {html.escape(settings.get("canonical_gateway_mac"))}</span>' if settings.get("canonical_gateway_mac") else ''}
          </div>
          <p class="muted">Namespace <code>{html.escape(selected_target['namespace'])}</code>, workload <code>{html.escape(selected_target['workload_kind'])}/{html.escape(selected_target['workload_name'])}</code>, selector <code>{html.escape(selected_target['workload_selector'])}</code>.</p>
        </section>
        """

    body = f"""
    <h1>Portable OpenSSH Dashboard</h1>
    <p class="muted">Users now get a stored default Ed25519 keypair automatically when they are first created without keys. The page is also split into menu-driven sections so you only load the part you are managing right now.</p>
    <section class="card">
      <h2>Targets</h2>
      <p class="muted">Choose the SSH target first, then open the section you want to work in.</p>
      <div class="target-pills">{target_switcher}</div>
      <nav class="menu">{menu_html}</nav>
    </section>
    {target_context}
    {section_body}
    """
    return html_page("Portable OpenSSH Dashboard", body)


class Handler(BaseHTTPRequestHandler):
    store = None

    def do_GET(self):
        parsed = urlparse(self.path)
        state = {key: values[-1] for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
        if parsed.path == "/healthz":
            return self.send_json(HTTPStatus.OK, {"ok": True})
        if parsed.path == "/api/targets":
            return self.send_json(HTTPStatus.OK, self.serialize_dashboard())
        if parsed.path == "/api/topology":
            return self.send_json(HTTPStatus.OK, self.serialize_topology())
        if parsed.path == "/bundles/download":
            try:
                filename, bundle = self.store.build_endpoint_bundle(
                    int(required(state, "user_id")),
                    required(state, "gateway_host"),
                    gateway_port=state.get("gateway_port"),
                    ssh_bin=state.get("ssh_bin") or ("/opt/openssh/bin/ssh" if normalize_endpoint_bundle_format(state.get("bundle_format")) == DEFAULT_ENDPOINT_BUNDLE_FORMAT else "ssh"),
                    endpoint_iface=state.get("endpoint_iface") or "cmx0",
                    bundle_format=state.get("bundle_format") or DEFAULT_ENDPOINT_BUNDLE_FORMAT,
                )
            except Exception as exc:
                return self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return self.send_bytes(
                HTTPStatus.OK,
                bundle,
                "application/gzip",
                filename=filename,
            )
        if parsed.path == "/":
            return self.send_html(HTTPStatus.OK, render_dashboard_html(self.serialize_dashboard(), state))
        return self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self):
        parsed = urlparse(self.path)
        form, files = self.read_form_body()
        try:
            if parsed.path == "/targets":
                target = self.store.upsert_target(
                    {
                        "name": required(form, "name"),
                        "namespace": required(form, "namespace"),
                        "workload_kind": form.get("workload_kind", "Deployment"),
                        "workload_name": required(form, "workload_name"),
                        "workload_selector": required(form, "workload_selector"),
                        "account_root_path": required(form, "account_root_path"),
                        "home_root_path": required(form, "home_root_path"),
                        "runtime_root_path": form.get("runtime_root_path"),
                        "ssh_configmap_name": form.get("ssh_configmap_name"),
                        "ssh_configmap_key": form.get("ssh_configmap_key", "sshd_config"),
                    }
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(target["id"], "targets"))
            if parsed.path == "/groups":
                group = self.store.upsert_group(
                    {
                        "group_id": form.get("group_id"),
                        "target_id": required(form, "target_id"),
                        "name": required(form, "name"),
                        "gid": form.get("gid"),
                        "comment": form.get("comment"),
                        "enabled": bool_from_form(form, "enabled", True),
                    }
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(group["target_id"], "groups"))
            if parsed.path == "/groups/toggle":
                group = self.store.toggle_group(int(required(form, "group_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(group["target_id"], "groups"))
            if parsed.path == "/groups/delete":
                group = self.store.get_group(int(required(form, "group_id")))
                self.store.delete_group(int(required(form, "group_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(group["target_id"], "groups"))
            if parsed.path == "/published-services":
                service = self.store.upsert_published_service(
                    {
                        "service_id": form.get("service_id"),
                        "target_id": required(form, "target_id"),
                        "owner_user_id": required(form, "owner_user_id"),
                        "alias": required(form, "alias"),
                        "canonical_ipv6": form.get("canonical_ipv6"),
                        "port": required(form, "port"),
                        "protocol": form.get("protocol", "http"),
                        "description": form.get("description"),
                        "enabled": bool_from_form(form, "enabled", True),
                    }
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(service["target_id"], "services"))
            if parsed.path == "/published-services/toggle":
                service = self.store.toggle_published_service(int(required(form, "service_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(service["target_id"], "services"))
            if parsed.path == "/published-services/delete":
                service = self.store.get_published_service(int(required(form, "service_id")))
                self.store.delete_published_service(int(required(form, "service_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(service["target_id"], "services"))
            if parsed.path == "/service-access":
                grant = self.store.upsert_service_access_grant(
                    {
                        "grant_id": form.get("grant_id"),
                        "target_id": required(form, "target_id"),
                        "service_id": required(form, "service_id"),
                        "grantee_user_id": required(form, "grantee_user_id"),
                        "context_alias": form.get("context_alias"),
                        "description": form.get("description"),
                        "enabled": bool_from_form(form, "enabled", True),
                    }
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(grant["target_id"], "services"))
            if parsed.path == "/service-access/toggle":
                grant = self.store.toggle_service_access_grant(int(required(form, "grant_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(grant["target_id"], "services"))
            if parsed.path == "/service-access/delete":
                grant = self.store.get_service_access_grant(int(required(form, "grant_id")))
                self.store.delete_service_access_grant(int(required(form, "grant_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(grant["target_id"], "services"))
            if parsed.path == "/policies":
                self.store.upsert_policy_profile(
                    int(required(form, "target_id")),
                    {
                        "name": required(form, "name"),
                        "description": form.get("description"),
                        "force_command": form.get("force_command"),
                        "allow_port_forwarding": bool_from_form(form, "allow_port_forwarding", True),
                        "allow_pty": bool_from_form(form, "allow_pty", False),
                        "allow_agent_forwarding": bool_from_form(form, "allow_agent_forwarding", False),
                        "allow_x11_forwarding": bool_from_form(form, "allow_x11_forwarding", False),
                        "permit_open_json": form.get("permit_open_json", "[]"),
                        "permit_listen_json": form.get("permit_listen_json", "[]"),
                    },
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(int(required(form, "target_id")), "policies"))
            if parsed.path == "/users":
                username = required(form, "username")
                user_row = self.store.upsert_user(
                    {
                        "target_id": required(form, "target_id"),
                        "username": username,
                        "alias": form.get("alias"),
                        "uid": form.get("uid"),
                        "group_id": form.get("group_id"),
                        "gid": form.get("gid"),
                        "home_dir": form.get("home_dir", f"/home/{username}"),
                        "shell": form.get("shell", "/bin/sh"),
                        "comment": form.get("comment"),
                        "is_iot_device": bool_from_form(form, "is_iot_device", False),
                        "is_iot_platform": bool_from_form(form, "is_iot_platform", False),
                        "default_policy_profile_id": form.get("default_policy_profile_id"),
                        "enabled": bool_from_form(form, "enabled", True),
                    }
                )
                if not self.store.list_public_keys_for_user(user_row["id"]):
                    target_row = self.store.get_target(user_row["target_id"])
                    self.store.create_generated_keypair_for_user(user_row, target_row)
                return self.redirect_to(dashboard_path(user_row["target_id"], "users", user_row["id"]))
            if parsed.path == "/users/batch":
                file_blob = files.get("user_file", {}).get("content", b"")
                pasted_text = form.get("user_batch_text", "")
                raw_text = pasted_text
                if file_blob:
                    raw_text = file_blob.decode("utf-8", errors="replace")
                usernames = [line.strip() for line in raw_text.splitlines() if line.strip()]
                if not usernames:
                    raise ValueError("batch upload did not contain any usernames")
                result = self.store.batch_create_users(
                    int(required(form, "target_id")),
                    usernames,
                    default_policy_profile_id=form.get("default_policy_profile_id"),
                    shell=form.get("shell", "/bin/sh"),
                    is_iot_device=bool_from_form(form, "is_iot_device", True),
                    is_iot_platform=bool_from_form(form, "is_iot_platform", False),
                )
                first_created = result["created"][0]["id"] if result["created"] else None
                return self.redirect_to(
                    form.get("return_to")
                    or dashboard_path(int(required(form, "target_id")), "users", first_created)
                )
            if parsed.path == "/users/toggle":
                user_row = self.store.toggle_user(int(required(form, "user_id")))
                return self.redirect_to(form.get("return_to") or dashboard_path(user_row["target_id"], "users", user_row["id"]))
            if parsed.path == "/users/delete":
                deleted_user = self.store.get_user(int(required(form, "user_id")))
                self.store.delete_user(int(required(form, "user_id")))
                target_id = deleted_user["target_id"] if deleted_user else None
                return self.redirect_to(form.get("return_to") or dashboard_path(target_id, "users"))
            if parsed.path == "/keys":
                key_row = self.store.upsert_public_key(
                    {
                        "key_id": form.get("key_id"),
                        "user_id": required(form, "user_id"),
                        "label": form.get("label"),
                        "public_key": required(form, "public_key"),
                        "private_key": form.get("private_key"),
                        "policy_profile_id": form.get("policy_profile_id"),
                        "generated": bool_from_form(form, "generated", False),
                        "enabled": bool_from_form(form, "enabled", True),
                    }
                )
                user_row = self.store.get_user(key_row["user_id"])
                return self.redirect_to(form.get("return_to") or dashboard_path(user_row["target_id"], "users", user_row["id"]))
            if parsed.path == "/keys/generate":
                user_row = self.store.get_user(int(required(form, "user_id")))
                target_row = self.store.get_target(user_row["target_id"])
                self.store.create_generated_keypair_for_user(user_row, target_row)
                return self.redirect_to(form.get("return_to") or dashboard_path(user_row["target_id"], "users", user_row["id"]))
            if parsed.path == "/keys/toggle":
                key_row = self.store.toggle_public_key(int(required(form, "key_id")))
                user_row = self.store.get_user(key_row["user_id"])
                return self.redirect_to(form.get("return_to") or dashboard_path(user_row["target_id"], "users", user_row["id"]))
            if parsed.path == "/keys/delete":
                key_id = int(required(form, "key_id"))
                key_row = self.store.get_public_key(key_id)
                self.store.delete_public_key(key_id)
                if key_row:
                    user_row = self.store.get_user(key_row["user_id"])
                    return self.redirect_to(form.get("return_to") or dashboard_path(user_row["target_id"], "users", user_row["id"]))
                return self.redirect_to(form.get("return_to"))
            if parsed.path == "/server-settings":
                self.store.upsert_server_settings(
                    int(required(form, "target_id")),
                    {
                        "canonical_gateway_mac": form.get("canonical_gateway_mac"),
                        "listen_port": required(form, "listen_port"),
                        "allow_tcp_forwarding": bool_from_form(form, "allow_tcp_forwarding", True),
                        "gateway_ports": bool_from_form(form, "gateway_ports", False),
                        "permit_tunnel": bool_from_form(form, "permit_tunnel", False),
                        "x11_forwarding": bool_from_form(form, "x11_forwarding", False),
                        "log_level": form.get("log_level", "VERBOSE"),
                    },
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(int(required(form, "target_id")), "server"))
            if parsed.path == "/reconcile":
                self.store.queue_reconcile(
                    int(required(form, "target_id")),
                    required(form, "requested_action"),
                    requested_by="dashboard-ui",
                )
                return self.redirect_to(form.get("return_to") or dashboard_path(int(required(form, "target_id")), "reconcile"))
        except Exception as exc:
            return self.send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        return self.send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def read_form_body(self):
        content_type = self.headers.get("Content-Type", "")
        if content_type.startswith("multipart/form-data"):
            form = {}
            files = {}
            field_storage = cgi.FieldStorage(
                fp=self.rfile,
                headers=self.headers,
                environ={
                    "REQUEST_METHOD": "POST",
                    "CONTENT_TYPE": content_type,
                    "CONTENT_LENGTH": self.headers.get("Content-Length", "0"),
                },
                keep_blank_values=True,
            )
            for item in field_storage.list or []:
                if item.filename:
                    files[item.name] = {
                        "filename": item.filename,
                        "content": item.file.read(),
                    }
                else:
                    form[item.name] = item.value
            return form, files
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        parsed = parse_qs(raw, keep_blank_values=True)
        form = {key: values[-1] if len(values) == 1 else values for key, values in parsed.items()}
        return form, {}

    def serialize_dashboard(self):
        targets = self.store.list_targets()
        payload = []
        for target in targets:
            policies = self.store.list_policy_profiles(target["id"])
            users = self.store.list_users(target["id"])
            groups = self.store.list_groups(target["id"])
            published_services = self.store.list_published_services(target["id"])
            service_access_grants = self.store.list_service_access_grants(target["id"])
            keys = self.store.list_public_keys(target["id"])
            settings = self.store.get_server_settings(target["id"]) or {}
            keys_by_user = {}
            for key in keys:
                keys_by_user.setdefault(key["username"], []).append(key)
            payload.append(
                {
                    "target": target,
                    "policies": policies,
                    "users": users,
                    "groups": groups,
                    "published_services": published_services,
                    "service_access_grants": service_access_grants,
                    "keys_by_user": keys_by_user,
                    "server_settings": settings,
                }
            )
        return {"targets": payload, "reconcile_runs": self.store.list_reconcile_runs()}

    def identity_for_topology(self, user):
        canonical_ipv6 = user_canonical_ipv6(user)
        alias = user_display_alias(user)
        return {
            "id": user["id"],
            "target_id": user["target_id"],
            "username": user["username"],
            "alias": alias,
            "display_name": alias or user["username"],
            "canonical_ipv6": canonical_ipv6,
            "is_iot_device": bool(user.get("is_iot_device")),
            "is_iot_platform": bool(user.get("is_iot_platform")),
            "role_labels": user_role_labels(user),
            "uid": user["uid"],
            "gid": user["gid"],
            "group_name": user.get("group_name"),
            "comment": user.get("comment"),
            "enabled": bool(user["enabled"]),
        }

    def serialize_topology(self):
        targets = self.store.list_targets()
        payload = []
        for target in targets:
            users = self.store.list_users(target["id"])
            user_by_id = {user["id"]: user for user in users}
            identities = [self.identity_for_topology(user) for user in users]
            identity_by_id = {identity["id"]: identity for identity in identities}
            services = []
            service_by_id = {}
            for service in self.store.list_published_services(target["id"]):
                owner_identity = identity_by_id.get(service["owner_user_id"])
                row = {
                    "id": service["id"],
                    "target_id": service["target_id"],
                    "owner_user_id": service["owner_user_id"],
                    "owner_username": service["owner_username"],
                    "owner_alias": owner_identity.get("alias") if owner_identity else "",
                    "owner_display_name": owner_identity.get("display_name") if owner_identity else service["owner_username"],
                    "alias": service["alias"],
                    "protocol": service.get("protocol") or "tcp",
                    "canonical_ipv6": maybe_normalize_ipv6_text(service["canonical_ipv6"]) or service["canonical_ipv6"],
                    "port": int(service["port"]),
                    "description": service.get("description"),
                    "enabled": bool(service["enabled"]),
                    "enabled_grant_count": int(service.get("enabled_grant_count") or 0),
                    "grantee_usernames": service.get("grantee_usernames") or "",
                }
                services.append(row)
                service_by_id[row["id"]] = row

            grants = []
            registered_paths = []
            for grant in self.store.list_service_access_grants(target["id"]):
                service = service_by_id.get(grant["service_id"])
                consumer = identity_by_id.get(grant["grantee_user_id"])
                owner_user = user_by_id.get(service["owner_user_id"]) if service else None
                publisher = identity_by_id.get(owner_user["id"]) if owner_user else None
                grant_row = {
                    "id": grant["id"],
                    "target_id": grant["target_id"],
                    "service_id": grant["service_id"],
                    "grantee_user_id": grant["grantee_user_id"],
                    "context_alias": grant.get("context_alias"),
                    "description": grant.get("description"),
                    "enabled": bool(grant["enabled"]),
                    "service_alias": grant["service_alias"],
                    "service_canonical_ipv6": maybe_normalize_ipv6_text(grant["service_canonical_ipv6"]) or grant["service_canonical_ipv6"],
                    "service_port": int(grant["service_port"]),
                    "service_protocol": grant.get("service_protocol") or "tcp",
                    "owner_username": grant["owner_username"],
                    "grantee_username": grant["grantee_username"],
                    "grantee_alias": consumer.get("alias") if consumer else "",
                    "owner_alias": publisher.get("alias") if publisher else "",
                }
                grants.append(grant_row)
                if not service or not consumer or not publisher:
                    continue
                path_enabled = bool(grant["enabled"]) and bool(service["enabled"]) and bool(consumer["enabled"]) and bool(publisher["enabled"])
                registered_paths.append(
                    {
                        "id": f"{target['id']}:{grant['id']}",
                        "target_id": target["id"],
                        "context_alias": grant.get("context_alias") or f"{consumer['display_name']} to {service['alias']}",
                        "enabled": path_enabled,
                        "state": "registered-enabled" if path_enabled else "registered-disabled",
                        "consumer": consumer,
                        "publisher": publisher,
                        "service": service,
                    }
                )

            payload.append(
                {
                    "target": {
                        "id": target["id"],
                        "name": target["name"],
                        "namespace": target["namespace"],
                        "workload_kind": target["workload_kind"],
                        "workload_name": target["workload_name"],
                        "enabled": bool(target["enabled"]),
                    },
                    "identities": identities,
                    "published_services": services,
                    "service_access_grants": grants,
                    "registered_paths": registered_paths,
                }
            )
        return {"generated_at": now_iso(), "targets": payload}

    def send_json(self, status, payload):
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, status, body):
        payload = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def send_bytes(self, status, payload, content_type, filename=None):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.end_headers()
        self.wfile.write(payload)

    def redirect_to(self, location):
        self.send_response(HTTPStatus.SEE_OTHER)
        self.send_header("Location", safe_redirect_path(location))
        self.end_headers()

    def log_message(self, format_, *args):
        return


def main():
    store = Store(DB_DSN)
    store.bootstrap_sample()
    kube = KubernetesHelper()
    reconciler = Reconciler(store, kube)
    worker = Worker(store, reconciler)
    worker.start()
    Handler.store = store
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    finally:
        worker.stop()
        server.server_close()


if __name__ == "__main__":
    main()

