#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional, Set


DEFAULT_SOCKET_PATH = "/var/run/cmxsafe-endpointd.sock"
DEFAULT_IFACE = "cmx0"
DEFAULT_REAP_INTERVAL = 30.0
REQUEST_ENCODING = "utf-8"
PID_RE = re.compile(r"(?:^|:)pid:(\d+)(?:$|:)")


def _normalise_ipv6(value: str) -> str:
    return ipaddress.IPv6Address(value).exploded.lower()


def _run_ip(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["ip", *args],
        text=True,
        capture_output=True,
        check=False,
    )


def _pid_from_owner(owner_id: str) -> Optional[int]:
    match = PID_RE.search(owner_id)
    if not match:
        return None
    return int(match.group(1))


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@dataclass
class OwnerEntry:
    owner_id: str
    scope: str
    addresses: Set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    last_seen_at: float = field(default_factory=time.time)
    pid: Optional[int] = None


@dataclass
class AddressEntry:
    ipv6: str
    present_on_iface: bool = False
    self_refcount: int = 0
    peer_refcount: int = 0
    owners: Set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    last_used_at: float = field(default_factory=time.time)

    @property
    def refcount(self) -> int:
        return self.self_refcount + self.peer_refcount


class EndpointState:
    def __init__(self, iface_name: str) -> None:
        self.iface_name = iface_name
        self.iface_created_by_daemon = False
        self.iface_up = False
        self.iface_last_refresh_at = 0.0
        self.addresses: Dict[str, AddressEntry] = {}
        self.owners: Dict[str, OwnerEntry] = {}

    def ensure_iface(self) -> None:
        show = _run_ip("link", "show", "dev", self.iface_name)
        if show.returncode != 0:
            create = _run_ip("link", "add", self.iface_name, "type", "dummy")
            if create.returncode != 0:
                raise RuntimeError(create.stderr.strip() or create.stdout.strip() or "failed to create dummy interface")
            self.iface_created_by_daemon = True
        up = _run_ip("link", "set", self.iface_name, "up")
        if up.returncode != 0:
            raise RuntimeError(up.stderr.strip() or up.stdout.strip() or "failed to bring dummy interface up")
        self.iface_up = True
        self.iface_last_refresh_at = time.time()

    def ensure_address(self, scope: str, owner_id: str, ipv6: str) -> Dict[str, object]:
        if scope not in {"self", "peer"}:
            raise ValueError("invalid scope")
        ipv6 = _normalise_ipv6(ipv6)
        self.ensure_iface()
        entry = self.addresses.get(ipv6)
        created = False
        if entry is None:
            entry = AddressEntry(ipv6=ipv6)
            self.addresses[ipv6] = entry
            created = True
        if owner_id not in self.owners:
            self.owners[owner_id] = OwnerEntry(
                owner_id=owner_id,
                scope=scope,
                pid=_pid_from_owner(owner_id),
            )
        owner = self.owners[owner_id]
        owner.last_seen_at = time.time()
        if ipv6 not in owner.addresses:
            owner.addresses.add(ipv6)
            if scope == "self":
                entry.self_refcount += 1
            else:
                entry.peer_refcount += 1
            entry.owners.add(owner_id)
        add = _run_ip("-6", "addr", "add", f"{ipv6}/128", "dev", self.iface_name)
        if add.returncode == 0:
            entry.present_on_iface = True
        else:
            stderr = (add.stderr or add.stdout or "").strip()
            if "File exists" in stderr:
                entry.present_on_iface = True
            else:
                raise RuntimeError(stderr or f"failed to add {ipv6}/128 to {self.iface_name}")
        entry.last_used_at = time.time()
        return {
            "created": created,
            "refcount": entry.refcount,
            "ipv6": ipv6,
        }

    def release_address(self, scope: str, owner_id: str, ipv6: str) -> Dict[str, object]:
        if scope not in {"self", "peer"}:
            raise ValueError("invalid scope")
        ipv6 = _normalise_ipv6(ipv6)
        entry = self.addresses.get(ipv6)
        owner = self.owners.get(owner_id)
        if entry is None or owner is None or ipv6 not in owner.addresses:
            return {"released": False, "refcount": entry.refcount if entry else 0, "ipv6": ipv6}
        owner.addresses.discard(ipv6)
        owner.last_seen_at = time.time()
        if scope == "self" and entry.self_refcount > 0:
            entry.self_refcount -= 1
        elif scope == "peer" and entry.peer_refcount > 0:
            entry.peer_refcount -= 1
        entry.owners.discard(owner_id)
        entry.last_used_at = time.time()
        if not owner.addresses:
            self.owners.pop(owner_id, None)
        if entry.refcount == 0:
            remove = _run_ip("-6", "addr", "del", f"{ipv6}/128", "dev", self.iface_name)
            stderr = (remove.stderr or remove.stdout or "").strip()
            if remove.returncode == 0 or "Cannot assign requested address" in stderr:
                entry.present_on_iface = False
                self.addresses.pop(ipv6, None)
            else:
                raise RuntimeError(stderr or f"failed to delete {ipv6}/128 from {self.iface_name}")
        return {
            "released": True,
            "refcount": entry.refcount if ipv6 in self.addresses else 0,
            "ipv6": ipv6,
        }

    def reap(self) -> Dict[str, object]:
        released = 0
        stale_owners = []
        for owner_id, owner in list(self.owners.items()):
            if owner.pid is None:
                continue
            if _pid_alive(owner.pid):
                continue
            stale_owners.append(owner_id)
            for ipv6 in list(owner.addresses):
                scope = owner.scope if owner.scope in {"self", "peer"} else "peer"
                result = self.release_address(scope, owner_id, ipv6)
                if result.get("released"):
                    released += 1
        return {
            "stale_owners": stale_owners,
            "released_addresses": released,
        }

    def dump(self) -> Dict[str, object]:
        return {
            "iface": {
                "iface_name": self.iface_name,
                "created_by_daemon": self.iface_created_by_daemon,
                "is_up": self.iface_up,
                "last_refresh_at": self.iface_last_refresh_at,
            },
            "addresses": {
                ipv6: {
                    "present_on_iface": entry.present_on_iface,
                    "self_refcount": entry.self_refcount,
                    "peer_refcount": entry.peer_refcount,
                    "owners": sorted(entry.owners),
                    "created_at": entry.created_at,
                    "last_used_at": entry.last_used_at,
                }
                for ipv6, entry in sorted(self.addresses.items())
            },
            "owners": {
                owner_id: {
                    "scope": entry.scope,
                    "addresses": sorted(entry.addresses),
                    "created_at": entry.created_at,
                    "last_seen_at": entry.last_seen_at,
                    "pid": entry.pid,
                }
                for owner_id, entry in sorted(self.owners.items())
            },
        }


def _parse_request(line: str) -> Dict[str, str]:
    parts = line.rstrip("\n").split("\t")
    if not parts or not parts[0]:
        raise ValueError("empty request")
    op = parts[0]
    if op == "ping":
        return {"op": "ping"}
    if op == "dump":
        return {"op": "dump"}
    if op == "reap":
        return {"op": "reap"}
    if op in {"ensure", "release"}:
        if len(parts) != 4:
            raise ValueError(f"{op} requires scope, owner_id, ipv6")
        return {
            "op": op,
            "scope": parts[1],
            "owner_id": parts[2],
            "ipv6": parts[3],
        }
    raise ValueError(f"unknown operation: {op}")


def _handle_request(state: EndpointState, request: Dict[str, str]) -> Dict[str, object]:
    op = request["op"]
    if op == "ping":
        return {"ok": True}
    if op == "dump":
        return {"ok": True, "state": state.dump()}
    if op == "reap":
        return {"ok": True, **state.reap()}
    if op == "ensure":
        result = state.ensure_address(request["scope"], request["owner_id"], request["ipv6"])
        return {"ok": True, **result}
    if op == "release":
        result = state.release_address(request["scope"], request["owner_id"], request["ipv6"])
        return {"ok": True, **result}
    raise ValueError(f"unsupported op: {op}")


def _write_response(conn: socket.socket, payload: Dict[str, object]) -> None:
    conn.sendall((json.dumps(payload, sort_keys=True) + "\n").encode(REQUEST_ENCODING))


def serve(args: argparse.Namespace) -> int:
    state = EndpointState(args.iface)
    socket_path = Path(args.socket)
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists():
        socket_path.unlink()
    last_reap = time.time()
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    os.chmod(socket_path, args.socket_mode)
    server.listen(32)
    server.settimeout(1.0)
    stopping = False

    def _signal_handler(signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, _signal_handler)
    signal.signal(signal.SIGINT, _signal_handler)

    try:
        while not stopping:
            if args.reap_interval > 0 and (time.time() - last_reap) >= args.reap_interval:
                state.reap()
                last_reap = time.time()
            try:
                conn, _ = server.accept()
            except TimeoutError:
                continue
            except socket.timeout:
                continue
            with conn:
                try:
                    raw = b""
                    while not raw.endswith(b"\n"):
                        chunk = conn.recv(4096)
                        if not chunk:
                            break
                        raw += chunk
                    request = _parse_request(raw.decode(REQUEST_ENCODING))
                    response = _handle_request(state, request)
                except Exception as exc:  # broad on purpose for socket RPC surface
                    response = {"ok": False, "error": str(exc)}
                _write_response(conn, response)
    finally:
        server.close()
        try:
            socket_path.unlink()
        except FileNotFoundError:
            pass
    return 0


def _send_request(socket_path: str, line: str) -> Dict[str, object]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(socket_path)
        client.sendall((line + "\n").encode(REQUEST_ENCODING))
        raw = b""
        while not raw.endswith(b"\n"):
            chunk = client.recv(4096)
            if not chunk:
                break
            raw += chunk
    if not raw:
        raise RuntimeError("empty response from endpoint daemon")
    return json.loads(raw.decode(REQUEST_ENCODING))


def ensure_cmd(args: argparse.Namespace) -> int:
    response = _send_request(args.socket, f"ensure\t{args.scope}\t{args.owner}\t{_normalise_ipv6(args.ipv6)}")
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


def release_cmd(args: argparse.Namespace) -> int:
    response = _send_request(args.socket, f"release\t{args.scope}\t{args.owner}\t{_normalise_ipv6(args.ipv6)}")
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


def ping_cmd(args: argparse.Namespace) -> int:
    response = _send_request(args.socket, "ping")
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


def dump_cmd(args: argparse.Namespace) -> int:
    response = _send_request(args.socket, "dump")
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


def reap_cmd(args: argparse.Namespace) -> int:
    response = _send_request(args.socket, "reap")
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CMXsafe endpoint helper daemon and client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve", help="Run the privileged endpoint daemon")
    serve_parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH)
    serve_parser.add_argument("--iface", default=DEFAULT_IFACE)
    serve_parser.add_argument("--socket-mode", type=lambda value: int(value, 8), default=0o660)
    serve_parser.add_argument("--reap-interval", type=float, default=DEFAULT_REAP_INTERVAL)
    serve_parser.set_defaults(func=serve)

    ensure_parser = subparsers.add_parser("ensure", help="Ensure a canonical /128 exists on the dummy interface")
    ensure_parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH)
    ensure_parser.add_argument("--scope", choices=("self", "peer"), required=True)
    ensure_parser.add_argument("--owner", required=True)
    ensure_parser.add_argument("--ipv6", required=True)
    ensure_parser.set_defaults(func=ensure_cmd)

    release_parser = subparsers.add_parser("release", help="Release a previously ensured canonical /128")
    release_parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH)
    release_parser.add_argument("--scope", choices=("self", "peer"), required=True)
    release_parser.add_argument("--owner", required=True)
    release_parser.add_argument("--ipv6", required=True)
    release_parser.set_defaults(func=release_cmd)

    ping_parser = subparsers.add_parser("ping", help="Check whether the daemon is responding")
    ping_parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH)
    ping_parser.set_defaults(func=ping_cmd)

    dump_parser = subparsers.add_parser("dump", help="Return daemon state")
    dump_parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH)
    dump_parser.set_defaults(func=dump_cmd)

    reap_parser = subparsers.add_parser("reap", help="Run a stale-owner cleanup cycle")
    reap_parser.add_argument("--socket", default=DEFAULT_SOCKET_PATH)
    reap_parser.set_defaults(func=reap_cmd)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
