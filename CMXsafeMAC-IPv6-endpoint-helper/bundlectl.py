#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


def load_config(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def bundle_root(config_path: Path) -> Path:
    return config_path.resolve().parent


def run_dir(config: dict, root: Path) -> Path:
    configured = config.get("runtime", {}).get("run_dir")
    if configured:
        return Path(configured)
    return Path("/var/run") / ("cmxsafe-bundle-" + config["identity"]["username"])


def log_dir(root: Path) -> Path:
    return root / "logs"


def bin_root(root: Path) -> Path:
    configured = os.environ.get("CMXSAFE_BUNDLE_BIN_ROOT")
    if configured:
        return Path(configured)
    return root / "bin"


def bin_path(root: Path, name: str) -> Path:
    return bin_root(root) / name


def ssh_binary(config: dict) -> str:
    return config.get("ssh", {}).get("ssh_bin") or os.environ.get("CMXSAFE_SSH_BIN", "ssh")


def ssh_base(config: dict, control_socket: Path) -> list[str]:
    gateway = config["gateway"]
    return [
        ssh_binary(config),
        "-S",
        str(control_socket),
        "-F",
        "/dev/null",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-p",
        str(gateway["port"]),
        f"{config['identity']['username']}@{gateway['host']}",
    ]


def ssh_control_command(config: dict, control_socket: Path, operation: str, extra: list[str] | None = None) -> list[str]:
    base = ssh_base(config, control_socket)
    return base[:3] + ["-O", operation] + base[3:-1] + (extra or []) + [base[-1]]


def endpoint_env(config: dict, root: Path, endpoint_socket: Path) -> dict:
    env = os.environ.copy()
    env.setdefault("CMXSAFE_ENDPOINTD_PYTHON", sys.executable)
    env["CMXSAFE_ENDPOINTD_SOCK"] = str(endpoint_socket)
    env["CMXSAFE_ENDPOINTD_SCRIPT"] = str(bin_path(root, "endpointd.py"))
    env["CMXSAFE_CANONICAL_USER"] = config["identity"]["username"]
    env["CMXSAFE_SSH_BIN"] = ssh_binary(config)
    return env


def run_checked(command: list[str], *, env: dict | None = None, allow_failure: bool = False) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, text=True, capture_output=True, env=env, check=False)
    if result.returncode != 0 and not allow_failure:
        message = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RuntimeError(message)
    return result


def read_pid_file(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def remove_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def terminate_pid(pid: int | None) -> None:
    if not pid:
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return


def endpoint_ready(root: Path, endpoint_socket: Path) -> bool:
    result = run_checked(
        [sys.executable, str(bin_path(root, "endpointd.py")), "ping", "--socket", str(endpoint_socket)],
        allow_failure=True,
    )
    return result.returncode == 0


def start_endpointd(config: dict, root: Path, endpoint_socket: Path) -> None:
    if endpoint_socket.exists():
        if endpoint_ready(root, endpoint_socket):
            return
        remove_file(endpoint_socket)
    run = run_dir(config, root)
    run.mkdir(parents=True, exist_ok=True)
    log_dir(root).mkdir(parents=True, exist_ok=True)
    endpoint_log = (log_dir(root) / "endpointd.log").open("ab")
    process = subprocess.Popen(
        [
            sys.executable,
            str(bin_path(root, "endpointd.py")),
            "serve",
            "--socket",
            str(endpoint_socket),
            "--iface",
            config.get("runtime", {}).get("endpoint_iface", "cmx0"),
        ],
        stdout=endpoint_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    (run / "endpointd.pid").write_text(str(process.pid), encoding="utf-8")
    for _ in range(30):
        time.sleep(1)
        if endpoint_ready(root, endpoint_socket):
            return
    raise RuntimeError("endpointd did not become ready; check logs/endpointd.log")


def endpoint_address(root: Path, endpoint_socket: Path, command: str, scope: str, owner: str, ipv6: str) -> None:
    run_checked(
        [
            sys.executable,
            str(bin_path(root, "endpointd.py")),
            command,
            "--socket",
            str(endpoint_socket),
            "--scope",
            scope,
            "--owner",
            owner,
            "--ipv6",
            ipv6,
        ],
        allow_failure=(command == "release"),
    )


def master_ready(config: dict, control_socket: Path) -> bool:
    result = run_checked(ssh_control_command(config, control_socket, "check"), allow_failure=True)
    return result.returncode == 0


def start_master(config: dict, root: Path, endpoint_socket: Path, control_socket: Path) -> int:
    run = run_dir(config, root)
    pid_file = run / "ssh-master.pid"
    if control_socket.exists() and master_ready(config, control_socket):
        return read_pid_file(pid_file) or os.getpid()
    if control_socket.exists():
        remove_file(control_socket)
    key_path = root / config["ssh"]["identity_file"]
    key_path.chmod(0o600)
    command = [
        str(bin_path(root, "cmxsafe-ssh")),
        "-M",
        "-S",
        str(control_socket),
        "-F",
        "/dev/null",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ServerAliveInterval=15",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ConnectTimeout=10",
        "-i",
        str(key_path),
        "-p",
        str(config["gateway"]["port"]),
        f"{config['identity']['username']}@{config['gateway']['host']}",
    ]
    log_dir(root).mkdir(parents=True, exist_ok=True)
    master_log = (log_dir(root) / "ssh-master.log").open("ab")
    process = subprocess.Popen(
        command,
        stdout=master_log,
        stderr=subprocess.STDOUT,
        env=endpoint_env(config, root, endpoint_socket),
        start_new_session=True,
    )
    pid_file.write_text(str(process.pid), encoding="utf-8")
    for _ in range(30):
        time.sleep(1)
        if master_ready(config, control_socket):
            return process.pid
    raise RuntimeError("SSH control master did not become ready; check logs/ssh-master.log")


def install_forward(config: dict, control_socket: Path, flag: str, spec: str) -> None:
    run_checked(ssh_control_command(config, control_socket, "cancel", [flag, spec]), allow_failure=True)
    run_checked(ssh_control_command(config, control_socket, "forward", [flag, spec]))


def install_forwards(config: dict, root: Path, endpoint_socket: Path, control_socket: Path, owner_pid: int) -> None:
    for service in config.get("accessible_services", []):
        local_port = int(service.get("local_port") or service["port"])
        remote_port = int(service.get("remote_port") or service["port"])
        ipv6 = service["canonical_ipv6"]
        owner = f"peer:pid:{owner_pid}:listen:{local_port}:service:{service['alias']}"
        endpoint_address(root, endpoint_socket, "ensure", "peer", owner, ipv6)
        spec = f"[{ipv6}]:{local_port}:[{ipv6}]:{remote_port}"
        install_forward(config, control_socket, "-L", spec)
        print(f"direct forward ready: {service['alias']} {spec}")

    self_ipv6 = config["identity"]["canonical_ipv6"]
    for service in config.get("publishable_services", []):
        remote_port = int(service.get("remote_port") or service["port"])
        local_port = int(service.get("local_port") or service["port"])
        local_host = service.get("local_host") or "::1"
        spec = f"[{self_ipv6}]:{remote_port}:[{local_host}]:{local_port}"
        install_forward(config, control_socket, "-R", spec)
        print(f"reverse forward ready: {service['alias']} {spec}")


def command_connect(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    config = load_config(config_path)
    root = bundle_root(config_path)
    run = run_dir(config, root)
    run.mkdir(parents=True, exist_ok=True)
    endpoint_socket = run / "endpointd.sock"
    control_socket = run / "ssh-master.sock"
    start_endpointd(config, root, endpoint_socket)
    owner_pid = start_master(config, root, endpoint_socket, control_socket)
    install_forwards(config, root, endpoint_socket, control_socket, owner_pid)
    print("CMXsafe bundle connected.")
    return 0


def command_status(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    config = load_config(config_path)
    root = bundle_root(config_path)
    run = run_dir(config, root)
    endpoint_socket = run / "endpointd.sock"
    control_socket = run / "ssh-master.sock"
    status = {
        "ok": False,
        "bundle_root": str(root),
        "run_dir": str(run),
        "gateway": config["gateway"],
        "identity": config["identity"],
        "endpoint_socket": str(endpoint_socket),
        "control_socket": str(control_socket),
        "endpointd_ready": endpoint_socket.exists() and endpoint_ready(root, endpoint_socket),
        "ssh_master_ready": control_socket.exists() and master_ready(config, control_socket),
    }
    status["ok"] = bool(status["endpointd_ready"] and status["ssh_master_ready"])
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0 if status["ok"] else 1


def command_disconnect(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    config = load_config(config_path)
    root = bundle_root(config_path)
    run = run_dir(config, root)
    endpoint_socket = run / "endpointd.sock"
    control_socket = run / "ssh-master.sock"
    pid_file = run / "ssh-master.pid"
    endpointd_pid_file = run / "endpointd.pid"
    owner_pid = read_pid_file(pid_file)
    endpointd_pid = read_pid_file(endpointd_pid_file)
    if owner_pid:
        for service in config.get("accessible_services", []):
            local_port = int(service.get("local_port") or service["port"])
            owner = f"peer:pid:{owner_pid}:listen:{local_port}:service:{service['alias']}"
            endpoint_address(root, endpoint_socket, "release", "peer", owner, service["canonical_ipv6"])
    if control_socket.exists():
        run_checked(ssh_control_command(config, control_socket, "exit"), allow_failure=True)
    terminate_pid(owner_pid)
    terminate_pid(endpointd_pid)
    remove_file(control_socket)
    remove_file(pid_file)
    remove_file(endpoint_socket)
    remove_file(endpointd_pid_file)
    print("CMXsafe bundle disconnected.")
    return 0


def select_accessible_service(config: dict, alias: str | None) -> dict:
    services = config.get("accessible_services", [])
    if not services:
        raise RuntimeError("this bundle has no accessible services")
    if alias:
        for service in services:
            if service["alias"] == alias:
                return service
        raise RuntimeError(f"accessible service not found: {alias}")
    return services[0]


def command_send_message(args: argparse.Namespace) -> int:
    config_path = Path(args.config)
    config = load_config(config_path)
    service = select_accessible_service(config, args.service)
    if service.get("protocol", "http") != "http":
        raise RuntimeError("send-message only supports services labelled as http")
    port = int(service.get("local_port") or service["port"])
    url = f"http://[{service['canonical_ipv6']}]:{port}/message"
    payload = json.dumps({"content": args.message, "service": service["alias"]}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=args.timeout) as response:
        sys.stdout.write(response.read().decode("utf-8"))
        sys.stdout.write("\n")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="CMXsafe endpoint bundle helper")
    sub = parser.add_subparsers(dest="command", required=True)
    connect = sub.add_parser("connect", help="start endpointd, SSH master, and configured forwards")
    connect.add_argument("config")
    connect.set_defaults(func=command_connect)
    status = sub.add_parser("status", help="report whether endpointd and the SSH master are healthy")
    status.add_argument("config")
    status.set_defaults(func=command_status)
    disconnect = sub.add_parser("disconnect", help="stop the SSH master and release peer addresses")
    disconnect.add_argument("config")
    disconnect.set_defaults(func=command_disconnect)
    send = sub.add_parser("send-message", help="POST a message to an accessible HTTP service")
    send.add_argument("config")
    send.add_argument("message")
    send.add_argument("--service")
    send.add_argument("--timeout", type=int, default=10)
    send.set_defaults(func=command_send_message)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
