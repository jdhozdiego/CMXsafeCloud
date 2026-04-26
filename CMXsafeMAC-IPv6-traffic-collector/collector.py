from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse


CAPTURE_INTERFACE = os.getenv("CAPTURE_INTERFACE", "br-explicit-v6")
CAPTURE_FILTER = os.getenv("CAPTURE_FILTER", "ip6")
COLLECTOR_HTTP_PORT = int(os.getenv("COLLECTOR_HTTP_PORT", "8082"))
MAX_PACKET_EVENTS = int(os.getenv("MAX_PACKET_EVENTS", "20000"))
MAX_ERROR_EVENTS = int(os.getenv("MAX_ERROR_EVENTS", "64"))
STREAM_RESTART_SECONDS = max(1, int(os.getenv("STREAM_RESTART_SECONDS", "3")))


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def lower_or_none(value: str) -> str | None:
    value = value.strip()
    return value.lower() if value else None


def safe_int(value: str, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: str, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class CollectorState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: deque[dict[str, Any]] = deque(maxlen=MAX_PACKET_EVENTS)
        self.errors: deque[dict[str, str]] = deque(maxlen=MAX_ERROR_EVENTS)
        self.capture_active = False
        self.last_packet_at: str | None = None
        self.last_restart_at: str | None = None
        self.last_exit_code: int | None = None

    def add_event(self, event: dict[str, Any]) -> None:
        with self._lock:
            self.events.append(event)
            self.last_packet_at = event["last_seen"]

    def add_error(self, message: str) -> None:
        message = message.strip()
        if not message:
            return
        with self._lock:
            self.errors.append(
                {
                    "message": message,
                    "timestamp": utc_now(),
                }
            )

    def set_capture_state(self, active: bool, exit_code: int | None = None) -> None:
        with self._lock:
            self.capture_active = active
            self.last_restart_at = utc_now()
            if exit_code is not None:
                self.last_exit_code = exit_code

    def snapshot(self, window_seconds: int, limit: int) -> dict[str, Any]:
        cutoff = time.time() - window_seconds
        aggregate: dict[tuple[Any, ...], dict[str, Any]] = {}

        with self._lock:
            events = list(self.events)
            errors = list(self.errors)
            capture_active = self.capture_active
            last_packet_at = self.last_packet_at
            last_restart_at = self.last_restart_at
            last_exit_code = self.last_exit_code

        for event in events:
            if event["time_epoch"] < cutoff:
                continue
            key = (
                event["src_address"],
                event["dst_address"],
                event["protocol"],
                event["src_port"],
                event["dst_port"],
                event["icmp_type"],
            )
            current = aggregate.get(key)
            if current is None:
                current = {
                    "src_address": event["src_address"],
                    "dst_address": event["dst_address"],
                    "protocol": event["protocol"],
                    "src_port": event["src_port"],
                    "dst_port": event["dst_port"],
                    "icmp_type": event["icmp_type"],
                    "packets": 0,
                    "bytes": 0,
                    "first_seen": event["first_seen"],
                    "last_seen": event["last_seen"],
                }
                aggregate[key] = current
            current["packets"] += 1
            current["bytes"] += event["frame_len"]
            if event["first_seen"] < current["first_seen"]:
                current["first_seen"] = event["first_seen"]
            if event["last_seen"] > current["last_seen"]:
                current["last_seen"] = event["last_seen"]

        flows = sorted(
            aggregate.values(),
            key=lambda row: (row["last_seen"], row["packets"], row["bytes"]),
            reverse=True,
        )[:limit]

        return {
            "status": "ok",
            "generated_at": utc_now(),
            "capture_interface": CAPTURE_INTERFACE,
            "capture_filter": CAPTURE_FILTER,
            "capture_active": capture_active,
            "window_seconds": window_seconds,
            "limit": limit,
            "last_packet_at": last_packet_at,
            "last_restart_at": last_restart_at,
            "last_exit_code": last_exit_code,
            "errors": errors,
            "flows": flows,
        }


STATE = CollectorState()
STOP_EVENT = threading.Event()


def protocol_from_next_header(next_header: str, tcp_src: str, udp_src: str, icmp_type: str) -> str:
    if tcp_src:
        return "tcp"
    if udp_src:
        return "udp"
    if icmp_type:
        return "icmp6"

    match next_header.strip():
        case "6":
            return "tcp"
        case "17":
            return "udp"
        case "58":
            return "icmp6"
        case other if other:
            return f"nxt-{other}"
        case _:
            return "unknown"


def parse_tshark_line(line: str) -> dict[str, Any] | None:
    parts = [part.strip() for part in line.rstrip("\n").split("|")]
    if len(parts) < 10:
        return None

    timestamp = safe_float(parts[0], default=0.0)
    src_address = lower_or_none(parts[1])
    dst_address = lower_or_none(parts[2])
    next_header = parts[3]
    tcp_src = parts[4]
    tcp_dst = parts[5]
    udp_src = parts[6]
    udp_dst = parts[7]
    icmp_type = parts[8]
    frame_len = safe_int(parts[9], default=0)

    if not src_address or not dst_address or timestamp <= 0:
        return None

    protocol = protocol_from_next_header(next_header, tcp_src, udp_src, icmp_type)
    src_port = safe_int(tcp_src or udp_src, default=0)
    dst_port = safe_int(tcp_dst or udp_dst, default=0)
    icmp_value = safe_int(icmp_type, default=-1)
    iso_timestamp = datetime.fromtimestamp(timestamp, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return {
        "time_epoch": timestamp,
        "src_address": src_address,
        "dst_address": dst_address,
        "protocol": protocol,
        "src_port": src_port if src_port > 0 else None,
        "dst_port": dst_port if dst_port > 0 else None,
        "icmp_type": icmp_value if icmp_value >= 0 else None,
        "frame_len": frame_len,
        "first_seen": iso_timestamp,
        "last_seen": iso_timestamp,
    }


def tshark_command() -> list[str]:
    return [
        "tshark",
        "-l",
        "-n",
        "-i",
        CAPTURE_INTERFACE,
        "-f",
        CAPTURE_FILTER,
        "-Y",
        "ipv6",
        "-T",
        "fields",
        "-e",
        "frame.time_epoch",
        "-e",
        "ipv6.src",
        "-e",
        "ipv6.dst",
        "-e",
        "ipv6.nxt",
        "-e",
        "tcp.srcport",
        "-e",
        "tcp.dstport",
        "-e",
        "udp.srcport",
        "-e",
        "udp.dstport",
        "-e",
        "icmpv6.type",
        "-e",
        "frame.len",
        "-E",
        "header=n",
        "-E",
        "separator=|",
        "-E",
        "quote=n",
        "-E",
        "occurrence=f",
    ]


def capture_loop() -> None:
    while not STOP_EVENT.is_set():
        STATE.set_capture_state(active=False)
        process = subprocess.Popen(
            tshark_command(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        STATE.set_capture_state(active=True)

        try:
            assert process.stdout is not None
            for line in process.stdout:
                if STOP_EVENT.is_set():
                    break
                event = parse_tshark_line(line)
                if event is not None:
                    STATE.add_event(event)
                    continue

                text = line.strip()
                if text:
                    STATE.add_error(text)
        except Exception as exc:  # pragma: no cover - defensive runtime path
            STATE.add_error(f"collector read failed: {exc}")
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()

        exit_code = process.returncode
        STATE.set_capture_state(active=False, exit_code=exit_code)
        if STOP_EVENT.is_set():
            return
        STATE.add_error(
            f"tshark exited with code {exit_code}; restarting in {STREAM_RESTART_SECONDS}s"
        )
        STOP_EVENT.wait(STREAM_RESTART_SECONDS)


class Handler(BaseHTTPRequestHandler):
    server_version = "CMXsafeMACIPv6TrafficCollector/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _send_json(self, status_code: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=False).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            snapshot = STATE.snapshot(window_seconds=30, limit=1)
            payload = {
                "status": "ok",
                "generated_at": utc_now(),
                "capture_interface": CAPTURE_INTERFACE,
                "capture_active": snapshot["capture_active"],
                "last_packet_at": snapshot["last_packet_at"],
                "errors": snapshot["errors"][-5:],
            }
            self._send_json(200, payload)
            return

        if parsed.path == "/flows":
            query = parse_qs(parsed.query)
            window_seconds = max(10, min(600, safe_int((query.get("window_seconds") or ["60"])[0], 60)))
            limit = max(1, min(1000, safe_int((query.get("limit") or ["300"])[0], 300)))
            self._send_json(200, STATE.snapshot(window_seconds=window_seconds, limit=limit))
            return

        self._send_json(404, {"error": "Not found"})


def main() -> None:
    signal.signal(signal.SIGTERM, lambda _signo, _frame: STOP_EVENT.set())
    signal.signal(signal.SIGINT, lambda _signo, _frame: STOP_EVENT.set())

    capture_thread = threading.Thread(target=capture_loop, name="tshark-capture", daemon=True)
    capture_thread.start()

    server = ThreadingHTTPServer(("0.0.0.0", COLLECTOR_HTTP_PORT), Handler)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        STOP_EVENT.set()
        server.server_close()
        capture_thread.join(timeout=5)


if __name__ == "__main__":
    main()
