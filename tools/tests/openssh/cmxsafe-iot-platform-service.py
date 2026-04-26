#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import html
import ipaddress
import json
import os
import queue
import socket
import threading
from collections import deque
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


MAX_BODY_BYTES = 64 * 1024
SUBSCRIBER_QUEUE_SIZE = 32
DEFAULT_HISTORY_SIZE = max(16, int(os.environ.get("CMXSAFE_MONITOR_HISTORY_SIZE", "256")))


def utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def format_mac(raw: bytes) -> str:
    return ":".join(f"{byte:02x}" for byte in raw)


def decode_canonical_identity(source_ip: str) -> dict:
    """Decode the CMXsafe canonical IPv6 layout into observable identity fields."""
    try:
        address = ipaddress.IPv6Address(source_ip.split("%", 1)[0])
    except ValueError:
        return {
            "client": source_ip.lower(),
            "device_mac": "",
            "gw_tag": "",
            "gateway_mac": "",
            "canonical_counter": None,
            "identity_source": "unparsed-peer-address",
        }

    packed = address.packed
    return {
        "client": address.compressed.lower(),
        "device_mac": format_mac(packed[10:16]),
        "gw_tag": packed[:2].hex(),
        "gateway_mac": format_mac(packed[2:8]),
        "canonical_counter": int.from_bytes(packed[8:10], byteorder="big"),
        "identity_source": "canonical-ipv6",
    }


class ThreadingIPv6HTTPServer(ThreadingHTTPServer):
    address_family = socket.AF_INET6
    daemon_threads = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._subscribers: set[queue.Queue] = set()
        self._subscribers_lock = threading.Lock()
        self._event_id = 0
        self._history: deque[dict] = deque(maxlen=DEFAULT_HISTORY_SIZE)

    @property
    def subscriber_count(self) -> int:
        with self._subscribers_lock:
            return len(self._subscribers)

    def subscribe(self) -> queue.Queue:
        subscriber: queue.Queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE_SIZE)
        with self._subscribers_lock:
            self._subscribers.add(subscriber)
        return subscriber

    def unsubscribe(self, subscriber: queue.Queue) -> None:
        with self._subscribers_lock:
            self._subscribers.discard(subscriber)

    def publish(self, event: dict) -> dict:
        with self._subscribers_lock:
            self._event_id += 1
            published = dict(event)
            published["event_id"] = self._event_id
            self._history.append(published)
            subscribers = list(self._subscribers)

        for subscriber in subscribers:
            try:
                subscriber.put_nowait(published)
            except queue.Full:
                try:
                    subscriber.get_nowait()
                except queue.Empty:
                    pass
                try:
                    subscriber.put_nowait(published)
                except queue.Full:
                    pass
        return published

    def recent_events(self, limit: int) -> list[dict]:
        bounded = max(1, min(self._history.maxlen or DEFAULT_HISTORY_SIZE, int(limit)))
        with self._subscribers_lock:
            return list(reversed(list(self._history)[-bounded:]))


class Handler(BaseHTTPRequestHandler):
    server_version = "CMXsafeIoTPlatform/1.1"

    def _peer(self) -> tuple[str, int]:
        if isinstance(self.client_address, tuple) and len(self.client_address) >= 2:
            return str(self.client_address[0]).lower(), int(self.client_address[1])
        return "", 0

    def _read_body(self) -> tuple[str, str, object | None, str]:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        length_text = self.headers.get("Content-Length", "0").strip() or "0"
        try:
            content_length = int(length_text)
        except ValueError:
            content_length = 0
        if content_length > MAX_BODY_BYTES:
            raise ValueError(f"request body exceeds {MAX_BODY_BYTES} bytes")

        raw = self.rfile.read(content_length) if content_length > 0 else b""
        raw_text = raw.decode("utf-8", errors="replace")
        parsed_body: object | None = None
        content = raw_text

        if raw_text.strip() and "json" in content_type:
            try:
                parsed_body = json.loads(raw_text)
            except json.JSONDecodeError:
                parsed_body = None
            if isinstance(parsed_body, dict):
                value = parsed_body.get("content", parsed_body.get("message", raw_text))
                content = value if isinstance(value, str) else json.dumps(value, sort_keys=True)
            elif parsed_body is not None:
                content = parsed_body if isinstance(parsed_body, str) else json.dumps(parsed_body, sort_keys=True)

        return raw_text, content_type, parsed_body, content

    def _payload(
        self,
        *,
        ok: bool,
        method: str,
        path: str,
        content: str = "",
        raw_body: str = "",
        parsed_body: object | None = None,
        content_type: str = "",
    ) -> dict:
        client_host, client_port = self._peer()
        identity = decode_canonical_identity(client_host)
        payload = {
            "ok": ok,
            "client": identity["client"],
            "port": client_port,
            "device_mac": identity["device_mac"],
            "gw_tag": identity["gw_tag"],
            "gateway_mac": identity["gateway_mac"],
            "canonical_counter": identity["canonical_counter"],
            "identity_source": identity["identity_source"],
            "method": method,
            "path": path,
            "content": content,
            "raw_body": raw_body,
            "body": parsed_body,
            "content_type": content_type,
            "pod": os.environ.get("HOSTNAME", ""),
            "service_port": int(os.environ.get("CMXSAFE_SERVICE_PORT", "9000")),
            "received_at": utc_now_iso(),
            "monitor_subscribers": self.server.subscriber_count,
        }
        return payload

    def _send_json(self, status: HTTPStatus, payload: dict) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _handle_message(
        self,
        *,
        method: str,
        path: str,
        content: str,
        raw_body: str = "",
        parsed_body: object | None = None,
        content_type: str = "",
    ) -> None:
        payload = self._payload(
            ok=True,
            method=method,
            path=path,
            content=content,
            raw_body=raw_body,
            parsed_body=parsed_body,
            content_type=content_type,
        )
        published = self.server.publish(payload)
        self._send_json(HTTPStatus.OK, published)

    def _handle_sse(self) -> None:
        subscriber = self.server.subscribe()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        self.wfile.write(b": connected\n\n")
        self.wfile.flush()

        try:
            while True:
                try:
                    event = subscriber.get(timeout=15)
                    data = json.dumps(event, sort_keys=True)
                    self.wfile.write(f"id: {event['event_id']}\n".encode("utf-8"))
                    self.wfile.write(b"event: message\n")
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass
        finally:
            self.server.unsubscribe(subscriber)

    def _monitor_html(self) -> str:
        pod = html.escape(os.environ.get("HOSTNAME", ""))
        service_port = html.escape(os.environ.get("CMXSAFE_SERVICE_PORT", "9000"))
        return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CMXsafe IoT Platform Monitor</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #17211b;
      --muted: #5e6a61;
      --panel: #fffdf4;
      --accent: #1f7a53;
      --line: #d7decf;
      --bg: #eef3e7;
    }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background: radial-gradient(circle at 20% 10%, #fdf8d8 0, transparent 30rem), linear-gradient(135deg, #edf4e5, #f8fbf2);
      color: var(--ink);
    }}
    main {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 32px 20px;
    }}
    header {{
      display: flex;
      align-items: end;
      justify-content: space-between;
      gap: 24px;
      margin-bottom: 22px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2rem, 4vw, 4rem);
      line-height: 0.95;
      letter-spacing: -0.05em;
    }}
    .status {{
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255, 253, 244, 0.8);
      padding: 10px 16px;
      color: var(--accent);
      font-weight: 700;
      white-space: nowrap;
    }}
    .note {{
      color: var(--muted);
      max-width: 820px;
      font-size: 1.02rem;
    }}
    .events {{
      display: grid;
      gap: 14px;
      margin-top: 24px;
    }}
    .empty, .event {{
      border: 1px solid var(--line);
      border-radius: 24px;
      background: rgba(255, 253, 244, 0.92);
      box-shadow: 0 18px 50px rgba(31, 48, 35, 0.08);
      padding: 18px;
    }}
    .event {{
      animation: enter 260ms ease-out both;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }}
    .label {{
      color: var(--muted);
      font-size: 0.74rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .value {{
      font-family: "Courier New", monospace;
      overflow-wrap: anywhere;
      margin-top: 4px;
    }}
    .content {{
      margin-top: 16px;
      border-left: 4px solid var(--accent);
      padding: 12px 14px;
      background: #f7f4df;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      font-family: "Courier New", monospace;
    }}
    @keyframes enter {{
      from {{ opacity: 0; transform: translateY(8px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}
    @media (max-width: 760px) {{
      header {{ display: block; }}
      .status {{ display: inline-block; margin-top: 16px; }}
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>IoT Platform Live Monitor</h1>
        <p class="note">
          Live-only view for messages received by pod <strong>{pod}</strong> on service port <strong>{service_port}</strong>.
          The platform does not persist history; messages appear here only while this page is connected.
        </p>
      </div>
      <div id="status" class="status">Connecting...</div>
    </header>
    <section id="events" class="events">
      <div class="empty">Waiting for IoT device messages...</div>
    </section>
  </main>
  <script>
    const statusEl = document.getElementById("status");
    const eventsEl = document.getElementById("events");
    const source = new EventSource("/monitor/events");
    let count = 0;

    function field(label, value) {{
      const cell = document.createElement("div");
      const labelEl = document.createElement("div");
      labelEl.className = "label";
      labelEl.textContent = label;
      const valueEl = document.createElement("div");
      valueEl.className = "value";
      valueEl.textContent = value ?? "";
      cell.append(labelEl, valueEl);
      return cell;
    }}

    source.addEventListener("open", () => {{
      statusEl.textContent = "Connected";
    }});

    source.addEventListener("error", () => {{
      statusEl.textContent = "Reconnecting...";
    }});

    source.addEventListener("message", (event) => {{
      const msg = JSON.parse(event.data);
      count += 1;
      statusEl.textContent = `${{count}} live message${{count === 1 ? "" : "s"}}`;
      const empty = eventsEl.querySelector(".empty");
      if (empty) empty.remove();

      const card = document.createElement("article");
      card.className = "event";
      const grid = document.createElement("div");
      grid.className = "grid";
      grid.append(
        field("Device MAC", msg.device_mac),
        field("Source IPv6", msg.client),
        field("Source port", msg.port),
        field("Received", msg.received_at),
        field("Gateway tag", msg.gw_tag),
        field("Gateway MAC", msg.gateway_mac),
        field("Path", msg.path),
        field("Platform pod", msg.pod)
      );
      const content = document.createElement("div");
      content.className = "content";
      content.textContent = msg.content || msg.raw_body || "(empty message)";
      card.append(grid, content);
      eventsEl.prepend(card);
    }});
  </script>
</body>
</html>"""

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/monitor":
            self._send_html(self._monitor_html())
            return
        if parsed.path == "/monitor/events":
            self._handle_sse()
            return
        if parsed.path == "/monitor/status":
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "monitor_subscribers": self.server.subscriber_count,
                    "pod": os.environ.get("HOSTNAME", ""),
                    "service_port": int(os.environ.get("CMXSAFE_SERVICE_PORT", "9000")),
                },
            )
            return
        if parsed.path == "/monitor/recent":
            params = parse_qs(parsed.query)
            limit_text = (params.get("limit") or ["50"])[0]
            try:
                limit = int(limit_text)
            except ValueError:
                limit = 50
            self._send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "events": self.server.recent_events(limit),
                    "pod": os.environ.get("HOSTNAME", ""),
                    "service_port": int(os.environ.get("CMXSAFE_SERVICE_PORT", "9000")),
                    "generated_at": utc_now_iso(),
                },
            )
            return
        if parsed.path == "/healthz":
            self._send_json(HTTPStatus.OK, self._payload(ok=True, method="GET", path=parsed.path))
            return
        if parsed.path in {"/", "/message"}:
            params = parse_qs(parsed.query)
            content = (params.get("content") or params.get("message") or [""])[0]
            self._handle_message(
                method="GET",
                path=parsed.path,
                content=content,
                parsed_body={"query": params} if params else None,
                content_type="query-string" if params else "",
            )
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path not in {"/", "/message"}:
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Not found"})
            return
        try:
            raw_body, content_type, parsed_body, content = self._read_body()
        except ValueError as exc:
            self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"ok": False, "error": str(exc)})
            return
        self._handle_message(
            method="POST",
            path=parsed.path,
            content=content,
            raw_body=raw_body,
            parsed_body=parsed_body,
            content_type=content_type,
        )

    def log_message(self, fmt: str, *args) -> None:
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CMXsafe IoT platform loopback service and live monitor")
    parser.add_argument("--host", default=os.environ.get("CMXSAFE_PLATFORM_HOST", "::"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("CMXSAFE_SERVICE_PORT", "9000")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    with ThreadingIPv6HTTPServer((args.host, args.port), Handler) as server:
        server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
