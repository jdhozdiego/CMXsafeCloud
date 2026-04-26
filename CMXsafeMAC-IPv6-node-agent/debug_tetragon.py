import argparse
import sys
import time

from google.protobuf.json_format import MessageToDict
import grpc
from tetragon import events_pb2, sensors_pb2_grpc


def parse_args():
    parser = argparse.ArgumentParser(description="Stream Tetragon gRPC events in a compact debug format.")
    parser.add_argument("--address", default="unix:///var/run/cilium/tetragon/tetragon.sock")
    parser.add_argument("--namespace")
    parser.add_argument("--pod")
    parser.add_argument("--binary-substr")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-events", type=int, default=0)
    parser.add_argument("--show-args", action="store_true")
    parser.add_argument("--show-all", action="store_true")
    return parser.parse_args()


def event_summary(response, started_at):
    data = MessageToDict(response, preserving_proto_field_name=True)
    kind = next(iter(data.keys()), "unknown")
    payload = data.get(kind, {})
    process = payload.get("process", {})
    pod = process.get("pod", {})
    container = pod.get("container") or {}
    return {
        "t": round(time.time() - started_at, 3),
        "kind": kind,
        "namespace": pod.get("namespace"),
        "pod": pod.get("name"),
        "uid": pod.get("uid"),
        "binary": process.get("binary"),
        "arguments": process.get("arguments"),
        "pid": process.get("pid"),
        "container_pid": container.get("pid"),
        "labels": pod.get("pod_labels"),
        "annotation_keys": sorted((pod.get("pod_annotations") or {}).keys()),
    }


def should_print(summary, args):
    binary = summary.get("binary") or ""
    if args.binary_substr and args.binary_substr not in binary:
        return False
    if args.show_all:
        return True
    if args.namespace and summary.get("namespace") != args.namespace:
        return False
    if args.pod and summary.get("pod") != args.pod:
        return False
    if args.namespace or args.pod:
        return True
    return summary.get("namespace") is not None


def main():
    args = parse_args()
    options = [
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.keepalive_timeout_ms", 10000),
        ("grpc.keepalive_permit_without_calls", 1),
        ("grpc.http2.max_pings_without_data", 0),
    ]
    channel = grpc.insecure_channel(args.address, options=options)
    stub = sensors_pb2_grpc.FineGuidanceSensorsStub(channel)
    request = events_pb2.GetEventsRequest(
        allow_list=[
            events_pb2.Filter(
                event_set=[events_pb2.PROCESS_EXEC, events_pb2.PROCESS_EXIT],
            )
        ]
    )
    started_at = time.time()
    seen = 0
    print(f"debug_tetragon start address={args.address} timeout={args.timeout}s", flush=True)
    try:
        for response in stub.GetEvents(request, timeout=args.timeout):
            summary = event_summary(response, started_at)
            if should_print(summary, args):
                if not args.show_args:
                    summary.pop("arguments", None)
                print(summary, flush=True)
                seen += 1
                if args.max_events and seen >= args.max_events:
                    break
    except grpc.RpcError as exc:
        print(
            {
                "rpc_error": getattr(exc.code(), "name", str(exc.code())),
                "details": exc.details(),
                "elapsed": round(time.time() - started_at, 3),
            },
            flush=True,
        )
    finally:
        channel.close()
        print(f"debug_tetragon end seen={seen}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
