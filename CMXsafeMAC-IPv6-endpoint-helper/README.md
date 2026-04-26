# CMXsafe Endpoint Helper

This helper provides the endpoint-side mirror identity support for CMXsafe-style OpenSSH forwarding.

It has two pieces:

- `endpointd.py`
  A privileged Unix-socket daemon that owns a local dummy interface, `cmx0` by default, and ensures canonical `/128` IPv6 addresses exist on demand.
- `cmxsafe-ssh`
  A thin wrapper that derives the caller's canonical IPv6 from the 32-hex username, ensures that self identity before launching `ssh`, and releases it on exit.

The repository also now ships the reusable external endpoint runtime image inputs:

- `bundlectl.py`
  The local controller used by dashboard-generated bundles to start `endpointd`, open the SSH master session, install forwards, report health, and disconnect cleanly.
- `connect-platform`, `run-forever`, `send-message`, `disconnect`
  Thin shell entrypoints used both in self-contained bundles and in runtime-image bundles.
- `bundle-entrypoint.sh`
  Container entrypoint for the shared external endpoint image. It expects a mounted bundle at `/bundle` and runs `./run-forever` by default.

## Why it exists

The patched Portable OpenSSH relay can preserve identity in the source IPv6 address of forwarded sockets. On endpoint systems, those source IPv6 addresses must exist locally before `bind()` can succeed.

`endpointd.py` solves that by:

- creating `cmx0` on demand
- adding canonical `/128` addresses on demand
- reference-counting self and peer owners
- removing addresses when their owners release them
- reaping stale owners by PID

## Daemon state model

The daemon keeps three in-memory views:

- interface state
  - whether `cmx0` exists
  - whether it was created by the daemon
  - whether it is up
- address table
  - canonical IPv6 -> current presence on `cmx0`
  - separate `self` and `peer` refcounts
  - owners currently holding the address
- owner table
  - owner id -> scope, PID, and tracked addresses

This keeps the privilege boundary small:

- the daemon owns interface and address mutation
- the wrapper and patched `ssh` only ask for `ensure` / `release`

## Local control protocol

The daemon listens on a Unix socket, `/var/run/cmxsafe-endpointd.sock` by default, and accepts newline-delimited commands:

- `ping`
- `dump`
- `reap`
- `ensure<TAB>scope<TAB>owner_id<TAB>ipv6`
- `release<TAB>scope<TAB>owner_id<TAB>ipv6`

Responses are one-line JSON objects.

## Minimal OpenSSH callout

The patched client and relay use the helper through one narrow contract:

1. resolve the source IPv6 that should be used for the forwarded socket
2. if `CMXSAFE_ENDPOINTD_SOCK` is set:
   - connect to the Unix socket
   - send `ensure<TAB>scope<TAB>owner_id<TAB>ipv6`
   - require a JSON response containing `"ok": true`
3. only then call `bind()` on the forwarded socket

Important behavior:

- if `CMXSAFE_ENDPOINTD_SOCK` is unset, the daemon callout is skipped
- this keeps relay-side BusyBox pods simple, because they already own their authoritative canonical IPv6 on `net1`
- endpoint systems set `CMXSAFE_ENDPOINTD_SOCK`, so mirror `/128` addresses are created locally on `cmx0` before `bind()`

## Example

Run the daemon as root:

```sh
python3 endpointd.py serve --socket /var/run/cmxsafe-endpointd.sock --iface cmx0
```

Check it:

```sh
python3 endpointd.py ping --socket /var/run/cmxsafe-endpointd.sock
```

Ensure an address manually:

```sh
python3 endpointd.py ensure \
  --socket /var/run/cmxsafe-endpointd.sock \
  --scope self \
  --owner session:pid:4242:started:1713620000 \
  --ipv6 7101:d684:fe59:3c98:0000:aa55:0000:0001
```

Use the wrapper:

```sh
CMXSAFE_ENDPOINTD_SOCK=/var/run/cmxsafe-endpointd.sock \
./cmxsafe-ssh -N -L 7777:7102:d684:fe59:3c98:0000:aa55:0000:0002:8888 user@example
```

The wrapper expects the local Unix username to be the 32-character colonless canonical IPv6 representation, unless `CMXSAFE_CANONICAL_USER` overrides it.

Useful environment variables:

- `CMXSAFE_ENDPOINTD_SOCK`
  - Unix socket used by both the wrapper and the patched `ssh` callout
- `CMXSAFE_CANONICAL_USER`
  - overrides the Unix username when deriving the self canonical IPv6
- `CMXSAFE_SSH_BIN`
  - lets the wrapper launch a specific `ssh` binary, which is useful for testing patched Portable OpenSSH bundles
- `CMXSAFE_ENDPOINTD_PYTHON`
  - Python interpreter used to invoke `endpointd.py`
- `CMXSAFE_BUNDLE_BIN_ROOT`
  - optional runtime root for `bundlectl.py`, `endpointd.py`, and `cmxsafe-ssh`
  - self-contained bundles default to `<bundle>/bin`
  - the shared endpoint image sets this to `/opt/cmxsafe/bin`

## Reusable External Endpoint Image

Build the shared runtime image with:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\build-cmxsafe-endpoint-image.ps1
```

The resulting image:

- contains the patched CMXsafe Portable OpenSSH client in `/opt/openssh`
- includes `python3` and `iproute2`
- exports the helper scripts in `/opt/cmxsafe/bin`
- expects a bundle mounted at `/bundle`

With a runtime-image bundle extracted into the current directory, a typical launch shape is:

```sh
docker run --rm -it \
  --cap-add NET_ADMIN \
  --add-host gw.cmxsafe.lab:192.168.1.60 \
  -v "$PWD/cmxsafe-endpoint-<identity>:/bundle" \
  cmxsafemac-ipv6-endpoint-base:docker-desktop-v1
```
