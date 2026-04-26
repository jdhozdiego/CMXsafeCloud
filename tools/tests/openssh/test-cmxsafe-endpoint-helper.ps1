param(
    [string]$Image = "python:3.12-alpine",
    [string]$SocketPath = "/tmp/cmxsafe.sock",
    [string]$Iface = "cmx0",
    [string]$TestIpv6 = "7101:d684:fe59:3c98:0000:aa55:0000:0001"
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$HelperDir = Join-Path $RepoRoot "CMXsafeMAC-IPv6-endpoint-helper"
$HelperDir = (Resolve-Path -LiteralPath $HelperDir).Path

$script = @'
set -eu
apk add --no-cache iproute2 >/dev/null
python3 /work/endpointd.py serve --socket __SOCKET__ --iface __IFACE__ >/tmp/endpointd.log 2>&1 &
daemon=$!
for i in 1 2 3 4 5; do
  if [ -S __SOCKET__ ]; then
    break
  fi
  sleep 1
done
if [ ! -S __SOCKET__ ]; then
  echo 'endpointd socket never appeared' >&2
  cat /tmp/endpointd.log >&2 || true
  exit 1
fi
python3 /work/endpointd.py ping --socket __SOCKET__
python3 /work/endpointd.py ensure --socket __SOCKET__ --scope self --owner session:pid:123 --ipv6 __IPV6__
ip -6 addr show dev __IFACE__
python3 /work/endpointd.py release --socket __SOCKET__ --scope self --owner session:pid:123 --ipv6 __IPV6__
ip -6 addr show dev __IFACE__
kill "$daemon"
wait "$daemon" || true
'@

$script = $script.Replace("__SOCKET__", $SocketPath).Replace("__IFACE__", $Iface).Replace("__IPV6__", $TestIpv6)

docker run --rm `
    --cap-add NET_ADMIN `
    --mount "type=bind,source=$HelperDir,target=/work,readonly" `
    $Image `
    sh -lc $script

if ($LASTEXITCODE -ne 0) {
    throw "endpoint helper smoke test failed"
}

Write-Host "CMXsafe endpoint helper smoke test passed"
