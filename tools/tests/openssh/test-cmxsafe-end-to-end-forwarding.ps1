param(
    [string]$Version = "10.2p1",
    [string]$ImageTag,
    [string]$RunId,
    [int]$ListenPort = 2222,
    [int]$LocalForwardPort = 9000,
    [int]$ReversePort = 9000,
    [int]$ServicePort = 9000,
    [switch]$KeepArtifacts
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
if (-not $ImageTag) {
    $ImageTag = "cmxsafe-portable-openssh-build:$Version"
}
if (-not $RunId) {
    $RunId = [guid]::NewGuid().ToString("N").Substring(0, 8)
}

$buildScript = Join-Path $RepoRoot "tools\build-portable-openssh-bundle.ps1"
$helperDir = (Resolve-Path -LiteralPath (Join-Path $RepoRoot "CMXsafeMAC-IPv6-endpoint-helper")).Path

$userAHex = "7101d684fe593c980000aa5500000001"
$userAIpv6 = "7101:d684:fe59:3c98:0000:aa55:0000:0001"
$userBHex = "7102d684fe593c980000aa5500000002"
$userBIpv6 = "7102:d684:fe59:3c98:0000:aa55:0000:0002"

function Invoke-Docker {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    & docker @Args
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Args -join ' ') failed"
    }
}

function Remove-DockerObject {
    param(
        [ValidateSet("container", "network", "volume")]
        [string]$Kind,
        [string]$Name
    )

    switch ($Kind) {
        "container" {
            try { & docker rm -f $Name *> $null } catch { }
        }
        "network" {
            try { & docker network rm $Name *> $null } catch { }
        }
        "volume" {
            try { & docker volume rm -f $Name *> $null } catch { }
        }
    }
}

function Wait-ForExecSuccess {
    param(
        [string]$Container,
        [string]$Command,
        [int]$TimeoutSeconds = 30
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        & docker exec $Container sh -lc $Command *> $null
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Timed out waiting for command success in ${Container}: $Command"
}

function Start-DetachedDockerExec {
    param(
        [string]$Container,
        [string]$Command
    )

    & docker exec -d $Container sh -lc $Command
    if ($LASTEXITCODE -ne 0) {
        throw "docker exec -d $Container sh -lc $Command failed"
    }
}

$buildArgs = @{
    Version = $Version
    ImageTag = $ImageTag
    SkipExport = $true
    ApplyCmxsafePatch = $true
}
& $buildScript @buildArgs

$prefix = "cmxsafe-e2e-$RunId"
$network = "$prefix-net"
$relay = "$prefix-relay"
$clientA = "$prefix-client-a"
$clientB = "$prefix-client-b"
$bundleVolume = "$prefix-bundle"
$sshVolume = "$prefix-ssh"
$homeVolume = "$prefix-home"
$clientVolume = "$prefix-client"
$runVolume = "$prefix-run"
$tmpVolume = "$prefix-tmp"
$volumes = @($bundleVolume, $sshVolume, $homeVolume, $clientVolume, $runVolume, $tmpVolume)

$hostTemp = Join-Path $RepoRoot ".tmp\$prefix-host"
$passwdPath = Join-Path $hostTemp "passwd"
$groupPath = Join-Path $hostTemp "group"
$servicePyPath = Join-Path $hostTemp "cmxsafe_http_loopback.py"
$reverseScriptPath = Join-Path $hostTemp "reverse-forward.sh"
$forwardScriptPath = Join-Path $hostTemp "forward-client.sh"

if (Test-Path -LiteralPath $hostTemp) {
    Remove-Item -LiteralPath $hostTemp -Recurse -Force
}
New-Item -ItemType Directory -Path $hostTemp | Out-Null

[System.IO.File]::WriteAllText(
    $passwdPath,
    "root:x:0:0:root:/root:/bin/sh`nsshd:x:74:74:Privilege-separated SSH:/var/empty:/bin/false`n${userAHex}:x:1000:1000:Canonical User A:/home/${userAHex}:/bin/sh`n${userBHex}:x:1001:1001:Canonical User B:/home/${userBHex}:/bin/sh`n",
    [System.Text.ASCIIEncoding]::new()
)

[System.IO.File]::WriteAllText(
    $groupPath,
    "root:x:0:`nsshd:x:74:`n${userAHex}:x:1000:`n${userBHex}:x:1001:`n",
    [System.Text.ASCIIEncoding]::new()
)

[System.IO.File]::WriteAllText(
    $servicePyPath,
@"
import http.server
import json
import socket

TARGET_IPV6 = "::1"
TARGET_PORT = $ServicePort

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        payload = {
            "client": self.client_address[0],
            "port": self.client_address[1],
        }
        with open("/tmp/last-client.json", "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return

class Server(http.server.ThreadingHTTPServer):
    address_family = socket.AF_INET6

server = Server((TARGET_IPV6, TARGET_PORT, 0, 0), Handler)
server.serve_forever()
"@,
    [System.Text.ASCIIEncoding]::new()
)

[System.IO.File]::WriteAllText(
    $reverseScriptPath,
@"
#!/bin/sh
set -eu
CMXSAFE_ENDPOINTD_SCRIPT=/work/endpointd.py \
CMXSAFE_ENDPOINTD_PYTHON=python3 \
CMXSAFE_ENDPOINTD_SOCK=/tmp/cmxsafe.sock \
CMXSAFE_SSH_BIN=/opt/openssh/bin/ssh \
CMXSAFE_CANONICAL_USER=$userBHex \
exec /work/cmxsafe-ssh \
  -F /dev/null \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=5 \
  -i /client/userb_ed25519 \
  -p $ListenPort \
  -N \
  -R [$userBIpv6]:${ReversePort}:[::1]:${ServicePort} \
  $userBHex@$relay
"@,
    [System.Text.ASCIIEncoding]::new()
)

[System.IO.File]::WriteAllText(
    $forwardScriptPath,
@"
#!/bin/sh
set -eu
exec /opt/openssh/bin/ssh \
  -F /dev/null \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=5 \
  -i /client/usera_ed25519 \
  -p $ListenPort \
  -N \
  -L [::1]:${LocalForwardPort}:[$userBIpv6]:${ReversePort} \
  $userAHex@$relay
"@,
    [System.Text.ASCIIEncoding]::new()
)

foreach ($name in @($relay, $clientA, $clientB)) {
    Remove-DockerObject -Kind container -Name $name
}
Remove-DockerObject -Kind network -Name $network
foreach ($volume in $volumes) {
    Remove-DockerObject -Kind volume -Name $volume
}

try {
    Write-Host "RunId: $RunId"
    Write-Host "[1/8] Creating docker network and volumes"
    Invoke-Docker network create $network | Out-Null
    foreach ($volume in $volumes) {
        Invoke-Docker volume create $volume | Out-Null
    }

    Write-Host "[2/8] Staging patched OpenSSH bundle"
    Invoke-Docker run --rm `
        --mount "type=volume,source=$bundleVolume,target=/dest" `
        --mount "type=volume,source=$runVolume,target=/empty-run" `
        --mount "type=volume,source=$tmpVolume,target=/empty-tmp" `
        $ImageTag `
        sh -lc "mkdir -p /dest /empty-run /empty-tmp && cp -a /out/opt/openssh/. /dest/"

    $seedScript = @"
set -eu
mkdir -p /ssh /home/$userAHex/.ssh /home/$userBHex/.ssh /client
/bundle/bin/ssh-keygen -q -t ed25519 -N '' -f /ssh/ssh_host_ed25519_key
/bundle/bin/ssh-keygen -q -t ed25519 -N '' -f /client/usera_ed25519
/bundle/bin/ssh-keygen -q -t ed25519 -N '' -f /client/userb_ed25519
cp /client/usera_ed25519.pub /home/$userAHex/.ssh/authorized_keys
cp /client/userb_ed25519.pub /home/$userBHex/.ssh/authorized_keys
chown -R 1000:1000 /home/$userAHex
chown -R 1001:1001 /home/$userBHex
chmod 755 /home/$userAHex /home/$userBHex
chmod 700 /home/$userAHex/.ssh /home/$userBHex/.ssh
chmod 600 /home/$userAHex/.ssh/authorized_keys /home/$userBHex/.ssh/authorized_keys
cat > /ssh/sshd_config <<'EOF'
Port $ListenPort
Protocol 2
ListenAddress 0.0.0.0
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
PubkeyAuthentication yes
PermitRootLogin no
PermitEmptyPasswords no
HostKey /etc/ssh/ssh_host_ed25519_key
AuthorizedKeysFile .ssh/authorized_keys
PidFile /var/run/sshd.pid
UseDNS no
X11Forwarding no
AllowTcpForwarding yes
GatewayPorts clientspecified
PermitTunnel no
PrintMotd no
LogLevel VERBOSE
EOF
"@

    Write-Host "[3/8] Seeding SSH config, keys, and home state"
    Invoke-Docker run --rm `
        --mount "type=volume,source=$bundleVolume,target=/bundle,readonly" `
        --mount "type=volume,source=$sshVolume,target=/ssh" `
        --mount "type=volume,source=$homeVolume,target=/home" `
        --mount "type=volume,source=$clientVolume,target=/client" `
        alpine:3.21 `
        sh -lc $seedScript

    if (-not (Test-Path -LiteralPath $passwdPath) -or -not (Test-Path -LiteralPath $groupPath)) {
        throw "expected generated passwd/group fixtures to exist under $hostTemp"
    }
    $passwdFile = [System.IO.Path]::GetFullPath($passwdPath)
    $groupFile = [System.IO.Path]::GetFullPath($groupPath)

    $relayCommand = @"
set -eu
apk add --no-cache iproute2 >/dev/null
ip -6 addr add $userAIpv6/128 dev lo 2>/dev/null || true
ip -6 addr add $userBIpv6/128 dev lo 2>/dev/null || true
mkdir -p /var/run/sshd /tmp
/opt/openssh/sbin/sshd -t -f /etc/ssh/sshd_config
exec /opt/openssh/sbin/sshd -D -e -f /etc/ssh/sshd_config
"@

    Write-Host "[4/8] Starting relay"
    $relayId = docker run -d `
        --name $relay `
        --network $network `
        --cap-add NET_ADMIN `
        --mount "type=volume,source=$bundleVolume,target=/opt/openssh,readonly" `
        --mount "type=volume,source=$sshVolume,target=/etc/ssh,readonly" `
        --mount "type=volume,source=$homeVolume,target=/home,readonly" `
        --mount "type=volume,source=$runVolume,target=/var/run" `
        --mount "type=volume,source=$tmpVolume,target=/tmp" `
        --mount "type=bind,source=$passwdFile,target=/etc/passwd,readonly" `
        --mount "type=bind,source=$groupFile,target=/etc/group,readonly" `
        alpine:3.21 `
        sh -lc $relayCommand

    if ($LASTEXITCODE -ne 0 -or -not $relayId) {
        throw "failed to start relay container"
    }

    Start-Sleep -Seconds 2
    Invoke-Docker exec $relay sh -lc "/opt/openssh/sbin/sshd -T -f /etc/ssh/sshd_config >/dev/null"

    Write-Host "[5/8] Starting endpoint containers"
    foreach ($client in @($clientA, $clientB)) {
        $clientId = docker run -d `
            --name $client `
            --network $network `
            --cap-add NET_ADMIN `
            --mount "type=volume,source=$bundleVolume,target=/opt/openssh,readonly" `
            --mount "type=volume,source=$clientVolume,target=/client,readonly" `
            --mount "type=bind,source=$helperDir,target=/work,readonly" `
            --mount "type=bind,source=$hostTemp,target=/hosttmp,readonly" `
            python:3.12-alpine `
            sh -lc "apk add --no-cache iproute2 >/dev/null && tail -f /dev/null"

        if ($LASTEXITCODE -ne 0 -or -not $clientId) {
            throw "failed to start $client container"
        }
    }

    Start-Sleep -Seconds 2
    Invoke-Docker exec $clientA sh -lc "test -x /opt/openssh/bin/ssh"
    Invoke-Docker exec $clientB sh -lc "test -x /opt/openssh/bin/ssh"

    Write-Host "[6/8] Starting endpoint helper and local service on client B"
    Start-DetachedDockerExec -Container $clientB -Command "python3 /work/endpointd.py serve --socket /tmp/cmxsafe.sock --iface cmx0 >/tmp/endpointd.log 2>&1"
    Wait-ForExecSuccess -Container $clientB -Command "test -S /tmp/cmxsafe.sock"
    Start-DetachedDockerExec -Container $clientB -Command "python3 /hosttmp/cmxsafe_http_loopback.py >/tmp/service.log 2>&1"
    Wait-ForExecSuccess -Container $clientB -Command "ps -ef | grep '/hosttmp/cmxsafe_http_loopback.py' | grep -v grep"

    Write-Host "[7/8] Starting reverse and local forwards"
    Start-DetachedDockerExec -Container $clientB -Command "sh /hosttmp/reverse-forward.sh >/tmp/reverse.log 2>&1"
    Wait-ForExecSuccess -Container $clientB -Command "ps -ef | grep '/opt/openssh/bin/ssh' | grep -v grep"
    Wait-ForExecSuccess -Container $relay -Command "ss -ln | grep -F ':${ReversePort}'"

    Start-DetachedDockerExec -Container $clientA -Command "sh /hosttmp/forward-client.sh >/tmp/forward.log 2>&1"
    Wait-ForExecSuccess -Container $clientA -Command "ps -ef | grep '/opt/openssh/bin/ssh' | grep -v grep"
    Wait-ForExecSuccess -Container $clientA -Command "ss -ln | grep -F '[::1]:${LocalForwardPort}'"

    Write-Host "[8/8] Issuing end-to-end request and verifying observed source identity"
    $responseUrl = "http://[::1]:$LocalForwardPort"
    $response = & docker exec $clientA python3 -c "import urllib.request; print(urllib.request.urlopen('$responseUrl', timeout=10).read().decode('utf-8'))"
    if ($LASTEXITCODE -ne 0 -or -not $response) {
        docker logs $relay
        docker exec $clientA sh -lc "cat /tmp/forward.log || true"
        docker exec $clientB sh -lc "cat /tmp/reverse.log || true; cat /tmp/service.log || true; cat /tmp/endpointd.log || true"
        throw "end-to-end request through local+reverse forwarding failed"
    }

    $result = $response | ConvertFrom-Json
    if ($result.client -notin @($userAIpv6, "7101:d684:fe59:3c98:0:aa55:0:1")) {
        docker exec $clientB sh -lc "cat /tmp/last-client.json || true"
        throw "expected destination to observe source $userAIpv6, got $($result.client)"
    }

    Write-Host "CMXsafe end-to-end forwarding proof passed"
    Write-Host "Observed destination source IPv6: $($result.client)"
    Write-Host "Observed source port: $($result.port)"
}
finally {
    if (-not $KeepArtifacts) {
        foreach ($name in @($relay, $clientA, $clientB)) {
            Remove-DockerObject -Kind container -Name $name
        }
        Remove-DockerObject -Kind network -Name $network
        foreach ($volume in $volumes) {
            Remove-DockerObject -Kind volume -Name $volume
        }
        if (Test-Path -LiteralPath $hostTemp) {
            Remove-Item -LiteralPath $hostTemp -Recurse -Force
        }
    }
}
