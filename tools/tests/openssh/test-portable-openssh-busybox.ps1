param(
    [string]$Version = "10.2p1",
    [string]$ImageTag,
    [int]$ListenPort = 2222,
    [switch]$KeepArtifacts,
    [switch]$ApplyCmxsafePatch
)

$ErrorActionPreference = "Stop"

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path
$TempRoot = [System.IO.Path]::GetTempPath()
if (-not $ImageTag) {
    $ImageTag = "cmxsafe-portable-openssh-build:$Version"
}

$buildScript = Join-Path $RepoRoot "tools\build-portable-openssh-bundle.ps1"

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

$buildArgs = @{
    Version = $Version
    ImageTag = $ImageTag
    SkipExport = $true
}
if ($ApplyCmxsafePatch) {
    $buildArgs.ApplyCmxsafePatch = $true
}
& $buildScript @buildArgs

$prefix = "cmxsafe-portable-openssh-test"
$network = "$prefix-net"
$server = "$prefix-server"
$bundleVolume = "$prefix-bundle"
$emptyVolume = "$prefix-empty"
$sshVolume = "$prefix-ssh"
$homeVolume = "$prefix-home"
$runVolume = "$prefix-run"
$tmpVolume = "$prefix-tmp"
$clientVolume = "$prefix-client"

$allVolumes = @($bundleVolume, $emptyVolume, $sshVolume, $homeVolume, $runVolume, $tmpVolume, $clientVolume)

$hostTemp = Join-Path $TempRoot "$prefix-host"
$passwdPath = Join-Path $hostTemp "passwd"
$groupPath = Join-Path $hostTemp "group"

if (Test-Path -LiteralPath $hostTemp) {
    Remove-Item -LiteralPath $hostTemp -Recurse -Force
}
New-Item -ItemType Directory -Path $hostTemp | Out-Null

[System.IO.File]::WriteAllText(
    $passwdPath,
    "root:x:0:0:root:/root:/bin/sh`nsshd:x:74:74:Privilege-separated SSH:/var/empty:/bin/false`ndemo1:x:1000:1000:Demo One:/home/demo1:/bin/sh`ndemo2:x:1001:1001:Demo Two:/home/demo2:/bin/sh`n",
    [System.Text.ASCIIEncoding]::new()
)

[System.IO.File]::WriteAllText(
    $groupPath,
    "root:x:0:`nsshd:x:74:`ndemo1:x:1000:`ndemo2:x:1001:`n",
    [System.Text.ASCIIEncoding]::new()
)

Remove-DockerObject -Kind container -Name $server
Remove-DockerObject -Kind network -Name $network
foreach ($volume in $allVolumes) {
    Remove-DockerObject -Kind volume -Name $volume
}

try {
    Invoke-Docker network create $network | Out-Null
    foreach ($volume in $allVolumes) {
        Invoke-Docker volume create $volume | Out-Null
    }

    Invoke-Docker run --rm `
        --mount "type=volume,source=$bundleVolume,target=/dest" `
        --mount "type=volume,source=$emptyVolume,target=/empty" `
        $ImageTag `
        sh -lc "mkdir -p /dest /empty && cp -a /out/opt/openssh/. /dest/ && cp -a /out/var/empty/. /empty/ && chmod 755 /empty"

    $seedScript = @"
set -eu
mkdir -p /ssh /home/demo1/.ssh /home/demo2/.ssh /client
/bundle/bin/ssh-keygen -q -t ed25519 -N '' -f /ssh/ssh_host_ed25519_key
/bundle/bin/ssh-keygen -q -t ed25519 -N '' -f /client/demo1_ed25519
/bundle/bin/ssh-keygen -q -t ed25519 -N '' -f /client/demo2_ed25519
cp /client/demo1_ed25519.pub /home/demo1/.ssh/authorized_keys
cp /client/demo2_ed25519.pub /home/demo2/.ssh/authorized_keys
chown -R 1000:1000 /home/demo1
chown -R 1001:1001 /home/demo2
chmod 755 /home/demo1 /home/demo2
chmod 700 /home/demo1/.ssh /home/demo2/.ssh
chmod 600 /home/demo1/.ssh/authorized_keys /home/demo2/.ssh/authorized_keys
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
GatewayPorts no
PermitTunnel no
PrintMotd no
LogLevel VERBOSE
EOF
"@

    Invoke-Docker run --rm `
        --mount "type=volume,source=$bundleVolume,target=/bundle,readonly" `
        --mount "type=volume,source=$sshVolume,target=/ssh" `
        --mount "type=volume,source=$homeVolume,target=/home" `
        --mount "type=volume,source=$clientVolume,target=/client" `
        alpine:3.21 `
        sh -lc $seedScript

    $passwdFile = (Resolve-Path -LiteralPath $passwdPath).Path
    $groupFile = (Resolve-Path -LiteralPath $groupPath).Path

    $serverId = docker run -d `
        --name $server `
        --network $network `
        -p "${ListenPort}:$ListenPort" `
        --mount "type=volume,source=$bundleVolume,target=/opt/openssh,readonly" `
        --mount "type=volume,source=$emptyVolume,target=/var/empty,readonly" `
        --mount "type=volume,source=$sshVolume,target=/etc/ssh,readonly" `
        --mount "type=volume,source=$homeVolume,target=/home,readonly" `
        --mount "type=volume,source=$runVolume,target=/var/run" `
        --mount "type=volume,source=$tmpVolume,target=/tmp" `
        --mount "type=bind,source=$passwdFile,target=/etc/passwd,readonly" `
        --mount "type=bind,source=$groupFile,target=/etc/group,readonly" `
        busybox:1.36 `
        sh -lc "mkdir -p /var/run/sshd /tmp && /opt/openssh/sbin/sshd -t -f /etc/ssh/sshd_config && exec /opt/openssh/sbin/sshd -D -e -f /etc/ssh/sshd_config"

    if ($LASTEXITCODE -ne 0 -or -not $serverId) {
        throw "failed to start busybox sshd container"
    }

    Start-Sleep -Seconds 2

    $serverState = docker inspect --format '{{.State.Running}}' $server
    if ($serverState -ne "true") {
        docker logs $server
        throw "busybox sshd container is not running"
    }

    $demo1Output = docker run --rm `
        --network $network `
        --mount "type=volume,source=$clientVolume,target=/client,readonly" `
        --mount "type=volume,source=$bundleVolume,target=/opt/openssh,readonly" `
        alpine:3.21 `
        sh -lc "/opt/openssh/bin/ssh -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /client/demo1_ed25519 -p $ListenPort demo1@$server 'echo demo1-ok'"

    if ($LASTEXITCODE -ne 0) {
        throw "demo1 SSH login test failed"
    }

    $demo2Output = docker run --rm `
        --network $network `
        --mount "type=volume,source=$clientVolume,target=/client,readonly" `
        --mount "type=volume,source=$bundleVolume,target=/opt/openssh,readonly" `
        alpine:3.21 `
        sh -lc "/opt/openssh/bin/ssh -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /client/demo2_ed25519 -p $ListenPort demo2@$server 'echo demo2-ok'"

    if ($LASTEXITCODE -ne 0) {
        throw "demo2 SSH login test failed"
    }

    Write-Host "Portable OpenSSH bundle works in busybox:1.36"
    Write-Host "demo1 result: $demo1Output"
    Write-Host "demo2 result: $demo2Output"
}
finally {
    if (-not $KeepArtifacts) {
        Remove-DockerObject -Kind container -Name $server
        Remove-DockerObject -Kind network -Name $network
        foreach ($volume in $allVolumes) {
            Remove-DockerObject -Kind volume -Name $volume
        }
        if (Test-Path -LiteralPath $hostTemp) {
            Remove-Item -LiteralPath $hostTemp -Recurse -Force
        }
    }
}
