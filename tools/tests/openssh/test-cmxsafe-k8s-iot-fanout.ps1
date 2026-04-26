param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
    [string]$Namespace = "mac-ssh-demo",
    [string]$GatewayDeployment = "portable-openssh-busybox",
    [string]$GatewayService = "portable-openssh-busybox",
    [string]$GatewaySelector = "app=portable-openssh-busybox",
    [string]$GatewayConfigMap = "portable-openssh-etc",
    [string]$DashboardDeployment = "portable-openssh-dashboard",
    [string]$TargetName = "portable-openssh-busybox",
    [int]$GatewayReplicas = 2,
    [int]$DeviceCount = 10,
    [int]$ServicePort = 9000,
    [int]$SshPort = 2222,
    [int]$AllocatorPort = 18080,
    [int]$DashboardPort = 18084,
    [int]$MaxPlatformMoveAttempts = 20,
    [string]$PortableOpenSshVersion = "10.2p1",
    [string]$PortableOpenSshImageTag,
    [string]$EndpointImage = "python:3.12-alpine",
    [string]$GatewayGwTag = "9101",
    [string]$CanonicalGatewayMac = "f6:db:2b:39:78:94",
    [string]$IoTMacPrefix = "02:10:00:00:00",
    [string]$PlatformMac = "02:20:00:00:00:01",
    [switch]$CleanupEndpointResources
)

# Runs the larger CMXsafe OpenSSH rendezvous proof:
# - 10 IoT device endpoint pods
# - 1 IoT platform endpoint pod exposing a service on port 9000
# - 2 replicated Portable OpenSSH gateway pods
# - natural device SSH session distribution across the gateway replicas
# - platform reverse session moved to the other replica and revalidated
# The platform service must observe each device's canonical IPv6 as the
# source address, with the true ephemeral source port preserved.

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

. "$ProjectRoot\tools\CMXsafeMAC-IPv6-helpers.ps1"

if (-not (Get-Variable -Name IsWindows -Scope Global -ErrorAction SilentlyContinue)) {
    $global:IsWindows = $true
}

function Test-DashboardHealth {
    param([int]$Port = 18084)
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/" -TimeoutSec 3
        return $response.StatusCode -eq 200
    }
    catch {
        return $false
    }
}

function Start-DashboardPortForward {
    param(
        [int]$Port = 18084,
        [string]$TargetNamespace = "mac-ssh-demo",
        [string]$Service = "portable-openssh-dashboard",
        [string]$ProjectRoot = (Get-CMXsafeProjectRoot)
    )
    if ($Port -gt 0 -and (Test-DashboardHealth -Port $Port)) {
        return [pscustomobject]@{
            Port    = $Port
            BaseUrl = "http://127.0.0.1:$Port"
            Process = $null
            Started = $false
            LogFile = $null
        }
    }

    $actualPort = Get-FreeTcpPort -PreferredPort $Port
    $tmpDir = Join-Path $ProjectRoot "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $stdout = Join-Path $tmpDir "dashboard-port-forward.out.log"
    $stderr = Join-Path $tmpDir "dashboard-port-forward.err.log"

    $startProcessArgs = @{
        FilePath               = "kubectl"
        ArgumentList           = @("port-forward", "-n", $TargetNamespace, "svc/$Service", "$actualPort`:8084")
        WorkingDirectory       = $ProjectRoot
        PassThru               = $true
        RedirectStandardOutput = $stdout
        RedirectStandardError  = $stderr
    }
    if ($IsWindows) {
        $startProcessArgs.WindowStyle = "Hidden"
    }
    $process = Start-Process @startProcessArgs
    Wait-Until -Description "portable openssh dashboard port-forward on $actualPort" -TimeoutSeconds 30 -IntervalSeconds 1 -Condition {
        Test-DashboardHealth -Port $actualPort
    } | Out-Null
    return [pscustomobject]@{
        Port    = $actualPort
        BaseUrl = "http://127.0.0.1:$actualPort"
        Process = $process
        Started = $true
        LogFile = $stderr
    }
}

function Invoke-DashboardJson {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Path
    )
    return Invoke-RestMethod -Uri "$BaseUrl$Path" -TimeoutSec 30
}

function Invoke-DashboardFormPost {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][hashtable]$Body
    )
    try {
        return Invoke-WebRequest -UseBasicParsing -Method Post -Uri "$BaseUrl$Path" -Body $Body -MaximumRedirection 0 -ErrorAction Stop
    }
    catch [Microsoft.PowerShell.Commands.HttpResponseException] {
        $response = $_.Exception.Response
        if ($null -ne $response -and ($response.StatusCode.value__ -eq 302 -or $response.StatusCode.value__ -eq 303)) {
            return $response
        }
        throw
    }
    catch {
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode.value__ -in 302, 303) {
            return $_.Exception.Response
        }
        throw
    }
}

function Get-AllocationForPod {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$PodUid,
        [string]$ContainerIface = "eth0"
    )
    $rows = @(
        Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/allocations?pod_uid=$PodUid&status=ALLOCATED" |
            Where-Object { $_.container_iface -eq $ContainerIface }
    )
    if ($rows.Count -eq 0) {
        return $null
    }
    return $rows[0]
}

function Ensure-AllocationForPod {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)]$Pod,
        [string]$ContainerIface = "eth0",
        [string]$Prefix = "fd42:4242:4242:10::/64"
    )
    $existing = Get-AllocationForPod -BaseUrl $BaseUrl -PodUid $Pod.metadata.uid -ContainerIface $ContainerIface
    if ($existing) {
        return $existing
    }

    $owner = $null
    if ($Pod.metadata.ownerReferences -and $Pod.metadata.ownerReferences.Count -gt 0) {
        $owner = $Pod.metadata.ownerReferences[0]
    }

    $body = @{
        gw_iface        = $ContainerIface
        node_name       = $Pod.spec.nodeName
        namespace       = $Pod.metadata.namespace
        pod_name        = $Pod.metadata.name
        pod_uid         = $Pod.metadata.uid
        container_iface = $ContainerIface
        ipv6_prefix     = $Prefix
        mac_dev         = "ipv6-derived-only"
        owner_kind      = if ($owner) { $owner.kind } else { "Pod" }
        owner_name      = if ($owner) { $owner.name } else { $Pod.metadata.name }
        owner_uid       = if ($owner) { $owner.uid } else { $Pod.metadata.uid }
    }
    Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/allocations/ensure" -Method "POST" -Body $body | Out-Null
    return Wait-Until -Description "managed allocation for ssh pod $($Pod.metadata.name)" -TimeoutSeconds 120 -IntervalSeconds 3 -Condition {
        Get-AllocationForPod -BaseUrl $BaseUrl -PodUid $Pod.metadata.uid -ContainerIface $ContainerIface
    }
}

function Expand-Ipv6Username {
    param([Parameter(Mandatory = $true)][string]$RequestedIpv6)
    $addressBytes = [System.Net.IPAddress]::Parse($RequestedIpv6).GetAddressBytes()
    return (($addressBytes | ForEach-Object { $_.ToString("x2") }) -join "").ToLower()
}

function Ensure-CanonicalIdentity {
    param(
        [Parameter(Mandatory = $true)][string]$AllocatorBaseUrl,
        [Parameter(Mandatory = $true)]$ReferencePod,
        [Parameter(Mandatory = $true)][string]$GwTag,
        [Parameter(Mandatory = $true)][string]$MacDev,
        [Parameter(Mandatory = $true)][string]$CanonicalGatewayMac
    )
    return Invoke-AllocatorApi -BaseUrl $AllocatorBaseUrl -Path "/explicit-ipv6-assignments/ensure-by-pod" -Method "POST" -Body @{
        pod_uid               = $ReferencePod.metadata.uid
        gw_tag                = $GwTag
        mac_dev               = $MacDev
        canonical_gateway_mac = $CanonicalGatewayMac
    }
}

function Wait-ReconcileSucceeded {
    param(
        [Parameter(Mandatory = $true)][string]$DashboardBaseUrl,
        [long]$MinimumRunId = 0,
        [int]$TimeoutSeconds = 30
    )
    return Wait-Until -Description "dashboard reconcile success" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 1 -Condition {
        $state = Invoke-DashboardJson -BaseUrl $DashboardBaseUrl -Path "/api/targets"
        $run = @($state.reconcile_runs)[0]
        if ($run -and [long]$run.id -gt $MinimumRunId -and $run.status -eq "SUCCEEDED") {
            return $run
        }
        return $null
    }
}

function Set-DashboardCanonicalGatewayMac {
    param(
        [Parameter(Mandatory = $true)][string]$DashboardBaseUrl,
        [Parameter(Mandatory = $true)][string]$TargetName,
        [Parameter(Mandatory = $true)][string]$CanonicalGatewayMac,
        [int]$ListenPort = 2222
    )
    $block = Get-TargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
    $settings = $block.Target.server_settings
    $effectiveListenPort = if ($settings -and $settings.listen_port) { [int]$settings.listen_port } else { $ListenPort }
    $allowTcpForwarding = if ($null -eq $settings -or $settings.allow_tcp_forwarding -ne $false) { "on" } else { "" }
    $gatewayPorts = if ($null -ne $settings -and $settings.gateway_ports) { "on" } else { "" }
    $permitTunnel = if ($null -ne $settings -and $settings.permit_tunnel) { "on" } else { "" }
    $x11Forwarding = if ($null -ne $settings -and $settings.x11_forwarding) { "on" } else { "" }
    $logLevel = if ($null -ne $settings -and $settings.log_level) { "$($settings.log_level)" } else { "VERBOSE" }
    Invoke-DashboardFormPost -BaseUrl $DashboardBaseUrl -Path "/server-settings" -Body @{
        return_to             = "/"
        target_id             = "$($block.Target.target.id)"
        canonical_gateway_mac = $CanonicalGatewayMac
        listen_port           = "$effectiveListenPort"
        allow_tcp_forwarding  = $allowTcpForwarding
        gateway_ports         = $gatewayPorts
        permit_tunnel         = $permitTunnel
        x11_forwarding        = $x11Forwarding
        log_level             = $logLevel
    } | Out-Null
}

function Get-TargetBlock {
    param(
        [Parameter(Mandatory = $true)][string]$DashboardBaseUrl,
        [Parameter(Mandatory = $true)][string]$Name
    )
    $state = Invoke-DashboardJson -BaseUrl $DashboardBaseUrl -Path "/api/targets"
    $target = @($state.targets | Where-Object { $_.target.name -eq $Name })[0]
    if (-not $target) {
        throw "Target '$Name' not found in dashboard API"
    }
    return [pscustomobject]@{
        State  = $state
        Target = $target
    }
}

function Get-DashboardKeysForUsername {
    param(
        [Parameter(Mandatory = $true)]$TargetBlock,
        [Parameter(Mandatory = $true)][string]$Username
    )

    $keysByUser = $TargetBlock.Target.keys_by_user
    if ($null -eq $keysByUser) {
        return @()
    }
    if ($keysByUser -is [System.Collections.IDictionary]) {
        return @($keysByUser[$Username])
    }
    $property = $keysByUser.PSObject.Properties[$Username]
    if ($null -ne $property) {
        return @($property.Value)
    }
    return @()
}

function Convert-ToYamlLiteral {
    param(
        [Parameter(Mandatory = $true)][string]$Text,
        [int]$Indent = 4
    )
    $spaces = " " * $Indent
    $normalised = ($Text -replace "`r`n", "`n") -replace "`r", "`n"
    return (($normalised -split "`n", -1) | ForEach-Object { "$spaces$_" }) -join "`n"
}

function Get-GatewaySshdConfigText {
    param(
        [int]$ListenPort = 2222
    )
    return @"
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
"@.Trim() + "`n"
}

function Invoke-KubectlExecSh {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$Script
    )
    $tmpDir = Join-Path $ProjectRoot "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $token = [guid]::NewGuid().ToString("N")
    $localCopyPath = "tmp/kubectl-exec-$token.sh"
    $scriptPath = Join-Path $ProjectRoot $localCopyPath
    $remotePath = "/tmp/kubectl-exec-$token.sh"
    $normalisedScript = ($Script -replace "`r`n", "`n") -replace "`r", "`n"
    [System.IO.File]::WriteAllText($scriptPath, $normalisedScript, [System.Text.UTF8Encoding]::new($false))
    try {
        Invoke-Kubectl -Arguments @("cp", $localCopyPath, "${Namespace}/${PodName}:$remotePath") | Out-Null
        return Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "sh", $remotePath)
    }
    finally {
        try {
            Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "rm", "-f", $remotePath) | Out-Null
        }
        catch {
        }
        Remove-Item -LiteralPath $scriptPath -Force -ErrorAction SilentlyContinue
    }
}

function Restore-GatewaySshConfig {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$ConfigMapName,
        [int]$ListenPort = 2222
    )

    $tmpDir = Join-Path $ProjectRoot "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $configPath = Join-Path $tmpDir "portable-openssh-sshd-config.yaml"
    $sshdConfig = Get-GatewaySshdConfigText -ListenPort $ListenPort
    $yaml = @"
apiVersion: v1
kind: ConfigMap
metadata:
  name: $ConfigMapName
  namespace: $Namespace
data:
  sshd_config: |
$(Convert-ToYamlLiteral -Text $sshdConfig -Indent 4)
"@
    [System.IO.File]::WriteAllText($configPath, $yaml, [System.Text.UTF8Encoding]::new($false))
    Invoke-Kubectl -Arguments @("apply", "-f", $configPath) | Out-Null
}

function New-IdentitySpecs {
    param(
        [int]$DeviceCount,
        [string]$GwTag,
        [string]$IoTMacPrefix,
        [string]$PlatformMac
    )

    $platform = [pscustomobject]@{
        Role         = "platform"
        Label        = "platform"
        PodName      = "cmxsafe-iot-platform"
        SecretName   = "cmxsafe-iot-platform-key"
        GwTag        = $GwTag
        MacDev       = $PlatformMac.ToLower()
    }

    $devices = @()
    $cleanPrefix = ($IoTMacPrefix.TrimEnd(":")).ToLower()
    foreach ($index in 1..$DeviceCount) {
        $mac = "{0}:{1}" -f $cleanPrefix, $index.ToString("x2")
        $devices += [pscustomobject]@{
            Role       = "device"
            Label      = "device-{0:d2}" -f $index
            PodName    = "cmxsafe-iot-device-{0:d2}" -f $index
            SecretName = "cmxsafe-iot-device-{0:d2}-key" -f $index
            GwTag      = $GwTag
            MacDev     = $mac.ToLower()
            DeviceIndex = $index
        }
    }

    return [pscustomobject]@{
        Platform = $platform
        Devices  = $devices
        All      = @($platform) + $devices
    }
}

function Ensure-PatchedGatewayBundle {
    param(
        [string]$Version,
        [string]$ImageTag,
        [string]$Namespace,
        [string]$DeploymentName,
        [string]$BundleClaimName = "portable-openssh-runtime",
        [string]$ProjectRoot
    )

    $buildScript = Join-Path $ProjectRoot "tools\build-portable-openssh-bundle.ps1"
    $bundleOutputDir = Join-Path $ProjectRoot "tmp\cmxsafe-k8s-iot-fanout-bundle"
    & $buildScript -Version $Version -ImageTag $ImageTag -OutputDir $bundleOutputDir -ApplyCmxsafePatch

    $loaderPodName = "cmxsafe-portable-bundle-loader"
    $loaderManifestPath = Join-Path $ProjectRoot "tmp\cmxsafe-portable-bundle-loader.yaml"
    $loaderManifest = @"
apiVersion: v1
kind: Pod
metadata:
  name: $loaderPodName
  namespace: $Namespace
  labels:
    cmxsafe-test: iot-fanout
    cmxsafe-kind: bundle-loader
spec:
  restartPolicy: Never
  containers:
    - name: loader
      image: alpine:3.21
      command: ["sh", "-lc", "sleep 3600"]
      volumeMounts:
        - name: runtime
          mountPath: /runtime
  volumes:
    - name: runtime
      persistentVolumeClaim:
        claimName: $BundleClaimName
"@
    [System.IO.File]::WriteAllText($loaderManifestPath, $loaderManifest, [System.Text.UTF8Encoding]::new($false))

    Invoke-Kubectl -Arguments @("delete", "pod", $loaderPodName, "-n", $Namespace, "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", $loaderManifestPath) | Out-Null
    Wait-Until -Description "bundle loader pod running" -TimeoutSeconds 180 -IntervalSeconds 3 -Condition {
        $pod = Invoke-KubectlJson -Arguments @("get", "pod", $loaderPodName, "-n", $Namespace, "-o", "json")
        if ($pod.status.phase -eq "Running") {
            return $pod
        }
        return $null
    } | Out-Null

    Invoke-KubectlExecSh -Namespace $Namespace -PodName $loaderPodName -Script @'
set -eu
rm -rf /runtime/opt/openssh
mkdir -p /runtime/opt
'@ | Out-Null

    $relativeBundlePath = "tmp/cmxsafe-k8s-iot-fanout-bundle/opt/openssh"
    Invoke-Kubectl -Arguments @("cp", $relativeBundlePath, "${Namespace}/${loaderPodName}:/runtime/opt/openssh") | Out-Null
    Invoke-KubectlExecSh -Namespace $Namespace -PodName $loaderPodName -Script @'
set -eu
chmod -R u+rwX,go+rX /runtime/opt/openssh
for binary_dir in /runtime/opt/openssh/bin /runtime/opt/openssh/sbin /runtime/opt/openssh/libexec
do
  if [ -d "$binary_dir" ]; then
    find "$binary_dir" -type f -exec chmod 0555 {} \;
  fi
done
'@ | Out-Null

    Invoke-Kubectl -Arguments @("delete", "pod", $loaderPodName, "-n", $Namespace, "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "restart", "deployment/$DeploymentName", "-n", $Namespace) | Out-Null
}

function Ensure-CanonicalUsers {
    param(
        [string]$AllocatorBaseUrl,
        [string]$DashboardBaseUrl,
        [string]$TargetName,
        $ReferencePod,
        $IdentitySpecs,
        [Parameter(Mandatory = $true)][string]$CanonicalGatewayMac
    )

    $targetState = Get-TargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
    $target = $targetState.Target
    $targetId = $target.target.id
    $defaultPolicy = @($target.policies | Where-Object { $_.name -eq "forwarding-default" })[0]
    $defaultPolicyId = if ($defaultPolicy) { $defaultPolicy.id } else { $null }

    $prepared = @()
    foreach ($spec in $IdentitySpecs) {
        $identity = Ensure-CanonicalIdentity -AllocatorBaseUrl $AllocatorBaseUrl -ReferencePod $ReferencePod -GwTag $spec.GwTag -MacDev $spec.MacDev -CanonicalGatewayMac $CanonicalGatewayMac
        $requestedIpv6 = "$($identity.requested_ipv6)".ToLower()
        $username = Expand-Ipv6Username -RequestedIpv6 $requestedIpv6
        $prepared += [pscustomobject]@{
            Role          = $spec.Role
            Label         = $spec.Label
            PodName       = $spec.PodName
            SecretName    = $spec.SecretName
            GwTag         = $spec.GwTag
            MacDev        = $spec.MacDev
            RequestedIPv6 = $requestedIpv6
            Username      = $username
        }
    }

    $usernames = @($prepared | ForEach-Object { $_.Username })
    $latestRunId = 0
    if ($targetState.State.reconcile_runs -and @($targetState.State.reconcile_runs).Count -gt 0) {
        $latestRunId = [long](@($targetState.State.reconcile_runs)[0].id)
    }

    Invoke-DashboardFormPost -BaseUrl $DashboardBaseUrl -Path "/users/batch" -Body @{
        return_to                 = "/?target_id=$targetId&section=users"
        target_id                 = "$targetId"
        default_policy_profile_id = if ($null -ne $defaultPolicyId) { "$defaultPolicyId" } else { "" }
        user_batch_text           = ($usernames -join "`n")
    } | Out-Null

    $targetState = Get-TargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
    foreach ($row in $prepared) {
        $user = @($targetState.Target.users | Where-Object { $_.username -eq $row.Username })[0]
        if (-not $user) {
            throw "Failed to find dashboard user for $($row.Username)"
        }
        Invoke-DashboardFormPost -BaseUrl $DashboardBaseUrl -Path "/users" -Body @{
            return_to                 = "/?target_id=$targetId&section=users&user_id=$($user.id)"
            target_id                 = "$targetId"
            username                  = $row.Username
            uid                       = "$($user.uid)"
            group_id                  = ""
            home_dir                  = "/home/$($row.Username)"
            shell                     = "/bin/sh"
            comment                   = $row.RequestedIpv6
            enabled                   = "on"
            default_policy_profile_id = if ($null -ne $defaultPolicyId) { "$defaultPolicyId" } else { "" }
        } | Out-Null
    }

    $targetState = Get-TargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
    foreach ($row in $prepared) {
        $user = @($targetState.Target.users | Where-Object { $_.username -eq $row.Username })[0]
        $keys = @(Get-DashboardKeysForUsername -TargetBlock $targetState -Username $row.Username)
        $keyWithPrivate = @($keys | Where-Object { $_.enabled -and $_.private_key })[0]
        if (-not $keyWithPrivate) {
            Invoke-DashboardFormPost -BaseUrl $DashboardBaseUrl -Path "/keys/generate" -Body @{
                return_to = "/?target_id=$targetId&section=users&user_id=$($user.id)"
                user_id   = "$($user.id)"
            } | Out-Null
            $targetState = Get-TargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
            $keys = @(Get-DashboardKeysForUsername -TargetBlock $targetState -Username $row.Username)
            $keyWithPrivate = @($keys | Where-Object { $_.enabled -and $_.private_key })[0]
            if (-not $keyWithPrivate) {
                throw "User $($row.Username) still has no usable private key after generation"
            }
        }
    }

    Invoke-DashboardFormPost -BaseUrl $DashboardBaseUrl -Path "/reconcile" -Body @{
        return_to        = "/?target_id=$targetId&section=reconcile"
        target_id        = "$targetId"
        requested_action = "RENDER_ONLY"
    } | Out-Null
    Wait-ReconcileSucceeded -DashboardBaseUrl $DashboardBaseUrl -MinimumRunId $latestRunId | Out-Null

    $targetState = Get-TargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
    foreach ($row in $prepared) {
        $row | Add-Member -NotePropertyName UserId -NotePropertyValue (@($targetState.Target.users | Where-Object { $_.username -eq $row.Username })[0].id)
        $row | Add-Member -NotePropertyName PrivateKey -NotePropertyValue (@((Get-DashboardKeysForUsername -TargetBlock $targetState -Username $row.Username) | Where-Object { $_.enabled -and $_.private_key })[0].private_key)
    }

    return $prepared
}

function New-EndpointManifest {
    param(
        [string]$Namespace,
        [string]$EndpointImage,
        [int]$ServicePort,
        $IdentitySpecs
    )

    $endpointdText = Get-Content -LiteralPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-endpoint-helper\endpointd.py") -Raw
    $cmxsafeSshText = Get-Content -LiteralPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-endpoint-helper\cmxsafe-ssh") -Raw
    $platformServiceText = Get-Content -LiteralPath (Join-Path $ProjectRoot "tools\tests\openssh\cmxsafe-iot-platform-service.py") -Raw

    $endpointInitText = @'
#!/bin/sh
set -eu
if ! command -v ip >/dev/null 2>&1; then
  apk add --no-cache iproute2 >/dev/null
fi
python3 /opt/cmxsafe/endpointd.py serve --socket /var/run/cmxsafe-endpointd.sock --iface cmx0 >/var/run/endpointd.log 2>&1 &
echo $! >/var/run/endpointd.pid
attempt=0
while [ ! -S /var/run/cmxsafe-endpointd.sock ]
do
  attempt=$((attempt + 1))
  if [ "$attempt" -gt 30 ]; then
    echo "endpointd socket did not appear" >&2
    exit 1
  fi
  sleep 1
done
if [ "${CMXSAFE_ROLE:-device}" = "platform" ]; then
  python3 /opt/cmxsafe/iot-platform-service.py --host "${CMXSAFE_PLATFORM_HOST:-::}" --port "${CMXSAFE_SERVICE_PORT:-9000}" >/var/run/platform-service.log 2>&1 &
  echo $! >/var/run/platform-service.pid
fi
exec sleep infinity
'@

    $stopSessionText = @'
#!/bin/sh
set -eu
pid_file="${1:-}"
socket_file="${2:-}"
if [ -z "$pid_file" ]; then
  echo "usage: stop-session.sh <pid-file> [socket-file]" >&2
  exit 1
fi
if [ -n "$socket_file" ]; then
  for proc_pid in $(ps -ef | awk -v pat="$socket_file" -v self="$$" '$0 ~ pat && $1 != self { print $1 }'); do
    kill "$proc_pid" >/dev/null 2>&1 || true
  done
fi
if [ -f "$pid_file" ]; then
  pid="$(cat "$pid_file")"
  if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    wait "$pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
fi
if [ -n "$socket_file" ]; then
  rm -f "$socket_file"
fi
'@

    $sessionRunningText = @'
#!/bin/sh
set -eu
pid_file="${1:-}"
if [ -z "$pid_file" ] || [ ! -f "$pid_file" ]; then
  exit 1
fi
pid="$(cat "$pid_file")"
kill -0 "$pid" >/dev/null 2>&1
'@

    $startMasterText = @'
#!/bin/sh
set -eu
session_kind="${1:?session kind required}"
gateway_host="${2:?gateway host required}"
gateway_port="${3:?gateway port required}"
pid_file="/var/run/cmxsafe-${session_kind}.pid"
socket_file="/var/run/cmxsafe-${session_kind}.sock"
log_file="/var/run/cmxsafe-${session_kind}.log"
/opt/cmxsafe/stop-session.sh "$pid_file" "$socket_file" || true
export CMXSAFE_ENDPOINTD_SOCK="${CMXSAFE_ENDPOINTD_SOCK:-/var/run/cmxsafe-endpointd.sock}"
export CMXSAFE_SSH_BIN="${CMXSAFE_SSH_BIN:-/opt/openssh/bin/ssh}"
export CMXSAFE_ENDPOINTD_SCRIPT="${CMXSAFE_ENDPOINTD_SCRIPT:-/opt/cmxsafe/endpointd.py}"
export CMXSAFE_CANONICAL_USER="${CMXSAFE_CANONICAL_USER:?missing canonical user}"
nohup /opt/cmxsafe/cmxsafe-ssh \
  -M \
  -S "$socket_file" \
  -F /dev/null \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o ServerAliveInterval=15 \
  -o ServerAliveCountMax=3 \
  -o ConnectTimeout=10 \
  -i /credentials/id_ed25519 \
  -p "$gateway_port" \
  "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >"$log_file" 2>&1 &
echo $! >"$pid_file"
attempt=0
while ! /opt/openssh/bin/ssh -S "$socket_file" -O check -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$gateway_port" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >/dev/null 2>&1
do
  attempt=$((attempt + 1))
  if [ "$attempt" -gt 30 ]; then
    echo "SSH control master did not become ready" >&2
    exit 1
  fi
  sleep 1
done
/opt/cmxsafe/session-running.sh "$pid_file"
'@

    $installForwardText = @'
#!/bin/sh
set -eu
platform_ipv6="${1:?platform ipv6 required}"
remote_port="${2:?remote port required}"
local_port="${3:?local port required}"
gateway_host="${4:?gateway host required}"
gateway_port="${5:?gateway port required}"
socket_file="/var/run/cmxsafe-forward.sock"
log_file="/var/run/cmxsafe-forward.log"
endpointd_script="${CMXSAFE_ENDPOINTD_SCRIPT:-/opt/cmxsafe/endpointd.py}"
endpointd_sock="${CMXSAFE_ENDPOINTD_SOCK:-/var/run/cmxsafe-endpointd.sock}"
python_bin="${CMXSAFE_ENDPOINTD_PYTHON:-python3}"
owner_pid="$(cat /var/run/cmxsafe-forward.pid 2>/dev/null || echo $$)"
peer_owner="peer:pid:${owner_pid}:listen:${local_port}"
"$python_bin" "$endpointd_script" ensure --socket "$endpointd_sock" --scope peer --owner "$peer_owner" --ipv6 "$platform_ipv6" >/dev/null
/opt/openssh/bin/ssh -S "$socket_file" -O check -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$gateway_port" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >/dev/null 2>&1
/opt/openssh/bin/ssh -S "$socket_file" -O forward -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$gateway_port" -L "[${platform_ipv6}]:${local_port}:[${platform_ipv6}]:${remote_port}" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >>"$log_file" 2>&1
'@

    $installReverseText = @'
#!/bin/sh
set -eu
remote_port="${1:?remote port required}"
gateway_host="${2:?gateway host required}"
gateway_port="${3:?gateway port required}"
local_service_port="${4:?local service port required}"
socket_file="/var/run/cmxsafe-reverse.sock"
log_file="/var/run/cmxsafe-reverse.log"
python_bin="${CMXSAFE_ENDPOINTD_PYTHON:-python3}"
normalise_user_to_ipv6() {
    value=$(printf '%s' "$1" | tr 'A-F' 'a-f')
    case "$value" in
        *[!0-9a-f]*)
            return 1
            ;;
    esac
    if [ "${#value}" -ne 32 ]; then
        return 1
    fi
    printf '%s:%s:%s:%s:%s:%s:%s:%s' \
        "$(printf '%s' "$value" | cut -c1-4)" \
        "$(printf '%s' "$value" | cut -c5-8)" \
        "$(printf '%s' "$value" | cut -c9-12)" \
        "$(printf '%s' "$value" | cut -c13-16)" \
        "$(printf '%s' "$value" | cut -c17-20)" \
        "$(printf '%s' "$value" | cut -c21-24)" \
        "$(printf '%s' "$value" | cut -c25-28)" \
        "$(printf '%s' "$value" | cut -c29-32)"
}
listen_ipv6="$(normalise_user_to_ipv6 "${CMXSAFE_CANONICAL_USER:?missing canonical user}")"
if [ -n "${CMXSAFE_SELF_IPV6:-}" ] && [ "$listen_ipv6" != "$CMXSAFE_SELF_IPV6" ]; then
  env_ipv6="$("$python_bin" -c 'import ipaddress, sys; print(ipaddress.IPv6Address(sys.argv[1]).exploded)' "$CMXSAFE_SELF_IPV6")"
  if [ "$listen_ipv6" != "$env_ipv6" ]; then
    echo "canonical user IPv6 $listen_ipv6 does not match CMXSAFE_SELF_IPV6 $CMXSAFE_SELF_IPV6" >&2
    exit 1
  fi
fi
/opt/openssh/bin/ssh -S "$socket_file" -O check -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$gateway_port" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >/dev/null 2>&1
/opt/openssh/bin/ssh -S "$socket_file" -O forward -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$gateway_port" -R "[${listen_ipv6}]:${remote_port}:[::1]:${local_service_port}" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >>"$log_file" 2>&1
'@

    $pieces = @()
    $pieces += @"
apiVersion: v1
kind: ConfigMap
metadata:
  name: cmxsafe-iot-fanout-scripts
  namespace: $Namespace
  labels:
    cmxsafe-test: iot-fanout
data:
  endpointd.py: |
$(Convert-ToYamlLiteral -Text $endpointdText -Indent 4)
  cmxsafe-ssh: |
$(Convert-ToYamlLiteral -Text $cmxsafeSshText -Indent 4)
  endpoint-init.sh: |
$(Convert-ToYamlLiteral -Text $endpointInitText -Indent 4)
  stop-session.sh: |
$(Convert-ToYamlLiteral -Text $stopSessionText -Indent 4)
  session-running.sh: |
$(Convert-ToYamlLiteral -Text $sessionRunningText -Indent 4)
  start-master.sh: |
$(Convert-ToYamlLiteral -Text $startMasterText -Indent 4)
  install-forward.sh: |
$(Convert-ToYamlLiteral -Text $installForwardText -Indent 4)
  install-reverse.sh: |
$(Convert-ToYamlLiteral -Text $installReverseText -Indent 4)
  iot-platform-service.py: |
$(Convert-ToYamlLiteral -Text $platformServiceText -Indent 4)
"@

    foreach ($spec in $IdentitySpecs) {
        $pieces += @"
---
apiVersion: v1
kind: Secret
metadata:
  name: $($spec.SecretName)
  namespace: $Namespace
  labels:
    cmxsafe-test: iot-fanout
    cmxsafe-role: $($spec.Role)
type: Opaque
stringData:
  id_ed25519: |
$(Convert-ToYamlLiteral -Text $spec.PrivateKey -Indent 4)
"@

        $pieces += @"
---
apiVersion: v1
kind: Pod
metadata:
  name: $($spec.PodName)
  namespace: $Namespace
  labels:
    app: cmxsafe-iot-endpoint
    cmxsafe-test: iot-fanout
    cmxsafe-role: $($spec.Role)
spec:
  restartPolicy: Always
  containers:
    - name: endpoint
      image: $EndpointImage
      imagePullPolicy: IfNotPresent
      command: ["sh", "-lc", "exec /opt/cmxsafe/endpoint-init.sh"]
      env:
        - name: CMXSAFE_ROLE
          value: $($spec.Role)
        - name: CMXSAFE_CANONICAL_USER
          value: $($spec.Username)
        - name: CMXSAFE_SELF_IPV6
          value: $($spec.RequestedIPv6)
        - name: CMXSAFE_MAC_DEV
          value: $($spec.MacDev)
        - name: CMXSAFE_SERVICE_PORT
          value: "$ServicePort"
      securityContext:
        runAsUser: 0
        runAsGroup: 0
        allowPrivilegeEscalation: false
        capabilities:
          drop:
            - ALL
          add:
            - NET_ADMIN
      volumeMounts:
        - name: scripts
          mountPath: /opt/cmxsafe
          readOnly: true
        - name: credentials
          mountPath: /credentials
          readOnly: true
        - name: openssh-runtime
          mountPath: /opt/openssh
          subPath: opt/openssh
          readOnly: true
        - name: run-live
          mountPath: /var/run
  volumes:
    - name: scripts
      configMap:
        name: cmxsafe-iot-fanout-scripts
        defaultMode: 0555
    - name: credentials
      secret:
        secretName: $($spec.SecretName)
        defaultMode: 0400
    - name: openssh-runtime
      persistentVolumeClaim:
        claimName: portable-openssh-runtime
        readOnly: true
    - name: run-live
      emptyDir: {}
"@
    }

    return ($pieces -join "`n")
}

function Wait-EndpointRuntimeReady {
    param(
        [string]$Namespace,
        [string[]]$PodNames,
        [int]$ServicePort
    )

    foreach ($podName in $PodNames) {
        Wait-Until -Description "endpointd socket on $podName" -TimeoutSeconds 240 -IntervalSeconds 3 -Condition {
            try {
                Invoke-KubectlExecSh -Namespace $Namespace -PodName $podName -Script "test -S /var/run/cmxsafe-endpointd.sock"
                return $true
            }
            catch {
                return $null
            }
        } | Out-Null
    }

    $platformPod = @($PodNames | Where-Object { $_ -eq "cmxsafe-iot-platform" })[0]
    if ($platformPod) {
        Wait-Until -Description "platform loopback service on $platformPod" -TimeoutSeconds 240 -IntervalSeconds 3 -Condition {
            try {
                $result = Invoke-Kubectl -Arguments @(
                    "exec", "-n", $Namespace, $platformPod, "--",
                    "python3", "-c",
                    "import urllib.request; import sys; body = urllib.request.urlopen('http://[::1]:$ServicePort/healthz', timeout=5).read().decode('utf-8'); print(body)"
                )
                if ($result -match '"ok"\s*:\s*true') {
                    return $true
                }
                return $null
            }
            catch {
                return $null
            }
        } | Out-Null
    }
}

function Get-CanonicalOwnerPod {
    param(
        [string]$AllocatorBaseUrl,
        [string]$RequestedIpv6,
        $GatewayPods
    )

    $encodedIpv6 = [System.Uri]::EscapeDataString($RequestedIpv6.ToLower())
    $rows = @(Invoke-AllocatorApi -BaseUrl $AllocatorBaseUrl -Path "/explicit-ipv6-assignments?requested_ipv6=$encodedIpv6&status=ACTIVE")
    $row = @($rows | Where-Object { "$($_.requested_ipv6)".ToLower() -eq $RequestedIpv6.ToLower() } | Select-Object -First 1)[0]
    if (-not $row) {
        return $null
    }
    $pod = @($GatewayPods | Where-Object { $_.metadata.uid -eq $row.pod_uid } | Select-Object -First 1)[0]
    if (-not $pod) {
        return $null
    }
    return $pod
}

function Wait-CanonicalOwner {
    param(
        [string]$AllocatorBaseUrl,
        [string]$RequestedIpv6,
        $GatewayPods,
        [string]$Description
    )
    return Wait-Until -Description $Description -TimeoutSeconds 180 -IntervalSeconds 3 -Condition {
        Get-CanonicalOwnerPod -AllocatorBaseUrl $AllocatorBaseUrl -RequestedIpv6 $RequestedIpv6 -GatewayPods $GatewayPods
    }
}

function Wait-CanonicalAddressOnGateway {
    param(
        [string]$Namespace,
        [Parameter(Mandatory = $true)]$GatewayPod,
        [string]$RequestedIpv6,
        [int]$TimeoutSeconds = 30
    )
    return Wait-Until -Description "canonical IPv6 $RequestedIpv6 visible on $($GatewayPod.metadata.name)" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 1 -Condition {
        try {
            Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $GatewayPod.metadata.name, "--", "sh", "-lc", "ip -6 addr show dev net1 | grep -F '$RequestedIpv6/128'") | Out-Null
            return $true
        }
        catch {
            return $null
        }
    }
}

function Start-EndpointMasterSession {
    param(
        [string]$Namespace,
        [string]$PodName,
        [ValidateSet("forward", "reverse")][string]$SessionKind,
        [string]$GatewayHost,
        [int]$GatewayPort
    )
    Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "/opt/cmxsafe/start-master.sh", $SessionKind, $GatewayHost, "$GatewayPort") | Out-Null
}

function Install-PlatformReverseForward {
    param(
        [string]$Namespace,
        [string]$PodName,
        [int]$ServicePort,
        [string]$GatewayHost,
        [int]$GatewayPort
    )
    Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "/opt/cmxsafe/install-reverse.sh", "$ServicePort", $GatewayHost, "$GatewayPort", "$ServicePort") | Out-Null
}

function Assert-PlatformRejectsNonCanonicalReverse {
    param(
        [string]$Namespace,
        [string]$PodName,
        [int]$ServicePort,
        [string]$GatewayHost,
        [int]$GatewayPort
    )
    $badPort = $ServicePort + 101
    $script = @'
set -eu
bad_port="__BAD_PORT__"
gateway_host="__GATEWAY_HOST__"
gateway_port="__GATEWAY_PORT__"
service_port="__SERVICE_PORT__"
log_file="/tmp/cmxsafe-noncanonical-rforward.log"
if /opt/openssh/bin/ssh -S /var/run/cmxsafe-reverse.sock -O forward -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ExitOnForwardFailure=yes -p "$gateway_port" -R "[::1]:${bad_port}:[::1]:${service_port}" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >"$log_file" 2>&1; then
  /opt/openssh/bin/ssh -S /var/run/cmxsafe-reverse.sock -O cancel -F /dev/null -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$gateway_port" -R "[::1]:${bad_port}:[::1]:${service_port}" "${CMXSAFE_CANONICAL_USER}@${gateway_host}" >/dev/null 2>&1 || true
  cat "$log_file" >&2 || true
  echo "non-canonical reverse forward unexpectedly succeeded" >&2
  exit 1
fi
'@
    $script = $script.Replace("__BAD_PORT__", "$badPort").Replace("__GATEWAY_HOST__", $GatewayHost).Replace("__GATEWAY_PORT__", "$GatewayPort").Replace("__SERVICE_PORT__", "$ServicePort")
    Invoke-KubectlExecSh -Namespace $Namespace -PodName $PodName -Script $script | Out-Null
}

function Install-DeviceForward {
    param(
        [string]$Namespace,
        [string]$PodName,
        [string]$PlatformIpv6,
        [int]$ServicePort,
        [string]$GatewayHost,
        [int]$GatewayPort
    )
    Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "/opt/cmxsafe/install-forward.sh", $PlatformIpv6, "$ServicePort", "$ServicePort", $GatewayHost, "$GatewayPort") | Out-Null
}

function Stop-EndpointSession {
    param(
        [string]$Namespace,
        [string]$PodName,
        [ValidateSet("forward", "reverse")][string]$Type
    )
    $pidFile = if ($Type -eq "reverse") { "/var/run/cmxsafe-reverse.pid" } else { "/var/run/cmxsafe-forward.pid" }
    $socketFile = if ($Type -eq "reverse") { "/var/run/cmxsafe-reverse.sock" } else { "/var/run/cmxsafe-forward.sock" }
    Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "/opt/cmxsafe/stop-session.sh", $pidFile, $socketFile) | Out-Null
}

function Get-EndpointSessionLog {
    param(
        [string]$Namespace,
        [string]$PodName,
        [ValidateSet("forward", "reverse")][string]$Type
    )
    $logFile = if ($Type -eq "reverse") { "/var/run/cmxsafe-reverse.log" } else { "/var/run/cmxsafe-forward.log" }
    try {
        return Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "cat", $logFile)
    }
    catch {
        return ""
    }
}

function Invoke-DeviceProbe {
    param(
        [string]$Namespace,
        [string]$PodName,
        [string]$PlatformIpv6,
        [int]$ServicePort,
        [string]$DeviceMac,
        [string]$Content,
        [int]$Sequence
    )
    $payload = @{
        type       = "telemetry"
        sequence   = $Sequence
        device_pod = $PodName
        device_mac = $DeviceMac
        content    = $Content
    } | ConvertTo-Json -Compress
    $encodedPayload = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($payload))
    $json = Invoke-Kubectl -Arguments @(
        "exec", "-n", $Namespace, $PodName, "--",
        "python3", "-c",
        "import base64, urllib.request; data = base64.b64decode('$encodedPayload'); req = urllib.request.Request('http://[$PlatformIpv6]:$ServicePort/message', data=data, headers={'Content-Type':'application/json'}, method='POST'); print(urllib.request.urlopen(req, timeout=10).read().decode('utf-8'))"
    )
    return $json | ConvertFrom-Json
}

function Get-DeviceOwnerMap {
    param(
        [string]$AllocatorBaseUrl,
        $DeviceSpecs,
        $GatewayPods
    )
    $map = @{}
    foreach ($device in $DeviceSpecs) {
        $owner = Get-CanonicalOwnerPod -AllocatorBaseUrl $AllocatorBaseUrl -RequestedIpv6 $device.RequestedIpv6 -GatewayPods $GatewayPods
        $map[$device.PodName] = $owner
    }
    return $map
}

function Get-DistributionSummary {
    param(
        $GatewayPods,
        [hashtable]$OwnerMap
    )

    $summary = @{}
    foreach ($pod in $GatewayPods) {
        $summary[$pod.metadata.uid] = [pscustomobject]@{
            PodName = $pod.metadata.name
            PodUid  = $pod.metadata.uid
            Count   = 0
        }
    }
    foreach ($entry in $OwnerMap.GetEnumerator()) {
        if ($entry.Value) {
            $summary[$entry.Value.metadata.uid].Count++
        }
    }
    return @($summary.Values | Sort-Object PodName)
}

function Move-PlatformSession {
    param(
        [string]$AllocatorBaseUrl,
        [string]$Namespace,
        $PlatformSpec,
        $GatewayPods,
        [int]$ServicePort,
        [string]$GatewayHost,
        [int]$GatewayPort,
        [string]$CurrentPodUid,
        [int]$MaxAttempts
    )

    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        Stop-EndpointSession -Namespace $Namespace -PodName $PlatformSpec.PodName -Type reverse
        Start-EndpointMasterSession -Namespace $Namespace -PodName $PlatformSpec.PodName -SessionKind "reverse" -GatewayHost $GatewayHost -GatewayPort $GatewayPort
        $owner = Wait-CanonicalOwner -AllocatorBaseUrl $AllocatorBaseUrl -RequestedIpv6 $PlatformSpec.RequestedIpv6 -GatewayPods $GatewayPods -Description "platform canonical owner after move attempt $attempt"
        Wait-CanonicalAddressOnGateway -Namespace $Namespace -GatewayPod $owner -RequestedIpv6 $PlatformSpec.RequestedIpv6 | Out-Null
        if ($owner.metadata.uid -ne $CurrentPodUid) {
            Install-PlatformReverseForward -Namespace $Namespace -PodName $PlatformSpec.PodName -ServicePort $ServicePort -GatewayHost $GatewayHost -GatewayPort $GatewayPort
            return [pscustomobject]@{
                Pod      = $owner
                Attempts = $attempt
            }
        }
    }

    throw "Platform reverse session could not be moved to the other gateway replica after $MaxAttempts attempts"
}

try {
    Ensure-Command -Name "kubectl"
    Ensure-Command -Name "docker"

    if (-not $PortableOpenSshImageTag) {
        $PortableOpenSshImageTag = "cmxsafe-portable-openssh-build:$PortableOpenSshVersion"
    }

    $identities = New-IdentitySpecs -DeviceCount $DeviceCount -GwTag $GatewayGwTag -IoTMacPrefix $IoTMacPrefix -PlatformMac $PlatformMac
    $allEndpointPods = @($identities.All | ForEach-Object { $_.PodName })

    Write-Step "Applying gateway and dashboard manifests"
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\explicit-v6-network.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\busybox-portable-openssh-test.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\portable-openssh-dashboard.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("scale", "deployment/$GatewayDeployment", "-n", $Namespace, "--replicas=$GatewayReplicas") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/$DashboardDeployment", "-n", $Namespace, "--timeout=420s") | Out-Null

    Write-Step "Building and staging the patched Portable OpenSSH bundle"
    Ensure-PatchedGatewayBundle -Version $PortableOpenSshVersion -ImageTag $PortableOpenSshImageTag -Namespace $Namespace -DeploymentName $GatewayDeployment -ProjectRoot $ProjectRoot

    Write-Step "Waiting for the two gateway replicas"
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/$GatewayDeployment", "-n", $Namespace, "--timeout=420s") | Out-Null
    $gatewayPods = Wait-PodsReady -Namespace $Namespace -Selector $GatewaySelector -ExpectedCount $GatewayReplicas -TimeoutSeconds 420

    Write-Step "Opening allocator and dashboard API access"
    $allocatorForward = Start-AllocatorPortForward -Port $AllocatorPort -ProjectRoot $ProjectRoot
    $dashboardForward = Start-DashboardPortForward -Port $DashboardPort -ProjectRoot $ProjectRoot
    $allocatorBaseUrl = $allocatorForward.BaseUrl
    $dashboardBaseUrl = $dashboardForward.BaseUrl
    Write-Info "Allocator API: $allocatorBaseUrl"
    Write-Info "Dashboard API: $dashboardBaseUrl"
    Write-Step "Configuring stable canonical gateway MAC"
    Set-DashboardCanonicalGatewayMac -DashboardBaseUrl $dashboardBaseUrl -TargetName $TargetName -CanonicalGatewayMac $CanonicalGatewayMac -ListenPort $SshPort
    Write-Info "Canonical gateway MAC: $CanonicalGatewayMac"

    Write-Step "Ensuring managed allocations exist for both gateway replicas"
    foreach ($pod in $gatewayPods) {
        $allocation = Ensure-AllocationForPod -BaseUrl $allocatorBaseUrl -Pod $pod -ContainerIface "eth0" -Prefix "fd42:4242:4242:10::/64"
        Write-Info "$($pod.metadata.name) -> managed IPv6 $($allocation.assigned_ipv6), auto net1 $($allocation.auto_managed_explicit_ipv6)"
    }

    Write-Step "Creating canonical users and harvesting generated keypairs"
    $preparedIdentities = Ensure-CanonicalUsers -AllocatorBaseUrl $allocatorBaseUrl -DashboardBaseUrl $dashboardBaseUrl -TargetName $TargetName -ReferencePod ($gatewayPods | Select-Object -First 1) -IdentitySpecs $identities.All -CanonicalGatewayMac $CanonicalGatewayMac
    $platformSpec = @($preparedIdentities | Where-Object { $_.Role -eq "platform" })[0]
    $deviceSpecs = @($preparedIdentities | Where-Object { $_.Role -eq "device" } | Sort-Object PodName)

    Write-Step "Restoring sshd_config with GatewayPorts clientspecified"
    Restore-GatewaySshConfig -Namespace $Namespace -ConfigMapName $GatewayConfigMap -ListenPort $SshPort
    Invoke-Kubectl -Arguments @("rollout", "restart", "deployment/$GatewayDeployment", "-n", $Namespace) | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/$GatewayDeployment", "-n", $Namespace, "--timeout=420s") | Out-Null
    $gatewayPods = Wait-PodsReady -Namespace $Namespace -Selector $GatewaySelector -ExpectedCount $GatewayReplicas -TimeoutSeconds 420
    foreach ($pod in $gatewayPods) {
        $allocation = Ensure-AllocationForPod -BaseUrl $allocatorBaseUrl -Pod $pod -ContainerIface "eth0" -Prefix "fd42:4242:4242:10::/64"
        Write-Info "Reloaded $($pod.metadata.name) -> managed IPv6 $($allocation.assigned_ipv6), auto net1 $($allocation.auto_managed_explicit_ipv6)"
    }

    Write-Step "Deploying IoT device and platform endpoint pods"
    Invoke-Kubectl -Arguments @("delete", "pod,secret,configmap", "-n", $Namespace, "-l", "cmxsafe-test=iot-fanout", "--ignore-not-found=true") | Out-Null
    $endpointManifest = New-EndpointManifest -Namespace $Namespace -EndpointImage $EndpointImage -ServicePort $ServicePort -IdentitySpecs $preparedIdentities
    $endpointManifestPath = Join-Path $ProjectRoot "tmp\cmxsafe-iot-fanout-endpoints.yaml"
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $endpointManifestPath) | Out-Null
    [System.IO.File]::WriteAllText($endpointManifestPath, $endpointManifest, [System.Text.UTF8Encoding]::new($false))
    Invoke-Kubectl -Arguments @("apply", "-f", $endpointManifestPath) | Out-Null
    Wait-PodsReady -Namespace $Namespace -Selector "app=cmxsafe-iot-endpoint,cmxsafe-test=iot-fanout" -ExpectedCount ($DeviceCount + 1) -TimeoutSeconds 420 | Out-Null
    Wait-EndpointRuntimeReady -Namespace $Namespace -PodNames $allEndpointPods -ServicePort $ServicePort

    Write-Step "Starting the platform reverse session"
    Start-EndpointMasterSession -Namespace $Namespace -PodName $platformSpec.PodName -SessionKind "reverse" -GatewayHost $GatewayService -GatewayPort $SshPort
    $platformOwner = Wait-CanonicalOwner -AllocatorBaseUrl $allocatorBaseUrl -RequestedIpv6 $platformSpec.RequestedIPv6 -GatewayPods $gatewayPods -Description "platform reverse session owner"
    Wait-CanonicalAddressOnGateway -Namespace $Namespace -GatewayPod $platformOwner -RequestedIpv6 $platformSpec.RequestedIPv6 | Out-Null
    Install-PlatformReverseForward -Namespace $Namespace -PodName $platformSpec.PodName -ServicePort $ServicePort -GatewayHost $GatewayService -GatewayPort $SshPort
    Assert-PlatformRejectsNonCanonicalReverse -Namespace $Namespace -PodName $platformSpec.PodName -ServicePort $ServicePort -GatewayHost $GatewayService -GatewayPort $SshPort
    Write-Info "Platform session landed on $($platformOwner.metadata.name)"

    Write-Step "Starting the ten IoT device forward sessions"
    foreach ($device in $deviceSpecs) {
        Start-EndpointMasterSession -Namespace $Namespace -PodName $device.PodName -SessionKind "forward" -GatewayHost $GatewayService -GatewayPort $SshPort
        $owner = Wait-CanonicalOwner -AllocatorBaseUrl $allocatorBaseUrl -RequestedIpv6 $device.RequestedIPv6 -GatewayPods $gatewayPods -Description "device session owner for $($device.PodName)"
        Wait-CanonicalAddressOnGateway -Namespace $Namespace -GatewayPod $owner -RequestedIpv6 $device.RequestedIPv6 | Out-Null
        Install-DeviceForward -Namespace $Namespace -PodName $device.PodName -PlatformIpv6 $platformSpec.RequestedIPv6 -ServicePort $ServicePort -GatewayHost $GatewayService -GatewayPort $SshPort
        Write-Info "$($device.PodName) -> $($owner.metadata.name)"
    }

    Write-Step "Reporting natural device session distribution across gateway replicas"
    $naturalOwnerMap = Get-DeviceOwnerMap -AllocatorBaseUrl $allocatorBaseUrl -DeviceSpecs $deviceSpecs -GatewayPods $gatewayPods
    $naturalSummary = Get-DistributionSummary -GatewayPods $gatewayPods -OwnerMap $naturalOwnerMap
    foreach ($row in $naturalSummary) {
        Write-Info "$($row.PodName) => $($row.Count) device sessions"
    }

    Write-Step "Validating that all ten IoT devices can reach the platform service"
    $initialResponses = @()
    foreach ($device in $deviceSpecs) {
        $sequence = $initialResponses.Count + 1
        $content = "initial telemetry from $($device.PodName) mac=$($device.MacDev)"
        $response = Invoke-DeviceProbe -Namespace $Namespace -PodName $device.PodName -PlatformIpv6 $platformSpec.RequestedIPv6 -ServicePort $ServicePort -DeviceMac $device.MacDev -Content $content -Sequence $sequence
        if ("$($response.client)".ToLower() -ne $device.RequestedIPv6.ToLower()) {
            throw "Device $($device.PodName) reached the platform, but the observed source IPv6 was $($response.client) instead of $($device.RequestedIPv6)"
        }
        if ("$($response.device_mac)".ToLower() -ne $device.MacDev.ToLower()) {
            throw "Device $($device.PodName) reached the platform, but the decoded MAC was $($response.device_mac) instead of $($device.MacDev)"
        }
        if ("$($response.content)" -ne $content) {
            throw "Device $($device.PodName) reached the platform, but the observed message content was '$($response.content)' instead of '$content'"
        }
        $initialResponses += [pscustomobject]@{
            DevicePod     = $device.PodName
            DeviceMac     = $device.MacDev
            DeviceIPv6    = $device.RequestedIPv6
            ObservedIPv6  = "$($response.client)".ToLower()
            ObservedMac   = "$($response.device_mac)".ToLower()
            ObservedPort  = [int]$response.port
            Content       = "$($response.content)"
            PlatformPod   = "$($response.pod)"
        }
        Write-Info "$($device.PodName) observed as $($response.device_mac) $($response.client):$($response.port) content='$($response.content)'"
    }

    Write-Step "Moving the platform reverse session to the other gateway replica"
    $movedPlatform = Move-PlatformSession -AllocatorBaseUrl $allocatorBaseUrl -Namespace $Namespace -PlatformSpec $platformSpec -GatewayPods $gatewayPods -ServicePort $ServicePort -GatewayHost $GatewayService -GatewayPort $SshPort -CurrentPodUid $platformOwner.metadata.uid -MaxAttempts $MaxPlatformMoveAttempts
    Write-Info "Platform session moved to $($movedPlatform.Pod.metadata.name) after $($movedPlatform.Attempts) attempts"

    Write-Step "Re-validating all ten devices after the platform session move"
    $postMoveResponses = @()
    foreach ($device in $deviceSpecs) {
        $sequence = 100 + $postMoveResponses.Count + 1
        $content = "post-move telemetry from $($device.PodName) mac=$($device.MacDev)"
        $response = Invoke-DeviceProbe -Namespace $Namespace -PodName $device.PodName -PlatformIpv6 $platformSpec.RequestedIPv6 -ServicePort $ServicePort -DeviceMac $device.MacDev -Content $content -Sequence $sequence
        if ("$($response.client)".ToLower() -ne $device.RequestedIPv6.ToLower()) {
            throw "After moving the platform session, device $($device.PodName) was observed as $($response.client) instead of $($device.RequestedIPv6)"
        }
        if ("$($response.device_mac)".ToLower() -ne $device.MacDev.ToLower()) {
            throw "After moving the platform session, device $($device.PodName) decoded MAC was $($response.device_mac) instead of $($device.MacDev)"
        }
        if ("$($response.content)" -ne $content) {
            throw "After moving the platform session, device $($device.PodName) content was '$($response.content)' instead of '$content'"
        }
        $postMoveResponses += [pscustomobject]@{
            DevicePod     = $device.PodName
            DeviceMac     = $device.MacDev
            DeviceIPv6    = $device.RequestedIPv6
            ObservedIPv6  = "$($response.client)".ToLower()
            ObservedMac   = "$($response.device_mac)".ToLower()
            ObservedPort  = [int]$response.port
            Content       = "$($response.content)"
            PlatformPod   = "$($response.pod)"
        }
        Write-Info "$($device.PodName) still observed as $($response.device_mac) $($response.client):$($response.port) content='$($response.content)'"
    }

    Write-Host ""
    Write-Host "CMXsafe IoT fan-out test passed." -ForegroundColor Green
    Write-Host "Platform canonical IPv6: $($platformSpec.RequestedIPv6)" -ForegroundColor DarkGray
    Write-Host "Initial platform gateway pod: $($platformOwner.metadata.name)" -ForegroundColor DarkGray
    Write-Host "Moved platform gateway pod:   $($movedPlatform.Pod.metadata.name)" -ForegroundColor DarkGray
    Write-Host "Live monitor: kubectl port-forward -n $Namespace pod/$($platformSpec.PodName) 19000:$ServicePort; open http://127.0.0.1:19000/monitor" -ForegroundColor DarkGray
    Write-Host ""
    $initialResponses | Format-Table DevicePod, DeviceMac, DeviceIPv6, ObservedPort, Content, PlatformPod -AutoSize
}
finally {
    if ($CleanupEndpointResources) {
        try {
            Invoke-Kubectl -Arguments @("delete", "pod,secret,configmap", "-n", $Namespace, "-l", "cmxsafe-test=iot-fanout", "--ignore-not-found=true") | Out-Null
        }
        catch {
        }
    }
}
