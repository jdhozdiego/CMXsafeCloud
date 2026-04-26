param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
    [int]$AllocatorPort = 18080,
    [int]$DashboardPort = 18084,
    [string]$Namespace = "mac-ssh-demo",
    [string]$Selector = "app=portable-openssh-busybox",
    [string]$ManagedIface = "eth0",
    [string]$ExplicitIface = "net1",
    [string]$Ipv6Prefix = "fd42:4242:4242:10::/64",
    [string]$CanonicalGatewayMac = "f6:db:2b:39:78:94"
)

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

function Get-DashboardTargetBlock {
    param(
        [Parameter(Mandatory = $true)][string]$DashboardBaseUrl,
        [string]$Name = "portable-openssh-busybox"
    )
    $state = Invoke-DashboardJson -BaseUrl $DashboardBaseUrl -Path "/api/targets"
    $target = @($state.targets | Where-Object { $_.target.name -eq $Name })[0]
    if (-not $target) {
        throw "$Name target not found in dashboard API"
    }
    return $target
}

function Set-DashboardCanonicalGatewayMac {
    param(
        [Parameter(Mandatory = $true)][string]$DashboardBaseUrl,
        [Parameter(Mandatory = $true)][string]$CanonicalGatewayMac,
        [string]$TargetName = "portable-openssh-busybox",
        [int]$ListenPort = 2222
    )
    $target = Get-DashboardTargetBlock -DashboardBaseUrl $DashboardBaseUrl -Name $TargetName
    $settings = $target.server_settings
    $effectiveListenPort = if ($settings -and $settings.listen_port) { [int]$settings.listen_port } else { $ListenPort }
    $allowTcpForwarding = if ($null -eq $settings -or $settings.allow_tcp_forwarding -ne $false) { "on" } else { "" }
    $gatewayPorts = if ($null -ne $settings -and $settings.gateway_ports) { "on" } else { "" }
    $permitTunnel = if ($null -ne $settings -and $settings.permit_tunnel) { "on" } else { "" }
    $x11Forwarding = if ($null -ne $settings -and $settings.x11_forwarding) { "on" } else { "" }
    $logLevel = if ($null -ne $settings -and $settings.log_level) { "$($settings.log_level)" } else { "VERBOSE" }
    Invoke-DashboardFormPost -BaseUrl $DashboardBaseUrl -Path "/server-settings" -Body @{
        return_to             = "/"
        target_id             = "$($target.target.id)"
        canonical_gateway_mac = $CanonicalGatewayMac
        listen_port           = "$effectiveListenPort"
        allow_tcp_forwarding  = $allowTcpForwarding
        gateway_ports         = $gatewayPorts
        permit_tunnel         = $permitTunnel
        x11_forwarding        = $x11Forwarding
        log_level             = $logLevel
    } | Out-Null
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
        gw_iface       = $ContainerIface
        node_name      = $Pod.spec.nodeName
        namespace      = $Pod.metadata.namespace
        pod_name       = $Pod.metadata.name
        pod_uid        = $Pod.metadata.uid
        container_iface = $ContainerIface
        ipv6_prefix    = $Prefix
        mac_dev        = "ipv6-derived-only"
        owner_kind     = if ($owner) { $owner.kind } else { "Pod" }
        owner_name     = if ($owner) { $owner.name } else { $Pod.metadata.name }
        owner_uid      = if ($owner) { $owner.uid } else { $Pod.metadata.uid }
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

try {
    Write-Step "Applying portable OpenSSH canonical test manifests"
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\explicit-v6-network.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\busybox-portable-openssh-test.yaml")) | Out-Null

    Write-Step "Waiting for 4 portable OpenSSH replicas"
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/portable-openssh-busybox", "-n", $Namespace, "--timeout=420s") | Out-Null
    $sshPods = Wait-PodsReady -Namespace $Namespace -Selector $Selector -ExpectedCount 4 -TimeoutSeconds 420

    Write-Step "Opening allocator and dashboard API access"
    $allocatorForward = Start-AllocatorPortForward -Port $AllocatorPort -ProjectRoot $ProjectRoot
    $dashboardForward = Start-DashboardPortForward -Port $DashboardPort -ProjectRoot $ProjectRoot
    $allocatorBaseUrl = $allocatorForward.BaseUrl
    $dashboardBaseUrl = $dashboardForward.BaseUrl
    Write-Info "Allocator API: $allocatorBaseUrl"
    Write-Info "Dashboard API: $dashboardBaseUrl"
    Write-Step "Configuring stable canonical gateway MAC"
    Set-DashboardCanonicalGatewayMac -DashboardBaseUrl $dashboardBaseUrl -CanonicalGatewayMac $CanonicalGatewayMac
    Write-Info "Canonical gateway MAC: $CanonicalGatewayMac"

    Write-Step "Ensuring managed allocations exist for all SSH replicas"
    $allocations = @{}
    foreach ($pod in $sshPods) {
        $allocation = Ensure-AllocationForPod -BaseUrl $allocatorBaseUrl -Pod $pod -ContainerIface $ManagedIface -Prefix $Ipv6Prefix
        $allocations[$pod.metadata.uid] = $allocation
        Write-Info "$($pod.metadata.name) -> managed IPv6 $($allocation.assigned_ipv6), auto net1 $($allocation.auto_managed_explicit_ipv6)"
    }

    Write-Step "Preparing two canonical SSH identities"
    $referencePod = $sshPods | Select-Object -First 1
    $identitySpecs = @(
        [pscustomobject]@{ Name = "canonical-user-1"; GwTag = "7101"; MacDev = "aa:55:00:00:00:01" },
        [pscustomobject]@{ Name = "canonical-user-2"; GwTag = "7102"; MacDev = "aa:55:00:00:00:02" }
    )

    $state = Invoke-DashboardJson -BaseUrl $dashboardBaseUrl -Path "/api/targets"
    $target = Get-DashboardTargetBlock -DashboardBaseUrl $dashboardBaseUrl -Name "portable-openssh-busybox"
    $targetId = $target.target.id
    $defaultPolicy = @($target.policies | Where-Object { $_.name -eq "forwarding-default" })[0]
    $defaultPolicyId = if ($defaultPolicy) { $defaultPolicy.id } else { $null }

    $prepared = @()
    foreach ($spec in $identitySpecs) {
        $identity = Ensure-CanonicalIdentity -AllocatorBaseUrl $allocatorBaseUrl -ReferencePod $referencePod -GwTag $spec.GwTag -MacDev $spec.MacDev -CanonicalGatewayMac $CanonicalGatewayMac
        $requestedIpv6 = "$($identity.requested_ipv6)".ToLower()
        $username = Expand-Ipv6Username -RequestedIpv6 $requestedIpv6
        $existingUser = @($target.users | Where-Object { $_.username -eq $username })[0]

        if (-not $existingUser) {
            Write-Info "Creating SSH user $username for canonical IPv6 $requestedIpv6"
            $body = @{
                return_to                 = "/?target_id=$targetId&section=users"
                target_id                 = "$targetId"
                username                  = $username
                uid                       = ""
                group_id                  = ""
                home_dir                  = "/home/$username"
                shell                     = "/bin/sh"
                comment                   = $requestedIpv6
                enabled                   = "on"
            }
            if ($null -ne $defaultPolicyId) {
                $body.default_policy_profile_id = "$defaultPolicyId"
            }
            Invoke-DashboardFormPost -BaseUrl $dashboardBaseUrl -Path "/users" -Body $body | Out-Null
        }
        else {
            Write-Info "User $username already exists; keeping it"
        }

        $prepared += [pscustomobject]@{
            Label         = $spec.Name
            Username      = $username
            RequestedIPv6 = $requestedIpv6
            GwTag         = $spec.GwTag
            MacDev        = $spec.MacDev
        }
    }

    Write-Step "Reconciling dashboard-rendered account files"
    $latestRunId = 0
    if ($state.reconcile_runs -and @($state.reconcile_runs).Count -gt 0) {
        $latestRunId = [long](@($state.reconcile_runs)[0].id)
    }
    Invoke-DashboardFormPost -BaseUrl $dashboardBaseUrl -Path "/reconcile" -Body @{
        return_to        = "/?target_id=$targetId&section=reconcile"
        target_id        = "$targetId"
        requested_action = "RENDER_ONLY"
    } | Out-Null
    Wait-ReconcileSucceeded -DashboardBaseUrl $dashboardBaseUrl -MinimumRunId $latestRunId | Out-Null

    Write-Step "Canonical SSH identities prepared"
    $prepared | Format-Table Label, Username, RequestedIPv6, GwTag, MacDev -AutoSize

    Write-Host ""
    Write-Host "Next test shape:" -ForegroundColor Cyan
    Write-Host "  user 1 local forward: ssh -N -L 7777:[<user2 canonical ipv6>]:8888 <user1>@<ssh service>" -ForegroundColor DarkGray
    Write-Host "  user 2 reverse forward: ssh -N -R [<user2 canonical ipv6>]:8888:127.0.0.1:7777 <user2>@<ssh service>" -ForegroundColor DarkGray
}
finally {
}
