param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
    [int]$AllocatorPort = 0,
    [int]$DeploymentExplicitCount = 5,
    [int]$DeploymentReplicas = 4,
    [switch]$CleanupSamplesAfter
)

. "$ProjectRoot\tools\CMXsafeMAC-IPv6-helpers.ps1"

$statefulSelector = "app=demo"
$deploymentSelector = "app=demo-deployment"
$portForward = $null
$trafficPortForward = $null
$summary = [ordered]@{
    ManagedPodsValidated            = 0
    AutoNet1IPv6Validated          = 0
    ManagedIPv6Pings               = 0
    AutoNet1IPv6Pings              = 0
    DeploymentExplicitAssignments  = 0
    ExplicitIPv6Pings              = 0
    TrafficCollectorHealthy        = $false
    TrafficFlowsObserved           = 0
    CanonicalMoveVerified          = $false
    DeploymentReplacementVerified  = $false
}

function Get-AllocationForPod {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$PodUid
    )
    $rows = @(
        Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/allocations?pod_uid=$PodUid&status=ALLOCATED" |
            Where-Object { $_.container_iface -eq "eth0" }
    )
    if ($rows.Count -eq 0) {
        return $null
    }
    return $rows[0]
}

function Wait-AllocationForPod {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$PodUid,
        [int]$TimeoutSeconds = 180
    )
    return Wait-Until -Description "managed allocation for pod $PodUid" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 3 -Condition {
        Get-AllocationForPod -BaseUrl $BaseUrl -PodUid $PodUid
    }
}

function Test-PodRoutePresent {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$RoutePrefix
    )
    try {
        $text = Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "sh", "-lc", "ip -6 route show dev net1")
        return $text -match [regex]::Escape($RoutePrefix)
    }
    catch {
        return $false
    }
}

function Wait-PodHasIPv6 {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$Interface,
        [Parameter(Mandatory = $true)][string]$IPv6,
        [int]$TimeoutSeconds = 90
    )
    Wait-Until -Description "$PodName to have $IPv6 on $Interface" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 2 -Condition {
        Test-PodHasIPv6 -Namespace $Namespace -PodName $PodName -Interface $Interface -IPv6 $IPv6
    } | Out-Null
}

function Wait-PodLacksIPv6 {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$Interface,
        [Parameter(Mandatory = $true)][string]$IPv6,
        [int]$TimeoutSeconds = 90
    )
    Wait-Until -Description "$PodName to drop $IPv6 from $Interface" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 2 -Condition {
        -not (Test-PodHasIPv6 -Namespace $Namespace -PodName $PodName -Interface $Interface -IPv6 $IPv6)
    } | Out-Null
}

function Get-TrafficCollectorFlows {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [int]$WindowSeconds = 60
    )
    return Invoke-RestMethod -Uri "$BaseUrl/flows?window_seconds=$WindowSeconds&limit=500" -TimeoutSec 30
}

function Ensure-ExplicitIPv6ByPod {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)]$Pod,
        [Parameter(Mandatory = $true)][string]$GwTag,
        [Parameter(Mandatory = $true)][string]$MacDev
    )
    $body = @{
        pod_uid = $Pod.metadata.uid
        gw_tag  = $GwTag
        mac_dev = $MacDev
    }
    return Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/explicit-ipv6-assignments/ensure-by-pod" -Method "POST" -Body $body
}

function Reset-AllocatorState {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl
    )
    return Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/admin/reset" -Method "POST" -Body @{}
}

function Verify-ManagedPodState {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)]$Pod
    )
    $allocation = Wait-AllocationForPod -BaseUrl $BaseUrl -PodUid $Pod.metadata.uid
    $mac = Get-PodMac -Namespace $Pod.metadata.namespace -PodName $Pod.metadata.name -Interface "eth0"
    Assert-True -Condition ($mac -eq $allocation.assigned_mac) -Message "Managed MAC mismatch for $($Pod.metadata.namespace)/$($Pod.metadata.name): pod=$mac allocator=$($allocation.assigned_mac)"
    Wait-PodHasIPv6 -Namespace $Pod.metadata.namespace -PodName $Pod.metadata.name -Interface "eth0" -IPv6 $allocation.assigned_ipv6
    $net1Check = Invoke-Kubectl -Arguments @("exec", "-n", $Pod.metadata.namespace, $Pod.metadata.name, "--", "sh", "-lc", "ip link show dev net1")
    Assert-True -Condition ($net1Check -match "net1") -Message "net1 interface missing on $($Pod.metadata.namespace)/$($Pod.metadata.name)"
    Assert-True -Condition (-not [string]::IsNullOrWhiteSpace($allocation.auto_managed_explicit_ipv6)) -Message "Allocator did not derive auto_managed_explicit_ipv6 for $($Pod.metadata.namespace)/$($Pod.metadata.name)"
    Wait-PodHasIPv6 -Namespace $Pod.metadata.namespace -PodName $Pod.metadata.name -Interface "net1" -IPv6 $allocation.auto_managed_explicit_ipv6
    $summary.ManagedPodsValidated++
    $summary.AutoNet1IPv6Validated++
    return $allocation
}

function Add-ExplicitBatch {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)]$Pod,
        [Parameter(Mandatory = $true)][string]$GwTag,
        [Parameter(Mandatory = $true)][int]$Count,
        [Parameter(Mandatory = $true)][int]$FirstMacDevByte5
    )
    $assigned = @()
    for ($i = 1; $i -le $Count; $i++) {
        $macDev = "aa:bb:cc:dd:{0:x2}:{1:x2}" -f $FirstMacDevByte5, $i
        $response = Ensure-ExplicitIPv6ByPod -BaseUrl $BaseUrl -Pod $Pod -GwTag $GwTag -MacDev $macDev
        $requestedIPv6 = $response.requested_ipv6.ToLower()
        Wait-PodHasIPv6 -Namespace $Pod.metadata.namespace -PodName $Pod.metadata.name -Interface "net1" -IPv6 $requestedIPv6
        $assigned += $requestedIPv6
    }
    return $assigned
}

function Cleanup-Samples {
    param([string]$ProjectRoot)
    Invoke-Kubectl -Arguments @("delete", "statefulset", "demo", "-n", "mac-demo", "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("delete", "service", "demo", "-n", "mac-demo", "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("delete", "deployment", "demo-deployment", "-n", "mac-deployment-demo", "--ignore-not-found=true") | Out-Null
}

function Wait-NoAllocatedRowsForNamespace {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Namespace,
        [int]$TimeoutSeconds = 180
    )
    Wait-Until -Description "no allocated rows in namespace $Namespace" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 3 -Condition {
        Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/reconcile/live-pods" -Method "POST" -Body @{
            live_pod_uids = @()
            namespace     = $Namespace
            status        = "RELEASED"
        } | Out-Null
        $rows = @(
            Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/allocations?namespace=$Namespace&status=ALLOCATED"
        )
        $rows.Count -eq 0
    } | Out-Null
}

try {
    Write-Step "Checking core stack availability"
    Invoke-Kubectl -Arguments @("rollout", "status", "statefulset/net-identity-allocator-postgres", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/net-identity-allocator", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/cmxsafemac-ipv6-node-agent", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/cmxsafemac-ipv6-traffic-collector", "-n", "mac-allocator", "--timeout=180s") | Out-Null

    Write-Step "Resetting sample workloads"
    Cleanup-Samples -ProjectRoot $ProjectRoot
    Wait-Until -Description "mac-demo sample pods to disappear" -TimeoutSeconds 180 -IntervalSeconds 3 -Condition {
        (@(Get-ManagedPods -Namespace "mac-demo" -Selector $statefulSelector)).Count -eq 0
    } | Out-Null
    Wait-Until -Description "mac-deployment-demo sample pods to disappear" -TimeoutSeconds 180 -IntervalSeconds 3 -Condition {
        (@(Get-ManagedPods -Namespace "mac-deployment-demo" -Selector $deploymentSelector)).Count -eq 0
    } | Out-Null

    Write-Step "Starting allocator API access"
    $portForward = Start-AllocatorPortForward -Port $AllocatorPort -ProjectRoot $ProjectRoot
    $baseUrl = $portForward.BaseUrl
    Write-Info "Allocator API: $baseUrl"
    Invoke-AllocatorApi -BaseUrl $baseUrl -Path "/healthz" | Out-Null
    $trafficPortForward = Start-TrafficCollectorPortForward -ProjectRoot $ProjectRoot
    Write-Info "Traffic collector API: $($trafficPortForward.BaseUrl)"
    $summary.TrafficCollectorHealthy = $true

    Write-Step "Refreshing allocator state for a clean test run"
    Reset-AllocatorState -BaseUrl $baseUrl | Out-Null

    Write-Step "Deploying the 4-replica Deployment sample"
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\explicit-v6-network.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\demo-deployment.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("scale", "deployment/demo-deployment", "-n", "mac-deployment-demo", "--replicas=$DeploymentReplicas") | Out-Null
    $deploymentPods = Wait-PodsReady -Namespace "mac-deployment-demo" -Selector $deploymentSelector -ExpectedCount $DeploymentReplicas -TimeoutSeconds 420

    Write-Step "Verifying managed MACs, managed IPv6s, and net1 presence for Deployment replicas"
    $deploymentAllocations = @{}
    foreach ($pod in $deploymentPods) {
        $deploymentAllocations[$pod.metadata.name] = Verify-ManagedPodState -BaseUrl $baseUrl -Pod $pod
    }

    Write-Step "Verifying automatic managed net1 IPv6 connectivity across Deployment replicas"
    foreach ($source in $deploymentPods) {
        foreach ($target in $deploymentPods) {
            if ($source.metadata.name -eq $target.metadata.name) {
                continue
            }
            Invoke-PodPing6 -Namespace "mac-deployment-demo" -PodName $source.metadata.name -TargetIPv6 $deploymentAllocations[$target.metadata.name].auto_managed_explicit_ipv6 | Out-Null
            $summary.AutoNet1IPv6Pings++
        }
    }

    Write-Step "Assigning explicit IPv6 addresses to Deployment replicas"
    $deploymentExplicit = @{}
    for ($podIndex = 0; $podIndex -lt $deploymentPods.Count; $podIndex++) {
        $pod = $deploymentPods[$podIndex]
        $deploymentExplicit[$pod.metadata.name] = Add-ExplicitBatch -BaseUrl $baseUrl -Pod $pod -GwTag "6666" -Count $DeploymentExplicitCount -FirstMacDevByte5 (0x30 + $podIndex)
        $summary.DeploymentExplicitAssignments += $deploymentExplicit[$pod.metadata.name].Count
    }

    Write-Step "Verifying shared explicit prefix routing for existing prefix 6666::/16"
    foreach ($pod in $deploymentPods) {
        Wait-Until -Description "6666::/16 route on $($pod.metadata.name)" -TimeoutSeconds 60 -IntervalSeconds 2 -Condition {
            Test-PodRoutePresent -Namespace "mac-deployment-demo" -PodName $pod.metadata.name -RoutePrefix "6666::/16"
        } | Out-Null
    }

    Write-Step "Verifying explicit IPv6 connectivity across Deployment replicas"
    foreach ($source in $deploymentPods) {
        foreach ($target in $deploymentPods) {
            if ($source.metadata.name -eq $target.metadata.name) {
                continue
            }
            foreach ($targetIPv6 in $deploymentExplicit[$target.metadata.name]) {
                Invoke-PodPing6 -Namespace "mac-deployment-demo" -PodName $source.metadata.name -TargetIPv6 $targetIPv6 | Out-Null
                $summary.ExplicitIPv6Pings++
            }
        }
    }

    Write-Step "Adding a new explicit prefix route and checking its propagation"
    $newPrefixResponse = Ensure-ExplicitIPv6ByPod -BaseUrl $baseUrl -Pod $deploymentPods[0] -GwTag "7777" -MacDev "aa:bb:cc:dd:77:01"
    $newPrefixIPv6 = $newPrefixResponse.requested_ipv6.ToLower()
    Wait-PodHasIPv6 -Namespace "mac-deployment-demo" -PodName $deploymentPods[0].metadata.name -Interface "net1" -IPv6 $newPrefixIPv6
    Wait-Until -Description "7777::/16 route propagation" -TimeoutSeconds 90 -IntervalSeconds 2 -Condition {
        Test-PodRoutePresent -Namespace "mac-deployment-demo" -PodName $deploymentPods[1].metadata.name -RoutePrefix "7777::/16"
    } | Out-Null
    Invoke-PodPing6 -Namespace "mac-deployment-demo" -PodName $deploymentPods[1].metadata.name -TargetIPv6 $newPrefixIPv6 | Out-Null
    $summary.ExplicitIPv6Pings++

    Write-Step "Verifying canonical explicit IPv6 move behavior"
    $moveSource = $deploymentPods[2]
    $moveTarget = $deploymentPods[0]
    $moveObserver = $deploymentPods[1]
    $moveResponse1 = Ensure-ExplicitIPv6ByPod -BaseUrl $baseUrl -Pod $moveSource -GwTag "bbbb" -MacDev "aa:bb:cc:dd:ee:61"
    $canonicalIPv6 = $moveResponse1.requested_ipv6.ToLower()
    Wait-PodHasIPv6 -Namespace "mac-deployment-demo" -PodName $moveSource.metadata.name -Interface "net1" -IPv6 $canonicalIPv6
    Invoke-PodPing6 -Namespace "mac-deployment-demo" -PodName $moveObserver.metadata.name -TargetIPv6 $canonicalIPv6 | Out-Null
    $summary.ExplicitIPv6Pings++

    $moveResponse2 = Ensure-ExplicitIPv6ByPod -BaseUrl $baseUrl -Pod $moveTarget -GwTag "bbbb" -MacDev "aa:bb:cc:dd:ee:61"
    Wait-PodLacksIPv6 -Namespace "mac-deployment-demo" -PodName $moveSource.metadata.name -Interface "net1" -IPv6 $canonicalIPv6
    Wait-PodHasIPv6 -Namespace "mac-deployment-demo" -PodName $moveTarget.metadata.name -Interface "net1" -IPv6 $canonicalIPv6

    $assignmentRows = @(
        Invoke-AllocatorApi -BaseUrl $baseUrl -Path "/explicit-ipv6-assignments?requested_ipv6=$([uri]::EscapeDataString($canonicalIPv6))&status=ACTIVE"
    )
    Assert-True -Condition ($assignmentRows.Count -eq 1) -Message "Expected one active explicit assignment row for $canonicalIPv6"
    Assert-True -Condition ($assignmentRows[0].pod_uid -eq $moveTarget.metadata.uid) -Message "Canonical IPv6 $canonicalIPv6 is not mapped to the new target pod"
    Assert-True -Condition ($assignmentRows[0].target_counter -eq $deploymentAllocations[$moveTarget.metadata.name].counter) -Message "Canonical IPv6 $canonicalIPv6 did not update the stored target_counter metadata"
    Invoke-PodPing6 -Namespace "mac-deployment-demo" -PodName $deploymentPods[3].metadata.name -TargetIPv6 $canonicalIPv6 | Out-Null
    $summary.ExplicitIPv6Pings++
    $summary.CanonicalMoveVerified = $true

    Write-Step "Verifying the traffic collector sees deployment explicit traffic"
    $deploymentExplicitAddresses = @($deploymentExplicit.Values | ForEach-Object { $_ }) + @($newPrefixIPv6, $canonicalIPv6)
    $observedDeploymentFlow = Wait-Until -Description "deployment explicit traffic in collector" -TimeoutSeconds 90 -IntervalSeconds 3 -Condition {
        $payload = Get-TrafficCollectorFlows -BaseUrl $trafficPortForward.BaseUrl -WindowSeconds 60
        @(
            $payload.flows | Where-Object {
                $_.src_address -in $deploymentExplicitAddresses -or
                $_.dst_address -in $deploymentExplicitAddresses
            }
        )
    }
    $summary.TrafficFlowsObserved += @($observedDeploymentFlow).Count

    Write-Step "Deleting and replacing a Deployment-managed pod"
    $deploymentVictim = $deploymentPods[-1]
    $victimUid = $deploymentVictim.metadata.uid
    Invoke-Kubectl -Arguments @("delete", "pod", $deploymentVictim.metadata.name, "-n", "mac-deployment-demo", "--wait=false") | Out-Null
    $deploymentPods = Wait-PodsReady -Namespace "mac-deployment-demo" -Selector $deploymentSelector -ExpectedCount $DeploymentReplicas
    $newDeploymentPods = @($deploymentPods | Where-Object { $_.metadata.uid -ne $victimUid })
    Assert-True -Condition ($newDeploymentPods.Count -eq $DeploymentReplicas) -Message "Deployment replacement did not converge to $DeploymentReplicas fresh running pods"
    Wait-Until -Description "old deployment allocation row $victimUid to stop being ALLOCATED" -TimeoutSeconds 120 -IntervalSeconds 3 -Condition {
        $oldRows = @(
            Invoke-AllocatorApi -BaseUrl $baseUrl -Path "/allocations?pod_uid=$victimUid"
        )
        $oldAllocatedRows = @($oldRows | Where-Object { $_.status -eq "ALLOCATED" })
        $oldAllocatedRows.Count -eq 0
    } | Out-Null
    foreach ($pod in $deploymentPods) {
        $deploymentAllocations[$pod.metadata.name] = Verify-ManagedPodState -BaseUrl $baseUrl -Pod $pod
    }
    $summary.DeploymentReplacementVerified = $true

    Write-Step "Test summary"
    $summaryObject = [pscustomobject]$summary
    $summaryObject | Format-List | Out-String | Write-Host
    Write-Host "PASS: CMXsafeMAC-IPv6 end-to-end validation completed successfully." -ForegroundColor Green
}
finally {
    if ($CleanupSamplesAfter) {
        try {
            Write-Step "Cleaning up sample workloads"
            Cleanup-Samples -ProjectRoot $ProjectRoot
        }
        catch {
            Write-Host "Sample cleanup failed: $_" -ForegroundColor Yellow
        }
    }
    Stop-PortForward -PortForward $portForward
    Stop-PortForward -PortForward $trafficPortForward
}
