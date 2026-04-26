param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
    [int]$AllocatorPort = 0,
    [int]$AllocatorEnsureCount = 40,
    [int]$DeploymentReplicas = 4,
    [int]$ExplicitAssignmentsPerPod = 3,
    [int]$ParallelExplicitAssignmentsPerPod = 2,
    [int]$CanonicalMoveIterations = 4,
    [switch]$CleanupSamplesAfter
)

. "$ProjectRoot\tools\CMXsafeMAC-IPv6-helpers.ps1"

Add-Type -AssemblyName System.Net.Http

$deploymentNamespace = "mac-deployment-demo"
$deploymentSelector = "app=demo-deployment"
$portForward = $null

function Percentile {
    param(
        [double[]]$Values,
        [double]$Percent
    )
    if (-not $Values -or $Values.Count -eq 0) {
        return 0.0
    }
    $sorted = @($Values | Sort-Object)
    if ($sorted.Count -eq 1) {
        return [double]$sorted[0]
    }
    $position = ($Percent / 100.0) * ($sorted.Count - 1)
    $lower = [math]::Floor($position)
    $upper = [math]::Ceiling($position)
    if ($lower -eq $upper) {
        return [double]$sorted[$lower]
    }
    $weight = $position - $lower
    return [double]$sorted[$lower] + (([double]$sorted[$upper] - [double]$sorted[$lower]) * $weight)
}

function New-BenchmarkMetrics {
    return [ordered]@{
        Count       = 0
        TotalMs     = 0.0
        AvgMs       = 0.0
        P50Ms       = 0.0
        P95Ms       = 0.0
        OpsPerSec   = 0.0
    }
}

function New-ThroughputMetrics {
    return [ordered]@{
        Count       = 0
        TotalMs     = 0.0
        OpsPerSec   = 0.0
    }
}

function Get-LatencyMetrics {
    param(
        [Parameter(Mandatory = $true)][double[]]$DurationsMs
    )
    $metrics = New-BenchmarkMetrics
    if (-not $DurationsMs -or $DurationsMs.Count -eq 0) {
        return [pscustomobject]$metrics
    }
    $total = ($DurationsMs | Measure-Object -Sum).Sum
    $metrics.Count = $DurationsMs.Count
    $metrics.TotalMs = [math]::Round([double]$total, 2)
    $metrics.AvgMs = [math]::Round(([double]$total / $DurationsMs.Count), 2)
    $metrics.P50Ms = [math]::Round((Percentile -Values $DurationsMs -Percent 50), 2)
    $metrics.P95Ms = [math]::Round((Percentile -Values $DurationsMs -Percent 95), 2)
    if ($total -gt 0) {
        $metrics.OpsPerSec = [math]::Round(($DurationsMs.Count / ($total / 1000.0)), 2)
    }
    return [pscustomobject]$metrics
}

function Get-ThroughputMetrics {
    param(
        [Parameter(Mandatory = $true)][int]$Count,
        [Parameter(Mandatory = $true)][double]$TotalMs
    )
    $metrics = New-ThroughputMetrics
    $metrics.Count = $Count
    $metrics.TotalMs = [math]::Round($TotalMs, 2)
    if ($TotalMs -gt 0) {
        $metrics.OpsPerSec = [math]::Round(($Count / ($TotalMs / 1000.0)), 2)
    }
    return [pscustomobject]$metrics
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

function Reset-AllocatorState {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl
    )
    return Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/admin/reset" -Method "POST" -Body @{}
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

function Cleanup-Samples {
    Invoke-Kubectl -Arguments @("delete", "statefulset", "demo", "-n", "mac-demo", "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("delete", "service", "demo", "-n", "mac-demo", "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("delete", "deployment", "demo-deployment", "-n", $deploymentNamespace, "--ignore-not-found=true") | Out-Null
}

function Get-PrimaryNodeName {
    $nodes = @((Invoke-KubectlJson -Arguments @("get", "nodes", "-o", "json")).items)
    if ($nodes.Count -eq 0) {
        throw "No Kubernetes nodes were found."
    }
    return [string]$nodes[0].metadata.name
}

function Wait-NoDeploymentPods {
    Wait-Until -Description "deployment sample pods to disappear" -TimeoutSeconds 180 -IntervalSeconds 3 -Condition {
        (@(Get-ManagedPods -Namespace $deploymentNamespace -Selector $deploymentSelector)).Count -eq 0
    } | Out-Null
}

function Benchmark-AllocatorEnsure {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][int]$Count
    )
    $durations = New-Object System.Collections.Generic.List[double]
    $nodeName = Get-PrimaryNodeName
    for ($i = 0; $i -lt $Count; $i++) {
        $payload = @{
            gw_mac      = "7e:46:f8:e0:2d:3f"
            gw_iface    = "eth0"
            node_name   = $nodeName
            namespace   = "mac-benchmark"
            pod_name    = "allocator-bench-$i"
            pod_uid     = "allocator-bench-$i"
            owner_kind  = "deployment"
            owner_name  = "allocator-bench"
            container_iface = "eth0"
        }
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/allocations/ensure" -Method "POST" -Body $payload | Out-Null
        $sw.Stop()
        $durations.Add([double]$sw.Elapsed.TotalMilliseconds)
    }
    return Get-LatencyMetrics -DurationsMs $durations.ToArray()
}

function Benchmark-ExplicitAssignments {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Pods,
        [Parameter(Mandatory = $true)][int]$AssignmentsPerPod
    )
    $durations = New-Object System.Collections.Generic.List[double]
    $podIndex = 0
    foreach ($pod in $Pods) {
        $podIndex++
        for ($i = 1; $i -le $AssignmentsPerPod; $i++) {
            $macDev = "aa:bb:cc:dd:{0:x2}:{1:x2}" -f (0x40 + $podIndex), $i
            $sw = [System.Diagnostics.Stopwatch]::StartNew()
            $response = Ensure-ExplicitIPv6ByPod -BaseUrl $BaseUrl -Pod $pod -GwTag "6666" -MacDev $macDev
            Wait-PodHasIPv6 -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "net1" -IPv6 $response.requested_ipv6
            $sw.Stop()
            $durations.Add([double]$sw.Elapsed.TotalMilliseconds)
        }
    }
    return Get-LatencyMetrics -DurationsMs $durations.ToArray()
}

function Benchmark-ParallelExplicitAssignments {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Pods,
        [Parameter(Mandatory = $true)][int]$AssignmentsPerPod,
        [string]$GwTag = "6666"
    )
    $requests = @()
    if ([System.Net.ServicePointManager]::DefaultConnectionLimit -lt 256) {
        [System.Net.ServicePointManager]::DefaultConnectionLimit = 256
    }
    $httpHandler = [System.Net.Http.HttpClientHandler]::new()
    try {
        $httpHandler.MaxConnectionsPerServer = 256
    }
    catch {
    }
    $httpClient = [System.Net.Http.HttpClient]::new($httpHandler)
    $httpClient.Timeout = [TimeSpan]::FromSeconds(60)
    $podIndex = 0
    try {
        foreach ($pod in $Pods) {
            $podIndex++
            for ($i = 1; $i -le $AssignmentsPerPod; $i++) {
                $macDev = "aa:bb:cc:ee:{0:x2}:{1:x2}" -f (0x70 + $podIndex), $i
                $payload = @{
                    pod_uid = $pod.metadata.uid
                    gw_tag  = $GwTag
                    mac_dev = $macDev
                } | ConvertTo-Json -Compress
                $content = [System.Net.Http.StringContent]::new($payload, [System.Text.Encoding]::UTF8, "application/json")
                $task = $httpClient.PostAsync("$BaseUrl/explicit-ipv6-assignments/ensure-by-pod", $content)
                $requests += [pscustomobject]@{
                    Task   = $task
                    Pod    = $pod
                    MacDev = $macDev
                }
            }
        }
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        [System.Threading.Tasks.Task]::WaitAll(@($requests | ForEach-Object { $_.Task }))
        $sw.Stop()
        foreach ($request in $requests) {
            $httpResponse = $request.Task.Result
            $json = $httpResponse.Content.ReadAsStringAsync().Result
            if (-not $httpResponse.IsSuccessStatusCode) {
                throw "Parallel explicit assignment request failed for $($request.Pod.metadata.name) mac_dev=$($request.MacDev). status=$([int]$httpResponse.StatusCode) body=$json"
            }
            if ([string]::IsNullOrWhiteSpace($json)) {
                throw "Parallel explicit assignment request returned no JSON for $($request.Pod.metadata.name) mac_dev=$($request.MacDev)."
            }
            $response = $json | ConvertFrom-Json
            if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.requested_ipv6)) {
                throw "Parallel explicit assignment response was incomplete for $($request.Pod.metadata.name) mac_dev=$($request.MacDev). body=$json"
            }
            Wait-PodHasIPv6 -Namespace $request.Pod.metadata.namespace -PodName $request.Pod.metadata.name -Interface "net1" -IPv6 $response.requested_ipv6
        }
        return Get-ThroughputMetrics -Count $requests.Count -TotalMs $sw.Elapsed.TotalMilliseconds
    }
    finally {
        $httpClient.Dispose()
    }
}

function Benchmark-CanonicalMoves {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Pods,
        [Parameter(Mandatory = $true)][int]$Iterations
    )
    if ($Pods.Count -lt 3) {
        throw "Canonical move benchmark requires at least 3 managed pods."
    }
    $durations = New-Object System.Collections.Generic.List[double]
    $requestedIPv6 = $null
    $macDev = "aa:bb:cc:dd:ee:61"
    $currentOwner = $Pods[0]
    $observer = $Pods[2]

    $initial = Ensure-ExplicitIPv6ByPod -BaseUrl $BaseUrl -Pod $currentOwner -GwTag "bbbb" -MacDev $macDev
    $requestedIPv6 = $initial.requested_ipv6.ToLower()
    Wait-PodHasIPv6 -Namespace $currentOwner.metadata.namespace -PodName $currentOwner.metadata.name -Interface "net1" -IPv6 $requestedIPv6
    Wait-Until -Description "observer to reach initial canonical IPv6 owner" -TimeoutSeconds 60 -IntervalSeconds 2 -Condition {
        try {
            Invoke-PodPing6 -Namespace $observer.metadata.namespace -PodName $observer.metadata.name -TargetIPv6 $requestedIPv6 -Count 1 -TimeoutSeconds 2 | Out-Null
            return $true
        }
        catch {
            return $false
        }
    } | Out-Null

    for ($i = 0; $i -lt $Iterations; $i++) {
        $target = $Pods[($i % 2) + 1]
        if ($target.metadata.uid -eq $currentOwner.metadata.uid) {
            $target = $Pods[0]
        }
        $previousOwner = $currentOwner
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        $response = Ensure-ExplicitIPv6ByPod -BaseUrl $BaseUrl -Pod $target -GwTag "bbbb" -MacDev $macDev
        $movedIPv6 = $response.requested_ipv6.ToLower()
        Wait-PodHasIPv6 -Namespace $target.metadata.namespace -PodName $target.metadata.name -Interface "net1" -IPv6 $movedIPv6
        Wait-PodLacksIPv6 -Namespace $previousOwner.metadata.namespace -PodName $previousOwner.metadata.name -Interface "net1" -IPv6 $movedIPv6
        Wait-Until -Description "observer to reach moved canonical IPv6 owner" -TimeoutSeconds 60 -IntervalSeconds 1 -Condition {
            try {
                Invoke-PodPing6 -Namespace $observer.metadata.namespace -PodName $observer.metadata.name -TargetIPv6 $movedIPv6 -Count 1 -TimeoutSeconds 2 | Out-Null
                return $true
            }
            catch {
                return $false
            }
        } | Out-Null
        $sw.Stop()
        $durations.Add([double]$sw.Elapsed.TotalMilliseconds)
        $currentOwner = $target
    }
    return [pscustomobject]@{
        RequestedIPv6 = $requestedIPv6
        Metrics       = Get-LatencyMetrics -DurationsMs $durations.ToArray()
    }
}

$summary = [ordered]@{
    AllocatorBackend                  = ""
    PostgreSQLVerified                = $false
    AllocatorEnsureWrites             = $null
    ExplicitAssignmentEndToEnd        = $null
    ParallelExplicitAssignmentBurst   = $null
    CanonicalMoveLatency              = $null
}

try {
    Write-Step "Checking the core stack"
    Invoke-Kubectl -Arguments @("rollout", "status", "statefulset/net-identity-allocator-postgres", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/net-identity-allocator", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/cmxsafemac-ipv6-node-agent", "-n", "mac-allocator", "--timeout=180s") | Out-Null

    Write-Step "Resetting sample workloads"
    Cleanup-Samples
    Wait-NoDeploymentPods

    Write-Step "Starting allocator API access"
    $portForward = Start-AllocatorPortForward -Port $AllocatorPort -ProjectRoot $ProjectRoot
    $baseUrl = $portForward.BaseUrl
    Write-Info "Allocator API: $baseUrl"

    Write-Step "Checking allocator backend"
    $envDump = Invoke-Kubectl -Arguments @("exec", "-n", "mac-allocator", "deploy/net-identity-allocator", "--", "sh", "-lc", "env | sort | grep -E 'POSTGRES_HOST|POSTGRES_DB|POSTGRES_USER'")
    $summary.AllocatorBackend = "postgres"
    Assert-True -Condition ($envDump -match "POSTGRES_HOST=" -and $envDump -match "POSTGRES_DB=" -and $envDump -match "POSTGRES_USER=") -Message "Allocator PostgreSQL env is incomplete."
    $dbCheck = Invoke-Kubectl -Arguments @("exec", "-n", "mac-allocator", "net-identity-allocator-postgres-0", "--", "sh", "-lc", "PGPASSWORD='change-me-in-production' psql -t -A -U allocator -d cmxsafemac_ipv6 -c 'select current_database(), current_user'")
    $summary.PostgreSQLVerified = $dbCheck.Trim() -eq "cmxsafemac_ipv6|allocator"
    Assert-True -Condition $summary.PostgreSQLVerified -Message "PostgreSQL verification failed."

    Write-Step "Benchmarking allocator-only write throughput"
    Reset-AllocatorState -BaseUrl $baseUrl | Out-Null
    $summary.AllocatorEnsureWrites = Benchmark-AllocatorEnsure -BaseUrl $baseUrl -Count $AllocatorEnsureCount

    Write-Step "Resetting allocator state before pod benchmarks"
    Reset-AllocatorState -BaseUrl $baseUrl | Out-Null

    Write-Step "Deploying the benchmark Deployment sample"
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\explicit-v6-network.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\demo-deployment.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("scale", "deployment", "demo-deployment", "-n", $deploymentNamespace, "--replicas=$DeploymentReplicas") | Out-Null
    $pods = @(Wait-PodsReady -Namespace $deploymentNamespace -Selector $deploymentSelector -ExpectedCount $DeploymentReplicas -TimeoutSeconds 420)

    Write-Step "Waiting for managed allocations"
    foreach ($pod in $pods) {
        $allocation = Wait-AllocationForPod -BaseUrl $baseUrl -PodUid $pod.metadata.uid
        $managedMac = Get-PodMac -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "eth0"
        Assert-True -Condition ($managedMac -eq $allocation.assigned_mac) -Message "Managed MAC mismatch for $($pod.metadata.name)"
        Wait-PodHasIPv6 -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "eth0" -IPv6 $allocation.assigned_ipv6
    }

    Write-Step "Benchmarking end-to-end explicit IPv6 assignment throughput"
    $summary.ExplicitAssignmentEndToEnd = Benchmark-ExplicitAssignments -BaseUrl $baseUrl -Pods $pods -AssignmentsPerPod $ExplicitAssignmentsPerPod

    Write-Step "Benchmarking parallel explicit IPv6 assignment throughput under an existing prefix"
    $summary.ParallelExplicitAssignmentBurst = Benchmark-ParallelExplicitAssignments -BaseUrl $baseUrl -Pods $pods -AssignmentsPerPod $ParallelExplicitAssignmentsPerPod

    Write-Step "Benchmarking canonical explicit IPv6 move latency"
    $canonical = Benchmark-CanonicalMoves -BaseUrl $baseUrl -Pods $pods -Iterations $CanonicalMoveIterations
    $summary.CanonicalMoveLatency = $canonical.Metrics

    Write-Step "Benchmark summary"
    $summary | Format-List | Out-String | Write-Host
}
finally {
    Stop-PortForward -PortForward $portForward
    if ($CleanupSamplesAfter) {
        Write-Step "Cleaning sample workloads"
        Cleanup-Samples
        try {
            Wait-NoDeploymentPods
        }
        catch {
            Write-Info "Sample cleanup is still converging: $_"
        }
    }
}
