param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..\..")).Path,
    [int]$AllocatorPort = 0,
    [int]$DeploymentReplicas = 4,
    [int[]]$BatchSizes = @(10, 30, 60, 100),
    [switch]$CleanupSamplesAfter
)

. "$ProjectRoot\tools\CMXsafeMAC-IPv6-helpers.ps1"

Add-Type -AssemblyName System.Net.Http

$deploymentNamespace = "mac-deployment-demo"
$deploymentSelector = "app=demo-deployment"
$multusNet1Prefix = "fd42:4242:ff:"
$multusNet1Route = "fd42:4242:ff::/64"
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

function Get-LatencyMetrics {
    param(
        [Parameter(Mandatory = $true)][double[]]$DurationsMs
    )
    $metrics = [ordered]@{
        Count       = 0
        TotalMs     = 0.0
        AvgMs       = 0.0
        P50Ms       = 0.0
        P95Ms       = 0.0
        MaxMs       = 0.0
        OpsPerSec   = 0.0
    }
    if (-not $DurationsMs -or $DurationsMs.Count -eq 0) {
        return [pscustomobject]$metrics
    }
    $total = ($DurationsMs | Measure-Object -Sum).Sum
    $metrics.Count = $DurationsMs.Count
    $metrics.TotalMs = [math]::Round([double]$total, 2)
    $metrics.AvgMs = [math]::Round(([double]$total / $DurationsMs.Count), 2)
    $metrics.P50Ms = [math]::Round((Percentile -Values $DurationsMs -Percent 50), 2)
    $metrics.P95Ms = [math]::Round((Percentile -Values $DurationsMs -Percent 95), 2)
    $metrics.MaxMs = [math]::Round(([double]($DurationsMs | Measure-Object -Maximum).Maximum), 2)
    $batchCompletionMs = [double]($DurationsMs | Measure-Object -Maximum).Maximum
    if ($batchCompletionMs -gt 0) {
        $metrics.OpsPerSec = [math]::Round(($DurationsMs.Count / ($batchCompletionMs / 1000.0)), 2)
    }
    return [pscustomobject]$metrics
}

function Get-TracePodTargets {
    $allocatorPods = @(
        (Invoke-KubectlJson -Arguments @("get", "pods", "-n", "mac-allocator", "-l", "app=net-identity-allocator", "-o", "json")).items |
            Where-Object { $_.status.phase -eq "Running" } |
            ForEach-Object { [string]$_.metadata.name }
    )
    $nodeAgentPods = @(
        (Invoke-KubectlJson -Arguments @("get", "pods", "-n", "mac-allocator", "-l", "app=cmxsafemac-ipv6-node-agent", "-o", "json")).items |
            Where-Object { $_.status.phase -eq "Running" } |
            ForEach-Object { [string]$_.metadata.name }
    )
    return [pscustomobject]@{
        Namespace     = "mac-allocator"
        AllocatorPods = $allocatorPods
        NodeAgentPods = $nodeAgentPods
    }
}

function Get-TraceRecords {
    param(
        [Parameter(Mandatory = $true)]$TracePods,
        [string[]]$RequestedIPv6s = @(),
        [string[]]$TraceIds = @(),
        [Parameter(Mandatory = $true)][datetime]$PhaseStartUtc,
        [Parameter(Mandatory = $true)][datetime]$PhaseEndUtc
    )
    $records = @()
    $requestedSet = @{}
    $traceIdSet = @{}
    foreach ($ipv6 in $RequestedIPv6s) {
        if (-not [string]::IsNullOrWhiteSpace($ipv6)) {
            $requestedSet[$ipv6.Trim().ToLowerInvariant()] = $true
        }
    }
    foreach ($traceId in $TraceIds) {
        if (-not [string]::IsNullOrWhiteSpace($traceId)) {
            $traceIdSet[$traceId.Trim()] = $true
        }
    }
    if ($requestedSet.Count -eq 0 -and $traceIdSet.Count -eq 0) {
        return @()
    }
    $sinceTime = $PhaseStartUtc.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
    $pods = @($TracePods.AllocatorPods + $TracePods.NodeAgentPods)
    foreach ($podName in $pods) {
        $logText = Invoke-Kubectl -Arguments @("logs", "--timestamps", "-n", $TracePods.Namespace, $podName, "--since-time", $sinceTime)
        foreach ($line in @($logText -split "`r?`n")) {
            if ([string]::IsNullOrWhiteSpace($line) -or $line -notmatch "explicit-trace") {
                continue
            }
            $parts = $line -split "\s+", 2
            if ($parts.Count -lt 2) {
                continue
            }
            try {
                $timestamp = [datetimeoffset]::Parse($parts[0]).UtcDateTime
            }
            catch {
                continue
            }
            if ($timestamp -lt $PhaseStartUtc.ToUniversalTime() -or $timestamp -gt $PhaseEndUtc.ToUniversalTime()) {
                continue
            }
            $fields = @{}
            foreach ($match in [regex]::Matches($parts[1], '([A-Za-z0-9_]+)=([^\s]+)')) {
                $fields[$match.Groups[1].Value] = $match.Groups[2].Value
            }
            $traceId = [string]$fields["trace_id"]
            $requestedIPv6 = [string]$fields["requested_ipv6"]
            $matchesTraceId = (-not [string]::IsNullOrWhiteSpace($traceId)) -and $traceIdSet.ContainsKey($traceId.Trim())
            $matchesRequestedIPv6 = $false
            if (-not [string]::IsNullOrWhiteSpace($requestedIPv6)) {
                $requestedIPv6 = $requestedIPv6.Trim().ToLowerInvariant()
                $matchesRequestedIPv6 = $requestedSet.ContainsKey($requestedIPv6)
            }
            if (-not $matchesTraceId -and -not $matchesRequestedIPv6) {
                continue
            }
            $record = [ordered]@{
                Timestamp     = $timestamp
                Pod           = $podName
                RequestedIPv6 = $requestedIPv6
                Source        = [string]$fields["source"]
            }
            foreach ($key in $fields.Keys) {
                if ($record.Contains($key)) {
                    continue
                }
                $rawValue = [string]$fields[$key]
                $parsed = 0.0
                if ([double]::TryParse($rawValue, [System.Globalization.NumberStyles]::Float, [System.Globalization.CultureInfo]::InvariantCulture, [ref]$parsed)) {
                    $record[$key] = $parsed
                }
                else {
                    $record[$key] = $rawValue
                }
            }
            $records += [pscustomobject]$record
        }
    }
    return $records
}

function Get-DominantTraceStage {
    param([Parameter(Mandatory = $true)]$SummaryRow)
    $bestName = ""
    $bestValue = -1.0
    foreach ($property in $SummaryRow.PSObject.Properties) {
        if ($property.Name -notlike "*_ms") {
            continue
        }
        if ($property.Name -eq "total_ms") {
            continue
        }
        $value = [double]$property.Value
        if ($value -gt $bestValue) {
            $bestValue = $value
            $bestName = $property.Name
        }
    }
    if ([string]::IsNullOrWhiteSpace($bestName)) {
        return $null
    }
    return [pscustomobject]@{
        Name  = $bestName
        Value = [math]::Round($bestValue, 2)
    }
}

function Get-TraceSummary {
    param([object[]]$Records = @())
    $numericFields = @(
        "client_to_allocator_ms",
        "allocator_to_agent_ms",
        "client_to_agent_ms",
        "node_callback_to_allocator_ms",
        "allocation_lookup_ms",
        "upsert_ms",
        "db_update_ms",
        "queue_wait_ms",
        "node_call_ms",
        "lock_wait_ms",
        "resolve_runtime_ms",
        "evict_ms",
        "set_ms",
        "flush_ms",
        "mark_applied_ms",
        "prefix_sync_ms",
        "total_ms"
    )
    if (-not $Records -or $Records.Count -eq 0) {
        return @()
    }
    $summary = @()
    foreach ($group in @($Records | Group-Object Source)) {
        $row = [ordered]@{
            Source = $group.Name
            Count  = $group.Count
        }
        foreach ($field in $numericFields) {
            $values = @(
                $group.Group |
                    Where-Object { $_.PSObject.Properties.Name -contains $field } |
                    ForEach-Object { [double]$_.$field }
            )
            if ($values.Count -gt 0) {
                $row[$field] = [math]::Round((($values | Measure-Object -Average).Average), 2)
            }
        }
        $summary += [pscustomobject]$row
    }
    return $summary
}

function Write-TraceSummary {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [object[]]$Summary = @()
    )
    Write-Step $Label
    if (-not $Summary -or $Summary.Count -eq 0) {
        Write-Info "No explicit trace records were found."
        return
    }
    foreach ($row in $Summary) {
        $dominant = Get-DominantTraceStage -SummaryRow $row
        $dominantText = if ($null -ne $dominant) { "$($dominant.Name)=$($dominant.Value)ms" } else { "n/a" }
        $details = @()
        foreach ($field in @(
            "client_to_allocator_ms",
            "allocator_to_agent_ms",
            "client_to_agent_ms",
            "node_callback_to_allocator_ms",
            "allocation_lookup_ms",
            "upsert_ms",
            "queue_wait_ms",
            "node_call_ms",
            "lock_wait_ms",
            "resolve_runtime_ms",
            "evict_ms",
            "set_ms",
            "flush_ms",
            "mark_applied_ms",
            "prefix_sync_ms",
            "db_update_ms",
            "total_ms"
        )) {
            if ($row.PSObject.Properties.Name -contains $field) {
                $details += "${field}=$($row.$field)ms"
            }
        }
        Write-Info "$($row.Source) count=$($row.Count) dominant=$dominantText $($details -join ' ')"
    }
}

function Cleanup-Samples {
    Invoke-Kubectl -Arguments @("delete", "statefulset", "demo", "-n", "mac-demo", "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("delete", "service", "demo", "-n", "mac-demo", "--ignore-not-found=true") | Out-Null
    Invoke-Kubectl -Arguments @("delete", "deployment", "demo-deployment", "-n", $deploymentNamespace, "--ignore-not-found=true") | Out-Null
}

function Wait-NoDeploymentPods {
    Wait-Until -Description "deployment sample pods to disappear" -TimeoutSeconds 180 -IntervalSeconds 3 -Condition {
        (@(Get-ManagedPods -Namespace $deploymentNamespace -Selector $deploymentSelector)).Count -eq 0
    } | Out-Null
}

function Reset-AllocatorState {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl
    )
    return Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/admin/reset" -Method "POST" -Body @{}
}

function Reset-AllocatorExplicitState {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [string]$Namespace = "",
        [switch]$ClearRuntime
    )
    $body = @{}
    if (-not [string]::IsNullOrWhiteSpace($Namespace)) {
        $body.namespace = $Namespace
    }
    if ($ClearRuntime) {
        $body.clear_runtime = $true
    }
    return Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/admin/reset-explicit" -Method "POST" -Body $body
}

function Get-PodIPv6Routes {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$Interface
    )
    $command = "ip -6 route show dev $Interface table main type unicast 2>/dev/null | awk '{print `$1}'"
    $output = Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "sh", "-lc", $command)
    return @($output -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim().ToLowerInvariant() })
}

function Get-NormalizedValueSet {
    param([string[]]$Values)
    return @($Values | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | ForEach-Object { $_.Trim().ToLowerInvariant() } | Sort-Object -Unique)
}

function Test-ValueSetMatches {
    param(
        [string[]]$Expected,
        [string[]]$Actual
    )
    $expectedSet = @(Get-NormalizedValueSet -Values $Expected)
    $actualSet = @(Get-NormalizedValueSet -Values $Actual)
    return $null -eq (Compare-Object -ReferenceObject $expectedSet -DifferenceObject $actualSet)
}

function Remove-PodExplicitRoutes {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [string[]]$AllowedRoutes = @()
    )
    $routes = @(Get-PodIPv6Routes -Namespace $Namespace -PodName $PodName -Interface "net1")
    $allowed = @(Get-NormalizedValueSet -Values $AllowedRoutes)
    $explicitRoutes = @($routes | Where-Object { $_ -notin $allowed })
    foreach ($route in $explicitRoutes) {
        Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "sh", "-lc", "ip -6 route del $route dev net1 >/dev/null 2>&1 || true") | Out-Null
    }
}

function Clear-DeploymentExplicitState {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Pods,
        [Parameter(Mandatory = $true)][hashtable]$BaselineAddresses,
        [Parameter(Mandatory = $true)][hashtable]$BaselineRoutes
    )
    Reset-AllocatorExplicitState -BaseUrl $BaseUrl -Namespace $deploymentNamespace -ClearRuntime | Out-Null
    Wait-Until -Description "deployment explicit IPv6 state to clear" -TimeoutSeconds 120 -IntervalSeconds 2 -Condition {
        $rows = @(
            (Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/explicit-ipv6-assignments?namespace=$([uri]::EscapeDataString($deploymentNamespace))&status=ACTIVE") |
                Where-Object { $null -ne $_ }
        )
        if ($rows.Count -gt 0) {
            return $null
        }
        foreach ($pod in $Pods) {
            $addresses = @(Get-PodIPv6Addresses -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "net1")
            $expectedAddresses = @($BaselineAddresses[$pod.metadata.uid])
            if (-not (Test-ValueSetMatches -Expected $expectedAddresses -Actual $addresses)) {
                return $null
            }
        }
        return $true
    } | Out-Null
    foreach ($pod in $Pods) {
        Remove-PodExplicitRoutes -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -AllowedRoutes @($BaselineRoutes[$pod.metadata.uid])
    }
    Wait-Until -Description "deployment explicit IPv6 routes to clear" -TimeoutSeconds 60 -IntervalSeconds 2 -Condition {
        foreach ($pod in $Pods) {
            $routes = @(Get-PodIPv6Routes -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "net1")
            $expectedRoutes = @($BaselineRoutes[$pod.metadata.uid])
            if (-not (Test-ValueSetMatches -Expected $expectedRoutes -Actual $routes)) {
                return $null
            }
        }
        return $true
    } | Out-Null
}

function Wait-AllocationForPod {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$PodUid,
        [int]$TimeoutSeconds = 180
    )
    return Wait-Until -Description "managed allocation for pod $PodUid" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 3 -Condition {
        $rows = @(
            Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/allocations?pod_uid=$PodUid&status=ALLOCATED" |
                Where-Object { $_.container_iface -eq "eth0" }
        )
        if ($rows.Count -gt 0) {
            return $rows[0]
        }
        return $null
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

function Get-BatchGwTag {
    param([Parameter(Mandatory = $true)][int]$Size)
    return ("{0:x4}" -f (0xE000 + $Size))
}

function New-CanonicalBatchPlan {
    param(
        [Parameter(Mandatory = $true)][object[]]$Pods,
        [Parameter(Mandatory = $true)][int]$Count,
        [Parameter(Mandatory = $true)][string]$GwTag
    )
    $plan = @()
    for ($i = 0; $i -lt $Count; $i++) {
        $initialIndex = $i % $Pods.Count
        $moveIndex = ($initialIndex + 1) % $Pods.Count
        $entryNumber = $i + 1
        $scenarioByte = $Count -band 0xff
        $indexByte = $entryNumber -band 0xff
        $indexHigh = (($entryNumber -shr 8) -band 0xff)
        $macDev = "aa:bb:{0:x2}:{1:x2}:{2:x2}:01" -f $scenarioByte, $indexHigh, $indexByte
        $plan += [pscustomobject]@{
            Index      = $entryNumber
            GwTag      = $GwTag
            MacDev     = $macDev
            SourcePod  = $Pods[$initialIndex]
            TargetPod  = $Pods[$moveIndex]
            RequestedIPv6 = $null
        }
    }
    return $plan
}

function Get-TraceEpochMs {
    return [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
}

function Invoke-ParallelExplicitBatch {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Plan,
        [Parameter(Mandatory = $true)][string]$PodProperty,
        [Parameter(Mandatory = $true)][int]$BatchSize,
        [Parameter(Mandatory = $true)][string]$TracePhase
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
    $httpClient.Timeout = [TimeSpan]::FromSeconds(120)
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $durations = New-Object System.Collections.Generic.List[double]
    try {
        foreach ($entry in $Plan) {
            $pod = $entry.$PodProperty
            $traceId = "{0}-b{1}-i{2}-{3}" -f $TracePhase, $BatchSize, $entry.Index, ([guid]::NewGuid().ToString("N").Substring(0, 8))
            $payload = @{
                pod_uid = $pod.metadata.uid
                gw_tag  = $entry.GwTag
                mac_dev = $entry.MacDev
                trace_id = $traceId
                trace_phase = $TracePhase
                trace_batch_size = $BatchSize
                trace_request_index = $entry.Index
                trace_client_started_at_ms = Get-TraceEpochMs
            } | ConvertTo-Json -Compress
            $content = [System.Net.Http.StringContent]::new($payload, [System.Text.Encoding]::UTF8, "application/json")
            $task = $httpClient.PostAsync("$BaseUrl/explicit-ipv6-assignments/ensure-by-pod", $content)
            $requests += [pscustomobject]@{
                Entry      = $entry
                Pod        = $pod
                Payload    = $payload
                TraceId    = $traceId
                Task       = $task
                Done       = $false
                RetryCount = 0
            }
        }
        $deadline = (Get-Date).AddSeconds(300)
        while ((@($requests | Where-Object { -not $_.Done })).Count -gt 0) {
            if ((Get-Date) -gt $deadline) {
                throw "Timed out waiting for parallel canonical requests to finish."
            }
            foreach ($request in @($requests | Where-Object { -not $_.Done -and $_.Task.IsCompleted })) {
                try {
                    $httpResponse = $request.Task.GetAwaiter().GetResult()
                    $json = $httpResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult()
                }
                catch {
                    if ($request.RetryCount -lt 4) {
                        $request.RetryCount += 1
                        Start-Sleep -Milliseconds ([Math]::Min(100 * [Math]::Pow(2, $request.RetryCount - 1), 1000))
                        $retryContent = [System.Net.Http.StringContent]::new($request.Payload, [System.Text.Encoding]::UTF8, "application/json")
                        $request.Task = $httpClient.PostAsync("$BaseUrl/explicit-ipv6-assignments/ensure-by-pod", $retryContent)
                        continue
                    }
                    throw
                }
                if (-not $httpResponse.IsSuccessStatusCode) {
                    throw "Parallel canonical request failed for $($request.Pod.metadata.name) mac_dev=$($request.Entry.MacDev). status=$([int]$httpResponse.StatusCode) body=$json"
                }
                if ([string]::IsNullOrWhiteSpace($json)) {
                    throw "Parallel canonical request returned no JSON for $($request.Pod.metadata.name) mac_dev=$($request.Entry.MacDev)."
                }
                $response = $json | ConvertFrom-Json
                if ($null -eq $response -or [string]::IsNullOrWhiteSpace($response.requested_ipv6)) {
                    throw "Parallel canonical request response was incomplete for $($request.Pod.metadata.name) mac_dev=$($request.Entry.MacDev). body=$json"
                }
                $requestedIPv6 = $response.requested_ipv6.ToLower()
                if ([string]::IsNullOrWhiteSpace($request.Entry.RequestedIPv6)) {
                    $request.Entry.RequestedIPv6 = $requestedIPv6
                }
                elseif ($request.Entry.RequestedIPv6 -ne $requestedIPv6) {
                    throw "Canonical IPv6 changed unexpectedly for mac_dev=$($request.Entry.MacDev). expected=$($request.Entry.RequestedIPv6) actual=$requestedIPv6"
                }
                $durations.Add([double]$sw.Elapsed.TotalMilliseconds)
                $request.Done = $true
            }
            if ((@($requests | Where-Object { -not $_.Done })).Count -gt 0) {
                Start-Sleep -Milliseconds 50
            }
        }
        $sw.Stop()
        return [pscustomobject]@{
            LatencyMetrics = Get-LatencyMetrics -DurationsMs $durations.ToArray()
            RequestCount   = $requests.Count
            TraceIds       = @($requests | ForEach-Object { $_.TraceId })
        }
    }
    finally {
        $httpClient.Dispose()
    }
}

function Get-Net1AddressSnapshot {
    param(
        [Parameter(Mandatory = $true)][object[]]$Pods
    )
    $snapshot = @{}
    foreach ($pod in $Pods) {
        $key = $pod.metadata.uid
        $snapshot[$key] = @(
            Get-PodIPv6Addresses -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "net1"
        )
    }
    return $snapshot
}

function Get-Net1RouteSnapshot {
    param(
        [Parameter(Mandatory = $true)][object[]]$Pods
    )
    $snapshot = @{}
    foreach ($pod in $Pods) {
        $key = $pod.metadata.uid
        $snapshot[$key] = @(
            Get-PodIPv6Routes -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "net1"
        )
    }
    return $snapshot
}

function Get-ExplicitIPv6Assignment {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$RequestedIPv6
    )
    $rows = @(
        Invoke-AllocatorApi -BaseUrl $BaseUrl -Path "/explicit-ipv6-assignments?requested_ipv6=$([uri]::EscapeDataString($RequestedIPv6))&status=ACTIVE"
    )
    if ($rows.Count -eq 0) {
        return $null
    }
    return $rows[0]
}

function Wait-ParallelCreateCompletion {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Plan,
        [int]$TimeoutSeconds = 240
    )
    $pending = @{}
    foreach ($entry in $Plan) {
        $pending[$entry.MacDev] = $entry
    }
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($pending.Count -gt 0 -and (Get-Date) -lt $deadline) {
        $completed = @()
        foreach ($entry in $pending.Values) {
            $assignment = Get-ExplicitIPv6Assignment -BaseUrl $BaseUrl -RequestedIPv6 $entry.RequestedIPv6
            if (
                $null -ne $assignment -and
                $assignment.pod_uid -eq $entry.SourcePod.metadata.uid -and
                $assignment.container_iface -eq "net1" -and
                -not [string]::IsNullOrWhiteSpace($assignment.last_applied_at)
            ) {
                $completed += $entry.MacDev
            }
        }
        foreach ($key in $completed) {
            $pending.Remove($key) | Out-Null
        }
        if ($pending.Count -gt 0) {
            Start-Sleep -Milliseconds 500
        }
    }
    if ($pending.Count -gt 0) {
        throw "Timed out waiting for parallel canonical create completion for $($pending.Count) addresses."
    }
    return $true
}

function Wait-ParallelMoveCompletion {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Plan,
        [int]$TimeoutSeconds = 300
    )
    $pending = @{}
    foreach ($entry in $Plan) {
        $pending[$entry.MacDev] = $entry
    }
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ($pending.Count -gt 0 -and (Get-Date) -lt $deadline) {
        $completed = @()
        foreach ($entry in $pending.Values) {
            $assignment = Get-ExplicitIPv6Assignment -BaseUrl $BaseUrl -RequestedIPv6 $entry.RequestedIPv6
            if (
                $null -ne $assignment -and
                $assignment.pod_uid -eq $entry.TargetPod.metadata.uid -and
                $assignment.container_iface -eq "net1" -and
                -not [string]::IsNullOrWhiteSpace($assignment.last_applied_at)
            ) {
                $completed += $entry.MacDev
            }
        }
        foreach ($key in $completed) {
            $pending.Remove($key) | Out-Null
        }
        if ($pending.Count -gt 0) {
            Start-Sleep -Milliseconds 500
        }
    }
    if ($pending.Count -gt 0) {
        throw "Timed out waiting for parallel canonical move completion for $($pending.Count) addresses."
    }
    return $true
}

function Test-CanonicalReachabilitySample {
    param(
        [Parameter(Mandatory = $true)][object[]]$Plan,
        [int]$SampleCount = 8,
        [Parameter(Mandatory = $true)][string]$OwnerProperty
    )
    $sample = @($Plan | Select-Object -First ([math]::Min($SampleCount, $Plan.Count)))
    foreach ($entry in $sample) {
        $owner = $entry.$OwnerProperty
        $observer = @($Plan | ForEach-Object { $_.SourcePod; $_.TargetPod } | Where-Object { $_.metadata.uid -ne $owner.metadata.uid } | Select-Object -First 1)[0]
        Invoke-PodPing6 -Namespace $observer.metadata.namespace -PodName $observer.metadata.name -TargetIPv6 $entry.RequestedIPv6 -Count 1 -TimeoutSeconds 2 | Out-Null
    }
    return $sample.Count
}

function Prepare-DeploymentPods {
    param(
        [Parameter(Mandatory = $true)][string]$ProjectRoot,
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][int]$Replicas
    )
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\explicit-v6-network.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\demo-deployment.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("scale", "deployment", "demo-deployment", "-n", $deploymentNamespace, "--replicas=$Replicas") | Out-Null
    $pods = @(Wait-PodsReady -Namespace $deploymentNamespace -Selector $deploymentSelector -ExpectedCount $Replicas -TimeoutSeconds 420)
    foreach ($pod in $pods) {
        $allocation = Wait-AllocationForPod -BaseUrl $BaseUrl -PodUid $pod.metadata.uid
        $managedMac = Get-PodMac -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "eth0"
        Assert-True -Condition ($managedMac -eq $allocation.assigned_mac) -Message "Managed MAC mismatch for $($pod.metadata.name)"
        Wait-PodHasIPv6 -Namespace $pod.metadata.namespace -PodName $pod.metadata.name -Interface "eth0" -IPv6 $allocation.assigned_ipv6
    }
    return $pods
}

function Run-BatchScenario {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][object[]]$Pods,
        [Parameter(Mandatory = $true)][int]$BatchSize,
        [Parameter(Mandatory = $true)]$TracePods
    )
    $gwTag = Get-BatchGwTag -Size $BatchSize
    $plan = @(New-CanonicalBatchPlan -Pods $pods -Count $BatchSize -GwTag $gwTag)

    Write-Step "Scenario ${BatchSize}: creating $BatchSize canonical IPv6 addresses in parallel"
    $createStart = [datetime]::UtcNow
    $createResult = Invoke-ParallelExplicitBatch -BaseUrl $BaseUrl -Plan $plan -PodProperty "SourcePod" -BatchSize $BatchSize -TracePhase "create"
    Wait-ParallelCreateCompletion -BaseUrl $BaseUrl -Plan $plan | Out-Null
    $createEnd = [datetime]::UtcNow
    Start-Sleep -Milliseconds 500
    $createMetrics = $createResult.LatencyMetrics
    $requestedIPv6s = @($plan | ForEach-Object { $_.RequestedIPv6 })
    $createTrace = Get-TraceSummary -Records (Get-TraceRecords -TracePods $TracePods -RequestedIPv6s $requestedIPv6s -TraceIds $createResult.TraceIds -PhaseStartUtc $createStart -PhaseEndUtc $createEnd)
    Write-TraceSummary -Label "Scenario ${BatchSize}: create trace summary" -Summary $createTrace
    $createReachability = Test-CanonicalReachabilitySample -Plan $plan -OwnerProperty "SourcePod"

    Write-Step "Scenario ${BatchSize}: moving $BatchSize canonical IPv6 addresses in parallel"
    $moveStart = [datetime]::UtcNow
    $moveResult = Invoke-ParallelExplicitBatch -BaseUrl $BaseUrl -Plan $plan -PodProperty "TargetPod" -BatchSize $BatchSize -TracePhase "move"
    Wait-ParallelMoveCompletion -BaseUrl $BaseUrl -Plan $plan | Out-Null
    $moveEnd = [datetime]::UtcNow
    Start-Sleep -Milliseconds 500
    $moveMetrics = $moveResult.LatencyMetrics
    $moveTrace = Get-TraceSummary -Records (Get-TraceRecords -TracePods $TracePods -RequestedIPv6s $requestedIPv6s -TraceIds $moveResult.TraceIds -PhaseStartUtc $moveStart -PhaseEndUtc $moveEnd)
    Write-TraceSummary -Label "Scenario ${BatchSize}: move trace summary" -Summary $moveTrace
    $moveReachability = Test-CanonicalReachabilitySample -Plan $plan -OwnerProperty "TargetPod"

    return [pscustomobject]@{
        BatchSize                = $BatchSize
        GwTag                    = $gwTag
        CreateMetrics            = $createMetrics
        MoveMetrics              = $moveMetrics
        CreateTrace              = $createTrace
        MoveTrace                = $moveTrace
        CreateReachabilitySample = $createReachability
        MoveReachabilitySample   = $moveReachability
    }
}

$summary = [ordered]@{
    AllocatorBackend   = ""
    PostgreSQLVerified = $false
    Scenarios          = @()
}

try {
    Write-Step "Checking the core stack"
    Invoke-Kubectl -Arguments @("rollout", "status", "statefulset/net-identity-allocator-postgres", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/net-identity-allocator", "-n", "mac-allocator", "--timeout=180s") | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/cmxsafemac-ipv6-node-agent", "-n", "mac-allocator", "--timeout=180s") | Out-Null

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
    $tracePods = Get-TracePodTargets
    Write-Info "Allocator trace pod(s): $($tracePods.AllocatorPods -join ', ')"
    Write-Info "Node-agent trace pod(s): $($tracePods.NodeAgentPods -join ', ')"

    Write-Step "Preparing the shared 4-replica benchmark workload"
    Cleanup-Samples
    try {
        Wait-NoDeploymentPods
    }
    catch {
        Write-Info "Sample cleanup is still converging: $_"
    }
    Reset-AllocatorState -BaseUrl $baseUrl | Out-Null
    $pods = @(Prepare-DeploymentPods -ProjectRoot $ProjectRoot -BaseUrl $baseUrl -Replicas $DeploymentReplicas)
    $baselineNet1Addresses = Get-Net1AddressSnapshot -Pods $pods
    $baselineNet1Routes = Get-Net1RouteSnapshot -Pods $pods

    for ($index = 0; $index -lt $BatchSizes.Count; $index++) {
        $batchSize = $BatchSizes[$index]
        if ($index -gt 0) {
            Write-Step "Scenario ${batchSize}: clearing explicit IPv6 state from the existing deployment"
            Clear-DeploymentExplicitState -BaseUrl $baseUrl -Pods $pods -BaselineAddresses $baselineNet1Addresses -BaselineRoutes $baselineNet1Routes
            $pods = @(Wait-PodsReady -Namespace $deploymentNamespace -Selector $deploymentSelector -ExpectedCount $DeploymentReplicas -TimeoutSeconds 180)
        }
        $summary.Scenarios += Run-BatchScenario -BaseUrl $baseUrl -Pods $pods -BatchSize $batchSize -TracePods $tracePods
    }

    Write-Step "Parallel canonical batch summary"
    $table = @(
        $summary.Scenarios | ForEach-Object {
            [pscustomobject]@{
                BatchSize        = $_.BatchSize
                CreateAvgMs      = $_.CreateMetrics.AvgMs
                CreateP95Ms      = $_.CreateMetrics.P95Ms
                CreateMaxMs      = $_.CreateMetrics.MaxMs
                CreateOpsPerSec  = $_.CreateMetrics.OpsPerSec
                MoveAvgMs        = $_.MoveMetrics.AvgMs
                MoveP95Ms        = $_.MoveMetrics.P95Ms
                MoveMaxMs        = $_.MoveMetrics.MaxMs
                MoveOpsPerSec    = $_.MoveMetrics.OpsPerSec
                CreatePingSample = $_.CreateReachabilitySample
                MovePingSample   = $_.MoveReachabilitySample
            }
        }
    )
    $table | Format-Table -AutoSize | Out-String | Write-Host
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
