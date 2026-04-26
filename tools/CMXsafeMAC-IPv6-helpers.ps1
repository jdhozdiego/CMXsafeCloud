Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-CMXsafeProjectRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

function Write-Step {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Write-Info {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Host "    $Message" -ForegroundColor DarkGray
}

function Ensure-Command {
    param([Parameter(Mandatory = $true)][string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found on PATH: $Name"
    }
}

function Format-NativeArgument {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Argument)
    if ($Argument -notmatch '[\s"]') {
        return $Argument
    }
    $escaped = $Argument -replace '(\\*)"', '$1$1\"'
    $escaped = $escaped -replace '(\\+)$', '$1$1'
    return '"' + $escaped + '"'
}

function Invoke-Tool {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$WorkingDirectory = (Get-CMXsafeProjectRoot)
    )
    $tmpDir = Join-Path (Get-CMXsafeProjectRoot) "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $token = [guid]::NewGuid().ToString("N")
    $stdout = Join-Path $tmpDir "$token.stdout.log"
    $stderr = Join-Path $tmpDir "$token.stderr.log"
    $argumentString = ($Arguments | ForEach-Object { Format-NativeArgument -Argument $_ }) -join " "
    $process = Start-Process `
        -FilePath $Command `
        -ArgumentList $argumentString `
        -WorkingDirectory $WorkingDirectory `
        -PassThru `
        -NoNewWindow `
        -Wait `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr
    $text = ""
    if (Test-Path $stdout) {
        $text += Get-Content $stdout -Raw
    }
    if (Test-Path $stderr) {
        $stderrText = Get-Content $stderr -Raw
        if ($stderrText) {
            if ($text) {
                $text += "`n"
            }
            $text += $stderrText
        }
    }
    $exitCode = $process.ExitCode
    Remove-Item $stdout, $stderr -ErrorAction SilentlyContinue
    $text = $text.Trim()
    if ($exitCode -ne 0) {
        throw "$Command $($Arguments -join ' ') failed.`n$text"
    }
    return $text
}

function Invoke-Kubectl {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$WorkingDirectory = (Get-CMXsafeProjectRoot)
    )
    $attempt = 0
    $delaysMs = @(300, 1000, 2500)
    while ($true) {
        try {
            return Invoke-Tool -Command "kubectl" -Arguments $Arguments -WorkingDirectory $WorkingDirectory
        }
        catch {
            $attempt++
            $message = $_.Exception.Message
            $isTransient = (
                $message -match "TLS handshake timeout" -or
                $message -match "i/o timeout" -or
                $message -match "connection refused" -or
                $message -match "service unavailable" -or
                $message -match "EOF"
            )
            if (-not $isTransient -or $attempt -gt $delaysMs.Count) {
                throw
            }
            Start-Sleep -Milliseconds $delaysMs[$attempt - 1]
        }
    }
}

function Invoke-Docker {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$WorkingDirectory = (Get-CMXsafeProjectRoot)
    )
    return Invoke-Tool -Command "docker" -Arguments $Arguments -WorkingDirectory $WorkingDirectory
}

function Invoke-KubectlJson {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [string]$WorkingDirectory = (Get-CMXsafeProjectRoot)
    )
    $json = Invoke-Kubectl -Arguments $Arguments -WorkingDirectory $WorkingDirectory
    if (-not $json) {
        return $null
    }
    return $json | ConvertFrom-Json
}

function Wait-Until {
    param(
        [Parameter(Mandatory = $true)][scriptblock]$Condition,
        [Parameter(Mandatory = $true)][string]$Description,
        [int]$TimeoutSeconds = 180,
        [int]$IntervalSeconds = 2
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastError = $null
    while ((Get-Date) -lt $deadline) {
        try {
            $result = & $Condition
            if ($result) {
                return $result
            }
        }
        catch {
            $lastError = $_
        }
        Start-Sleep -Seconds $IntervalSeconds
    }
    if ($lastError) {
        throw "Timed out waiting for $Description. Last error: $lastError"
    }
    throw "Timed out waiting for $Description."
}

function Get-StackImageConfig {
    param([string]$ProjectRoot = (Get-CMXsafeProjectRoot))
    $allocatorStack = Get-Content (Join-Path $ProjectRoot "k8s\allocator-stack.yaml") -Raw
    $phpDeployment = Get-Content (Join-Path $ProjectRoot "k8s\php-monitor-deployment.yaml") -Raw
    $trafficCollector = Get-Content (Join-Path $ProjectRoot "k8s\traffic-collector.yaml") -Raw

    $allocatorImage = [regex]::Match($allocatorStack, 'image:\s*(net-identity-allocator:[^\s]+)')
    $nodeAgentImage = [regex]::Match($allocatorStack, 'image:\s*(cmxsafemac-ipv6-node-agent:[^\s]+)')
    $phpImage = [regex]::Match($phpDeployment, 'image:\s*(cmxsafemac-ipv6-php-monitor:[^\s]+)')
    $trafficCollectorImage = [regex]::Match($trafficCollector, 'image:\s*(cmxsafemac-ipv6-traffic-collector:[^\s]+)')

    if (-not $allocatorImage.Success) {
        throw "Unable to find allocator image in k8s/allocator-stack.yaml"
    }
    if (-not $nodeAgentImage.Success) {
        throw "Unable to find node-agent image in k8s/allocator-stack.yaml"
    }
    if (-not $phpImage.Success) {
        throw "Unable to find PHP monitor image in k8s/php-monitor-deployment.yaml"
    }
    if (-not $trafficCollectorImage.Success) {
        throw "Unable to find traffic collector image in k8s/traffic-collector.yaml"
    }

    return [pscustomobject]@{
        AllocatorImage        = $allocatorImage.Groups[1].Value
        NodeAgentImage        = $nodeAgentImage.Groups[1].Value
        PhpImage              = $phpImage.Groups[1].Value
        TrafficCollectorImage = $trafficCollectorImage.Groups[1].Value
    }
}

function Test-AllocatorHealth {
    param([int]$Port = 18080)
    try {
        $response = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/healthz" -TimeoutSec 3
        return $response.status -eq "ok"
    }
    catch {
        return $false
    }
}

function Test-TrafficCollectorHealth {
    param([int]$Port = 18082)
    try {
        $response = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/healthz" -TimeoutSec 3
        return $response.status -eq "ok"
    }
    catch {
        return $false
    }
}

function Get-FreeTcpPort {
    param([int]$PreferredPort = 0)
    if ($PreferredPort -gt 0) {
        try {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $PreferredPort)
            $listener.Start()
            $listener.Stop()
            return $PreferredPort
        }
        catch {
        }
    }
    $ephemeral = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    $ephemeral.Start()
    $port = ([System.Net.IPEndPoint]$ephemeral.LocalEndpoint).Port
    $ephemeral.Stop()
    return $port
}

function Start-AllocatorPortForward {
    param(
        [int]$Port = 18080,
        [string]$Namespace = "mac-allocator",
        [string]$Service = "net-identity-allocator",
        [string]$ProjectRoot = (Get-CMXsafeProjectRoot)
    )
    if ($Port -gt 0 -and (Test-AllocatorHealth -Port $Port)) {
        return [pscustomobject]@{
            Port     = $Port
            BaseUrl  = "http://127.0.0.1:$Port"
            Process  = $null
            Started  = $false
            LogFile  = $null
        }
    }
    $actualPort = Get-FreeTcpPort -PreferredPort $Port

    $tmpDir = Join-Path $ProjectRoot "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $stdout = Join-Path $tmpDir "allocator-port-forward.out.log"
    $stderr = Join-Path $tmpDir "allocator-port-forward.err.log"

    $startProcessArgs = @{
        FilePath               = "kubectl"
        ArgumentList           = @("port-forward", "-n", $Namespace, "svc/$Service", "$actualPort`:8080")
        WorkingDirectory       = $ProjectRoot
        PassThru               = $true
        RedirectStandardOutput = $stdout
        RedirectStandardError  = $stderr
    }
    if ($IsWindows) {
        $startProcessArgs.WindowStyle = "Hidden"
    }
    $process = Start-Process @startProcessArgs

    Wait-Until -Description "allocator port-forward on port $actualPort" -TimeoutSeconds 40 -IntervalSeconds 2 -Condition {
        if ($process.HasExited) {
            $stderrText = if (Test-Path $stderr) { Get-Content $stderr -Raw } else { "" }
            throw "kubectl port-forward exited early. $stderrText"
        }
        Test-AllocatorHealth -Port $actualPort
    } | Out-Null

    return [pscustomobject]@{
        Port     = $actualPort
        BaseUrl  = "http://127.0.0.1:$actualPort"
        Process  = $process
        Started  = $true
        LogFile  = $stdout
    }
}

function Start-TrafficCollectorPortForward {
    param(
        [int]$Port = 18082,
        [string]$Namespace = "mac-allocator",
        [string]$Service = "cmxsafemac-ipv6-traffic-collector",
        [string]$ProjectRoot = (Get-CMXsafeProjectRoot)
    )
    if ($Port -gt 0 -and (Test-TrafficCollectorHealth -Port $Port)) {
        return [pscustomobject]@{
            Port     = $Port
            BaseUrl  = "http://127.0.0.1:$Port"
            Process  = $null
            Started  = $false
            LogFile  = $null
        }
    }
    $actualPort = Get-FreeTcpPort -PreferredPort $Port

    $tmpDir = Join-Path $ProjectRoot "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $stdout = Join-Path $tmpDir "traffic-collector-port-forward.out.log"
    $stderr = Join-Path $tmpDir "traffic-collector-port-forward.err.log"

    $startProcessArgs = @{
        FilePath               = "kubectl"
        ArgumentList           = @("port-forward", "-n", $Namespace, "svc/$Service", "$actualPort`:8082")
        WorkingDirectory       = $ProjectRoot
        PassThru               = $true
        RedirectStandardOutput = $stdout
        RedirectStandardError  = $stderr
    }
    if ($IsWindows) {
        $startProcessArgs.WindowStyle = "Hidden"
    }
    $process = Start-Process @startProcessArgs

    Wait-Until -Description "traffic collector port-forward on port $actualPort" -TimeoutSeconds 40 -IntervalSeconds 2 -Condition {
        if ($process.HasExited) {
            $stderrText = if (Test-Path $stderr) { Get-Content $stderr -Raw } else { "" }
            throw "kubectl port-forward exited early. $stderrText"
        }
        Test-TrafficCollectorHealth -Port $actualPort
    } | Out-Null

    return [pscustomobject]@{
        Port     = $actualPort
        BaseUrl  = "http://127.0.0.1:$actualPort"
        Process  = $process
        Started  = $true
        LogFile  = $stdout
    }
}

function Stop-PortForward {
    param($PortForward)
    if ($null -eq $PortForward) {
        return
    }
    if ($PortForward.Started -and $null -ne $PortForward.Process) {
        try {
            if (-not $PortForward.Process.HasExited) {
                Stop-Process -Id $PortForward.Process.Id -Force
            }
        }
        catch {
        }
    }
}

function Invoke-AllocatorApi {
    param(
        [Parameter(Mandatory = $true)][string]$BaseUrl,
        [Parameter(Mandatory = $true)][string]$Path,
        [ValidateSet("GET", "POST")][string]$Method = "GET",
        $Body = $null
    )
    $uri = "$BaseUrl$Path"
    if ($Method -eq "GET") {
        return Invoke-RestMethod -Method Get -Uri $uri -TimeoutSec 30
    }
    $json = if ($null -eq $Body) { "{}" } else { $Body | ConvertTo-Json -Depth 8 }
    return Invoke-RestMethod -Method Post -Uri $uri -ContentType "application/json" -Body $json -TimeoutSec 30
}

function Assert-True {
    param(
        [Parameter(Mandatory = $true)][bool]$Condition,
        [Parameter(Mandatory = $true)][string]$Message
    )
    if (-not $Condition) {
        throw $Message
    }
}

function Get-ManagedPods {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$Selector
    )
    $payload = Invoke-KubectlJson -Arguments @("get", "pods", "-n", $Namespace, "-l", $Selector, "-o", "json")
    $items = @($payload.items | Sort-Object { $_.metadata.name })
    return $items
}

function Wait-PodsReady {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$Selector,
        [Parameter(Mandatory = $true)][int]$ExpectedCount,
        [int]$TimeoutSeconds = 240
    )
    return Wait-Until -Description "Ready pods for $Selector in $Namespace" -TimeoutSeconds $TimeoutSeconds -IntervalSeconds 3 -Condition {
        $pods = @(Get-ManagedPods -Namespace $Namespace -Selector $Selector)
        if ($pods.Count -ne $ExpectedCount) {
            return $null
        }
        $ready = @(
            $pods | Where-Object {
                $notReadyStatuses = @($_.status.containerStatuses | Where-Object { -not $_.ready })
                $_.status.phase -eq "Running" -and
                ($notReadyStatuses.Count -eq 0)
            }
        )
        if ($ready.Count -eq $ExpectedCount) {
            return $pods
        }
        return $null
    }
}

function Get-PodMac {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [string]$Interface = "eth0"
    )
    return (Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "cat", "/sys/class/net/$Interface/address")).Trim().ToLower()
}

function Get-PodIPv6Addresses {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$Interface
    )
    $command = "ip -6 -o addr show dev $Interface scope global | awk '{print `$4}' | cut -d/ -f1"
    $text = Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "sh", "-lc", $command)
    if (-not $text) {
        return @()
    }
    return @($text -split "`r?`n" | ForEach-Object { $_.Trim().ToLower() } | Where-Object { $_ })
}

function Test-PodHasIPv6 {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$Interface,
        [Parameter(Mandatory = $true)][string]$IPv6
    )
    return (Get-PodIPv6Addresses -Namespace $Namespace -PodName $PodName -Interface $Interface) -contains $IPv6.ToLower()
}

function Invoke-PodPing6 {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$PodName,
        [Parameter(Mandatory = $true)][string]$TargetIPv6,
        [int]$Count = 1,
        [int]$TimeoutSeconds = 2
    )
    return Invoke-Kubectl -Arguments @("exec", "-n", $Namespace, $PodName, "--", "sh", "-lc", "ping -6 -c $Count -W $TimeoutSeconds $TargetIPv6")
}
