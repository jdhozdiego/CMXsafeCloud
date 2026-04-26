param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [int]$PhpMonitorPort = 18082,
    [int]$TrafficCollectorPort = 18083,
    [switch]$OpenBrowser,
    [switch]$SkipInstall,
    [switch]$SkipE2E,
    [switch]$CleanupAfter
)

. "$PSScriptRoot\CMXsafeMAC-IPv6-helpers.ps1"

function Test-UrlReady {
    param(
        [Parameter(Mandatory = $true)][string]$Url,
        [int]$TimeoutSeconds = 3
    )
    try {
        $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec $TimeoutSeconds
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 400
    }
    catch {
        return $false
    }
}

function Start-HttpPortForward {
    param(
        [Parameter(Mandatory = $true)][string]$Namespace,
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][int]$RemotePort,
        [Parameter(Mandatory = $true)][int]$PreferredPort,
        [Parameter(Mandatory = $true)][string]$HealthUrl,
        [Parameter(Mandatory = $true)][string]$LogStem,
        [string]$ProjectRoot = (Get-CMXsafeProjectRoot)
    )

    if ($PreferredPort -gt 0 -and (Test-UrlReady -Url $HealthUrl)) {
        return [pscustomobject]@{
            Port    = $PreferredPort
            Url     = $HealthUrl
            Process = $null
            Started = $false
        }
    }

    $actualPort = Get-FreeTcpPort -PreferredPort $PreferredPort
    $tmpDir = Join-Path $ProjectRoot "tmp"
    New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
    $stdout = Join-Path $tmpDir "$LogStem.out.log"
    $stderr = Join-Path $tmpDir "$LogStem.err.log"

    $process = Start-Process `
        -FilePath "kubectl" `
        -ArgumentList @("port-forward", "-n", $Namespace, "svc/$Service", "$actualPort`:$RemotePort") `
        -WorkingDirectory $ProjectRoot `
        -WindowStyle Hidden `
        -PassThru `
        -RedirectStandardOutput $stdout `
        -RedirectStandardError $stderr

    $url = $HealthUrl -replace [regex]::Escape(([string]$PreferredPort)), ([string]$actualPort)
    Wait-Until -Description "port-forward for $Service" -TimeoutSeconds 40 -IntervalSeconds 2 -Condition {
        if ($process.HasExited) {
            $stderrText = if (Test-Path $stderr) { Get-Content $stderr -Raw } else { "" }
            throw "kubectl port-forward exited early for $Service. $stderrText"
        }
        Test-UrlReady -Url $url
    } | Out-Null

    return [pscustomobject]@{
        Port    = $actualPort
        Url     = $url
        Process = $process
        Started = $true
    }
}

$phpPortForward = $null
$collectorPortForward = $null

try {
    Write-Step "CMXsafeMAC-IPv6 live demo session"
    Write-Info "This script will:"
    Write-Info "1. Ensure the core stack is installed"
    Write-Info "2. Expose the PHP monitor and traffic collector locally"
    Write-Info "3. Tell you what to open in your browser"
    Write-Info "4. Run the end-to-end demo traffic so you can watch the monitor live"
    Write-Host ""

    if (-not $SkipInstall) {
        Write-Step "Installing or refreshing the core stack"
        & (Join-Path $PSScriptRoot "install-docker-desktop-kind-stack.ps1")
    } else {
        Write-Step "Skipping install phase as requested"
    }

    Write-Step "Starting local access to the monitor and collector"
    $phpPortForward = Start-HttpPortForward `
        -Namespace "mac-allocator" `
        -Service "net-identity-allocator-php-monitor" `
        -RemotePort 80 `
        -PreferredPort $PhpMonitorPort `
        -HealthUrl "http://127.0.0.1:$PhpMonitorPort/" `
        -LogStem "php-monitor-port-forward" `
        -ProjectRoot $ProjectRoot

    $collectorPortForward = Start-HttpPortForward `
        -Namespace "mac-allocator" `
        -Service "cmxsafemac-ipv6-traffic-collector" `
        -RemotePort 8082 `
        -PreferredPort $TrafficCollectorPort `
        -HealthUrl "http://127.0.0.1:$TrafficCollectorPort/healthz" `
        -LogStem "traffic-collector-port-forward" `
        -ProjectRoot $ProjectRoot

    Write-Step "Open the browser now"
    Write-Host "PHP monitor:" -ForegroundColor Green
    Write-Host "  $($phpPortForward.Url)" -ForegroundColor White
    Write-Host ""
    Write-Host "Optional raw collector endpoints:" -ForegroundColor Green
    Write-Host "  $($collectorPortForward.Url)" -ForegroundColor White
    Write-Host "  http://127.0.0.1:$($collectorPortForward.Port)/flows?window_seconds=60" -ForegroundColor White
    Write-Host ""
    Write-Host "What you should do now:" -ForegroundColor Cyan
    Write-Host "  1. Open the PHP monitor URL above in your browser."
    Write-Host "  2. Keep it open."
    Write-Host "  3. Watch the topology and flow table while the demo traffic runs."
    Write-Host ""
    Write-Host "What you should expect:" -ForegroundColor Cyan
    Write-Host "  - First the monitor may look quiet."
    Write-Host "  - Then managed pods, managed IPv6s, and explicit IPv6s should appear."
    Write-Host "  - During the demo, the flow graph and flow table should populate."
    Write-Host "  - Prefix filters such as 6666, 7777, and bbbb should become available."
    Write-Host ""

    if ($OpenBrowser) {
        Start-Process $phpPortForward.Url | Out-Null
    }

    Start-Sleep -Seconds 3

    if (-not $SkipE2E) {
        Write-Step "Running the demo workload traffic"
        if ($CleanupAfter) {
            & (Join-Path $PSScriptRoot "tests\core\test-local-e2e.ps1") -CleanupSamplesAfter
        } else {
            & (Join-Path $PSScriptRoot "tests\core\test-local-e2e.ps1")
        }
    } else {
        Write-Step "Skipping end-to-end traffic run as requested"
    }

    Write-Step "Demo session complete"
    if (-not $CleanupAfter) {
        Write-Host "The sample namespaces are still present so you can keep inspecting the monitor." -ForegroundColor Green
    } else {
        Write-Host "The sample namespaces were cleaned after the demo." -ForegroundColor Green
    }
    Write-Host ""
    Write-Host "To stop the local access processes later:" -ForegroundColor Cyan
    if ($phpPortForward.Started -and $phpPortForward.Process) {
        Write-Host "  Stop-Process -Id $($phpPortForward.Process.Id)"
    }
    if ($collectorPortForward.Started -and $collectorPortForward.Process) {
        Write-Host "  Stop-Process -Id $($collectorPortForward.Process.Id)"
    }
    Write-Host ""
    Write-Host "To keep the cluster stable when you are done:" -ForegroundColor Cyan
    Write-Host "  kubectl delete namespace mac-demo mac-deployment-demo --ignore-not-found=true"
    Write-Host ""
    Write-Host "To reset allocator rows for another clean test run:" -ForegroundColor Cyan
    Write-Host "  powershell -NoProfile -ExecutionPolicy Bypass -File .\\tools\\tests\\core\\test-local-e2e.ps1"
}
catch {
    Write-Host ""
    Write-Host "Demo session failed: $_" -ForegroundColor Red
    throw
}
