param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$NodeContainer = "desktop-control-plane",
    [switch]$SkipMultusInstall,
    [switch]$SkipPhpMonitor,
    [switch]$SkipTrafficCollector,
    [switch]$SkipImageImport,
    [switch]$SkipTetragonCheck,
    [switch]$ForceRebuild,
    [switch]$ForceImageImport
)

. "$PSScriptRoot\CMXsafeMAC-IPv6-helpers.ps1"

$images = Get-StackImageConfig -ProjectRoot $ProjectRoot
$tmpDir = Join-Path $ProjectRoot "tmp"
$imageStateDir = Join-Path $tmpDir "image-state"
New-Item -ItemType Directory -Force -Path $tmpDir | Out-Null
New-Item -ItemType Directory -Force -Path $imageStateDir | Out-Null

function Get-SafeImageStateName {
    param(
        [Parameter(Mandatory = $true)][string]$Image
    )
    return ($Image -replace "[:/]", "-")
}

function Get-SourceFingerprint {
    param(
        [Parameter(Mandatory = $true)][string]$ContextPath
    )
    $resolved = (Resolve-Path $ContextPath).Path
    $files = @(
        Get-ChildItem -Path $resolved -Recurse -File -Force |
            Where-Object {
                $_.FullName -notmatch '\\(__pycache__|tmp|\.git|node_modules)(\\|$)' -and
                $_.Name -notmatch '\.(pyc|pyo)$'
            } |
            Sort-Object FullName
    )
    $builder = [System.Text.StringBuilder]::new()
    foreach ($file in $files) {
        $relativePath = $file.FullName.Substring($resolved.Length).TrimStart('\').Replace('\', '/')
        $contentHash = (Get-FileHash -Algorithm SHA256 -Path $file.FullName).Hash.ToLowerInvariant()
        [void]$builder.AppendLine("$relativePath|$contentHash")
    }
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($builder.ToString())
    $stream = [System.IO.MemoryStream]::new($bytes)
    try {
        return (Get-FileHash -Algorithm SHA256 -InputStream $stream).Hash.ToLowerInvariant()
    }
    finally {
        $stream.Dispose()
    }
}

function Get-LocalImageStatePath {
    param(
        [Parameter(Mandatory = $true)][string]$Image
    )
    $safeName = Get-SafeImageStateName -Image $Image
    return Join-Path $imageStateDir "$safeName.sha256"
}

function Get-LocalRecordedSourceHash {
    param(
        [Parameter(Mandatory = $true)][string]$Image
    )
    $statePath = Get-LocalImageStatePath -Image $Image
    if (-not (Test-Path $statePath)) {
        return $null
    }
    return (Get-Content -Path $statePath -Raw).Trim().ToLowerInvariant()
}

function Set-LocalRecordedSourceHash {
    param(
        [Parameter(Mandatory = $true)][string]$Image,
        [Parameter(Mandatory = $true)][string]$SourceHash
    )
    $statePath = Get-LocalImageStatePath -Image $Image
    Set-Content -Path $statePath -Value ($SourceHash.ToLowerInvariant()) -NoNewline
}

function Get-NodeImageStatePath {
    param(
        [Parameter(Mandatory = $true)][string]$Image
    )
    $safeName = Get-SafeImageStateName -Image $Image
    return "/root/cmxsafe-image-state/$safeName.sha256"
}

function Get-NodeRecordedSourceHash {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$Image
    )
    try {
        $statePath = Get-NodeImageStatePath -Image $Image
        $value = Invoke-Docker -Arguments @("exec", $ContainerName, "sh", "-lc", "test -f $statePath && cat $statePath || true")
        $trimmed = $value.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) {
            return $null
        }
        return $trimmed.ToLowerInvariant()
    }
    catch {
        return $null
    }
}

function Set-NodeRecordedSourceHash {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$Image,
        [Parameter(Mandatory = $true)][string]$SourceHash
    )
    $statePath = Get-NodeImageStatePath -Image $Image
    $stateDir = [System.IO.Path]::GetDirectoryName($statePath).Replace('\', '/')
    $escapedHash = $SourceHash.ToLowerInvariant().Replace("'", "''")
    Invoke-Docker -Arguments @("exec", $ContainerName, "sh", "-lc", "mkdir -p $stateDir && printf '%s' '$escapedHash' > $statePath") | Out-Null
}

function Test-NodeContainerHasCniPlugin {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$PluginName
    )
    try {
        $result = Invoke-Docker -Arguments @("exec", $ContainerName, "sh", "-lc", "test -x /opt/cni/bin/$PluginName && echo yes || true")
        return $result.Trim() -eq "yes"
    }
    catch {
        return $false
    }
}

function Test-LocalDockerImageExists {
    param(
        [Parameter(Mandatory = $true)][string]$Image
    )
    try {
        Invoke-Docker -Arguments @("image", "inspect", $Image) | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

function Test-NodeContainerHasImage {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$Image
    )
    try {
        $listing = Invoke-Docker -Arguments @("exec", $ContainerName, "ctr", "-n", "k8s.io", "images", "ls", "-q")
        $refs = @($listing -split "`r?`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ })
        if ($refs -contains $Image) {
            return $true
        }
        if ($Image -notmatch '^[^/]+\.[^/]+/' -and $Image -notmatch '/') {
            return $refs -contains "docker.io/library/$Image"
        }
        return $false
    }
    catch {
        return $false
    }
}

function Import-ImageIntoNode {
    param(
        [Parameter(Mandatory = $true)][string]$Image,
        [Parameter(Mandatory = $true)][string]$ContainerName
    )
    $safeName = ($Image -replace "[:/]", "-")
    $tarPath = Join-Path $tmpDir "$safeName.tar"
    Write-Info "Importing $Image into $ContainerName"
    Invoke-Docker -Arguments @("save", "-o", $tarPath, $Image) | Out-Null
    Invoke-Docker -Arguments @("cp", $tarPath, "${ContainerName}:/root/$safeName.tar") | Out-Null
    Invoke-Docker -Arguments @("exec", $ContainerName, "ctr", "-n", "k8s.io", "images", "import", "/root/$safeName.tar") | Out-Null
}

function Ensure-LocalImage {
    param(
        [Parameter(Mandatory = $true)][string]$Image,
        [Parameter(Mandatory = $true)][string]$ContextPath,
        [Parameter(Mandatory = $true)][string]$SourceHash
    )
    $recordedHash = Get-LocalRecordedSourceHash -Image $Image
    if (-not $ForceRebuild -and (Test-LocalDockerImageExists -Image $Image) -and $recordedHash -eq $SourceHash) {
        Write-Info "Reusing local image $Image (source unchanged)"
        return
    }
    if ($ForceRebuild) {
        Write-Info "Rebuilding $Image because -ForceRebuild was requested"
    }
    elseif (-not (Test-LocalDockerImageExists -Image $Image)) {
        Write-Info "Building $Image because the local image is missing"
    }
    else {
        Write-Info "Rebuilding $Image because the source fingerprint changed"
    }
    Invoke-Docker -Arguments @("build", "-t", $Image, $ContextPath) | Out-Null
    Set-LocalRecordedSourceHash -Image $Image -SourceHash $SourceHash
}

function Ensure-NodeImage {
    param(
        [Parameter(Mandatory = $true)][string]$Image,
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][string]$SourceHash
    )
    $recordedHash = Get-NodeRecordedSourceHash -ContainerName $ContainerName -Image $Image
    if (-not $ForceImageImport -and (Test-NodeContainerHasImage -ContainerName $ContainerName -Image $Image) -and $recordedHash -eq $SourceHash) {
        Write-Info "Reusing node image $Image in $ContainerName (source unchanged)"
        return
    }
    if ($ForceImageImport) {
        Write-Info "Re-importing $Image into $ContainerName because -ForceImageImport was requested"
    }
    elseif (-not (Test-NodeContainerHasImage -ContainerName $ContainerName -Image $Image)) {
        Write-Info "Importing $Image into $ContainerName because the node image is missing"
    }
    else {
        Write-Info "Re-importing $Image into $ContainerName because the source fingerprint changed"
    }
    Import-ImageIntoNode -Image $Image -ContainerName $ContainerName
    Set-NodeRecordedSourceHash -ContainerName $ContainerName -Image $Image -SourceHash $SourceHash
}

Write-Step "Validating local tools and cluster access"
Ensure-Command -Name "kubectl"
Ensure-Command -Name "docker"
Invoke-Kubectl -Arguments @("version", "--client") | Out-Null
$currentContext = Invoke-Kubectl -Arguments @("config", "current-context")
Write-Info "kubectl context: $currentContext"
Invoke-Kubectl -Arguments @("get", "nodes") | Out-Null
Invoke-Docker -Arguments @("inspect", $NodeContainer) | Out-Null

if (-not $SkipTetragonCheck) {
    Write-Step "Checking Tetragon availability"
    $tetragonPods = Invoke-KubectlJson -Arguments @("get", "pods", "-n", "kube-system", "-o", "json")
    $runningTetragon = @(
        $tetragonPods.items | Where-Object {
            $_.metadata.name -like "tetragon*" -and
            $_.status.phase -eq "Running"
        }
    )
    Assert-True -Condition ($runningTetragon.Count -gt 0) -Message "Tetragon is required but no running tetragon pods were found in kube-system."
    Write-Info "Found $($runningTetragon.Count) running Tetragon pod(s)"
}

if (-not $SkipMultusInstall) {
    Write-Step "Ensuring Multus is installed"
    try {
        Invoke-Kubectl -Arguments @("get", "daemonset", "kube-multus-ds", "-n", "kube-system") | Out-Null
        Write-Info "Multus daemonset already present"
    }
    catch {
        Invoke-Kubectl -Arguments @("apply", "-f", "https://raw.githubusercontent.com/k8snetworkplumbingwg/multus-cni/master/deployments/multus-daemonset.yml") | Out-Null
    }
    Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/kube-multus-ds", "-n", "kube-system", "--timeout=180s") | Out-Null

    Write-Step "Ensuring required CNI plugins exist in the node container"
    $requiredPlugins = @("multus", "bridge", "host-local")
    $missingPlugins = @($requiredPlugins | Where-Object { -not (Test-NodeContainerHasCniPlugin -ContainerName $NodeContainer -PluginName $_) })
    if ($missingPlugins.Count -gt 0) {
        Write-Info "Installing missing plugins: $($missingPlugins -join ', ')"
        Invoke-Docker -Arguments @(
            "exec",
            $NodeContainer,
            "sh",
            "-lc",
            "cd /opt/cni/bin && wget -q https://github.com/containernetworking/plugins/releases/download/v1.8.0/cni-plugins-linux-amd64-v1.8.0.tgz && tar -xzf cni-plugins-linux-amd64-v1.8.0.tgz && rm -f cni-plugins-linux-amd64-v1.8.0.tgz"
        ) | Out-Null
    }
    $pluginListing = Invoke-Docker -Arguments @("exec", $NodeContainer, "sh", "-lc", "ls /opt/cni/bin | sort | grep -E '^(bridge|host-local|multus)$'")
    Write-Info "Available plugins:`n$pluginListing"
}

Write-Step "Applying sample namespace and Multus network definitions"
Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\explicit-v6-network.yaml")) | Out-Null

Write-Step "Building the project images"
$allocatorSourceHash = Get-SourceFingerprint -ContextPath (Join-Path $ProjectRoot "net-identity-allocator")
$nodeAgentSourceHash = Get-SourceFingerprint -ContextPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-node-agent")
Ensure-LocalImage -Image $images.AllocatorImage -ContextPath (Join-Path $ProjectRoot "net-identity-allocator") -SourceHash $allocatorSourceHash
Ensure-LocalImage -Image $images.NodeAgentImage -ContextPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-node-agent") -SourceHash $nodeAgentSourceHash
if (-not $SkipPhpMonitor) {
    $phpMonitorSourceHash = Get-SourceFingerprint -ContextPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-php-monitor")
    Ensure-LocalImage -Image $images.PhpImage -ContextPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-php-monitor") -SourceHash $phpMonitorSourceHash
}
if (-not $SkipTrafficCollector) {
    $trafficCollectorSourceHash = Get-SourceFingerprint -ContextPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-traffic-collector")
    Ensure-LocalImage -Image $images.TrafficCollectorImage -ContextPath (Join-Path $ProjectRoot "CMXsafeMAC-IPv6-traffic-collector") -SourceHash $trafficCollectorSourceHash
}

if (-not $SkipImageImport) {
    Write-Step "Importing the project images into the kind node container"
    Ensure-NodeImage -Image $images.AllocatorImage -ContainerName $NodeContainer -SourceHash $allocatorSourceHash
    Ensure-NodeImage -Image $images.NodeAgentImage -ContainerName $NodeContainer -SourceHash $nodeAgentSourceHash
    if (-not $SkipPhpMonitor) {
        Ensure-NodeImage -Image $images.PhpImage -ContainerName $NodeContainer -SourceHash $phpMonitorSourceHash
    }
    if (-not $SkipTrafficCollector) {
        Ensure-NodeImage -Image $images.TrafficCollectorImage -ContainerName $NodeContainer -SourceHash $trafficCollectorSourceHash
    }
}

Write-Step "Deploying the core CMXsafeMAC-IPv6 stack"
try {
    Invoke-Kubectl -Arguments @("get", "namespace", "mac-allocator") | Out-Null
}
catch {
    Invoke-Kubectl -Arguments @("create", "namespace", "mac-allocator") | Out-Null
}
Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\net-identity-allocator-postgres-secret.yaml")) | Out-Null
Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\allocator-stack.yaml")) | Out-Null
Invoke-Kubectl -Arguments @("rollout", "status", "statefulset/net-identity-allocator-postgres", "-n", "mac-allocator", "--timeout=180s") | Out-Null
Invoke-Kubectl -Arguments @("rollout", "status", "deployment/net-identity-allocator", "-n", "mac-allocator", "--timeout=180s") | Out-Null
Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/cmxsafemac-ipv6-node-agent", "-n", "mac-allocator", "--timeout=180s") | Out-Null

if (-not $SkipTrafficCollector) {
    Write-Step "Deploying the traffic collector"
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\traffic-collector.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "daemonset/cmxsafemac-ipv6-traffic-collector", "-n", "mac-allocator", "--timeout=180s") | Out-Null
}

if (-not $SkipPhpMonitor) {
    Write-Step "Deploying the PHP monitor"
    Invoke-Kubectl -Arguments @("apply", "-f", (Join-Path $ProjectRoot "k8s\php-monitor-deployment.yaml")) | Out-Null
    Invoke-Kubectl -Arguments @("rollout", "status", "deployment/net-identity-allocator-php-monitor", "-n", "mac-allocator", "--timeout=180s") | Out-Null
}

Write-Step "Installation complete"
Write-Info "Core stack namespace: mac-allocator"
Write-Info "Sample namespaces and Multus networks: mac-demo, mac-deployment-demo"
Write-Info "Next step: run tools\\tests\\core\\test-local-e2e.ps1 to validate managed MAC, managed IPv6, explicit IPv6, connectivity, deletion, and canonical moves."
