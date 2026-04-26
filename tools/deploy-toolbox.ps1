param(
    [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path,
    [string]$NodeContainer = "desktop-control-plane",
    [switch]$SkipImageImport,
    [switch]$ForceRebuild,
    [switch]$ForceImageImport
)

. "$PSScriptRoot\CMXsafeMAC-IPv6-helpers.ps1"

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
Invoke-Kubectl -Arguments @("get", "nodes") | Out-Null
Invoke-Docker -Arguments @("inspect", $NodeContainer) | Out-Null

$manifestPath = Join-Path $ProjectRoot "k8s\toolbox.yaml"
$manifest = Get-Content $manifestPath -Raw
$toolboxImageMatch = [regex]::Match($manifest, 'image:\s*(cmxsafemac-ipv6-toolbox:[^\s]+)')
if (-not $toolboxImageMatch.Success) {
    throw "Unable to find toolbox image in k8s/toolbox.yaml"
}
$toolboxImage = $toolboxImageMatch.Groups[1].Value
$toolboxContext = Join-Path $ProjectRoot "CMXsafeMAC-IPv6-toolbox"
$sourceHash = Get-SourceFingerprint -ContextPath $toolboxContext

Write-Step "Building the toolbox image"
Ensure-LocalImage -Image $toolboxImage -ContextPath $toolboxContext -SourceHash $sourceHash

if (-not $SkipImageImport) {
    Write-Step "Importing the toolbox image into the kind node container"
    Ensure-NodeImage -Image $toolboxImage -ContainerName $NodeContainer -SourceHash $sourceHash
}

Write-Step "Deploying the toolbox"
Invoke-Kubectl -Arguments @("apply", "-f", $manifestPath) | Out-Null
Invoke-Kubectl -Arguments @("rollout", "status", "deployment/cmxsafemac-ipv6-toolbox", "-n", "mac-allocator", "--timeout=180s") | Out-Null

Write-Step "Toolbox ready"
Write-Info "Connect with:"
Write-Host "    powershell -NoProfile -ExecutionPolicy Bypass -File .\tools\connect-toolbox.ps1" -ForegroundColor DarkGray
