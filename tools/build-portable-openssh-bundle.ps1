param(
    [string]$Version = "10.2p1",
    [string]$ImageTag,
    [string]$OutputDir,
    [switch]$SkipExport,
    [switch]$ApplyCmxsafePatch
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$TempRoot = [System.IO.Path]::GetTempPath()
if (-not $ImageTag) {
    $ImageTag = "cmxsafe-portable-openssh-build:$Version"
}
if (-not $OutputDir) {
    $OutputDir = Join-Path $TempRoot "cmxsafe-portable-openssh\$Version"
}

$Dockerfile = Join-Path $PSScriptRoot "portable-openssh-bundle.Dockerfile"

Write-Host "Building Portable OpenSSH $Version into image $ImageTag"
$dockerBuildArgs = @(
    "build",
    "--build-arg", "OPENSSH_VERSION=$Version"
)

if ($ApplyCmxsafePatch) {
    $dockerBuildArgs += @("--build-arg", "APPLY_CMXSAFE_PATCH=1")
}

$dockerBuildArgs += @(
    "--tag", $ImageTag,
    "--file", $Dockerfile,
    $RepoRoot
)

docker @dockerBuildArgs

if ($LASTEXITCODE -ne 0) {
    throw "docker build failed"
}

if ($SkipExport) {
    return
}

Write-Host "Exporting staged bundle to $OutputDir"
if (Test-Path -LiteralPath $OutputDir) {
    Remove-Item -LiteralPath $OutputDir -Recurse -Force
}
New-Item -ItemType Directory -Path $OutputDir | Out-Null

$containerId = docker create $ImageTag
if ($LASTEXITCODE -ne 0 -or -not $containerId) {
    throw "docker create failed"
}

try {
    docker cp "${containerId}:/out/." $OutputDir
    if ($LASTEXITCODE -ne 0) {
        throw "docker cp failed"
    }
}
finally {
    docker rm -f $containerId | Out-Null
}

Write-Host "Bundle exported:"
Get-ChildItem -Force $OutputDir
