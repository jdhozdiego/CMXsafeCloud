param(
    [string]$ImageTag = "cmxsafemac-ipv6-endpoint-base:docker-desktop-v1",
    [string]$OpenSshVersion = "10.2p1"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Dockerfile = Join-Path $RepoRoot "CMXsafeMAC-IPv6-endpoint-helper\Dockerfile"

Write-Host "Building CMXsafe endpoint runtime image $ImageTag"
$dockerBuildArgs = @(
    "build",
    "--build-arg", "OPENSSH_VERSION=$OpenSshVersion",
    "--tag", $ImageTag,
    "--file", $Dockerfile,
    $RepoRoot
)

docker @dockerBuildArgs

if ($LASTEXITCODE -ne 0) {
    throw "docker build failed"
}

Write-Host "Built image:"
docker image inspect $ImageTag --format "{{.Id}}"
