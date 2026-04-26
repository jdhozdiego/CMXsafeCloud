param(
    [string]$RepoRoot = "C:\Users\el_de\Documents\New project\CMXsafeMAC-IPv6"
)

$ErrorActionPreference = "Stop"

$docsApi = Join-Path $RepoRoot "docs_api"
New-Item -ItemType Directory -Force -Path $docsApi | Out-Null

$initPath = Join-Path $docsApi "__init__.py"
if (-not (Test-Path $initPath)) {
    @'
"""Mirrored Python modules for the MkDocs reference site.

These modules are generated from the runtime sources by
`tools/sync-docs-api.ps1` so mkdocstrings can document them under stable,
non-conflicting module names.
"""
'@ | Set-Content -Path $initPath
}

$targets = @(
    @{
        Source = Join-Path $RepoRoot "net-identity-allocator\app.py"
        Dest = Join-Path $docsApi "allocator_app.py"
        Header = "# Generated mirror of net-identity-allocator/app.py for MkDocs reference.`r`n"
    },
    @{
        Source = Join-Path $RepoRoot "CMXsafeMAC-IPv6-node-agent\agent.py"
        Dest = Join-Path $docsApi "node_agent.py"
        Header = "# Generated mirror of CMXsafeMAC-IPv6-node-agent/agent.py for MkDocs reference.`r`n"
    },
    @{
        Source = Join-Path $RepoRoot "CMXsafeMAC-IPv6-ssh-dashboard\app.py"
        Dest = Join-Path $docsApi "ssh_dashboard_app.py"
        Header = "# Generated mirror of CMXsafeMAC-IPv6-ssh-dashboard/app.py for MkDocs reference.`r`n"
    },
    @{
        Source = Join-Path $RepoRoot "CMXsafeMAC-IPv6-traffic-collector\collector.py"
        Dest = Join-Path $docsApi "traffic_collector.py"
        Header = "# Generated mirror of CMXsafeMAC-IPv6-traffic-collector/collector.py for MkDocs reference.`r`n"
    }
)

foreach ($target in $targets) {
    if (-not (Test-Path $target.Source)) {
        throw "Missing source file: $($target.Source)"
    }
    $sourceContent = Get-Content -Path $target.Source -Raw
    ($target.Header + $sourceContent) | Set-Content -Path $target.Dest
}

Write-Host "Synchronized docs_api mirrors in $docsApi"
