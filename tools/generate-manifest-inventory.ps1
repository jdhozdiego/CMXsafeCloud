param(
    [string]$RepoRoot = "C:\Users\el_de\Documents\New project\CMXsafeMAC-IPv6",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"

function Get-YamlDocumentMetadata {
    param(
        [string]$Text
    )

    $kind = $null
    $name = $null
    $namespace = $null
    $inMetadata = $false
    $metadataIndent = 0

    foreach ($line in ($Text -split "`r?`n")) {
        if (-not $kind -and $line -match '^\s*kind:\s*(.+?)\s*$') {
            $kind = $matches[1].Trim(" `"'")
            continue
        }

        if ($line -match '^(?<indent>\s*)metadata:\s*$') {
            $inMetadata = $true
            $metadataIndent = $matches['indent'].Length
            continue
        }

        if ($inMetadata) {
            if ($line.Trim().Length -eq 0) {
                continue
            }

            $indentMatch = [regex]::Match($line, '^(?<indent>\s*)')
            $indent = $indentMatch.Groups['indent'].Value.Length
            if ($indent -le $metadataIndent) {
                $inMetadata = $false
                continue
            }

            if (-not $name -and $line -match '^\s*name:\s*(.+?)\s*$') {
                $name = $matches[1].Trim(" `"'")
                continue
            }
            if (-not $namespace -and $line -match '^\s*namespace:\s*(.+?)\s*$') {
                $namespace = $matches[1].Trim(" `"'")
                continue
            }
        }
    }

    [pscustomobject]@{
        Kind = $kind
        Name = $name
        Namespace = $namespace
    }
}

function Get-Purpose {
    param(
        [string]$FileName,
        [string]$Kind,
        [string]$Name
    )

    $key = "$Kind|$Name"
    $map = @{
        "Namespace|mac-allocator" = "Core allocator stack namespace."
        "Namespace|mac-ssh-demo" = "Portable OpenSSH sample namespace."
        "Service|net-identity-allocator" = "Stable allocator API endpoint."
        "Deployment|net-identity-allocator" = "Central allocator service and batching control plane."
        "StatefulSet|net-identity-allocator-postgres" = "Allocator PostgreSQL persistence backend."
        "Service|net-identity-allocator-postgres" = "Allocator PostgreSQL service endpoint."
        "DaemonSet|cmxsafemac-ipv6-node-agent" = "Node-local executor for MAC and IPv6 identity changes."
        "DaemonSet|cmxsafemac-ipv6-traffic-collector" = "Traffic collector for the explicit IPv6 bridge."
        "Service|cmxsafemac-ipv6-traffic-collector" = "Traffic collector API endpoint."
        "Deployment|net-identity-allocator-php-monitor" = "PHP monitoring dashboard."
        "Service|net-identity-allocator-php-monitor" = "PHP monitoring dashboard service."
        "Deployment|cmxsafemac-ipv6-toolbox" = "Optional in-cluster toolbox for testing and benchmarking."
        "Service|portable-openssh-dashboard" = "Service for the SSH dashboard."
        "Deployment|portable-openssh-dashboard" = "Dashboard and reconcile worker for the Portable OpenSSH sample."
        "Service|portable-openssh-busybox" = "SSH service in front of the Portable OpenSSH replicas."
        "Service|portable-openssh-busybox-external" = "External LoadBalancer SSH entry point for Docker or Linux endpoints outside Kubernetes."
        "Deployment|portable-openssh-busybox" = "Multi-replica Portable OpenSSH sample workload."
        "ConfigMap|portable-openssh-etc" = "Portable OpenSSH sshd configuration."
        "ConfigMap|portable-openssh-policy" = "Forced-command and forwarding policy scripts."
        "NetworkAttachmentDefinition|explicit-v6-lan" = "Shared Multus IPv6 LAN for net1 explicit identities."
    }

    if ($map.ContainsKey($key)) {
        return $map[$key]
    }

    if ($FileName -eq "demo-deployment.yaml") {
        return "Sample allocator-managed Deployment workload."
    }
    if ($FileName -eq "demo-statefulset.yaml") {
        return "Sample allocator-managed StatefulSet workload."
    }
    if ($FileName -eq "explicit-v6-network.yaml") {
        return "Shared explicit-v6-lan Multus network definition."
    }
    if ($Kind -eq "PersistentVolumeClaim") {
        return "PVC used by a stateful or rendered-runtime component."
    }
    if ($Kind -eq "Secret") {
        return "Secret consumed by a stack component."
    }
    if ($Kind -eq "ConfigMap") {
        return "Configuration object for a stack component."
    }
    if ($Kind -eq "ServiceAccount") {
        return "Service account used by a Kubernetes workload."
    }
    if ($Kind -eq "ClusterRole" -or $Kind -eq "ClusterRoleBinding" -or $Kind -eq "Role" -or $Kind -eq "RoleBinding") {
        return "RBAC resource supporting a component."
    }

    return "Kubernetes resource in the documented stack."
}

function Escape-Markdown {
    param([string]$Value)
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return ""
    }
    return $Value.Replace("|", "\|")
}

if (-not $OutputPath) {
    $OutputPath = Join-Path $RepoRoot "docs\reference\manifests\index.md"
}

$k8sDir = Join-Path $RepoRoot "k8s"
$outputDir = Split-Path -Parent $OutputPath
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null

$resources = @()
foreach ($file in Get-ChildItem -Path $k8sDir -File -Filter "*.yaml" | Sort-Object Name) {
    $raw = Get-Content -Path $file.FullName -Raw
    $docs = [regex]::Split($raw, '(?m)^\s*---\s*$')
    foreach ($doc in $docs) {
        if ([string]::IsNullOrWhiteSpace($doc)) {
            continue
        }
        $meta = Get-YamlDocumentMetadata -Text $doc
        if (-not $meta.Kind -or -not $meta.Name) {
            continue
        }
        $resources += [pscustomobject]@{
            FileName = $file.Name
            Kind = $meta.Kind
            Name = $meta.Name
            Namespace = if ($meta.Namespace) { $meta.Namespace } else { "(cluster-scoped or default)" }
            Purpose = Get-Purpose -FileName $file.Name -Kind $meta.Kind -Name $meta.Name
        }
    }
}

$generatedAt = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss K")
$lineCount = $resources.Count

$content = New-Object System.Collections.Generic.List[string]
$content.Add("# Manifest Inventory")
$content.Add("")
$content.Add("> Generated by [generate-manifest-inventory.ps1](../../../tools/generate-manifest-inventory.ps1). Do not edit this page manually.")
$content.Add("")
$content.Add("This page inventories the Kubernetes resources defined under [k8s](/C:/Users/el_de/Documents/New%20project/CMXsafeMAC-IPv6/k8s).")
$content.Add("")
$content.Add("- Generated at: ``$generatedAt``")
$content.Add("- Resource count: ``$lineCount``")
$content.Add("")
$content.Add("## By Namespace")
$content.Add("")

$byNamespace = $resources | Group-Object Namespace | Sort-Object Name
foreach ($group in $byNamespace) {
    $content.Add("### $($group.Name)")
    $content.Add("")
    $content.Add("| Kind | Name | File | Purpose |")
    $content.Add("| --- | --- | --- | --- |")
    foreach ($item in ($group.Group | Sort-Object Kind, Name)) {
        $content.Add("| $(Escape-Markdown $item.Kind) | $(Escape-Markdown $item.Name) | $(Escape-Markdown $item.FileName) | $(Escape-Markdown $item.Purpose) |")
    }
    $content.Add("")
}

$content.Add("## By Manifest File")
$content.Add("")

$byFile = $resources | Group-Object FileName | Sort-Object Name
foreach ($group in $byFile) {
    $content.Add("### $($group.Name)")
    $content.Add("")
    $content.Add("| Kind | Name | Namespace | Purpose |")
    $content.Add("| --- | --- | --- | --- |")
    foreach ($item in ($group.Group | Sort-Object Kind, Name)) {
        $content.Add("| $(Escape-Markdown $item.Kind) | $(Escape-Markdown $item.Name) | $(Escape-Markdown $item.Namespace) | $(Escape-Markdown $item.Purpose) |")
    }
    $content.Add("")
}

$content -join "`r`n" | Set-Content -Path $OutputPath
Write-Host "Generated manifest inventory at $OutputPath"
