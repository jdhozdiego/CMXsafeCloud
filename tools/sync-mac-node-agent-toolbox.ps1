param(
    [string]$Namespace = "mac-allocator",
    [string]$LabelSelector = "app=cmxsafemac-ipv6-node-agent-toolbox"
)

$pod = kubectl get pods -n $Namespace -l $LabelSelector -o jsonpath="{.items[0].metadata.name}"
if (-not $pod) {
    throw "No toolbox pod found in namespace '$Namespace' with selector '$LabelSelector'."
}

kubectl cp "CMXsafeMAC-IPv6-node-agent/agent.py" "${Namespace}/${pod}:/app/agent.py"
kubectl cp "CMXsafeMAC-IPv6-node-agent/debug_tetragon.py" "${Namespace}/${pod}:/app/debug_tetragon.py"

Write-Host "Synced files to $Namespace/$pod"
