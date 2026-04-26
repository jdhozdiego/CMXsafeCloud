param(
    [string]$Namespace = "mac-allocator",
    [string]$Deployment = "cmxsafemac-ipv6-toolbox",
    [string]$Shell = "bash"
)

kubectl exec -it -n $Namespace deployment/$Deployment -- $Shell
