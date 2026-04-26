# Reference

This section is the generated and semi-generated reference layer for the project.

Use it when you already understand the high-level architecture and want to answer questions like:

- which Python module owns a particular function or queue loop?
- what does the allocator expose at the code level?
- which Kubernetes resources exist in each manifest?
- where does a Deployment, Service, PVC, or ConfigMap come from?

## The Two Reference Views

### Python API reference

The Python reference pages are built with `mkdocstrings` from mirrored source modules in [docs_api](/C:/Users/el_de/Documents/New%20project/CMXsafeMAC-IPv6/docs_api).

Those mirrors exist so the docs site can document the real component code under stable module names such as:

- `docs_api.allocator_app`
- `docs_api.node_agent`
- `docs_api.ssh_dashboard_app`
- `docs_api.traffic_collector`

Start here:

- [Python API Overview](./python/index.md)

### Kubernetes manifest inventory

The manifest inventory is a generated Markdown view of the YAML resources in [k8s](/C:/Users/el_de/Documents/New%20project/CMXsafeMAC-IPv6/k8s).

It is useful when you want a quick answer to:

- what resource kinds are defined?
- in which namespace?
- in which file?
- what purpose does each resource serve?

Start here:

- [Manifest Inventory](./manifests/index.md)

## How To Use This Section With The Main Docs

The recommended pairing is:

- [System Overview](../system-overview.md) for the whole map
- [Explicit IPv6 Apply And Move Pipeline](../explicit-ipv6-apply-move-pipeline.md) for execution mechanics
- this reference section for code and manifest lookup
