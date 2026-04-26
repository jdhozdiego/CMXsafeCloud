# CMXsafeMAC-IPv6 Docs Site

This site keeps two documentation layers together:

- **human-first system docs**
  the architecture, identity model, SSH rendezvous design, and the end-to-end flow explanations
- **generated reference docs**
  Python API reference for the main components and a generated inventory of Kubernetes resources

If you are new to the project, start here:

1. [System Overview](./system-overview.md)
2. [Architecture](./CMXsafeMAC-IPv6-architecture.md)
3. [External Endpoint Rollout](./external-endpoint-rollout.md)
4. [Explicit IPv6 Apply And Move Pipeline](./explicit-ipv6-apply-move-pipeline.md)
5. [Portable OpenSSH Canonical Routing](./portable-openssh-canonical-routing.md)
6. [Secure Path Observer Model](./secure-path-observer-model.md)

## What This Site Adds

Compared with the repository Markdown alone, this site adds:

- one browsable navigation tree across all major docs
- generated Python reference pages using `mkdocstrings`
- a generated Kubernetes manifest inventory
- a single place where architecture, mechanics, and code reference stay linked together

## Documentation Layers

### Architecture and flows

These pages explain the system as a whole:

- [System Overview](./system-overview.md)
- [CMXsafeMAC-IPv6 Architecture](./CMXsafeMAC-IPv6-architecture.md)
- [External Endpoint Rollout](./external-endpoint-rollout.md)
- [Explicit IPv6 Apply And Move Pipeline](./explicit-ipv6-apply-move-pipeline.md)
- [Explicit IPv6 Create And Move Parallelism](./explicit-ipv6-parallelism.md)
- [Portable OpenSSH Canonical Routing](./portable-openssh-canonical-routing.md)
- [Secure Path Observer Model](./secure-path-observer-model.md)

### Generated reference

These pages explain the code and manifest surface:

- [Python API Reference](./reference/python/index.md)
- [Kubernetes Manifest Inventory](./reference/manifests/index.md)

## Build Notes

The repo includes Docker-friendly helpers so this site can be built even when Python is not installed locally:

- [build-docs-site.ps1](../tools/build-docs-site.ps1)
- [serve-docs-site.ps1](../tools/serve-docs-site.ps1)

The Python API reference is generated from mirrored source modules under [docs_api](/C:/Users/el_de/Documents/New%20project/CMXsafeMAC-IPv6/docs_api), and the Kubernetes manifest inventory is generated from [k8s](/C:/Users/el_de/Documents/New%20project/CMXsafeMAC-IPv6/k8s) by [generate-manifest-inventory.ps1](../tools/generate-manifest-inventory.ps1).
