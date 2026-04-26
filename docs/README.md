# Documentation Index

This folder contains the documentation for the OpenSSH forwarding rendezvous system and the identity-management engine underneath it.

## Start Here

- [system-overview.md](./system-overview.md)
  Reader-friendly overview of the whole project, including the full component map, identity chain, the core-vs-add-ons split, and the multi-replica SSH forwarding goal.

- [CMXsafeMAC-IPv6-architecture.md](./CMXsafeMAC-IPv6-architecture.md)
  Overall architecture, interactions, flows, data model, and runtime behavior, with the SSH forwarding problem framed first.

## Core Identity Engine

- [net-identity-allocator.md](./net-identity-allocator.md)
  The `net-identity-allocator` API, PostgreSQL-backed tables, PHP monitor integration, configuration, and API usage.

- [CMXsafeMAC-IPv6-node-agent.md](./CMXsafeMAC-IPv6-node-agent.md)
  The `CMXsafeMAC-IPv6-node-agent`, Tetragon integration, safety reconcile behavior, and network mutation logic.

- [explicit-ipv6-parallelism.md](./explicit-ipv6-parallelism.md)
  Detailed create and move flow documentation for explicit IPv6 requests, including the `5000`-request worked example, metric definitions, bottleneck stages, tuning knobs, and flow diagrams.

- [explicit-ipv6-apply-move-pipeline.md](./explicit-ipv6-apply-move-pipeline.md)
  Detailed mechanics guide for the explicit IPv6 apply and move pipeline, including batch initiation, allocator and node-agent function ownership, per-pod netlink batching, and how session bursts can collapse into grouped execution.

- [kubernetes-manifests.md](./kubernetes-manifests.md)
  The Kubernetes YAML files, including the separate PHP monitor and traffic collector resources.

## Primary OpenSSH Surface

- [busybox-portable-openssh.md](./busybox-portable-openssh.md)
  Checklist, build/test helpers, and sample manifest for testing an official portable OpenSSH server inside a `busybox:1.36` pod with read-only `/etc/ssh` and `/home`.

- [portable-openssh-canonical-routing.md](./portable-openssh-canonical-routing.md)
  Step-by-step multi-replica SSH example showing how canonical IPv6 identities let two SSH users land on different replicas and still forward traffic end to end through a stable canonical rendezvous address.

- [portable-openssh-dashboard.md](./portable-openssh-dashboard.md)
  PostgreSQL-backed dashboard, background reconcile worker, and rendered-file model for the BusyBox Portable OpenSSH sample.

## Tests And Proofs

- [tests/index.md](./tests/index.md)
  Index for repeatable validation harnesses and proof scenarios.

- [tests/portable-openssh-iot-fanout-testbed.md](./tests/portable-openssh-iot-fanout-testbed.md)
  Larger Kubernetes proof with 10 IoT device endpoints, 1 IoT platform endpoint, 2 replicated Portable OpenSSH gateways, `5/5` device-session balancing, platform reverse-session movement, and source IPv6/source-port preservation.

## Add-Ons, Operations, And Samples

- [deployment-and-samples.md](./deployment-and-samples.md)
  How to build images, deploy the services, expose the monitors, and run the sample workloads.

## Docs Site

- published site URL:
  [https://cmxsafe.github.io/CMXsafeMAC-IPv6/](https://cmxsafe.github.io/CMXsafeMAC-IPv6/)

- [../mkdocs.yml](../mkdocs.yml)
  MkDocs Material configuration for the browsable documentation site.

- [../docs/reference/index.md](../docs/reference/index.md)
  Reference-layer entry point for generated Python API pages and the generated Kubernetes manifest inventory.

- [../tools/build-docs-site.ps1](../tools/build-docs-site.ps1)
  Builds the documentation site in Docker, including regenerated Python reference mirrors and the manifest inventory.

- [../tools/serve-docs-site.ps1](../tools/serve-docs-site.ps1)
  Serves the documentation site locally in Docker on port `8000` by default.

- [../.github/workflows/docs-site.yml](../.github/workflows/docs-site.yml)
  GitHub Actions workflow that builds the rendered site from `main` and publishes it to GitHub Pages.

## Automation Helpers

- [../tools/install-docker-desktop-kind-stack.ps1](../tools/install-docker-desktop-kind-stack.ps1)
  Automated local install path for the validated Docker Desktop `kind` single-node environment.

- [../tools/tests/core/test-local-e2e.ps1](../tools/tests/core/test-local-e2e.ps1)
  Repeatable end-to-end regression script for managed MAC, managed IPv6, explicit IPv6, connectivity, deletion, and canonical move behavior.

- [../tools/tests/benchmarks/benchmark-control-plane.ps1](../tools/tests/benchmarks/benchmark-control-plane.ps1)
  Control-plane benchmark for allocator-only writes, explicit IPv6 attachment, and single-canonical-IP move latency.

- [../tools/tests/benchmarks/benchmark-parallel-canonical-batches.ps1](../tools/tests/benchmarks/benchmark-parallel-canonical-batches.ps1)
  Serial default `10/30/60/100` benchmark for parallel canonical explicit IPv6 creation and parallel reassignment across one reused 4-replica Deployment sample, clearing only explicit state between scenarios and reporting both request-latency and batch-drain timing.

- [../tools/tests/benchmarks/benchmark-parallel-canonical-batches.sh](../tools/tests/benchmarks/benchmark-parallel-canonical-batches.sh)
  Linux/bash version of the same benchmark, intended to run from inside the toolbox with direct cluster-DNS access; defaults to `10/30/60/100`, but supports larger custom batch sizes too.

- [../tools/deploy-toolbox.ps1](../tools/deploy-toolbox.ps1)
  Deploys the optional long-lived in-cluster Linux toolbox used for shell access and in-cluster measurements.

- [../tools/connect-toolbox.ps1](../tools/connect-toolbox.ps1)
  Opens an interactive shell in the in-cluster toolbox.

- [../tools/tests/openssh/prepare-portable-openssh-canonical-test.ps1](../tools/tests/openssh/prepare-portable-openssh-canonical-test.ps1)
  Applies the multi-replica Portable OpenSSH sample, prepares two canonical SSH identities, and reconciles the dashboard-rendered account files for the canonical-routing example.

- [../tools/tests/openssh/test-cmxsafe-k8s-iot-fanout.ps1](../tools/tests/openssh/test-cmxsafe-k8s-iot-fanout.ps1)
  Runs the larger Kubernetes fan-out proof with 10 IoT device endpoints, 1 IoT platform endpoint, and 2 replicated Portable OpenSSH gateways; it verifies balanced device sessions, platform reverse-session movement, and source IPv6/source-port preservation.
