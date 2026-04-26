# Test And Proof Scripts

This folder contains repeatable validation, benchmark, and OpenSSH proof harnesses.

The root [tools](../) folder is reserved for operational helpers, build helpers, documentation generators, and developer utilities. Test harnesses live here so they can grow without making the operational script surface harder to scan.

## Layout

- [core](./core)
  End-to-end regression tests for the allocator and managed pod identity engine.

- [benchmarks](./benchmarks)
  Control-plane and explicit IPv6 batching benchmarks.

- [openssh](./openssh)
  Portable OpenSSH, endpoint-helper, canonical-routing, and IoT fan-out proofs.

## Common Entry Points

- [core/test-local-e2e.ps1](./core/test-local-e2e.ps1)
  Main local regression test for managed MAC, managed IPv6, explicit IPv6, connectivity, deletion, and canonical moves.

- [benchmarks/benchmark-control-plane.ps1](./benchmarks/benchmark-control-plane.ps1)
  Control-plane benchmark for allocator writes and explicit IPv6 operations.

- [benchmarks/benchmark-parallel-canonical-batches.ps1](./benchmarks/benchmark-parallel-canonical-batches.ps1)
  Windows/PowerShell benchmark for parallel canonical explicit IPv6 create and move batches.

- [benchmarks/benchmark-parallel-canonical-batches.sh](./benchmarks/benchmark-parallel-canonical-batches.sh)
  Linux/bash version of the parallel canonical benchmark, intended for the in-cluster toolbox.

- [openssh/test-cmxsafe-k8s-iot-fanout.ps1](./openssh/test-cmxsafe-k8s-iot-fanout.ps1)
  Larger Kubernetes proof with 10 IoT device endpoints, 1 IoT platform endpoint, and 2 replicated Portable OpenSSH gateways.
