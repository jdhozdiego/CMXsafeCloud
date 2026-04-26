# Tests And Proofs

This section contains documentation for repeatable validation harnesses and proof scenarios.

The main architecture documents explain the system design. The pages here explain how specific tests are assembled, what assumptions they make, how to run them, and what their results prove.

## Available Testbed Documents

- [portable-openssh-iot-fanout-testbed.md](./portable-openssh-iot-fanout-testbed.md)
  Larger Kubernetes proof with 10 IoT device endpoints, 1 IoT platform endpoint, and 2 replicated Portable OpenSSH gateways.

## Script Locations

The matching scripts live under [tools/tests](../../tools/tests):

- [tools/tests/core](../../tools/tests/core)
- [tools/tests/benchmarks](../../tools/tests/benchmarks)
- [tools/tests/openssh](../../tools/tests/openssh)
