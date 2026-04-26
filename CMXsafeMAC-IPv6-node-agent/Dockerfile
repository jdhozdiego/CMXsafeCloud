FROM python:3.12-slim

ARG CRICTL_VERSION=v1.34.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl iproute2 util-linux \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir grpcio==1.71.0 grpcio-tools==1.71.0 protobuf==5.29.3 pyroute2

RUN curl -fsSL -o /tmp/crictl.tgz "https://github.com/kubernetes-sigs/cri-tools/releases/download/${CRICTL_VERSION}/crictl-${CRICTL_VERSION}-linux-amd64.tar.gz" \
    && tar -C /usr/local/bin -xzf /tmp/crictl.tgz \
    && rm /tmp/crictl.tgz

WORKDIR /app

COPY proto /app/proto
RUN python -m grpc_tools.protoc \
      -I/app/proto \
      -I/usr/local/lib/python3.12/site-packages/grpc_tools/_proto \
      --python_out=/app \
      --grpc_python_out=/app \
      /app/proto/tetragon/bpf.proto \
      /app/proto/tetragon/capabilities.proto \
      /app/proto/tetragon/events.proto \
      /app/proto/tetragon/sensors.proto \
      /app/proto/tetragon/stack.proto \
      /app/proto/tetragon/tetragon.proto \
    && python -c "from pathlib import Path; Path('/app/tetragon/__init__.py').touch()"

COPY agent.py /app/agent.py

CMD ["python", "/app/agent.py"]
