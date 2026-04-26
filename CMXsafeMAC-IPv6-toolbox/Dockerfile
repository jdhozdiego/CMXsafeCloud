FROM alpine:3.20

RUN apk add --no-cache \
    bash \
    bind-tools \
    coreutils \
    curl \
    git \
    iproute2 \
    iputils \
    jq \
    kubectl \
    procps \
    py3-pip \
    python3

RUN ln -sf /usr/bin/python3 /usr/local/bin/python

WORKDIR /workspace

CMD ["bash", "-lc", "while true; do sleep 3600; done"]
