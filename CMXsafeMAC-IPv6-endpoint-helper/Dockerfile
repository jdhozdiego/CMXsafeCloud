FROM alpine:3.21 AS openssh-builder

ARG OPENSSH_VERSION=10.2p1

RUN apk add --no-cache \
    build-base \
    ca-certificates \
    linux-headers \
    openssl-dev \
    openssl-libs-static \
    patch \
    perl \
    wget \
    zlib-dev \
    zlib-static

WORKDIR /build

COPY tools/openssh-cmxsafe-10.2p1.patch /build/openssh-cmxsafe.patch

RUN wget -O openssh.tar.gz "https://cdn.openbsd.org/pub/OpenBSD/OpenSSH/portable/openssh-${OPENSSH_VERSION}.tar.gz" \
 && tar -xzf openssh.tar.gz \
 && mv "openssh-${OPENSSH_VERSION}" src

WORKDIR /build/src

RUN if [ "$OPENSSH_VERSION" != "10.2p1" ]; then \
      echo "CMXsafe endpoint image currently expects the 10.2p1 patch set" >&2; \
      exit 1; \
    fi \
 && patch -p1 < /build/openssh-cmxsafe.patch

RUN ./configure \
      --prefix=/opt/openssh \
      --sysconfdir=/etc/ssh \
      --without-pam \
      --without-shadow \
      --without-lastlog \
      --without-libedit \
      --with-privsep-path=/var/empty \
      --with-privsep-user=sshd \
      CFLAGS='-Os' LDFLAGS='-static' \
 || { cat config.log; exit 1; }

RUN make -j"$(getconf _NPROCESSORS_ONLN)"
RUN make DESTDIR=/out install

FROM python:3.12-alpine

RUN apk add --no-cache \
    ca-certificates \
    iproute2

WORKDIR /bundle

COPY --from=openssh-builder /out/opt/openssh /opt/openssh
COPY CMXsafeMAC-IPv6-endpoint-helper/endpointd.py /opt/cmxsafe/bin/endpointd.py
COPY CMXsafeMAC-IPv6-endpoint-helper/cmxsafe-ssh /opt/cmxsafe/bin/cmxsafe-ssh
COPY CMXsafeMAC-IPv6-endpoint-helper/bundlectl.py /opt/cmxsafe/bin/bundlectl.py
COPY CMXsafeMAC-IPv6-endpoint-helper/connect-platform /opt/cmxsafe/bin/connect-platform
COPY CMXsafeMAC-IPv6-endpoint-helper/run-forever /opt/cmxsafe/bin/run-forever
COPY CMXsafeMAC-IPv6-endpoint-helper/send-message /opt/cmxsafe/bin/send-message
COPY CMXsafeMAC-IPv6-endpoint-helper/disconnect /opt/cmxsafe/bin/disconnect
COPY CMXsafeMAC-IPv6-endpoint-helper/bundle-entrypoint.sh /opt/cmxsafe/bin/bundle-entrypoint.sh
COPY CMXsafeMAC-IPv6-endpoint-helper/README.md /opt/cmxsafe/share/README.md

RUN chmod 0755 /opt/cmxsafe/bin/* \
 && ln -sf /opt/cmxsafe/bin/connect-platform /usr/local/bin/connect-platform \
 && ln -sf /opt/cmxsafe/bin/run-forever /usr/local/bin/run-forever \
 && ln -sf /opt/cmxsafe/bin/send-message /usr/local/bin/send-message \
 && ln -sf /opt/cmxsafe/bin/disconnect /usr/local/bin/disconnect \
 && ln -sf /opt/cmxsafe/bin/bundlectl.py /usr/local/bin/cmxsafe-bundlectl \
 && ln -sf /opt/cmxsafe/bin/cmxsafe-ssh /usr/local/bin/cmxsafe-ssh

ENV CMXSAFE_BUNDLE_BIN_ROOT=/opt/cmxsafe/bin \
    CMXSAFE_BUNDLE_ROOT=/bundle \
    PATH=/opt/openssh/bin:/opt/cmxsafe/bin:${PATH}

ENTRYPOINT ["/opt/cmxsafe/bin/bundle-entrypoint.sh"]
CMD ["./run-forever"]
