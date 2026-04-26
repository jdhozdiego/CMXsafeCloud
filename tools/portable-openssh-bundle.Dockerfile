FROM alpine:3.21

ARG OPENSSH_VERSION=10.2p1
ARG APPLY_CMXSAFE_PATCH=0

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

RUN if [ "$APPLY_CMXSAFE_PATCH" = "1" ]; then \
      if [ "$OPENSSH_VERSION" != "10.2p1" ]; then \
        echo "CMXsafe OpenSSH patch is currently forward-ported only for 10.2p1" >&2; \
        exit 1; \
      fi; \
      patch -p1 < /build/openssh-cmxsafe.patch; \
    fi

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

RUN file /out/opt/openssh/sbin/sshd \
 && ldd /out/opt/openssh/sbin/sshd || true
