FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN echo "wireshark-common wireshark-common/install-setuid boolean false" | debconf-set-selections \
    && apt-get update \
    && apt-get install -y --no-install-recommends tshark ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY collector.py /app/collector.py

CMD ["python", "/app/collector.py"]
