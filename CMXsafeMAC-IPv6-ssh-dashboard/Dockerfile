FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY CMXsafeMAC-IPv6-ssh-dashboard/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY CMXsafeMAC-IPv6-ssh-dashboard/app.py .
COPY CMXsafeMAC-IPv6-endpoint-helper ./bundle_assets/endpoint-helper

EXPOSE 8084

CMD ["python", "app.py"]
