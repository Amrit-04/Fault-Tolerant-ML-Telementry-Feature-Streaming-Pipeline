FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for confluent-kafka
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

COPY shared/ /app/shared/
COPY scripts/ /app/scripts/
COPY producer/ /app/producer/

ENTRYPOINT ["python", "-m", "producer.producer"]
