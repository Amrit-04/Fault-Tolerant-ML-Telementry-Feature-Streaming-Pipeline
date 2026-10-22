FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY writer/requirements.txt /app/writer/requirements.txt
RUN pip install --no-cache-dir -r writer/requirements.txt

COPY shared/ /app/shared/
COPY scripts/ /app/scripts/
COPY writer/ /app/writer/

ENTRYPOINT ["python", "-m", "writer.writer"]
