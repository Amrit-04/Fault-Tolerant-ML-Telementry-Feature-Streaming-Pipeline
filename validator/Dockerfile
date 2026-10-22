FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

COPY validator/requirements.txt /app/validator/requirements.txt
RUN pip install --no-cache-dir -r validator/requirements.txt

COPY shared/ /app/shared/
COPY scripts/ /app/scripts/
COPY schemas/ /app/schemas/
COPY validator/ /app/validator/

ENTRYPOINT ["python", "-m", "validator.validator"]
