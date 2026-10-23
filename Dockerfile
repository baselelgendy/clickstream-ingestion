# One image for every step of the pipeline (prepare, topics, consumer, replay, dimensions).
# Python 3.11 on purpose: kafka-python works there without any version tricks.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Code only. Settings come from .env (env_file in docker-compose.yml) and the data
# folders are mounted at run time, so nothing secret or huge is baked into the image.
COPY config ./config
COPY src ./src
COPY generator ./generator
