# syntax=docker/dockerfile:1.6
#
# uball-cv-merge — CPU container that runs after both side-A and side-B
# fusion jobs finish. Downloads both detection_results.json, merges, attributes
# team per shot, appends cv_shot entries to Firebase logs, and drives the
# existing plays_sync pipeline to create UBall plays.
#
# Pure Python — no GPU needed.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# ca-certificates needed for boto3 + requests TLS
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps first for layer caching
COPY deploy/cv-merge/requirements.lock /app/deploy/cv-merge/requirements.lock
RUN pip3 install --upgrade pip \
 && pip3 install -r /app/deploy/cv-merge/requirements.lock

# Copy the minimal set of source modules the merge container needs.
# Using explicit paths here (not `COPY . .`) so image contents are predictable.
COPY cv_merge /app/cv_merge
COPY firebase_service.py /app/firebase_service.py
COPY uball_client.py /app/uball_client.py
COPY plays_sync.py /app/plays_sync.py
COPY logging_service.py /app/logging_service.py
COPY cv_metrics.py /app/cv_metrics.py
COPY deploy/cv-merge/entrypoint.py /app/deploy/cv-merge/entrypoint.py

# Firebase Admin SDK credentials are mounted at runtime via Secrets Manager /
# Batch containerOverrides. The code reads FIREBASE_CREDENTIALS_PATH.

ENTRYPOINT ["python3", "/app/deploy/cv-merge/entrypoint.py"]
