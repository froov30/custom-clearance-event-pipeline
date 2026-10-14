# One image for every service (D-008); each service sets its own command. Base tag verified in D-033.
FROM python:3.11.17-slim-trixie AS builder
WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
# Wheels for the app and every dependency; confluent-kafka ships a manylinux wheel, so no compiler is needed.
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.11.17-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HEARTBEAT_DIR=/tmp \
    API_HOST=0.0.0.0 \
    REF_DATA_DIR=/app/data/ref
RUN useradd --create-home --uid 10001 ccp
COPY --from=builder /wheels /wheels
# --no-index: the runtime stage installs only what the builder stage produced.
RUN pip install --no-cache-dir --no-index /wheels/*.whl && rm -rf /wheels
WORKDIR /app
COPY data ./data
USER ccp
# Exec form, so SIGTERM reaches Python as PID 1, where the services' handlers run (A11).
CMD ["python", "-m", "ccp.api"]
