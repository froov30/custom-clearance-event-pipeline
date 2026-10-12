# Skeleton (P0). Completed and built in P10. Base tag verified in DECISIONS.md D-033.
FROM python:3.11.17-slim-trixie AS builder
WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.11.17-slim-trixie
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HEARTBEAT_DIR=/tmp
RUN useradd --create-home --uid 10001 ccp
COPY --from=builder /wheels /wheels
RUN pip install --no-cache-dir /wheels/* && rm -rf /wheels
WORKDIR /app
COPY data ./data
USER ccp
# Exec form, so SIGTERM reaches Python as PID 1 (A11). Each service overrides the module.
CMD ["python", "-m", "ccp.api"]
