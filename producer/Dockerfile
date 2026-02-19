FROM python:3.12-slim

# Security: run as non-root user
RUN groupadd --gid 1001 appgroup && \
    useradd  --uid 1001 --gid appgroup --no-create-home appuser

WORKDIR /app

# Install dependencies as root, then drop privileges
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

USER appuser

CMD ["python", "-u", "producer.py"]
