FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN --mount=type=secret,id=pip_ca \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi \
    && pip install --no-cache-dir -r requirements.txt

COPY main.py metrics.py s3_export.py test_aggregation.py ./
COPY sql/ ./sql/
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
