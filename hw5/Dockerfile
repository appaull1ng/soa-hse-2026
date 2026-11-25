FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN --mount=type=secret,id=pip_ca \
    if [ -f /run/secrets/pip_ca ]; then export PIP_CERT=/run/secrets/pip_ca; fi \
    && pip install --no-cache-dir -r requirements.txt

COPY movie_event.avsc events.py producer.py main.py test_pipeline.py ./
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
