FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fraud_detector ./fraud_detector
COPY pipeline ./pipeline
COPY producer ./producer
COPY data_loader ./data_loader
CMD ["python", "-m", "fraud_detector.app.app"]
