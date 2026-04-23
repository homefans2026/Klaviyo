FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1
ENV PORT=8080

CMD ["sh", "-c", "python3 klaviyo_order_recommendation_webhook.py --serve --host 0.0.0.0 --port ${PORT} --generic-fallback"]
