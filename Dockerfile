
FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc \
  && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY key.json /app/key.json
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/key.json"

ENV INSTANCE_CONNECTION_NAME="cs493-project2-tayljose:us-central1:business-review-instance"
ENV DB_NAME="business-reviews-db"
ENV DB_USER="appuser"
ENV DB_PASS="Thrawn'srevenge3"

ENV PORT=8080
EXPOSE 8080

COPY . .

CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 8 --timeout 0 main:app
