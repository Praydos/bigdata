FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install python-snappy

COPY consumer.py .
COPY kafka_utils.py .

CMD ["python", "consumer.py"]