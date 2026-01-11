FROM python:3.9-slim

WORKDIR /app

# Ajout des dépendances système nécessaires à la compilation (gcc et libkrb5-dev)
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    curl \
    gcc \
    python3-dev \
    libkrb5-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Mise à jour de pip pour éviter certains conflits de wheel
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY hdfs_writer.py .
COPY hdfs_utils.py .

CMD ["python", "hdfs_writer.py"]