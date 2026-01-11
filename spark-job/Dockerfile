FROM apache/spark-py:latest

USER root

# Installer les dépendances Python
RUN pip install --no-cache-dir \
    pandas==2.0.3 \
    pyarrow==14.0.0 \
    requests==2.31.0

# Copier les scripts
WORKDIR /app
COPY spark_processing.py .
COPY traffic_analyzer.py .
COPY requirements.txt .

# Installer les dépendances Python supplémentaires
RUN pip install --no-cache-dir -r requirements.txt


# Créer le répertoire de sortie
RUN mkdir -p /tmp/spark_results

# Commande par défaut
CMD ["bash", "-c", "while true; do spark-submit --master ${SPARK_MASTER} --conf spark.sql.warehouse.dir=/tmp/spark-warehouse --conf spark.sql.parquet.compression.codec=snappy --conf spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.driver.memory=1g --conf spark.executor.memory=1g /app/spark_processing.py; sleep ${JOB_INTERVAL:-300}; done"]