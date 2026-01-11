import os
import sys
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, when, round as spark_round, date_format, hour
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType

# Configuration
HDFS_NAMENODE = os.getenv('HDFS_NAMENODE', "hdfs://namenode:8020")
INPUT_PATH = os.getenv('INPUT_PATH', '/data/raw/traffic')
OUTPUT_PATH = os.getenv('OUTPUT_PATH', '/data/processed/traffic')

# Schéma des données de trafic
TRAFFIC_SCHEMA = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("road_id", StringType(), True),
    StructField("road_type", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lane", StringType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", FloatType(), True),
    StructField("occupancy_rate", FloatType(), True),
    StructField("event_time", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("hour", IntegerType(), True)
])

def create_spark_session():
    """Crée et configure une session Spark"""
    return SparkSession.builder \
        .appName("TrafficDataProcessing") \
        .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

def read_traffic_data(spark, input_path):
    """Lit les données avec schéma explicite pour éviter le scan complet"""
    # On s'assure que le chemin est complet
    full_path = f"{HDFS_NAMENODE}{input_path}/*/*/*/*/*.json"
    print(f"Tentative de lecture sur : {full_path}")
    
    try:
        # Appliquer le schéma TRAFFIC_SCHEMA ici
        df = spark.read \
            .schema(TRAFFIC_SCHEMA) \
            .option("multiLine", "false") \
            .json(full_path)
        
        # Action déclenchante pour vérifier la connectivité immédiatement
        count = df.count()
        print(f"Nombre d'enregistrements lus: {count}")
        return df
    except Exception as e:
        print(f"ERREUR CRITIQUE de connexion HDFS : {str(e)}")
        return None

def calculate_congestion_level(occupancy, speed, road_type):
    """UDF pour calculer le niveau de congestion"""
    if road_type == "autoroute":
        if occupancy > 85 or speed < 40:
            return "CRITICAL"
        elif occupancy > 70 or speed < 60:
            return "HIGH"
        elif occupancy > 50 or speed < 80:
            return "MEDIUM"
        else:
            return "LOW"
    elif road_type == "boulevard":
        if occupancy > 80 or speed < 20:
            return "CRITICAL"
        elif occupancy > 65 or speed < 30:
            return "HIGH"
        elif occupancy > 45 or speed < 40:
            return "MEDIUM"
        else:
            return "LOW"
    else:
        if occupancy > 75 or speed < 10:
            return "CRITICAL"
        elif occupancy > 60 or speed < 15:
            return "HIGH"
        elif occupancy > 40 or speed < 20:
            return "MEDIUM"
        else:
            return "LOW"

def process_traffic_data(df):
    """Traite les données de trafic et calcule les métriques"""
    if df is None or df.count() == 0:
        print("Aucune donnée à traiter")
        return None
    
    # Ajouter des colonnes de timestamp
    from pyspark.sql.functions import to_timestamp, expr
    
    df_processed = df.withColumn(
        "event_timestamp",
        to_timestamp(col("event_time"))
    ).withColumn(
        "date",
        date_format(col("event_timestamp"), "yyyy-MM-dd")
    ).withColumn(
        "hour_of_day",
        hour(col("event_timestamp"))
    )
    
    # Enregistrer l'UDF pour le niveau de congestion
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    congestion_udf = udf(calculate_congestion_level, StringType())
    
    df_processed = df_processed.withColumn(
        "congestion_level",
        congestion_udf(col("occupancy_rate"), col("average_speed"), col("road_type"))
    )
    
    return df_processed

def calculate_zone_metrics(df):
    """Calcule les métriques par zone"""
    print("Calcul des métriques par zone...")
    
    metrics_by_zone = df.groupBy("zone", "date").agg(
        spark_round(avg("vehicle_count"), 2).alias("avg_vehicle_count"),
        spark_round(avg("average_speed"), 2).alias("avg_speed"),
        spark_round(avg("occupancy_rate"), 2).alias("avg_occupancy_rate"),
        count("*").alias("total_measurements"),
        sum("vehicle_count").alias("total_vehicles"),
        spark_round(avg(when(col("congestion_level").isin(["HIGH", "CRITICAL"]), 1).otherwise(0)) * 100, 2).alias("congestion_rate")
    ).withColumn(
        "processing_time",
        expr("current_timestamp()")
    )
    
    return metrics_by_zone

def calculate_road_metrics(df):
    """Calcule les métriques par route"""
    print("Calcul des métriques par route...")
    
    metrics_by_road = df.groupBy("road_id", "road_type", "zone", "date").agg(
        spark_round(avg("average_speed"), 2).alias("avg_speed"),
        spark_round(avg("vehicle_count"), 2).alias("avg_vehicle_count"),
        count("*").alias("total_measurements"),
        sum(when(col("congestion_level").isin(["HIGH", "CRITICAL"]), 1).otherwise(0)).alias("congestion_count")
    ).withColumn(
        "congestion_percentage",
        spark_round((col("congestion_count") / col("total_measurements")) * 100, 2)
    ).withColumn(
        "processing_time",
        expr("current_timestamp()")
    )
    
    return metrics_by_road

def calculate_hourly_metrics(df):
    """Calcule les métriques horaires"""
    print("Calcul des métriques horaires...")
    
    hourly_metrics = df.groupBy("zone", "date", "hour_of_day").agg(
        spark_round(avg("vehicle_count"), 2).alias("avg_vehicle_count"),
        spark_round(avg("average_speed"), 2).alias("avg_speed"),
        spark_round(avg("occupancy_rate"), 2).alias("avg_occupancy_rate"),
        count("*").alias("total_measurements")
    ).withColumn(
        "time_period",
        when(col("hour_of_day").between(7, 9), "Matin (7h-9h)")
        .when(col("hour_of_day").between(12, 14), "Midi (12h-14h)")
        .when(col("hour_of_day").between(17, 19), "Soir (17h-19h)")
        .otherwise("Autre")
    ).withColumn(
        "processing_time",
        expr("current_timestamp()")
    )
    
    return hourly_metrics

def identify_congestion_hotspots(df):
    """Identifie les points de congestion"""
    print("Identification des points de congestion...")
    
    congestion_hotspots = df.filter(
        col("congestion_level").isin(["HIGH", "CRITICAL"])
    ).groupBy("zone", "road_id", "road_type", "date").agg(
        count("*").alias("congestion_events"),
        spark_round(avg("occupancy_rate"), 2).alias("avg_occupancy_during_congestion"),
        spark_round(avg("average_speed"), 2).alias("avg_speed_during_congestion"),
        spark_round(avg("vehicle_count"), 2).alias("avg_vehicles_during_congestion")
    ).withColumn(
        "severity_level",
        when(col("avg_occupancy_during_congestion") > 80, "EXTRÊME")
        .when(col("avg_occupancy_during_congestion") > 70, "ÉLEVÉE")
        .otherwise("MODÉRÉE")
    ).withColumn(
        "processing_time",
        expr("current_timestamp()")
    )
    
    return congestion_hotspots

def write_results_to_hdfs(df_dict, output_path):
    """Écrit les résultats en s'assurant du format Parquet optimal"""
    for name, df in df_dict.items():
        if df is not None:
            # On utilise .coalesce(1) si les datasets sont petits pour éviter 
            # de créer des centaines de micro-fichiers sur HDFS
            target_path = f"{HDFS_NAMENODE}{output_path}/{name}"
            print(f"Écriture de {name} vers {target_path}")
            
            df.write \
                .mode("append") \
                .parquet(target_path)

def main():
    """Fonction principale"""
    print("=" * 70)
    print("DÉMARRAGE DU TRAITEMENT SPARK - DONNÉES DE TRAFIC")
    print(f"Date: {datetime.now().isoformat()}")
    print("=" * 70)
    
    # Créer la session Spark
    spark = create_spark_session()
    
    try:
        # 1. Lire les données
        df_raw = read_traffic_data(spark, INPUT_PATH)
        
        if df_raw is None or df_raw.count() == 0:
            print("Aucune donnée disponible pour le traitement")
            return
        
        # 2. Prétraiter les données
        df_processed = process_traffic_data(df_raw)
        
        # 3. Calculer les différentes métriques
        zone_metrics = calculate_zone_metrics(df_processed)
        road_metrics = calculate_road_metrics(df_processed)
        hourly_metrics = calculate_hourly_metrics(df_processed)
        congestion_hotspots = identify_congestion_hotspots(df_processed)
        
        # 4. Afficher les statistiques
        print("\n" + "=" * 70)
        print("STATISTIQUES DU TRAITEMENT")
        print("=" * 70)
        print(f"Données traitées: {df_processed.count()} événements")
        print(f"Zones analysées: {df_processed.select('zone').distinct().count()}")
        print(f"Routes analysées: {df_processed.select('road_id').distinct().count()}")
        
        # Aperçu des métriques par zone
        print("\nMétriques par zone (aperçu):")
        zone_metrics.show(5, truncate=False)
        
        # Points de congestion critiques
        print("\nPoints de congestion critiques:")
        congestion_hotspots.filter(col("severity_level") == "EXTRÊME").show(5, truncate=False)
        
        # 5. Écrire les résultats
        results = {
            "zone_metrics": zone_metrics,
            "road_metrics": road_metrics,
            "hourly_metrics": hourly_metrics,
            "congestion_hotspots": congestion_hotspots
        }
        
        write_results_to_hdfs(results, OUTPUT_PATH)

        ANALYTICS_PATH = "/data/analytics/traffic"

        zone_metrics.write.mode("overwrite").parquet(
            f"{HDFS_NAMENODE}{ANALYTICS_PATH}/zone_metrics"
        )

        road_metrics.write.mode("overwrite").parquet(
            f"{HDFS_NAMENODE}{ANALYTICS_PATH}/road_metrics"
        )

        hourly_metrics.write.mode("overwrite").parquet(
            f"{HDFS_NAMENODE}{ANALYTICS_PATH}/hourly_metrics"
        )

        congestion_hotspots.write.mode("overwrite").parquet(
            f"{HDFS_NAMENODE}{ANALYTICS_PATH}/congestion_hotspots"
        )

        # 6. Sauvegarder un rapport JSON
        report = {
            "processing_time": datetime.now().isoformat(),
            "input_path": INPUT_PATH,
            "output_path": OUTPUT_PATH,
            "records_processed": df_processed.count(),
            "zones_count": df_processed.select('zone').distinct().count(),
            "roads_count": df_processed.select('road_id').distinct().count(),
            "metrics_generated": len(results)
        }
        
        # Sauvegarder le rapport dans HDFS
        report_df = spark.createDataFrame([report])
        report_df.write \
            .mode("append") \
            .json(f"{HDFS_NAMENODE}{OUTPUT_PATH}/reports")
        
        print("\n" + "=" * 70)
        print("TRAITEMENT TERMINÉ AVEC SUCCÈS")
        print("=" * 70)
        
    except Exception as e:
        print(f"ERREUR lors du traitement: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("Session Spark arrêtée")

if __name__ == "__main__":
    main()