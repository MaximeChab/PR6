from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct
import json

# Initialiser Spark
spark = SparkSession.builder.appName("JSON to HDFS").config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000").getOrCreate()

def save_json(data, filename):
    """Save data as a JSON file locally."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def save_dataframe_spark(df, hdfs_path, file_name):
    """Sauvegarde un DataFrame Spark sur HDFS en CSV et Parquet"""

    # Convertir les colonnes complexes en JSON string (notamment applicationPeriods)
    for column in df.columns:
        if df.schema[column].dataType.simpleString().startswith("array") or df.schema[column].dataType.simpleString().startswith("map"):
            df = df.withColumn(column, to_json(col(column)))

    # Sauvegarde en CSV
    df.write.csv(f"{hdfs_path}{file_name}.csv", header=True, mode="overwrite")

    # Sauvegarde en Parquet (qui supporte les structures complexes)
    df.write.parquet(f"{hdfs_path}{file_name}.parquet", mode="overwrite")

    print(f"✅ Data saved to HDFS: {hdfs_path}{file_name}")

def process_and_save_spark(data, hdfs_path="hdfs://localhost:9000/user/ubuntu/data"):
    """Transforme le JSON en DataFrame Spark et sauvegarde dans HDFS"""
    if not isinstance(data, dict):
        raise ValueError("❌ L'entrée doit être un dictionnaire JSON déjà chargé en mémoire.")

    disruptions = data.get("disruptions", [])
    lines = data.get("lines", [])

    if disruptions:
        df_disruptions = spark.createDataFrame(disruptions)
        save_dataframe_spark(df_disruptions, hdfs_path, "disruptions_spark")

    if lines:
        df_lines = spark.createDataFrame(lines)
        save_dataframe_spark(df_lines, hdfs_path, "lines_spark")
