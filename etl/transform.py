from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct, count, month, explode, unix_timestamp, lit, to_timestamp
from pyspark.sql.window import Window

# Initialiser Spark
spark = SparkSession.builder \
    .appName("JSON to HDFS") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

def save_dataframe_spark(df, hdfs_path, file_name):
    """Sauvegarde un DataFrame Spark sur HDFS en CSV et Parquet"""

    # Convertir les colonnes complexes en JSON string
    for column in df.columns:
        if df.schema[column].dataType.simpleString().startswith("array") or df.schema[column].dataType.simpleString().startswith("map"):
            df = df.withColumn(column, to_json(col(column)))

    # Sauvegarde en CSV
    df.write.csv(f"{hdfs_path}{file_name}.csv", header=True, mode="overwrite")

    # Sauvegarde en Parquet (qui supporte les structures complexes)
    df.write.parquet(f"{hdfs_path}{file_name}.parquet", mode="overwrite")

    print(f"✅ Data saved to HDFS: {hdfs_path}{file_name}")

def process_and_save_spark(data, hdfs_path="hdfs://localhost:9000/user/ubuntu/data"):
    """Transforme le JSON en DataFrame Spark et effectue des analyses"""

    if not isinstance(data, dict):
        raise ValueError("❌ L'entrée doit être un dictionnaire JSON déjà chargé en mémoire.")

    disruptions = data.get("disruptions", [])
    lines = data.get("lines", [])

    # ✅ Traitement des perturbations (df_disruptions)
    if disruptions:
        df_disruptions = spark.createDataFrame(disruptions)

        # ✅ Convertir lastUpdate en Timestamp
        if "lastUpdate" in df_disruptions.columns:
            df_disruptions = df_disruptions.withColumn(
                "lastUpdate", to_timestamp(df_disruptions["lastUpdate"], "yyyyMMdd'T'HHmmss")
            )

        print("📌 Colonnes disponibles dans df_disruptions :")
        df_disruptions.printSchema()

        # ✅ Analyse 1 : Nombre de perturbations par cause
        df_cause_count = df_disruptions.groupBy("cause").agg(count("*").alias("nombre_perturbations"))
        print("📊 Nombre de perturbations par cause :")
        df_cause_count.show()
        save_dataframe_spark(df_cause_count, hdfs_path, "cause_count")

        # ✅ Analyse 2 : Nombre de perturbations par mois
        df_monthly = df_disruptions.withColumn("mois", month("lastUpdate")) \
                                   .groupBy("mois") \
                                   .agg(count("*").alias("nombre_perturbations")).orderBy(col("mois").asc())

        print("📊 Nombre de perturbations par mois :")
        df_monthly.show()
        save_dataframe_spark(df_monthly, hdfs_path, "monthly_disruptions")

        # ✅ Analyse 3 : Calcul de la durée des perturbations
        if "applicationPeriods" in df_disruptions.columns:
            df_durations = df_disruptions.withColumn(
                "start_time", unix_timestamp(col("applicationPeriods")[0]["begin"], "yyyyMMdd'T'HHmmss")
            ).withColumn(
                "end_time", unix_timestamp(col("applicationPeriods")[0]["end"], "yyyyMMdd'T'HHmmss")
            ).withColumn(
                "duration_hours", (col("end_time") - col("start_time")) / 3600
            )

            print("📊 Durée des perturbations :")
            df_durations.select("id", "cause", "duration_hours").show()
            save_dataframe_spark(df_durations.select("id", "cause", "duration_hours"), hdfs_path, "disruptions_durations")

            # ✅ Analyse 4 : Identifier les perturbations de longue durée (> 24h)
            df_long_disruptions = df_durations.filter(col("duration_hours") > 24)
            print("📊 Perturbations de longue durée (> 24h) :")
            df_long_disruptions.show()
            save_dataframe_spark(df_long_disruptions, hdfs_path, "long_disruptions")

        save_dataframe_spark(df_disruptions, hdfs_path, "disruptions_spark")

    # ✅ Traitement des lignes de transport (df_lines)
    if lines:
        df_lines = spark.createDataFrame(lines)
        print("📊 Données des lignes :")
        df_lines.show()

        # ✅ Analyse 5 : Perturbations les plus fréquentes par ligne (en utilisant `impactedObjects`)
        if "impactedObjects" in df_lines.columns:
            df_lines_exploded = df_lines.withColumn("impacted_line", explode("impactedObjects")) \
                                        .filter(col("impacted_line")["type"] == "line") \
                                        .withColumn("line_id", col("impacted_line")["id"]) \
                                        .withColumn("line_name", col("impacted_line")["name"]) \
                                        .groupBy("line_id", "line_name") \
                                        .agg(count("*").alias("nombre_perturbations")) \
                                        .orderBy(col("nombre_perturbations").asc())

            print("📊 Perturbations les plus fréquentes par ligne :")
            df_lines_exploded.show()
            save_dataframe_spark(df_lines_exploded, hdfs_path, "lines_disruptions")

