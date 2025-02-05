from etl.extract import get_disruptions
from etl.transform import process_and_save_spark
import datetime

HDFS_PATH = "hdfs://localhost:9000/user/mon_dossier/"  # Modifier avec ton chemin HDFS

if __name__ == "__main__":
    print(f"🚀 Début du cron exécuté à {datetime.datetime.now()}")
    data = get_disruptions()
    
    if data:
        process_and_save_spark(data, HDFS_PATH)
        print("✅ Data processing completed successfully!")
    else:
        print("⚠️ Failed to retrieve data.")
    
    print(f"✅ Fin du cron à {datetime.datetime.now()}")
