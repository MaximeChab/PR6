# PR6

## Mettre en place le cron

executer dans un cmd la commande : `crontab -e`

puis, en bas du fichier, ajouter la ligne (en partant du principe que le projet est dans le repertoire Desktop):

```* * * * * /usr/bin/python3 /home/ubuntu/Desktop/PR6/main.py >> /home/ubuntu/Desktop/PR6/log.txt 2>&1```

Pour quitter le fichier faire `ctrl + x`, normalement le cron est effectif toutes les 1 minute (changer le premier `*` par `*/5` pour que ce soit toute les 5 minutes)

--------------------------

## Lancer HDFS

Executer dans un cmd la commande : `su hdp` pour se connecter sur le compte hadoop

Puis faire `start-all.sh` pour lancer le HDFS

--------------------------

## Stockage dans HDFS
 
df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/ubuntu/data/nom_du_fichier.parquet")

Recuperation depuis HDFS

df = spark.read.csv("hdfs://localhost:9000/user/ubuntu/data/chemin_du_fichier.csv")
