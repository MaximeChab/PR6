# PR6

## HDFS

### Lancer HDFS

Executer dans un cmd la commande : `su hdp` pour se connecter sur le compte hadoop

Puis faire `start-all.sh` pour lancer le HDFS

### Probleme de perm HDFS

Si quand le cron installé on a l'erreur de permission, il faut changer celle-ci dans le HDFS, pour cela il faut se connecter avec hdp: `su hdp`

Puis changer les perms du hdfs : `hdfs dfs -chown ubuntu /user`, faire pareil avec les autres dossier si necessaire. Si necessaire, faire pareil avec les droits `hdfs dfs -chown 777 /user`.

### Probleme au demarage

C'est possible qu'il y ai un probleme au redemarage de SDFS, c'est probablement un pb de safety, il faut regarder si c'est le cas avec : `hdfs dfsadmin -safemode get`

Si c'est écrit ON, alors il faut désactivé (cela formatte toutes les données dans le hdfs) : `hdfs namenode -format`

--------------------------

## Stockage dans HDFS
 
df.write.mode("overwrite").parquet("hdfs://localhost:9000/user/ubuntu/data/nom_du_fichier.parquet")

Recuperation depuis HDFS

df = spark.read.csv("hdfs://localhost:9000/user/ubuntu/data/chemin_du_fichier.csv")

--------------------------

## Mettre en place le cron

executer dans un cmd la commande : `crontab -e`

puis, en bas du fichier, ajouter la ligne (en partant du principe que le projet est dans le repertoire Desktop):

```* * * * * /usr/bin/python3 /home/ubuntu/Desktop/PR6/main.py >> /home/ubuntu/Desktop/PR6/log.txt 2>&1```

Pour quitter le fichier faire `ctrl + x`, normalement le cron est effectif toutes les 1 minute (changer le premier `*` par `*/5` pour que ce soit toute les 5 minutes)

--------------------------

## Commandes Pyspark 

df = spark.read.parquet("hdfs://localhost:9000/user/mon_dossier/disruptions_spark.parquet")
df.printSchema()
df.show(5)
