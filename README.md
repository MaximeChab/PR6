# PR6

## Mettre en place le cron

executer dans un cmd la commande : `crontab -e`

puis, en bas du fichier, ajouter la ligne (en partant du principe que le projet est dans le repertoire Desktop):

```* * * * * /usr/bin/python3 /home/ubuntu/Desktop/PR6/main.py >> /home/ubuntu/Desktop/PR6/log.txt 2>&1```

Pour quitter le fichier faire `ctrl + x`, normalement le cron est effectif toutes les 1 minute (changer le premier `*` par `*/5` pour que ce soit toute les 5 minutes)
