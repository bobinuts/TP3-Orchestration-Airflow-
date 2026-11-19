# Image Airflow officielle, version FIGÉE (jamais "latest" : le comportement du scheduler
# change entre versions mineures, et votre corrigé ne serait plus reproductible).
# 2.10.5 : dernière 2.10, Python 3.12 (même version que les images producer et consumer).
FROM apache/airflow:2.10.5-python3.12

# L'image officielle tourne déjà en utilisateur non-root (airflow, uid 50000).
# pip s'exécute donc sans privilège : c'est voulu, ne repassez pas root.
# --no-cache-dir : on n'embarque pas le cache pip dans la couche.
# confluent-kafka==2.6.1 : exactement la version des séances 1 et 2 (producer, consumer) ;
# il sert à producer.py lancé par produire_fenetre, et à l'AdminClient / au Consumer des tâches.
RUN pip install --no-cache-dir \
      confluent-kafka==2.6.1
