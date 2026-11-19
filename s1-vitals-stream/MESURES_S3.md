# Mesures — séance 3

_Étudiant : PELLICANO Théo — Master 2 YNOV Campus Montpellier — Séance 3 : orchestration avec Apache Airflow (DAG, reprise sur échec, idempotence, rattrapage)_

Toutes les valeurs ci-dessous proviennent d'une exécution réelle le 19/11/2026 sur Docker Desktop (Docker 29.8.2, Compose v5.5.1, image `apache/airflow:2.10.5-python3.12`, `confluent-kafka` 2.6.1 dans l'image Airflow). Les sorties complètes (commandes, résultats et journaux bruts) ont été conservées hors du rendu. Les heures sont données en heure de Paris (UTC+1) sauf mention « UTC ».

## Décisions et adaptations

| Sujet | Choix | Raison |
|---|---|---|
| Exécuteur | `LocalExecutor`, base de métadonnées dans le PostgreSQL de la séance 2 (base `airflow`, créée en UTF-8 par `airflow-db-init`) | pas de Redis ni de Celery ; un seul serveur PostgreSQL pour le poste de TP |
| Plafonds mémoire | scheduler 2 Gio, webserver 1 Gio (`deploy.resources.limits.memory`) | un conteneur qui dérape ne doit pas emporter le moteur Docker |
| Retries de `healthstream_ingestion` | `retries=2`, `retry_delay=30 s`, backoff exponentiel, `max_retry_delay=5 min`, `execution_timeout=10 min` | trois exécutions au maximum par tâche, avec un délai qui grandit |
| Fenêtre de traitement | `data_interval_start` → `data_interval_end` (heure complète précédente), filtre sur `ingested_at` | un run traite la dernière période terminée, pas l'instant du déclenchement |
| Rapport | `INSERT … ON CONFLICT (dag_id, window_start) DO UPDATE`, fichier JSON au nom déterministe `ingestion_AAAAMMJJ_HH.json` | un rejeu met la ligne à jour et réécrit le même fichier |
| Coupe n°1 de la section 1.5 | **appliquée à partir de la partie 3** : `docker compose stop producer` | c'est l'architecture cible de la séance : le DAG lance lui-même le producteur ; sans cela les messages du service Compose se mélangeraient à ceux de `produire_fenetre` |
| Coupes 2 à 5 | **aucune** | le moteur disposait de 7,44 Go, le scheduler n'a jamais été tué (`OOMKilled=false`) |

Adaptations à mon environnement (Windows, séances 1 et 2 conservées) :

- Le broker est `confluentinc/cp-kafka:7.8.0` : les outils s'appellent `kafka-topics`, `kafka-consumer-groups`, `kafka-get-offsets` (sans `/opt/kafka/bin/….sh`).
- Le `run_id` d'Airflow contient `:` et `+`, interdits dans un nom de fichier Windows, et `logs/` est un bind mount vers le disque Windows. Le Compose fixe donc `AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE` (gabarit par défaut, avec `:` et `+` remplacés par `_` dans le nom du dossier). Les alertes suivent la même règle : `alerte_manual__2026-11-19T11_29_42_00_00_verifier_broker.json`.
- Les boucles `while ! … | grep -q …` et `find … | xargs` de l'énoncé supposent bash ; les relevés ont été pilotés par des scripts Python (sonde de la table `dag` toutes les 2 s, lecture directe des fichiers de `logs/`).
- Le topic `mimic-vitals` n'existait plus (volume `kafka-data` neuf) : il a été recréé avec 6 partitions, comme en séance 2.
- Les deux DAG ont été mis de côté au départ et déposés dans `dags/` au moment voulu par le TP (partie 2 puis partie 3), pour que la mesure 3 chronomètre un fichier nouveau.

## Vérification des prérequis

| Grandeur | Valeur relevée sur mon poste |
|---|---|
| Version de Docker Engine | **29.8.2** |
| Mémoire allouée au moteur Docker (Go) | **7,44 Go** (7 985 115 136 octets) |
| Mémoire consommée par la pile de la séance 2 seule (`docker stats --no-stream`) | **548 Mio** (0,54 Go) : Kafka 451 Mio, PostgreSQL 66 Mio, consommateur 22 Mio, producteur 9 Mio |

Contrôle d'entrée : `mimic-vitals` avait bien **6 partitions** ; `vitals_raw` contenait **0 ligne** avant tout producteur ; une minute de pile S2 seule a ingéré 697 lignes pour 12 patients.

## Mesures

### Mesure 1 — coût mémoire d'Airflow

| Grandeur | Valeur relevée |
|---|---|
| Mémoire totale de la pile séance 2 seule | **548 Mio** |
| Mémoire totale de la pile avec Airflow | **1 699 Mio** (1,66 Go) sur 6 conteneurs en marche |
| Conteneur le plus gourmand, et sa consommation | **`vitals-airflow-web` : 762 Mio** (sur un plafond de 1 Gio) |

Surcoût d'Airflow : **1 151 Mio**. Scheduler : 320 Mio sur 2 Gio. Kafka est passé de 451 à 503 Mio.

### Mesure 2 — coût en temps du démarrage

| Grandeur | Valeur relevée |
|---|---|
| Durée de `airflow-init` (migration + création du compte) | **24,9 s** (code de sortie 0) |
| Délai entre `up -d` et webserver healthy | **105,6 s** (horloge de l'hôte) |

En complément : 85,6 s entre le démarrage de `airflow-db-init` et le premier healthy du webserver (horloge Docker seule) ; construction de l'image : 73 s.

### Mesure 3 — délai de détection

| Grandeur | Valeur relevée |
|---|---|
| Délai entre l'enregistrement de `premier_dag_s3.py` et son apparition dans la base | **60,6 s** (58,8 s d'après `dag.last_parsed_time`) |
| Valeur de `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` dans mon Compose | **60 s** |

Le délai mesuré colle à l'intervalle de listage du dossier : le scheduler ne redécouvre les nouveaux fichiers qu'une fois par minute. (La commande `airflow dags list` voit le fichier tout de suite car elle parse elle-même le dossier.)

Le DAG est arrivé en pause. Après `airflow dags unpause`, le scheduler a créé de lui-même un run `scheduled__2026-11-17T23:00:00+00:00`, puis `airflow dags trigger` a créé le run manuel `manual__2026-11-19T11:08:57+00:00` ; les deux ont réussi.

### Mesure 4 — la fenêtre d'ingestion

Run manuel `manual__2026-11-19T11:22:57+00:00`, réussi (8 tâches), fenêtre `[10:00 UTC ; 11:00 UTC[`.

| Grandeur | Valeur relevée |
|---|---|
| `lag_before` (journaux de `relever_offsets_avant`) | **0** |
| Lag maximal observé pendant `controler_lag` | **0** |
| `lag_after` final | **0** |
| `rows_ingested` dans le rapport | **2 365** |
| Écart entre `rows_ingested` et 12 × `FENETRE_DUREE_S` (12 × 60 = 720) | **+1 645** |

Lecture des chiffres, sans les arranger :

- Le rapport porte sur l'**heure complète précédente** (data interval), pas sur les messages produits pendant le run. Les 2 365 lignes sont celles ingérées entre 10:00 et 11:00 UTC par la pile (le service `producer` de la séance 2 n'a tourné qu'une partie de cette heure : la pile n'a démarré qu'à 10:47 UTC, puis a été interrompue), d'où l'écart avec 720.
- Les messages de `produire_fenetre` pour ce run sont bien **720** (720 envoyés, 720 livrés, 0 non livré, 12,0 msg/s) : écart nul avec 12 × 60. Ils sont tombés dans la fenêtre suivante.
- Le lag mesuré par `controler_lag` (après `produire_fenetre`) vaut 0, mais le lag réel pendant l'envoi n'est pas nul : `kafka-consumer-groups --describe` échantillonné toutes les 15 s donne **11, 47, 83, 35, 71** messages. Le consommateur vide le topic entre deux lots, ce qui reste très loin du seuil de 200.
- La partition 2 n'a reçu aucun message (offsets 4 728 / 1 576 / 0 / 4 728 / 4 728 / 3 152 en fin de run).
- Le premier run (créé automatiquement à l'activation du DAG) a produit les mêmes valeurs : `lag_before` 0, lag max 0, `lag_after` 0, succès.

Rapport en base après ce run : `window_start = 2026-11-19 10:00:00+00`, 2 365 lignes, 12 patients, `run_count = 2` (le run automatique et le run manuel portent sur la même fenêtre). Fichier : `data/rapports/ingestion_20261119_10.json`.

### Mesure 5 — comportement à l'échec

Kafka arrêté (`docker compose stop kafka`), puis run déclenché à la main (`manual__2026-11-19T11:29:42+00:00`).

| Observation | Valeur relevée |
|---|---|
| Nombre total d'exécutions de `verifier_broker` avant abandon | **3** (1 exécution + 2 nouvelles tentatives) |
| Délai entre la tentative 1 et la tentative 2 | **30,8 s** (fin → début) ; 41,1 s de début à début |
| Délai entre la tentative 2 et la tentative 3 | **77,5 s** (fin → début) ; 87,8 s de début à début |
| État affiché des tâches situées en aval de la tâche échouée | **`upstream_failed`** (les six : `relever_offsets_avant`, `produire_fenetre`, `controler_lag`, `verifier_integrite`, `ecrire_rapport`, `fin_pipeline`) |

Chaque tentative dure environ 10 s (attente de réponse du broker : `Failed to get metadata: Local: Broker transport failure`). Le run est passé en `failed` au bout de 157 s. L'alerte a été écrite à l'échec définitif : `data/alertes/alerte_manual__2026-11-19T11_29_42_00_00_verifier_broker.json` (gravité CRITIQUE, tentative 3).

Reprise (4.3) : après `docker compose start kafka`, `airflow tasks clear … --task-regex verifier_broker --downstream` a relancé `verifier_broker` (tentative 4) et tout l'aval ; le run est passé en `success` en 74 s.

### Mesure 6 — idempotence

| Grandeur | Valeur relevée |
|---|---|
| Nombre de lignes dans `ingestion_report` pour la fenêtre, après 1 exécution | **1** |
| Nombre de lignes pour la même fenêtre, après 2 rejeux de `ecrire_rapport` | **1** |
| Valeur de `run_count` après les rejeux | **5** (3 avant, 4 après le 1er rejeu, 5 après le 2e) |
| Nombre de fichiers dans `data/rapports/` pour cette heure | **1** (`ingestion_20261119_10.json`) |

`rows_ingested` est resté à 2 365 : un rejeu met la ligne à jour (`run_count + 1`, `updated_at`) sans la dupliquer. Un `INSERT` nu aurait laissé 3 lignes puis 5.

### Mesure 7 — profil de durée d'un run réussi

Run manuel `manual__2026-11-19T11:36:54+00:00`, réussi.

| Tâche | Durée relevée |
|---|---|
| `verifier_broker` | **0,6 s** |
| `produire_fenetre` | **60,8 s** |
| `controler_lag` | **0,4 s** |
| `verifier_integrite` + `ecrire_rapport` | **0,7 s** (0,37 + 0,36) |
| Durée totale du DagRun | **72,2 s** |

Autres tâches : `creer_table_rapport` 0,5 s, `relever_offsets_avant` 0,6 s, `fin_pipeline` 0,0 s. Somme des durées de toutes les tâches : **63,6 s**. Aucun chevauchement (vue Gantt : chaque tâche commence après la fin de la précédente).

### Mesure 8 — rattrapage

Commande : `airflow dags backfill healthstream_ingestion --start-date 2026-11-19T09:00:00+01:00 --end-date 2026-11-19T11:00:00+01:00 --reset-dagruns --yes` (trois dates logiques demandées : 09:00, 10:00 et 11:00 heure de Paris, soit 08:00, 09:00 et 10:00 UTC).

| Grandeur | Valeur relevée |
|---|---|
| Nombre de DagRun créés par le backfill | **2** : `backfill__2026-11-19T09:00:00+01:00` (nouveau) et `scheduled__2026-11-19T10:00:00+00:00` (existant, remis à zéro puis rejoué, devenu `run_type=backfill`) |
| Nombre de lignes ajoutées dans `ingestion_report` | **0** (la table contient toujours 1 ligne ; `run_count` est passé à 7) |
| Nombre de ces runs ayant échoué, et sur quelle tâche | **1 run**, sur **`verifier_integrite`** (3 tentatives, `ValueError: aucune ligne ingérée dans la fenêtre [08:00 UTC ; 09:00 UTC[`) ; `ecrire_rapport` et `fin_pipeline` sont en `upstream_failed` |

Durée du backfill : 219 s, commande terminée en erreur (code 1, `BackfillUnfinished`). Une alerte a été écrite : `alerte_backfill__2026-11-19T09_00_00_01_00_verifier_integrite.json`. La fenêtre 10:00 → 11:00 UTC a réussi avec les mêmes 2 365 lignes qu'avant.

Point à signaler : la troisième fenêtre demandée (09:00 → 10:00 UTC) **n'a pas été créée** : `airflow dags list-runs` n'en montre aucune. La commande s'est arrêtée sur l'échec de la fenêtre 08:00 UTC avant de la traiter. Je n'ai pas vérifié la cause exacte ; l'hypothèse la plus probable est que `max_active_runs=1` a empêché d'ouvrir ce run pendant que les deux autres tournaient. Cette fenêtre était de toute façon vide (la pile n'ingérait pas avant 10:47 UTC) et aurait échoué comme celle de 08:00 UTC.

Le rapport actuel affiche `lag_after = 11` : la dernière écriture est celle du run rejoué par le backfill.

## Contrôle de fin

| Contrôle | Valeur relevée |
|---|---|
| Taille du dossier `logs/` après la séance | **445,8 Kio** |
| Nombre de conteneurs encore en vie (`docker ps -q \| wc -l`) | **0** |

Volumes conservés après `docker compose down` : `s1-vitals-stream_kafka-data`, `s1-vitals-stream_pg-data`, `s1-vitals-stream_vitals-logs`. Taille de `data/` : 1,2 Kio. Contrôle 5.3 : aucune erreur d'import, 4 runs en succès (dont le run scheduled rejoué par le backfill), 1 ligne dans `ingestion_report`, 1 rapport JSON et 2 alertes JSON sur disque.

## Réponses aux questions

### Question 1.1 — base de métadonnées dans le même PostgreSQL

Sur ce poste de TP, cela économise un conteneur et sa mémoire (un seul serveur à démarrer, aux identifiants déjà connus : la base `airflow` pèse 10 Mo à côté des 8 Mo de `healthstream`). En production, le scheduler interroge sa base en permanence (6 connexions ouvertes pour `airflow` contre 2 pour `healthstream` sur mon poste) : cette charge entre en concurrence avec les requêtes sur les données cliniques, et une panne ou une maintenance du serveur arrête à la fois l'orchestrateur et les données. Il faut aussi séparer les droits, les sauvegardes et les montées de version d'un outil d'exploitation de celles de données de santé.

### Question 2.1 — valeur de `data_interval_start`

`data_interval_start` vaut `2026-11-17 23:00:00+00:00`, soit **hier** 18/11 à 00:00 heure de Paris, et non la date d'aujourd'hui (19/11). Le DAG est planifié en `@daily` : chaque run traite une période complète, qui n'est connue qu'une fois terminée, donc un run déclenché aujourd'hui reçoit la dernière période close, de minuit hier à minuit aujourd'hui (`data_interval_end = 2026-11-18 23:00 UTC`). La `logical_date` d'un run manuel vaut en revanche l'instant du déclenchement (2026-11-19 11:08:57 UTC), d'où la différence avec l'intervalle.

### Question 3.1 — `controler_lag` sans `subscribe()`

Si la tâche appelait `subscribe()`, elle deviendrait membre du groupe `vitals-persister` et déclencherait un rééquilibrage : le coordinateur lui attribuerait des partitions que le consommateur de la séance 2 cesserait de lire le temps qu'elle les détient. Le lag mesuré serait alors faussé par la mesure elle-même, puisque ces messages ne seraient plus lus par personne ; sans `subscribe()`, `committed()` et `get_watermark_offsets()` se contentent de lire des offsets, sans entrer dans le groupe.

### Question 3.2 — `ingested_at` plutôt que `event_time`

Un relevé daté 10:59:58 (`event_time`) mais écrit en base à 11:03 parce que le consommateur avait du retard compte dans la fenêtre 10 h avec un filtre sur `event_time`, et dans la fenêtre 11 h avec un filtre sur `ingested_at` ; de même, après une remise à zéro du groupe (`earliest`), des relevés anciens arrivent avec un `ingested_at` récent. Pour un rapport d'ingestion, c'est `ingested_at` qui convient : il mesure ce que la chaîne a réellement écrit pendant la fenêtre, indépendamment de l'âge des relevés. Il évite aussi qu'un retard modifie après coup une fenêtre déjà publiée.

### Question 4.1 — état des tâches aval

Elles sont en **`upstream_failed`** et non en `failed` : elles n'ont jamais démarré, parce qu'une tâche dont elles dépendent a échoué définitivement. Pour qui diagnostique, une tâche `failed` porte la cause (elle s'est exécutée et a levé une erreur, journal à lire), alors qu'une tâche `upstream_failed` n'a rien à dire : on remonte au premier `failed` (ici `verifier_broker`) et on ne cherche pas de bug dans l'aval.

### Question 4.2 — `produire_fenetre` n'est pas idempotente

C'est une propriété acceptable ici, pas un défaut à corriger : le producteur est un simulateur, donc chaque rejeu émet de nouveaux relevés (720 par exécution, 720 livrés sur 720) et non des copies des précédents. La clé primaire `(kafka_partition, kafka_offset)` de `vitals_raw` garantit déjà côté consommateur qu'un même message Kafka, redistribué après un plantage ou un rééquilibrage, n'est jamais inséré deux fois ; les messages rejoués ont de nouveaux offsets, donc de nouvelles lignes légitimes. L'idempotence qui compte est celle de la tâche qui écrit le rapport, et elle est assurée par l'upsert (mesure 6) : rejouer ne change pas la nature du résultat, seulement `run_count`.

### Question 5.1 — somme des tâches et durée du DagRun

La somme des durées de tâches est de 63,6 s et le DagRun dure 72,2 s : l'écart n'est pas nul, il vaut **8,6 s**. Première cause : le temps de passage entre deux tâches (le scheduler constate la fin de la précédente, met la suivante en file et l'exécuteur lance son processus) : 1,2 + 1,6 + 0,8 + 1,4 + 1,4 + 0,4 + 0,5 = **7,3 s** relevées entre les 8 tâches. Deuxième cause : le temps hors tâche au début et à la fin du run (création et mise en file du DagRun, puis clôture de son état une fois la dernière tâche terminée), soit les 1,3 s restants.

### Question 5.2 — rattrapage d'une fenêtre passée

Pour une fenêtre de 10 h rattrapée à 14 h, `rows_ingested` vaut ce qui avait déjà été écrit entre 10 h et 11 h, et rien de plus : les 720 messages de `produire_fenetre` ont un `ingested_at` de 14 h, donc ils tombent dans la fenêtre de 14 h. J'ai observé les deux cas : la fenêtre 10:00 UTC est restée à 2 365 lignes après le backfill, et la fenêtre vide de 08:00 UTC a fait échouer `verifier_integrite` (« aucune ligne ingérée »). Pour qu'un rattrapage ait un sens, le run de rattrapage ne devrait pas produire de nouveaux messages mais relire Kafka à partir de la fenêtre (par exemple avec `offsets_for_times` sur l'heure de début), ou rejouer les relevés avec leurs dates d'origine.

### Question 6.1 — ce qui survit à `docker compose down` sans `-v`

(a) **DAG : oui.** Les fichiers sont dans `dags/` sur le disque de l'hôte (bind mount), pas dans un conteneur ; ils étaient bien présents après l'arrêt (`healthstream_ingestion.py`, `premier_dag_s3.py`). (b) **Historique des DagRun et des tentatives : oui.** Il est dans les tables `dag_run` et `task_instance` de la base `airflow`, donc dans le volume nommé `pg-data` ; après un `down` puis le redémarrage de PostgreSQL seul, j'ai retrouvé 7 `dag_run` et 46 `task_instance`, comme avant. (c) **Lignes de `vitals_raw` : oui.** Elles sont dans la base `healthstream`, donc dans le même volume `pg-data` : 21 792 lignes avant et après. (d) **Compte admin : oui.** Il est dans la table `ab_user` de la base `airflow` (volume `pg-data`), et j'en ai retrouvé 1 après redémarrage ; c'est `down -v` qui supprimerait les trois derniers, pas `down`.
