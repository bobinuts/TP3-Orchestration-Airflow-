"""DAG d'orchestration HealthStream : rapport d'ingestion horaire (séance 3).

Chaîne pilotée (rien n'est réécrit, Airflow déclenche, surveille, contrôle et rend compte) :
    producer.py -> topic mimic-vitals -> consommateur vitals-persister -> PostgreSQL vitals_raw

Huit tâches strictement linéaires :
    creer_table_rapport >> verifier_broker >> relever_offsets_avant >> produire_fenetre
        >> controler_lag >> verifier_integrite >> ecrire_rapport >> fin_pipeline

Règle de la séance : toutes les tâches travaillent sur l'INTERVALLE DE DONNÉES du run
(data_interval_start / data_interval_end), jamais sur l'heure courante. C'est ce qui rend un
run rejouable : relancé demain, il traite la même fenêtre.

Ce fichier est du code de DESCRIPTION, ré-exécuté par le scheduler toutes les 30 s : les imports
lourds (confluent_kafka) se font à l'intérieur des fonctions de tâche, et aucun appel réseau
n'a lieu au niveau du module.
"""
import json
import logging
import time
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# --- Constantes de module ---------------------------------------------------------------------
CONN_PG = "healthstream_pg"                      # AIRFLOW_CONN_HEALTHSTREAM_PG (Compose)
DOSSIER_RAPPORTS = Path("/opt/airflow/data/rapports")
DOSSIER_ALERTES = Path("/opt/airflow/data/alertes")
DOSSIER_LOGS = "/opt/airflow/logs"

# Paramètres de controler_lag : boucle toutes les 10 s, attente maximale de 120 s.
LAG_PAS_S = 10
LAG_ATTENTE_MAX_S = 120

# La fenêtre (dag_id, window_start) porte la clé primaire : c'est elle qui rend l'upsert de
# ecrire_rapport, donc la tâche, rejouable. SQL volontairement sans accents (aucune dépendance
# à l'encodage du client).
SQL_CREER_TABLE_RAPPORT = """
CREATE TABLE IF NOT EXISTS ingestion_report (
    dag_id         TEXT          NOT NULL,
    window_start   TIMESTAMPTZ   NOT NULL,
    window_end     TIMESTAMPTZ   NOT NULL,
    rows_ingested  BIGINT        NOT NULL,
    patients       INTEGER       NOT NULL,
    null_values    INTEGER       NOT NULL,
    unknown_types  INTEGER       NOT NULL,
    lag_before     BIGINT        NOT NULL,
    lag_after      BIGINT        NOT NULL,
    run_count      INTEGER       NOT NULL DEFAULT 1,
    updated_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, window_start)
);
"""


# =================================================================================================
# Outil partagé par relever_offsets_avant et controler_lag
# =================================================================================================
def _lag_du_groupe(bootstrap, topic, groupe):
    """Lag total d'un consumer group sur un topic, et son détail par partition.

    Le Consumer porte le group.id du consommateur de la séance 2 mais NE S'ABONNE PAS :
    committed() envoie une requête OffsetFetch sans rejoindre le groupe. Un subscribe()
    déclencherait un rééquilibrage et perturberait le consommateur réel.
    """
    from confluent_kafka import Consumer, TopicPartition   # import lourd : dans la fonction

    consommateur = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": groupe,
        "enable.auto.commit": False,
        "client.id": "airflow-lag-probe",
    })
    try:
        meta = consommateur.list_topics(topic, timeout=10)
        if topic not in meta.topics or meta.topics[topic].error is not None:
            raise RuntimeError(f"topic {topic} introuvable sur {bootstrap}")
        partitions = [TopicPartition(topic, p) for p in sorted(meta.topics[topic].partitions)]
        commites = consommateur.committed(partitions, timeout=10)

        total = 0
        detail = {}
        for tp in commites:
            bas, haut = consommateur.get_watermark_offsets(
                TopicPartition(topic, tp.partition), timeout=10, cached=False)
            # Offset négatif = aucun commit pour cette partition : le groupe n'a rien lu, son
            # retard est tout ce que la partition contient (haut - bas).
            commit = tp.offset if tp.offset >= 0 else None
            lag = haut - (commit if commit is not None else bas)
            detail[str(tp.partition)] = {"commit": commit, "fin": haut, "lag": lag}
            total += lag
        return total, detail
    finally:
        consommateur.close()


# =================================================================================================
# Fonctions de tâche
# =================================================================================================
def verifier_broker(**context):
    """Le broker répond et le topic existe. Lecture seule."""
    from confluent_kafka.admin import AdminClient

    bootstrap = Variable.get("kafka_bootstrap")
    topic = Variable.get("vitals_topic")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    meta = admin.list_topics(timeout=10)
    if topic not in meta.topics:
        raise RuntimeError(f"topic {topic} absent du broker {bootstrap}")
    nb_partitions = len(meta.topics[topic].partitions)
    logger.info("broker %s joignable, topic %s : %d partitions", bootstrap, topic, nb_partitions)
    return nb_partitions


def relever_offsets_avant(ti, **context):
    """Note le retard du groupe avant la production. Lecture seule."""
    total, detail = _lag_du_groupe(
        Variable.get("kafka_bootstrap"), Variable.get("vitals_topic"), Variable.get("vitals_group"))
    logger.info("lag_avant=%d detail=%s", total, json.dumps(detail, sort_keys=True))
    ti.xcom_push(key="lag_avant", value=total)


def controler_lag(ti, **context):
    """Attend que le groupe ait rattrapé son retard (lag <= seuil). Lecture seule.

    Mesure toutes les 10 s, attente maximale de 120 s ; au-delà, RuntimeError qui cite le lag,
    le seuil et le détail par partition.
    """
    bootstrap = Variable.get("kafka_bootstrap")
    topic = Variable.get("vitals_topic")
    groupe = Variable.get("vitals_group")
    seuil = int(Variable.get("lag_max"))

    debut = time.monotonic()
    lag_max_observe = 0
    while True:
        total, detail = _lag_du_groupe(bootstrap, topic, groupe)
        ecoule = time.monotonic() - debut
        lag_max_observe = max(lag_max_observe, total)
        logger.info("t=%3.0fs lag=%d seuil=%d detail=%s",
                    ecoule, total, seuil, json.dumps(detail, sort_keys=True))
        if total <= seuil:
            break
        if ecoule >= LAG_ATTENTE_MAX_S:
            raise RuntimeError(
                f"lag {total} toujours au-dessus du seuil {seuil} après {LAG_ATTENTE_MAX_S} s ; "
                f"détail par partition : {json.dumps(detail, sort_keys=True)}")
        time.sleep(LAG_PAS_S)

    logger.info("lag_apres=%d lag_max_observe=%d attente=%.0fs", total, lag_max_observe, ecoule)
    ti.xcom_push(key="lag_apres", value=total)
    ti.xcom_push(key="lag_detail", value=detail)


def verifier_integrite(ti, **context):
    """Contrôle d'intégrité de vitals_raw sur la fenêtre du RUN. Lecture seule.

    Le filtre porte sur ingested_at (ce que la chaîne a ingéré), pas sur event_time (ce que le
    moniteur a mesuré) : la fenêtre du rapport décrit l'ingestion.
    """
    # L'intervalle du RUN, pas l'heure courante : c'est ce qui rend le run rejouable.
    debut = context["data_interval_start"]          # et NON pendulum.now()
    fin = context["data_interval_end"]
    requete = """
        SELECT count(*)                                                      AS lignes,
               count(DISTINCT patient_id)                                    AS patients,
               count(*) FILTER (WHERE value IS NULL)                         AS nulles,
               count(*) FILTER (WHERE vital_type NOT IN ('HR', 'SPO2', 'ABP_SYS'))
                                                                             AS types_inconnus,
               count(*) FILTER (WHERE vital_type = 'SPO2' AND value > 100)   AS spo2_impossibles
        FROM vitals_raw
        WHERE ingested_at >= %s AND ingested_at < %s
    """
    hook = PostgresHook(postgres_conn_id=CONN_PG)
    lignes, patients, nulles, types_inconnus, spo2 = hook.get_first(
        requete, parameters=(debut, fin)
    )
    integrite = {"lignes": lignes, "patients": patients, "nulles": nulles,
                 "types_inconnus": types_inconnus, "spo2_impossibles": spo2}
    logger.info("fenetre [%s ; %s[ integrite=%s", debut, fin, json.dumps(integrite))
    ti.xcom_push(key="integrite", value=integrite)

    if lignes == 0:
        raise ValueError(f"aucune ligne ingérée dans la fenêtre [{debut} ; {fin}[")
    if nulles or types_inconnus or spo2:
        raise ValueError(f"anomalies dans la fenêtre [{debut} ; {fin}[ : {json.dumps(integrite)}")


def ecrire_rapport(ti, **context):
    """Écrit le rapport horaire en base et en fichier JSON. Idempotente (même fenêtre, même état)."""
    debut = context["data_interval_start"]
    fin = context["data_interval_end"]
    lag_avant = ti.xcom_pull(task_ids="relever_offsets_avant", key="lag_avant")
    lag_apres = ti.xcom_pull(task_ids="controler_lag", key="lag_apres")
    integrite = ti.xcom_pull(task_ids="verifier_integrite", key="integrite")

    valeurs = (context["dag"].dag_id, debut, fin, integrite["lignes"], integrite["patients"],
               integrite["nulles"], integrite["types_inconnus"], lag_avant, lag_apres)

    # En base : la clé primaire (dag_id, window_start) absorbe le rejeu.
    hook = PostgresHook(postgres_conn_id=CONN_PG)
    hook.run("""
        INSERT INTO ingestion_report
            (dag_id, window_start, window_end, rows_ingested, patients,
             null_values, unknown_types, lag_before, lag_after)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dag_id, window_start) DO UPDATE SET
            window_end    = EXCLUDED.window_end,
            rows_ingested = EXCLUDED.rows_ingested,
            patients      = EXCLUDED.patients,
            null_values   = EXCLUDED.null_values,
            unknown_types = EXCLUDED.unknown_types,
            lag_before    = EXCLUDED.lag_before,
            lag_after     = EXCLUDED.lag_after,
            run_count     = ingestion_report.run_count + 1,
            updated_at    = now();
    """, parameters=valeurs)

    # Sur disque : nom dérivé du DÉBUT DE FENÊTRE, jamais de l'heure courante.
    # Un rejeu ÉCRASE le fichier, il n'en crée pas un second.
    DOSSIER_RAPPORTS.mkdir(parents=True, exist_ok=True)
    chemin = DOSSIER_RAPPORTS / f"ingestion_{debut.format('YYYYMMDD_HH')}.json"
    rapport = {
        "dag_id": valeurs[0],
        "window_start": str(debut),
        "window_end": str(fin),
        "rows_ingested": integrite["lignes"],
        "patients": integrite["patients"],
        "null_values": integrite["nulles"],
        "unknown_types": integrite["types_inconnus"],
        "lag_before": lag_avant,
        "lag_after": lag_apres,
    }
    chemin.write_text(json.dumps(rapport, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("rapport écrit : base (upsert) et %s", chemin)


# =================================================================================================
# Alertes (partie 4.2) : définies AVANT default_args, qui les référence
# =================================================================================================
def _sans_caracteres_interdits(texte):
    """run_id -> nom de fichier : « : » et « + » sont interdits sur un disque Windows (bind mount)."""
    return str(texte).replace(":", "_").replace("+", "_")


def alerter_echec(context):
    """Échec définitif d'une tâche : le fichier joue le rôle de canal d'alerte (Slack, PagerDuty,
    webhook en production). Répond aux quatre questions : quoi, quand, pourquoi, où regarder."""
    ti = context["task_instance"]
    run_id = context["run_id"]
    charge = {
        "gravite": "CRITIQUE",
        "dag_id": context["dag"].dag_id,                     # quoi
        "task_id": ti.task_id,
        "run_id": run_id,                                    # quand : la fenêtre de données traitée
        "fenetre": str(context["data_interval_start"]),
        "tentative": ti.try_number,                          # quoi, précisément
        "erreur": str(context.get("exception", "inconnue")),   # pourquoi
        # où regarder : le dossier du journal de cette tentative (la forme du nom de dossier
        # suit le gabarit AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE du Compose)
        "journal": f"{DOSSIER_LOGS}/dag_id={context['dag'].dag_id}"
                   f"/run_id={_sans_caracteres_interdits(run_id)}/task_id={ti.task_id}/",
        "horodatage": pendulum.now("Europe/Paris").isoformat(),   # quand : l'heure réelle de l'incident
    }
    DOSSIER_ALERTES.mkdir(parents=True, exist_ok=True)
    fichier = DOSSIER_ALERTES / f"alerte_{_sans_caracteres_interdits(run_id)}_{ti.task_id}.json"
    fichier.write_text(json.dumps(charge, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.error("ALERTE %s", json.dumps(charge, ensure_ascii=False))


def tracer_tentative(context):
    """Appelée avant chaque nouvelle tentative : rend le backoff visible dans les journaux."""
    ti = context["task_instance"]
    logger.warning("NOUVELLE TENTATIVE prévue pour %s (run %s) après l'échec n°%s : %s",
                   ti.task_id, context["run_id"], ti.try_number,
                   str(context.get("exception", "inconnue")))


# Robustesse (partie 4.1) : retries petits, délai croissant, plafond, délai d'attente maximal.
default_args = {
    "owner": "icu-ops",
    "retries": 2,                                   # trois exécutions au total au maximum
    "retry_delay": timedelta(seconds=30),           # base du délai : 30 s, puis 60 s (+ jitter)
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    # Doit rester très supérieur à la tâche la plus lente (controler_lag : 120 s d'attente max).
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": alerter_echec,
    "on_retry_callback": tracer_tentative,
}


with DAG(
    dag_id="healthstream_ingestion",
    default_args=default_args,
    schedule="@hourly",            # une exécution par heure : la période du rapport d'ingestion
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),
    catchup=False,                 # un DAG en pause ne rejoue pas seul les heures manquées
    max_active_runs=1,             # deux producteurs en parallèle fausseraient débit et lag
    tags=["s3", "healthstream"],
) as dag:

    creer_table_rapport = SQLExecuteQueryOperator(
        task_id="creer_table_rapport",
        conn_id=CONN_PG,
        sql=SQL_CREER_TABLE_RAPPORT,      # CREATE TABLE IF NOT EXISTS : rejouable à l'infini
    )

    t_verifier_broker = PythonOperator(task_id="verifier_broker", python_callable=verifier_broker)

    t_relever_offsets_avant = PythonOperator(
        task_id="relever_offsets_avant", python_callable=relever_offsets_avant)

    # Non idempotente par nature : la rejouer ajoute des messages dans Kafka (la clé primaire
    # (kafka_partition, kafka_offset) de vitals_raw garantit seulement qu'un même message n'est
    # jamais écrit deux fois, pas que deux messages différents ne sont pas produits).
    # timeout --preserve-status : sans cette option, timeout renverrait 124 à l'issue de la
    # fenêtre et la tâche serait marquée en échec alors que tout s'est bien passé.
    produire_fenetre = BashOperator(
        task_id="produire_fenetre",
        bash_command=("timeout --preserve-status --signal=TERM {{ var.value.fenetre_duree_s }} "
                      "python /opt/airflow/producer/producer.py"),
        append_env=True,
        env={
            "KAFKA_BOOTSTRAP": "{{ var.value.kafka_bootstrap }}",
            "VITALS_TOPIC": "{{ var.value.vitals_topic }}",
            "NB_PATIENTS": "12",
            "INTERVAL_S": "1.0",   # 12 relevés par tour, un tour par seconde : 12 msg/s nominal
        },
    )

    t_controler_lag = PythonOperator(task_id="controler_lag", python_callable=controler_lag)

    t_verifier_integrite = PythonOperator(
        task_id="verifier_integrite", python_callable=verifier_integrite)

    t_ecrire_rapport = PythonOperator(task_id="ecrire_rapport", python_callable=ecrire_rapport)

    fin_pipeline = EmptyOperator(task_id="fin_pipeline")    # repère de fin de graphe

    (creer_table_rapport >> t_verifier_broker >> t_relever_offsets_avant >> produire_fenetre
     >> t_controler_lag >> t_verifier_integrite >> t_ecrire_rapport >> fin_pipeline)
