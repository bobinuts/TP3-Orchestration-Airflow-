"""Premier DAG de la séance 3 : prise en main (tâches, dépendances, planification).

Trois tâches strictement en file : debut >> horodater >> contexte.
Il ne touche ni à Kafka ni à PostgreSQL : il sert à lire, dans les journaux, ce que
le scheduler passe à une tâche (surtout data_interval_start, qui n'est PAS « maintenant »).

Ce fichier est du code de DESCRIPTION : le scheduler le ré-exécute en entier toutes les 30 s.
Rien de lourd au niveau du module (pas d'appel réseau, pas de requête).
"""
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Hérité par toutes les tâches du DAG, sauf surcharge locale.
default_args = {
    "owner": "icu-ops",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}


def afficher_contexte(**context):
    """Affiche les cinq valeurs qui distinguent un run d'un autre."""
    print(f"logical_date         = {context['logical_date']}")
    print(f"data_interval_start  = {context['data_interval_start']}")
    print(f"data_interval_end    = {context['data_interval_end']}")
    print(f"run_id               = {context['run_id']}")
    print(f"numero de tentative  = {context['task_instance'].try_number}")


with DAG(
    dag_id="premier_dag_s3",
    default_args=default_args,
    schedule="@daily",                                              # une fois par jour
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Paris"),    # fuseau explicite
    catchup=False,   # sans cela, Airflow lancerait un run pour chaque jour depuis start_date
    tags=["s3", "prise-en-main"],
) as dag:

    debut = EmptyOperator(task_id="debut")      # repère de graphe : ne fait rien

    # Les accolades doubles sont des modèles Jinja, rendus à l'exécution (pas au parsing).
    horodater = BashOperator(
        task_id="horodater",
        bash_command='echo "run={{ run_id }} debut_intervalle={{ data_interval_start }}"; date -u',
    )

    contexte = PythonOperator(
        task_id="contexte",
        python_callable=afficher_contexte,
    )

    debut >> horodater >> contexte
