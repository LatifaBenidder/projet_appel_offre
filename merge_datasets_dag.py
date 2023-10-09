from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd 

# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'latifa',
    'start_date': datetime(2023, 1, 1),  # Date de début de votre DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG pour la fusion des datasets
dag = DAG(
    'merge_datasets_dag',  # Nom de votre DAG
    default_args=default_args,
    schedule_interval=None,  # Aucune planification automatique
    catchup=False,
)

# Créer des opérateurs fictifs pour représenter les deux premiers DAGs
run_j360_dag = TriggerDagRunOperator(
    task_id='run_j360_dag',
    trigger_dag_id="j360_dag",  # Remplacez par le nom du premier DAG
    dag=dag,
)

run_globaltenders_dag = TriggerDagRunOperator(
    task_id='run_globaltenders_dag',
    trigger_dag_id="globaltenders_dag",  # Remplacez par le nom du deuxième DAG
    dag=dag,
)

# Créer une fonction pour fusionner les datasets (comme précédemment)
def merge_datasets():
    # Charger les deux datasets précédemment créés
    dataset1 = pd.read_csv("j360_data.csv")
    dataset2 = pd.read_csv("globaltenders.csv")

    # Fusionner les deux datasets en utilisant la fonction concat de pandas
    merged_dataset = pd.concat([dataset1, dataset2], ignore_index=True)

    # Enregistrer le dataset fusionné dans un fichier CSV
    merged_dataset.to_csv("merged_data.csv", index=False)

# Créer une tâche pour exécuter la fusion des datasets
merge_datasets_task = PythonOperator(
    task_id='merge_datasets_task',
    python_callable=merge_datasets,
    dag=dag,
)

# Définir les dépendances
[run_j360_dag, run_globaltenders_dag] >> merge_datasets_task
