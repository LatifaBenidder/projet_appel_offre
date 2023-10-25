import datetime
import airflow
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
    ClusterGenerator
)
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
)
import requests
from bs4 import BeautifulSoup
import pandas as pd

# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    "owner": "Composer Example",
    "depends_on_past": False,
    "email": [""],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
    "start_date": YESTERDAY,
}


def scrapy_globaltenders():
    from datetime import datetime
    result = requests.get("https://www.globaltenders.com/software-tenders")
    var = result.content
    soup = BeautifulSoup(var, 'lxml')
    data=soup.find_all('a', class_='link-dark text-decoration-none stretched-link')
    links = [ ] #Creating a list, in which we will store all our links
    for a_tag in soup.find_all('a', class_='link-dark text-decoration-none stretched-link'):
        links.append(a_tag.attrs['href'])
    description=[]
    location=[]
    Type=[]
    Deadline=[]
    lien=[]
    for link in links:
        result1 = requests.get(link)
        var1 = result1.content
        soup1 = BeautifulSoup(var1, 'lxml')
        for div_ in soup1.find_all('div', class_="row row-cols-4 text-dark text-left"):
            description.append(div_.find('div',itemprop="description").get_text())
            location.append(div_.find('div',itemprop="location").get_text())
            Type.append(div_.find('div',itemprop="name").get_text())
            Deadline.append(div_.find('div',itemprop="endDate").get_text())
            lien.append(div_.find('a',itemprop="url").attrs['href'])

    dic={"title": description,"pays": location,"deadline": Deadline,"lien": lien}
    dataframe=pd.DataFrame(dic)
    dataframe['deadline'] = pd.to_datetime(dataframe['deadline'], format='%d %b %Y', errors='coerce')
    aujourdhui = datetime.today()
    # Calculer la différence en jours entre 'Deadline' et la date d'aujourd'hui
    dataframe['remaining_times'] = (dataframe['deadline'] - aujourdhui).dt.days
    dataframe['remaining_times']=dataframe['remaining_times'].astype(str) + " days"
    dataframe.to_csv("/home/airflow/gcs/data/data.csv")

PROJECT_ID="starry-sunup-401714"
BUCKET_NAME = 'input_bucket_project'
CLUSTER_NAME = 'pysparkcluster'
REGION = 'us-central1'
PYSPARK_URI = f'gs://input_bucket_project/pyspark-job.py'

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    zone="us-central1-a",
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    worker_disk_size=300,
    master_disk_size=300,
    storage_bucket=BUCKET_NAME,
).make()



with airflow.DAG(
    "composer_sample_dag",
    catchup=False,
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    # Print the dag_run id from the Airflow logs
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src='/home/airflow/gcs/data/data.csv',
        dst='data2.csv',
        bucket='input_bucket_project',

    )
    scrape_sports_task = PythonOperator(
    task_id='scrape_globaltenders',
    python_callable=scrapy_globaltenders,
)
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
)
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID,
)
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", 
        project_id=PROJECT_ID, 
        cluster_name=CLUSTER_NAME, 
        region=REGION,
    )
    create_external_table = BigQueryCreateExternalTableOperator(
    task_id="create_external_table_in_BIGQUERY",
    destination_project_dataset_table="Dataset.tenders_table",
    bucket="input_bucket_project",
    source_objects=["data_cleaned.csv"],
    schema_fields=[
        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
        {"name": "deadline", "type": "DATE", "mode": "NULLABLE"},
        {"name": "link", "type": "STRING", "mode": "NULLABLE"},
        {"name": "remaining_times_days", "type": "INTEGER", "mode": "NULLABLE"},

    ],
)

    
scrape_sports_task >> upload_file >> create_cluster >> pyspark_task >> delete_cluster >> create_external_table