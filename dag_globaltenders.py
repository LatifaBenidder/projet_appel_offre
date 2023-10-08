
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,10, 8),
    'retries': 2,
}

# Définir le DAG
dag = DAG(
    'globaltenders_dag',
    default_args=default_args,
    schedule_interval='@daily', 
    catchup=False, 
    dagrun_timeout=None,
)

def scrapy_globaltenders():
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
    dataframe.to_csv("data.csv")


scrape_sports_task = PythonOperator(
    task_id='scrape_globaltenders',
    python_callable=scrapy_globaltenders,
    dag=dag,
)
