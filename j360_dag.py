from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd 

# Définissez les arguments par défaut du DAG
default_args = {
    'owner': 'latifa',
    'start_date': datetime(2023, 1, 1),  # Date de début de votre DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définissez le DAG
dag = DAG(
    'j360_dag',  # Nom de votre DAG
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
    catchup=False,
)

# Fonction pour exécuter votre code
def run_my_code():
    import requests
    from bs4 import BeautifulSoup
    from datetime import datetime
    import pandas as pd 
    # L'URL de la page web que vous souhaitez analyser
    url = "https://www.j360.info/appels-d-offres/?cat=informatique-telecoms"
    # Envoyer une requête HTTP pour obtenir le contenu de la page
    response = requests.get(url)
    countries_list = [
        "afrique-du-sud", "Algerie", "Angola", "Benin", "Botswana", "Burkina-Faso", "Burundi",
        "Cameroun", "Cap-Vert", "Comores", "Congo", "cote-divoire", "Djibouti", "egypte",
        "erythree", "ethiopie", "Gabon", "Gambie", "Ghana", "Guinee", "Guinee-Bissau",
        "Guinee-equatoriale", "Kenya", "Lesotho", "Liberia", "libyenne-jamahiriya-arabe", "Madagascar", "Malawi",
        "Mali", "Maroc", "Maurice", "Mauritanie", "Mozambique", "Namibie", "Niger", "Nigeria",
        "Ouganda", "centrafricaine-republique", "congo-la-republique-democratique-du", "Rwanda",
        "Sahara-Occidental", "Sao-Tome-Et-Principe", "Senegal", "Seychelles", "Sierra-Leone",
        "Somalie", "Soudan", "Sud-Soudan", "Swaziland", "tanzanie-republique-unie-de", "Tchad", "Togo", "Tunisie",
        "Zambie", "Zimbabwe"]
    countries_list_lower = [country.lower() for country in countries_list]
    base_url = "https://www.j360.info/appels-d-offres/afrique/"

    # Créez des listes vides pour stocker les données extraites
    titles = []
    localisations = []
    temps_restants = []
    deadline_dates = []
    links = []

    # Boucle sur chaque pays
    for country in countries_list_lower:
        for page in range(1, 11):  # Boucle sur les 10 premières pages
            url = f"{base_url}{country}/?cat=informatique-telecoms&page={page}"
            response = requests.get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Trouvez tous les éléments de lien
                link_elements = soup.select('a.stretched-link')
                for link_element in link_elements[:-1]:
                    link = link_element.get('href')
                    links.append(link)

                # Trouvez tous les éléments de carte
                elements = soup.find_all(class_='card rounded-1 results-item mb-3')
                for element in elements:
                    # Extrayez les données
                    title = element.find(class_='card-title').find('a').text.strip()

                    # Trouvez l'élément "Localisation" s'il existe
                    localisation_element = element.find('span', text='Localisation')
                    localisation = localisation_element.find_next('span').text.strip() if localisation_element else None

                    # Trouvez l'élément "Temps restant" s'il existe
                    temps_restant_element = element.find('span', text='Temps restant')
                    temps_restant = temps_restant_element.find_next('div').text.strip() if temps_restant_element else None

                    # Trouvez l'élément "deadline" s'il existe
                    deadline_element = element.find(class_='item-detail-subvalue deadline')
                    deadline_date = deadline_element.text.strip() if deadline_element else None

                    # Ajoutez les données extraites aux listes
                    titles.append(title)
                    temps_restants.append(temps_restant)
                    deadline_dates.append(deadline_date)
                    localisations.append(country)
            else:
                print(f"La requête a échoué avec le code de statut {response.status_code} pour l'URL : {url}")
    import pandas as pd 
    df = pd.DataFrame({'title': titles, 'pays': localisations,'deadline':deadline_dates,'lien':links})
    # Convertissez la colonne "deadline" en objets datetime
    df["deadline"] = pd.to_datetime(df["deadline"], format='%d/%m/%Y')
    # Obtenez la date actuelle
    date_actuelle = datetime.now()
    temps_restants = df["deadline"] - date_actuelle
    jours_restants = temps_restants.dt.days
    jours_restants_str = jours_restants.astype(str) + " days"
    df["remaining_times"]=jours_restants_str
    df.to_csv("j360_data.csv")
# Créez une tâche pour exécuter votre code
run_code_task = PythonOperator(
    task_id='run_my_code_task',
    python_callable=run_my_code,
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()


