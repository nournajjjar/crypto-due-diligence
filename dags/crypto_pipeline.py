from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime,timedelta
import requests
import pandas as pd
from sqlalchemy import create_engine
import time
import logging
from tenacity import retry, wait_exponential, stop_after_attempt
import random



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Retry mechanism with exponential backoff
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def fetch_data(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()

def extract_data():
    markets_url = "https://api.coingecko.com/api/v3/coins/markets"
    markets_params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,  # Reduced number of items per page
    }
    max_pages = 3 # Reduced number of pages
    all_data = []

    # Fetch data from multiple pages
    for page in range(1, max_pages + 1):
        markets_params["page"] = page
        try:
            page_data = fetch_data(markets_url, markets_params)
            all_data.extend(page_data)
            logger.info(f"Page {page} extracted successfully.")
        except Exception as e:
            logger.error(f"Failed to fetch data for page {page}: {e}")
            raise AirflowException(f"Failed to fetch data for page {page}")

        # Add a random delay between pages
        time.sleep(random.uniform(2, 5))

    # Fetch descriptions for each cryptocurrency
    """ descriptions = {}
    for crypto in all_data:
        crypto_id = crypto["id"]
        coin_url = f"https://api.coingecko.com/api/v3/coins/{crypto_id}"
        try:
            coin_data = fetch_data(coin_url, params={})
            descriptions[crypto_id] = coin_data["description"]["en"]
            logger.info(f"Description for {crypto_id} extracted successfully.")
        except Exception as e:
            logger.error(f"Failed to fetch description for {crypto_id}: {e}")
            descriptions[crypto_id] = "No description available" """

        # Add a random delay between requests
        # time.sleep(random.uniform(2, 5))

    # Add descriptions to the data
    """ for crypto in all_data:
        crypto_id = crypto["id"]
        crypto["description"] = descriptions.get(crypto_id, "No description available")"""

    return all_data 
# Fonction de transformation
def transform_data(**kwargs):
    raw_data = kwargs['ti'].xcom_pull(task_ids='extract_data')
    df = pd.DataFrame(raw_data)
    df = df[["id", "symbol", "current_price", "market_cap", "total_volume","high_24h","low_24h","price_change_24h","market_cap_change_24h","circulating_supply","total_supply","market_cap_rank","price_change_percentage_24h","image"]]
    df.columns = ["Name", "Symbol", "Price", "MarketCap", "Volume(24h)","High_24h","Low_24h","Price_change_24h","Market_cap_change_24h","Circulating_supply","Total_supply","Rank","Price_change_24P","image"]
    df["Volume_to_MarketCap"] = df["Volume(24h)"] / df["MarketCap"]
    df.to_csv("/tmp/transformed_data.csv", index=False)

# Fonction de chargement
def load_data():
    df = pd.read_csv("/tmp/transformed_data.csv")
    engine = create_engine("mysql+pymysql://root:root@mysql:3306/crypto_db")
    df.to_sql("crypto_data", con=engine, if_exists="replace", index=False)

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 5,
    'retry_delay': timedelta(seconds=10),  # Délai entre les tentatives
    'retry_exponential_backoff': True,  # Backoff exponentiel
    'max_retry_delay': timedelta(minutes=5),  # Délai maximum entre les tentatives

}

dag = DAG(
    'crypto_pipeline',
    default_args=default_args,
    description='Pipeline ETL pour les données de cryptomonnaies',
    #schedule_interval='@daily',  # Exécution toutes les 5 minutes
    schedule_interval=timedelta(hours=1),  # Exécution toutes les heures

    catchup=False  # Empêche l'exécution rétroactive

)

# Tâches du pipeline
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,  # Pour passer les données entre les tâches
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

# Définir l'ordre des tâches
extract_task >> transform_task >> load_task
