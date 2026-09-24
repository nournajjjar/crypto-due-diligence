from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import feedparser
import pandas as pd
from sqlalchemy import create_engine
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import logging

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialiser l'analyseur de sentiment
analyzer = SentimentIntensityAnalyzer()

# Connexion à la base de données MySQL
DATABASE_URI = "mysql+pymysql://root:root@mysql:3306/crypto_db"
engine = create_engine(DATABASE_URI)

# Mots-clés pour la recherche Google News
KEYWORDS = ["CryptoCurrency", "Bitcoin", "Ethereum", "Cardano", "Binance Coin", "Solana","SEC","DEFI"]

def calculate_fear_greed_index(sentiment_scores):
    """
    Calcule l'indice de peur et d'avidité à partir des scores de sentiment.
    """
    if not sentiment_scores:
        return 50  # Valeur neutre si aucun score n'est disponible
    
    # Calculer la moyenne des scores de sentiment
    average_sentiment = sum(sentiment_scores) / len(sentiment_scores)
    
    # Convertir la moyenne en indice de peur et d'avidité (0-100)
    fear_greed_index = (average_sentiment + 1) * 50  # Map [-1, 1] to [0, 100]
    return fear_greed_index

def fetch_google_news_rss():
    """
    Récupère les articles de Google News via RSS pour les mots-clés donnés,
    analyse le sentiment, calcule l'indice de peur et d'avidité, et stocke les données.
    """
    try:
        # Supprimer les anciennes données avant d'insérer les nouvelles
        with engine.connect() as connection:
            connection.execute("DELETE FROM google_news_data")
        logger.info("Anciennes données supprimées avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors de la suppression des anciennes données : {e}")
        return  # Arrêter l'exécution si la suppression échoue

    sentiment_scores = []  # Pour stocker les scores de sentiment

    for keyword in KEYWORDS:
        try:
            # URL du flux RSS Google News pour le mot-clé
            url = f"https://news.google.com/rss/search?q={keyword}&hl=en-US&gl=US&ceid=US:en"
            feed = feedparser.parse(url)
            
            for entry in feed.entries:
                try:
                    title = entry.get("title", "No title available")
                    description = entry.get("description", "No description available")
                    source = entry.get("source", {}).get("title", "No source available")
                    date = entry.get("published", "No date available")
                    link = entry.get("link", "No link available")
                    
                    # Analyse du sentiment du titre
                    sentiment = analyzer.polarity_scores(title)["compound"]
                    sentiment_scores.append(sentiment)  # Ajouter le score à la liste

                    # Création d'un DataFrame pour l'article
                    df = pd.DataFrame([{
                        "keyword": keyword,
                        "title": title,
                        "description": description,
                        "source": source,
                        "date": date,
                        "link": link,
                        "sentiment": sentiment
                    }])
                    
                    # Insertion dans la base de données
                    df.to_sql("google_news_data", con=engine, if_exists="append", index=False)
                    logger.info(f"Article collecté et stocké : {title} | Sentiment : {sentiment}")
                except Exception as e:
                    logger.error(f"Erreur lors du traitement de l'article : {e}")
        except Exception as e:
            logger.error(f"Erreur lors de la récupération du flux RSS pour le mot-clé {keyword} : {e}")

    # Calculer l'indice de peur et d'avidité
    fear_greed_index = calculate_fear_greed_index(sentiment_scores)
    logger.info(f"Indice de peur et d'avidité : {fear_greed_index:.2f}")

    # Stocker l'indice dans la base de données (optionnel)
    df_index = pd.DataFrame([{
        "timestamp": datetime.now(),
        "fear_greed_index": fear_greed_index
    }])
    df_index.to_sql("fear_greed_index", con=engine, if_exists="append", index=False)

# Définir les arguments par défaut du DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
dag = DAG(
    'google_news_sentiment_analysis_rss',
    default_args=default_args,
    description='DAG pour collecter et analyser les articles Google News via RSS',
    schedule_interval=timedelta(hours=1),  # Exécution toutes les heures
    catchup=False,
)

# Tâche pour récupérer les articles Google News via RSS
fetch_google_news_task = PythonOperator(
    task_id='fetch_google_news_rss',
    python_callable=fetch_google_news_rss,
    dag=dag,
)

# Définir l'ordre des tâches (ici, une seule tâche)
fetch_google_news_task