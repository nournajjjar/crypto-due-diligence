import os
import time
import requests
import logging
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException

# Fonction pour générer des questions avec Hugging Face API
def generate_questions():
    # Configuration de l'API Hugging Face
    API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-large"
    API_KEY = os.environ.get("HUGGINGFACE_API_KEY")
    headers = {"Authorization": f"Bearer {API_KEY}"}

    try:
        # Connexion à MySQL
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor()

        # Supprimer les anciennes questions
        cursor.execute("DELETE FROM question_bank;")
        conn.commit()
        logging.info("Old questions deleted from question_bank")

        # Récupérer toutes les données
        cursor.execute("SELECT * FROM crypto_data WHERE Name IS NOT NULL LIMIT 10;")
        data = cursor.fetchall()

        questions = set()  # Utiliser un ensemble pour éliminer les doublons

        for row in data:
            column_names = [desc[0] for desc in cursor.description]
            row_data = dict(zip(column_names, row))

            prompt = (
                "You are a cryptocurrency expert. Based on the following data, generate 3 simple and concise questions in English. "
                "Each question should be easy to understand and no longer than 15 words. "
                "Here is the data:\n"
                f"Name: {row_data['Name']}\n"
                f"Price: {row_data['Price']}\n"
                f"Market Capitalization: {row_data['MarketCap']}\n"
                f"24h Volume: {row_data['Volume(24h)']}\n"
                f"Rank: {row_data['Rank']}\n"
                "Questions:"
            )

            attempt = 0
            success = False
            while attempt < 3 and not success:
                try:
                    payload = {
                        "inputs": prompt,
                        "parameters": {
                            "max_length": 20,
                            "num_return_sequences": 2,
                            "do_sample": True,
                            "temperature": 0.5,
                        }
                    }
                    response = requests.post(API_URL, headers=headers, json=payload, timeout=40)
                    logging.info(f"API response for {row_data['Name']}: {response.status_code} - {response.text}")

                    if response.status_code == 200:
                        response_json = response.json()
                        if isinstance(response_json, list) and len(response_json) > 0:
                            for sequence in response_json:
                                generated_question = sequence.get("generated_text", "").strip()
                                if generated_question:
                                    questions.add(generated_question)  # Ajouter à l'ensemble
                            success = True
                        else:
                            logging.warning(f"Unexpected API response for {row_data['Name']}: {response_json}")
                            success = True
                    elif response.status_code == 503:
                        logging.warning(f"Model loading for {row_data['Name']}, retrying in 20 seconds...")
                        time.sleep(20)
                    else:
                        logging.error(f"API error ({response.status_code}): {response.text}")
                        success = True
                except requests.exceptions.RequestException as e:
                    logging.error(f"API error for {row_data['Name']}: {str(e)}")
                    success = True

                attempt += 1

            if not success:
                logging.error(f"Failed after 3 attempts for {row_data['Name']}")

        # Insérer les questions uniques
        if questions:
            unique_questions = list(questions)  # Convertir en liste pour l'insertion
            cursor.executemany("INSERT INTO question_bank (question) VALUES (%s)", [(q,) for q in unique_questions])
            conn.commit()
            logging.info(f"{len(unique_questions)} unique questions added to question_bank")

    except mysql.connector.Error as e:
        logging.error(f"Database connection error: {e}")
        raise AirflowException(f"Database connection error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
# Fonction pour générer des réponses avec Hugging Face API
def generate_answers():
    # Configuration de l'API Hugging Face
    API_URL = "https://api-inference.huggingface.co/models/google/flan-t5-large"
    API_KEY = os.environ.get("HUGGINGFACE_API_KEY")
    headers = {"Authorization": f"Bearer {API_KEY}"}

    try:
        # Connexion à MySQL
        conn = mysql.connector.connect(
            host="mysql",
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor()

        # Supprimer les anciennes réponses
        cursor.execute("DELETE FROM answer_bank;")
        conn.commit()
        logging.info("Old answers deleted from answer_bank")

        # Récupérer toutes les questions générées
        cursor.execute("SELECT id, question FROM question_bank;")
        questions = cursor.fetchall()

        for question in questions:
            question_id, question_text = question

            # Récupérer les données associées à la question (exemple : données de crypto_data)
            cursor.execute("SELECT * FROM crypto_data WHERE Name IS NOT NULL LIMIT 1;")
            data = cursor.fetchone()

            if data:
                column_names = [desc[0] for desc in cursor.description]
                row_data = dict(zip(column_names, data))

                prompt = (
                    "You are a cryptocurrency expert. Based on the following data, answer the following question in English. "
                    "The answer should be concise and no longer than 30 words. "
                    "Here is the data:\n"
                    f"Name: {row_data['Name']}\n"
                    f"Price: {row_data['Price']}\n"
                    f"Market Capitalization: {row_data['MarketCap']}\n"
                    f"24h Volume: {row_data['Volume(24h)']}\n"
                    f"Rank: {row_data['Rank']}\n"
                    f"Question: {question_text}\n"
                    "Answer:"
                )

                attempt = 0
                success = False
                while attempt < 3 and not success:
                    try:
                        payload = {
                            "inputs": prompt,
                            "parameters": {
                                "max_length": 30,
                                "num_return_sequences": 1,
                                "do_sample": True,
                                "temperature": 0.5,
                            }
                        }
                        response = requests.post(API_URL, headers=headers, json=payload, timeout=40)
                        logging.info(f"API response for question ID {question_id}: {response.status_code} - {response.text}")

                        if response.status_code == 200:
                            response_json = response.json()
                            if isinstance(response_json, list) and len(response_json) > 0:
                                generated_answer = response_json[0].get("generated_text", "").strip()
                                if generated_answer:
                                    # Insérer la réponse dans la table answer_bank
                                    cursor.execute(
                                        "INSERT INTO answer_bank (question_id, answer) VALUES (%s, %s)",
                                        (question_id, generated_answer)
                                    )
                                    conn.commit()
                                    logging.info(f"Answer for question ID {question_id} added to answer_bank")
                                    success = True
                            else:
                                logging.warning(f"Unexpected API response for question ID {question_id}: {response_json}")
                                success = True
                        elif response.status_code == 503:
                            logging.warning(f"Model loading for question ID {question_id}, retrying in 20 seconds...")
                            time.sleep(20)
                        else:
                            logging.error(f"API error ({response.status_code}): {response.text}")
                            success = True
                    except requests.exceptions.RequestException as e:
                        logging.error(f"API error for question ID {question_id}: {str(e)}")
                        success = True

                    attempt += 1

                if not success:
                    logging.error(f"Failed after 3 attempts for question ID {question_id}")

    except mysql.connector.Error as e:
        logging.error(f"Database connection error: {e}")
        raise AirflowException(f"Database connection error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# Définition du DAG Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 6),
    "retries": 3,  # Augmenter les tentatives dans le cas où l'API est temporairement hors ligne
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "generate_question_answer_bank_huggingface",
    default_args=default_args,
    description="Génère une banque de questions et réponses avec Hugging Face API",
    schedule_interval="@daily",
)

generate_questions_task = PythonOperator(
    task_id="generate_questions",
    python_callable=generate_questions,
    dag=dag,
)

generate_answers_task = PythonOperator(
    task_id="generate_answers",
    python_callable=generate_answers,
    dag=dag,
)

# Définir l'ordre des tâches
generate_questions_task >> generate_answers_task