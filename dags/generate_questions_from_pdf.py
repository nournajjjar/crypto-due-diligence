import time
import requests
import logging
import mysql.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import PyPDF2
from io import BytesIO
import base64
import uuid
from concurrent.futures import ThreadPoolExecutor

def extract_text_from_pdf(pdf_file):
    try:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        if len(pdf_reader.pages) == 0:
            logging.error("Le fichier PDF est vide ou corrompu.")
            raise AirflowException("Le fichier PDF est vide ou corrompu.")
        
        # Conserver le texte avec les numéros de page
        text_with_pages = []
        for page_num, page in enumerate(pdf_reader.pages, start=1):
            page_text = page.extract_text()
            if page_text:
                text_with_pages.append((page_num, page_text))
        
        logging.info(f"Texte extrait du PDF avec les numéros de page.")
        return text_with_pages
    except PyPDF2.errors.PdfReadError as e:
        logging.error(f"Erreur lors de la lecture du PDF : {e}")
        raise AirflowException(f"Erreur lors de la lecture du PDF : {e}")
    
def extract_keywords(text, max_keywords=5):
    """
    Extrait les mots-clés les plus pertinents d'un texte.
    """
    words = text.lower().split()
    keywords = [word for word in words if len(word) > 3]  # Ignorer les mots courts
    return keywords[:max_keywords]

def find_relevant_page(answer, text_with_pages):
    """
    Trouve la page pertinente en fonction des mots-clés de la réponse.
    """
    keywords = extract_keywords(answer)
    best_page = None
    best_match_count = 0
    
    for page_num, page_text in text_with_pages:
        match_count = sum(1 for keyword in keywords if keyword in page_text.lower())
        if match_count > best_match_count:
            best_match_count = match_count
            best_page = page_num
    
    return best_page if best_match_count > 0 else None

def generate_questions_from_pdf(**kwargs):
    OLLAMA_URL = "http://host.docker.internal:11434/api/generate"
    pdf_content = kwargs['dag_run'].conf.get('pdf_file')
    
    if not pdf_content:
        logging.error("Aucun fichier PDF fourni.")
        raise AirflowException("Aucun fichier PDF fourni.")
    
    pdf_id = str(uuid.uuid4())
    try:
        pdf_bytes = base64.b64decode(pdf_content)
    except Exception as e:
        logging.error(f"Erreur lors du décodage du PDF : {e}")
        raise AirflowException(f"Erreur lors du décodage du PDF : {e}")
    
    pdf_file = BytesIO(pdf_bytes)
    text = extract_text_from_pdf(pdf_file)
    
    try:
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor()
        
        prompt = f"""
        Based on the following text, generate 10 clear and concise cryptocurrency-related questions. 
        Ensure the questions cover technical, conceptual,financial and practical aspects of the content.
        Avoid redundant or overly simple questions. Each question should be unique and relevant to the text.
        Do not include any introductory phrases like "Here are the questions:" or "Based on the text:".

        Text:
        {text}

        Questions:
        """
        
        payload = {
            "model": "llama3",
            "prompt": prompt,
            "stream": False,
            "parameters": {
                "max_length": 2000,
                "num_return_sequences": 1,
                "do_sample": True,
                "temperature": 0.5
            }
        }
        
        response = requests.post(OLLAMA_URL, json=payload, timeout=240)
        response.raise_for_status()
        
        questions = []
        if response.status_code == 200:
            response_json = response.json()
            generated_text = response_json.get("response", "").strip()
            
            if generated_text:
                intro_phrases = [
                    "Here are 10 clear and concise cryptocurrency-related questions based on the provided text:",
                    "Here are 10 clear and concise cryptocurrency-related questions based on the text:",
                    "Based on the text:",
                    "Questions:",
                    "Here are the questions:"
                ]
        
                for phrase in intro_phrases:
                    generated_text = generated_text.replace(phrase, "")
        
                # Diviser les questions par saut de ligne et nettoyer
                questions = [q.strip() for q in generated_text.split("\n") if q.strip()]
                
                # Supprimer les numéros de question s'ils existent (ex: "1. ", "2. ")
                questions = [q.split('. ', 1)[1] if '. ' in q and q.split('. ', 1)[0].isdigit() else q for q in questions]
                
                # Garder uniquement les 400 premières questions valides
                questions = questions[:10]
        
        if questions:
            cursor.executemany("INSERT INTO question_bank_pdf (pdf_id, question) VALUES (%s, %s)", 
                               [(pdf_id, q) for q in questions])
            conn.commit()
            logging.info(f"{len(questions)} questions added from PDF {pdf_id}.")
        else:
            logging.error("Aucune question valide générée.")
            raise AirflowException("Aucune question valide générée.")
        
    except mysql.connector.Error as e:
        logging.error(f"Database error: {e}")
        raise AirflowException(f"Database error: {e}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors de la requête à Ollama : {e}")
        raise AirflowException(f"Erreur lors de la requête à Ollama : {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
    return pdf_id


def generate_answers_from_questions(**kwargs):
    OLLAMA_URL = "http://host.docker.internal:11434/api/generate"
    task_instance = kwargs['ti']
    pdf_id = task_instance.xcom_pull(task_ids='generate_questions_from_pdf')
    
    if not pdf_id:
        logging.error("Aucun identifiant PDF fourni.")
        raise AirflowException("Aucun identifiant PDF fourni.")
    
    try:
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor()
        cursor.execute("SELECT question FROM question_bank_pdf WHERE pdf_id = %s", (pdf_id,))
        questions = cursor.fetchall()
        
        if not questions:
            logging.error("Aucune question trouvée pour ce PDF.")
            raise AirflowException("Aucune question trouvée pour ce PDF.")
        
        # Récupérer le texte avec les numéros de page
        pdf_content = kwargs['dag_run'].conf.get('pdf_file')
        pdf_bytes = base64.b64decode(pdf_content)
        pdf_file = BytesIO(pdf_bytes)
        text_with_pages = extract_text_from_pdf(pdf_file)
        
        def generate_answer(question):
            prompt = f"""
            Provide a clear and concise answer to the following cryptocurrency-related question. 
            Ensure the answer is accurate, relevant, and based on the context provided in the text.
            Do not include any prefixes like "Answer:" or "Response:".

            Question:
            {question[0]}

            Answer:
            """
            payload = {
                "model": "llama3",
                "prompt": prompt,
                "stream": False,
                "parameters": {
                    "max_length": 50,
                    "num_return_sequences": 1,
                    "do_sample": True,
                    "temperature": 0.3
                }
            }
            try:
                response = requests.post(OLLAMA_URL, json=payload, timeout=240)
                response.raise_for_status()
                if response.status_code == 200:
                    response_json = response.json()
                    answer = response_json.get("response", "").strip()
                    answer = answer.replace("Answer:", "").strip()
                    
                    # Trouver la page pertinente en fonction des mots-clés
                    relevant_page = find_relevant_page(answer, text_with_pages)
                    
                    # Ajouter la source à la réponse
                    if relevant_page:
                        source = f"Source : Page {relevant_page}"
                    else:
                        source = "Source : Non spécifiée"
                    
                    return answer, source
            except requests.exceptions.RequestException as e:
                logging.error(f"Erreur lors de la requête à Ollama : {e}")
            return None, None

        with ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(generate_answer, questions))
        
        # Filtrer les résultats valides
        answers_with_sources = [(q[0], a, s) for q, (a, s) in zip(questions, results) if a and s]
        
        if answers_with_sources:
            cursor.executemany("INSERT INTO answer_bank_pdf (pdf_id, question, answer, source) VALUES (%s, %s, %s, %s)", 
                               [(pdf_id, q, a, s) for q, a, s in answers_with_sources])
            conn.commit()
            logging.info(f"{len(answers_with_sources)} réponses ajoutées pour le PDF {pdf_id}.")
        
    except mysql.connector.Error as e:
        logging.error(f"Database error: {e}")
        raise AirflowException(f"Database error: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

dag = DAG(
    "generate_questions_from_pdf",
    default_args=default_args,
    description="Génère des questions à partir d'un fichier PDF",
    schedule_interval=None,
)

generate_questions_task = PythonOperator(
    task_id="generate_questions_from_pdf",
    python_callable=generate_questions_from_pdf,
    provide_context=True,
    dag=dag,
)

generate_answers_task = PythonOperator(
    task_id="generate_answers_from_questions",
    python_callable=generate_answers_from_questions,
    provide_context=True,
    dag=dag,
    retries=5,
    retry_delay=timedelta(minutes=2),
)

generate_questions_task >> generate_answers_task