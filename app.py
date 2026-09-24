import eventlet
eventlet.monkey_patch()  # 🔁 important pour le bon fonctionnement
import os
import base64
import requests
import mysql.connector
from flask import Flask, render_template, request, redirect, url_for, flash,session
from jinja2 import TemplateNotFound
from pptx import Presentation
from io import BytesIO
from flask import send_file
from datetime import datetime
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from charts import generate_crypto_chart
from charts import CryptoAnalyzer
from flask_socketio import SocketIO, emit,join_room
from chatbot import stream_crypto_response
import json
import plotly.graph_objs as go
import plotly.io as pio
from decimal import Decimal, getcontext
from werkzeug.security import generate_password_hash, check_password_hash
from flask_session import Session
from video import generate_script, generate_video_from_prompt



# Configuration de Flask
app = Flask(__name__)
app.secret_key = os.urandom(24)

# ✅ Crée l'objet socketio ici AVANT de l'utiliser
socketio = SocketIO(app, cors_allowed_origins="*")
# Configuration de la précision décimale
getcontext().prec = 8
# Configuration de la session
app.config['SESSION_TYPE'] = 'filesystem'  # Stockage sur disque
app.config['SESSION_FILE_DIR'] = './flask_session'  # Dossier des sessions
app.config['SESSION_COOKIE_NAME'] = 'flask_socketio_session'
app.config['PERMANENT_SESSION_LIFETIME'] = 3600  # 1 heure

# Initialisation
Session(app)
socketio = SocketIO(app, cors_allowed_origins="*")


# URL d'Airflow dans Docker (remplace localhost par le nom du service Docker)
AIRFLOW_API_URL = "http://airflow-webserver:8080/api/v1/dags/generate_questions_from_pdf/dagRuns"
AIRFLOW_USERNAME = "airflow"
AIRFLOW_PASSWORD = "airflow"

# Dossier pour stocker temporairement les fichiers PDF
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Fonction pour récupérer les questions depuis la base de données
def get_questions_from_db(pdf_id=None):
    try:
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor(dictionary=True)

        if pdf_id:
            # Récupérer les questions pour un PDF spécifique
            cursor.execute("SELECT * FROM question_bank_pdf WHERE pdf_id = %s", (pdf_id,))
        else:
            # Récupérer toutes les questions
            cursor.execute("SELECT * FROM question_bank_pdf")

        questions = cursor.fetchall()
        cursor.close()
        conn.close()
        return questions

    except mysql.connector.Error as e:
        flash(f"Erreur lors de la récupération des questions : {e}", "danger")
        return []
# Fonction pour récupérer le dernier indice de peur et d'avidité
def get_latest_fear_greed_index():
    try:
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor(dictionary=True)

        # Récupérer le dernier indice
        cursor.execute("SELECT fear_greed_index FROM fear_greed_index ORDER BY timestamp DESC LIMIT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return result['fear_greed_index']
        return 50  # Valeur par défaut si aucune donnée n'est disponible

    except mysql.connector.Error as e:
        flash(f"Erreur lors de la récupération de l'indice de peur et d'avidité : {e}", "danger")
        return 50

# 🖥 Page d'accueil avec formulaire d'upload
@app.route("/", methods=["GET", "POST"])
def upload_file():
    if request.method == "POST":
        file = request.files["pdf_file"]
        if file and file.filename.endswith(".pdf"):
            file_path = os.path.join(app.config["UPLOAD_FOLDER"], file.filename)
            file.save(file_path)

            # Convertir le PDF en Base64
            with open(file_path, "rb") as pdf_file:
                pdf_base64 = base64.b64encode(pdf_file.read()).decode("utf-8")

            # Supprimer le fichier après conversion
            os.remove(file_path)

            # Envoyer le fichier PDF à Airflow
            response = trigger_airflow_dag(pdf_base64)
            if response.status_code == 200:
                flash("Votre file est en cours de traitement !", "success")
            else:
                flash(f"Erreur : {response.text}", "danger")

            return redirect(url_for("upload_file"))

    # Récupérer toutes les questions pour les afficher
    questions = get_questions_from_db()
    return render_template("PDF.html", questions=questions)
@app.route("/clear_questions", methods=["POST"])
def clear_questions():
    try:
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor()

        # Supprimer toutes les questions de la table
        cursor.execute("DELETE FROM question_bank_pdf")
        conn.commit()

        flash("Toutes les questions ont été supprimées avec succès !", "success")
    except mysql.connector.Error as e:
        flash(f"Erreur lors de la suppression des questions : {e}", "danger")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

    return redirect(url_for("upload_file"))
from flask import jsonify

@app.route("/get_answer")
def get_answer():
    question = request.args.get('question')
    if not question:
        return jsonify({"error": "Question parameter is missing"}), 400

    try:
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor(dictionary=True)

        # Récupérer la réponse ET la source depuis la base de données
        cursor.execute("SELECT answer, source FROM answer_bank_pdf WHERE question = %s", (question,))
        result = cursor.fetchone()

        if result:
            return jsonify({"answer": result['answer'], "source": result['source']})
        else:
            return jsonify({"error": "No answer found for this question"}), 404

    except mysql.connector.Error as e:
        return jsonify({"error": f"Database error: {e}"}), 500
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def get_db_connection():
    conn = mysql.connector.connect(
        host="mysql", 
        user="root",
        password="root",
        database="crypto_db"
    )
    return conn

@app.route('/chatbot')
def chatbot():
    return render_template('chatbot.html')

@app.route('/handle_message', methods=['POST'])
def handle_message():
    data = request.get_json()
    user_input = data['message']
    
    # Collecte toutes les réponses du chatbot
    full_response = ""
    for chunk in stream_crypto_response(user_input):
        try:
            content = json.loads(chunk).get("message", {}).get("content", "")
            if content:
                full_response += content
        except Exception:
            continue
    
    return jsonify({'response': full_response})
@app.route("/home")
def index():

 return render_template("index.html")
@app.route("/PDF")
def index1():

 return render_template("PDF.html")

@app.route("/about")
def about():

 return render_template("aboutus.html")
# Route pour récupérer l'indice de peur et d'avidité au format JSON
@app.route("/get_fear_greed_index")
def get_fear_greed_index():
    # Récupérer le dernier indice de peur et d'avidité
    fear_greed_index = get_latest_fear_greed_index()
    
    # Retourner l'indice au format JSON
    return jsonify({"fear_greed_index": fear_greed_index})

# @app.route('/get_top5_cryptos')
# def get_top5_cryptos():
#     try:
#         conn = mysql.connector.connect(
#             host="mysql", 
#             user="root",
#             password="root",
#             database="crypto_db"
#         )
#         cursor = conn.cursor(dictionary=True)
        
#         cursor.execute("""
#             SELECT 
#                 Name, 
#                 Symbol, 
#                 MarketCap,
#                 image,
#                 ROUND((MarketCap / (SELECT SUM(MarketCap) FROM crypto_data WHERE MarketCap IS NOT NULL) * 100, 1) as Percentage
#             FROM crypto_data 
#             WHERE MarketCap IS NOT NULL
#             ORDER BY MarketCap DESC 
#             LIMIT 5
#         """)
#         cryptos = cursor.fetchall()
        
#         return jsonify(cryptos)
        
#     except Exception as e:
#         print(f"Error fetching top 5 cryptos: {str(e)}")
#         return jsonify([])
#     finally:
#         if cursor:
#             cursor.close()
#         if conn and conn.is_connected():
#             conn.close()



@app.route('/market')
def index2():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Récupérer les données de la table crypto_data
    cursor.execute("SELECT * FROM crypto_data LIMIT 50")
    data = cursor.fetchall()  # Récupérer toutes les lignes
    
    # Récupérer les actualités depuis la table google_news_data
    cursor.execute("SELECT title, source, date, link FROM google_news_data LIMIT 50")  # Limite à 5 actualités
    news_items = cursor.fetchall()  # Récupérer toutes les lignes

    # Récupérer les Top 5 cryptos par part de marché
    cursor.execute("""
    SELECT 
        Name, 
        Symbol, 
        MarketCap,
        image,
        ROUND((MarketCap / (SELECT SUM(MarketCap) FROM crypto_data WHERE MarketCap IS NOT NULL)) * 100, 1) as Percentage
    FROM crypto_data 
    WHERE MarketCap IS NOT NULL
    ORDER BY MarketCap DESC 
    LIMIT 5
""")
    top5_cryptos = cursor.fetchall()
    
    conn.close()
    
    # Passer les données au template
    return render_template('market.html', data=data, news_items=news_items,top5_cryptos=top5_cryptos)
@app.route('/get_market_data')
def get_market_data():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Récupérer les données de la table crypto_data
    cursor.execute("SELECT * FROM crypto_data LIMIT 100")
    data = cursor.fetchall()  # Récupérer toutes les lignes
     # Récupérer les actualités depuis la table google_news_data
    cursor.execute("SELECT title, source, date, link FROM google_news_data LIMIT 2")  # Limite à 5 actualités
    news_items = cursor.fetchall()  # Récupérer toutes les lignes
    
    conn.close()
    
    # Retourner les données au format JSON
    return jsonify(data=data, news_items=news_items)

@app.route('/api/market_cap')
def get_total_market_cap():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT 
                SUM(MarketCap) as total_market_cap, 
                SUM(Market_cap_change_24h) as total_change 
            FROM crypto_data
        """)
        result = cursor.fetchone()
        
        total = float(result['total_market_cap'] or 0)
        change = float(result['total_change'] or 0)
        percentage = (change / total * 100) if total > 0 else 0
        
        # Formatage
        if total >= 1e12:
            formatted = f"${total/1e12:.2f}T"
        elif total >= 1e9:
            formatted = f"${total/1e9:.2f}B"
        else:
            formatted = f"${total:,.2f}"
            
        return jsonify({
            'total_market_cap': total,
            'formatted_total': formatted,
            'percentage_change': round(percentage, 2),
            'trend': 'up' if percentage >= 0 else 'down'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.route('/crypto_dashboard')
def crypto_dashboard():
    chart_image = generate_crypto_chart()  # Appel direct maintenant
    return render_template('crypto_dashboard.html',
                         chart_image=chart_image)

@app.route('/generate_report', methods=['POST'])
def generate_report():
    try:
        # Récupérer toutes les questions et réponses depuis la base de données
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor(dictionary=True)
        
        # Jointure entre les tables question_bank_pdf et answer_bank_pdf
        cursor.execute("""
            SELECT q.question, a.answer, a.source 
            FROM question_bank_pdf q
            JOIN answer_bank_pdf a ON q.question = a.question
            ORDER BY q.id
        """)
        qa_pairs = cursor.fetchall()
        
        if not qa_pairs:
            flash("Aucune question/réponse disponible pour générer un rapport", "warning")
            return redirect(url_for("upload_file"))
        
        # Créer une nouvelle présentation PowerPoint
        prs = Presentation()
        
        # ========== PAGE DE TITRE AVEC LOGO ==========
        slide_layout = prs.slide_layouts[0]
        slide = prs.slides.add_slide(slide_layout)
        
        # Ajouter le logo (remplacez par le chemin de votre logo)
        try:
            logo_path = "static/assets/img/logo.png"  # Chemin relatif à votre projet
            left = Inches(7.5)
            top = Inches(0.5)
            height = Inches(1)
            slide.shapes.add_picture(logo_path, left, top, height=height)
        except Exception as e:
            print(f"Logo non trouvé: {e}")

        title = slide.shapes.title
        subtitle = slide.placeholders[1]
        
        title.text = "Rapport Questions/Réponses"
        subtitle.text = f"Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}\nCryptocurrency Analysis"
        
        # ========== TABLE DES MATIÈRES ==========
        toc_slide = prs.slides.add_slide(prs.slide_layouts[5])
        toc_title = toc_slide.shapes.title
        toc_title.text = "Table des Matières"
        
        left = Inches(0.5)
        top = Inches(1.5)
        width = Inches(9)
        height = Inches(5)
        toc_box = toc_slide.shapes.add_textbox(left, top, width, height)
        toc_frame = toc_box.text_frame
        
        # Ajouter les entrées de la table des matières
        for i, pair in enumerate(qa_pairs, start=1):
            p = toc_frame.add_paragraph()
            p.text = f"Question {i}: {pair['question'][:50]}..."  # Limite à 50 caractères
            p.level = 0
            p.font.size = Pt(14)
            p.space_after = Inches(0.1)
        
        # ========== DIAPOSITIVES QUESTIONS/RÉPONSES ==========
        for i, pair in enumerate(qa_pairs, start=1):
            answer_text = pair['answer']
            max_chars = 1500  # Nombre max de caractères par diapositive
            parts = [answer_text[i:i+max_chars] for i in range(0, len(answer_text), max_chars)]
            
            for part_num, part in enumerate(parts, 1):
                slide = prs.slides.add_slide(prs.slide_layouts[5])  # Layout vide
                
                # Style de la question (uniquement sur la première diapositive de la question)
                if part_num == 1:
                    left = Inches(0.5)
                    top = Inches(0.5)
                    width = Inches(9)
                    height = Inches(1.5)
                    question_box = slide.shapes.add_textbox(left, top, width, height)
                    q_frame = question_box.text_frame
                    q_frame.word_wrap = True
                    
                    p = q_frame.add_paragraph()
                    p.text = f"Question {i}: {pair['question']}"
                    p.font.bold = True
                    p.font.size = Pt(20)
                    p.font.color.rgb = RGBColor(0, 32, 96)  # Bleu foncé
                
                # Style de la réponse
                left = Inches(0.5)
                top = Inches(2)
                width = Inches(9)
                height = Inches(2)
                answer_box = slide.shapes.add_textbox(left, top, width, height)
                a_frame = answer_box.text_frame
                a_frame.word_wrap = True  # Retour à la ligne automatique
                
                p = a_frame.add_paragraph()
                p.text = part if part_num == 1 else f"(Suite) {part}"
                p.font.size = Pt(14)
                p.space_after = Inches(0.1)
                
                # Style de la source (uniquement sur la première diapositive de la question)
                
        
        # ========== DIAPOSITIVE DE CONCLUSION ==========
        end_slide = prs.slides.add_slide(prs.slide_layouts[5])
        left = Inches(1)
        top = Inches(2)
        width = Inches(8)
        height = Inches(3)
        box = end_slide.shapes.add_textbox(left, top, width, height)
        frame = box.text_frame
        
        p = frame.add_paragraph()
        p.text = "Merci pour votre attention"
        p.font.size = Pt(28)
        p.font.color.rgb = RGBColor(0, 32, 96)
        p.alignment = PP_ALIGN.CENTER
        
        # Sauvegarder dans un buffer mémoire
        buffer = BytesIO()
        prs.save(buffer)
        buffer.seek(0)
        
        # Envoyer le fichier au client
        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"rapport_crypto_{datetime.now().strftime('%Y%m%d_%H%M')}.pptx",
            mimetype="application/vnd.openxmlformats-officedocument.presentationml.presentation"
        )
        
    except mysql.connector.Error as e:
        flash(f"Erreur de base de données: {e}", "danger")
        return redirect(url_for("upload_file"))
    except Exception as e:
        flash(f"Erreur lors de la génération du rapport: {e}", "danger")
        return redirect(url_for("upload_file"))
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

analyzer = CryptoAnalyzer()
@app.route('/pred', methods=['GET', 'POST'])
def pred():
    if request.method == 'POST':
        crypto = request.form.get('crypto')
        if crypto in analyzer.CRYPTOS:
            # Génère les graphiques de base
            plots = analyzer.generate_plots(crypto)
            
            # Entraîne le modèle et fait des prédictions
            predictions = analyzer.train_and_predict(crypto)
            
            return render_template('pred.html', 
                                cryptos=analyzer.CRYPTOS, 
                                selected_crypto=crypto,
                                plots=plots,
                                predictions=predictions)
    
    return render_template('pred.html', cryptos=analyzer.CRYPTOS)

@app.route("/learn")
def learn():
    return render_template("learn.html")


# Routes d'authentification
@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        email = request.form['email']
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            hashed_password = generate_password_hash(password)
            cursor.execute(
                "INSERT INTO users (username, password, email) VALUES (%s, %s, %s)",
                (username, hashed_password, email)
            )
            
            # Créer le solde initial pour l'utilisateur
            user_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO solde (user_id, solde) VALUES (%s, %s)",
                (user_id, 10000.00)
            )
            
            conn.commit()
            flash('Inscription réussie! Vous pouvez maintenant vous connecter.', 'success')
            return redirect('/login')
        except mysql.connector.Error as err:
            conn.rollback()
            flash("Erreur d'inscription: " + str(err), 'danger')
        finally:
            conn.close()
    
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()
        conn.close()
        
        if user and check_password_hash(user['password'], password):
            # Configuration robuste de la session
            session.clear()
            session['user_id'] = user['id']
            session['username'] = user['username']
            session.permanent = True  # Session persistante
            
            flash('Connexion réussie!', 'success')
            return redirect('/sim')
        else:
            flash('Identifiants incorrects', 'danger')
    
    return render_template('login.html')

@app.route('/logout')
def logout():
    # Nettoie complètement la session
    session.clear()
    
    # Réponse qui supprime le cookie
    response = redirect('/login')
    response.delete_cookie('session')
    
    flash('Vous avez été déconnecté', 'info')
    return response

@socketio.on('connect')
def handle_ws_connect():
    # Récupère l'ID de session depuis les cookies
    session_id = request.cookies.get(app.session_cookie_name)
    
    if not session_id:
        print("Refusé: Pas de cookie de session")
        return False
        
    # Charge la session existante
    session.sid = session_id
    session.modified = True
    
    if 'user_id' not in session:
        print(f"Session invalide: {dict(session)}")
        return False
    
    print(f"Connexion WebSocket acceptée pour user_id: {session['user_id']}")

# Routes principales
@app.route('/hom')
def home():
    if 'user_id' not in session:
        return redirect('/login')
    return redirect('/sim')

@app.route('/sim')
def trading_simulator():
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Récupération des cryptos
    cursor.execute("""
        SELECT Name, Price,image, Price_change_24P as Change24h, MarketCap 
        FROM crypto_data 
        ORDER BY MarketCap DESC
        LIMIT 50
    """)
    cryptos = cursor.fetchall()
    
    # Récupération du solde
    cursor.execute("SELECT solde FROM solde WHERE user_id = %s", (user_id,))
    solde = cursor.fetchone()['solde']
    
    conn.close()
    
    return render_template("sim.html", cryptos=cryptos, solde=solde)
@socketio.on('connect', namespace='/sim')
def handle_sim_connect():
    if 'user_id' not in session:
        return False  # Rejette la connexion si non authentifié
    emit('connection_response', {'status': 'connected'})

def update_crypto_prices():
    while True:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("""
            SELECT Name, Price, Price_change_24P as Change24h 
            FROM crypto_data 
            ORDER BY MarketCap DESC
            LIMIT 50
        """)
        cryptos = cursor.fetchall()
        conn.close()
        
        socketio.emit('crypto_update', {'cryptos': cryptos}, namespace='/sim')
        socketio.sleep(5)  # Mise à jour toutes les 5 secondes

@socketio.on('request_initial_data', namespace='/sim')
def handle_initial_data_request():
    if 'user_id' not in session:
        return
    
    user_id = session['user_id']
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    cursor.execute("""
        SELECT Name, Price, Price_change_24P as Change24h, MarketCap 
        FROM crypto_data 
        ORDER BY MarketCap DESC
        LIMIT 50
    """)
    cryptos = cursor.fetchall()
    
    cursor.execute("SELECT solde FROM solde WHERE user_id = %s", (user_id,))
    solde = cursor.fetchone()['solde']
    
    conn.close()
    
    emit('initial_data', {
        'cryptos': cryptos,
        'solde': solde
    })

@app.route('/trade', methods=["POST"])
def trade():
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    
    try:
        action = request.form['action']
        crypto = request.form['crypto']
        amount = Decimal(request.form['amount'])
        
        if amount <= 0:
            flash("Montant invalide", 'danger')
            return redirect('/sim')
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Récupération du prix actuel avec verrouillage
        cursor.execute("""
            SELECT Price, Price_change_24P as Change24h 
            FROM crypto_data 
            WHERE Name = %s FOR UPDATE
        """, (crypto,))
        result = cursor.fetchone()
        
        if not result:
            conn.close()
            flash("Cryptomonnaie introuvable", 'danger')
            return redirect('/sim')
        
        price = Decimal(str(result['Price']))
        quantite = amount / price
        
        # Récupération du solde avec verrouillage
        cursor.execute("""
            SELECT solde FROM solde 
            WHERE user_id = %s FOR UPDATE
        """, (user_id,))
        solde = Decimal(str(cursor.fetchone()['solde']))
        
        if action == "buy":
            if amount > solde:
                conn.close()
                flash("❌ Solde insuffisant", 'danger')
                return redirect('/sim')
            
            new_solde = solde - amount
            
            # Vérification si la crypto existe déjà dans le portefeuille
            cursor.execute("""
                SELECT * FROM portefeuille 
                WHERE user_id = %s AND crypto_name = %s FOR UPDATE
            """, (user_id, crypto))
            row = cursor.fetchone()
            
            if row:
                total_quantite = Decimal(str(row['quantite'])) + quantite
                total_investi = Decimal(str(row['quantite'])) * Decimal(str(row['prix_achat'])) + amount
                nouveau_prix = total_investi / total_quantite
                
                cursor.execute("""
                    UPDATE portefeuille 
                    SET quantite = %s, prix_achat = %s 
                    WHERE id = %s
                """, (float(total_quantite), float(nouveau_prix), row['id']))
            else:
                cursor.execute("""
                    INSERT INTO portefeuille 
                    (user_id, crypto_name, quantite, prix_achat) 
                    VALUES (%s, %s, %s, %s)
                """, (user_id, crypto, float(quantite), float(price)))
        
        elif action == "sell":
            cursor.execute("""
                SELECT * FROM portefeuille 
                WHERE user_id = %s AND crypto_name = %s FOR UPDATE
            """, (user_id, crypto))
            row = cursor.fetchone()
            
            if not row or Decimal(str(row['quantite'])) < quantite:
                conn.close()
                flash("❌ Quantité insuffisante dans votre portefeuille", 'danger')
                return redirect('/sim')
            
            new_solde = solde + amount
            reste = Decimal(str(row['quantite'])) - quantite
            
            if reste == 0:
                cursor.execute("""
                    DELETE FROM portefeuille 
                    WHERE id = %s
                """, (row['id'],))
            else:
                cursor.execute("""
                    UPDATE portefeuille 
                    SET quantite = %s 
                    WHERE id = %s
                """, (float(reste), row['id']))
        
        # Mise à jour du solde
        cursor.execute("""
            UPDATE solde 
            SET solde = %s 
            WHERE user_id = %s
        """, (float(new_solde), user_id))
        
        # Enregistrement du trade
        cursor.execute("""
            INSERT INTO trades 
            (user_id, crypto, quantite, prix, action, montant) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (user_id, crypto, float(quantite), float(price), action, float(amount)))
        
        conn.commit()
        conn.close()
        
        flash(f"Opération {action} réussie pour {crypto}", 'success')
        return redirect('/sim')
    
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        flash("Une erreur est survenue lors de l'opération", 'danger')
        return redirect('/sim')
@app.route('/summary')
def portfolio_summary():
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Récupération du solde
        cursor.execute("SELECT solde FROM solde WHERE user_id = %s", (user_id,))
        solde_data = cursor.fetchone()
        solde = Decimal(str(solde_data['solde'])) if solde_data else Decimal('0')

        # 2. Récupération du portefeuille avec vérification des données
        cursor.execute("""
            SELECT 
                p.id,
                p.crypto_name,
                p.quantite,
                p.prix_achat,
                c.Price as prix_actuel,
                (c.Price - p.prix_achat) * p.quantite as profit,
                ((c.Price - p.prix_achat) / p.prix_achat) * 100 as pourcentage_profit
            FROM portefeuille p
            LEFT JOIN crypto_data c ON p.crypto_name = c.Name
            WHERE p.user_id = %s
        """, (user_id,))
        
        assets = cursor.fetchall()
        
        # Conversion et calcul des valeurs
        portefeuille = []
        total_value = Decimal('0')
        total_investment = Decimal('0')
        
        for asset in assets:
            # Conversion des valeurs en Decimal
            quantite = Decimal(str(asset['quantite']))
            prix_achat = Decimal(str(asset['prix_achat']))
            prix_actuel = Decimal(str(asset['prix_actuel'])) if asset['prix_actuel'] else Decimal('0')
            
            # Calcul des valeurs
            valeur_actuelle = prix_actuel * quantite
            profit =  valeur_actuelle - ( prix_achat*quantite)
            
            portefeuille.append({
                'id': asset['id'],
                'crypto': asset['crypto_name'],
                'quantite': quantite,
                'prix_achat': prix_achat,
                'prix_actuel': prix_actuel,
                'valeur_actuelle': valeur_actuelle,
                'profit': profit,
                'pourcentage_profit': asset['pourcentage_profit']
            })
            
            total_value += valeur_actuelle
            total_investment += prix_achat * quantite
        
        total_assets = total_value + solde
        performance = ((total_value - total_investment) / total_investment * 100) if total_investment > 0 else 0
        
        # Debug avant envoi au template
        print("Données envoyées au template:")
        print("Portefeuille:", portefeuille)
        print("Solde:", solde)
        print("Total Value:", total_value)
        print("Performance:", performance)
        
        conn.close()
        
        return render_template(
            "summary.html",
            assets=portefeuille,  # Changé de 'portefeuille' à 'assets' pour plus de clarté
            solde=solde,
            valeur_crypto=total_value,  # Renommé pour cohérence
            valeur_totale=total_assets,
            performance=performance
        )
        
    except Exception as e:
        print(f"Erreur critique: {str(e)}")
        return render_template(
            "summary.html",
            assets=[],
            solde=0,
            valeur_crypto=0,
            valeur_totale=0,
            performance=0
        )
# Handler de connexion WebSocket
@socketio.on('connect', namespace='/summary')
def handle_summary_connect():
    if 'user_id' not in session:
        print("Connexion rejetée : pas de user_id")
        return False
    user_id = session['user_id']
    join_room(str(user_id))  # <-- Cette ligne manque
    print(f"Client connecté (user_id: {user_id})")
    emit('summary_connected', {'status': 'connected'})

# Fonction de mise à jour en arrière-plan
def background_portfolio_updater():
    while True:
        with app.app_context():
            try:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                
                # Récupérer tous les utilisateurs avec un portefeuille
                cursor.execute("SELECT DISTINCT user_id FROM portefeuille")
                users = cursor.fetchall()
                
                for user in users:
                    user_id = user['user_id']
                    try:
                        # 1. Récupération du solde
                        cursor.execute("SELECT solde FROM solde WHERE user_id = %s", (user_id,))
                        solde_data = cursor.fetchone()
                        solde = float(solde_data['solde']) if solde_data else 0.0

                        # 2. Récupération du portefeuille
                        cursor.execute("""
                            SELECT 
                                p.crypto_name as crypto, 
                                p.quantite, 
                                p.prix_achat, 
                                c.Price as prix_actuel,
                                (c.Price - p.prix_achat) * p.quantite as profit,
                                ((c.Price - p.prix_achat) / p.prix_achat) * 100 as pourcentage_profit
                            FROM portefeuille p
                            LEFT JOIN crypto_data c ON p.crypto_name = c.Name
                            WHERE p.user_id = %s
                        """, (user_id,))
                        assets = cursor.fetchall()
                        
                        # Calcul des totaux
                        total_value = 0.0
                        total_investment = 0.0
                        assets_serializable = []
                        
                        for asset in assets:
                            prix_actuel = float(asset['prix_actuel']) if asset['prix_actuel'] else 0.0
                            quantite = float(asset['quantite'])
                            prix_achat = float(asset['prix_achat'])
                            
                            valeur_actuelle = prix_actuel * quantite
                            profit = valeur_actuelle - (prix_achat * quantite)
                            
                            assets_serializable.append({
                                'crypto': asset['crypto'],
                                'quantite': quantite,
                                'prix_achat': prix_achat,
                                'prix_actuel': prix_actuel,
                                'profit': profit,
                                'pourcentage_profit': (profit / (prix_achat * quantite)) * 100 if prix_achat * quantite > 0 else 0,
                                'valeur_actuelle': valeur_actuelle
                            })
                            
                            total_value += valeur_actuelle
                            total_investment += prix_achat * quantite
                        
                        total_assets = total_value + solde
                        performance = ((total_value - total_investment) / total_investment * 100) if total_investment > 0 else 0
                        
                        # Envoi des données
                        socketio.emit('portfolio_update', {
                            'assets': assets_serializable,
                            'solde': solde,
                            'valeur_crypto': total_value,
                            'valeur_totale': total_assets,
                            'performance': performance
                        }, namespace='/summary', room=str(user_id))
                        
                    except Exception as e:
                        print(f"Erreur user {user_id}: {e}")
                        continue
                
                conn.close()
            except Exception as e:
                print(f"Erreur globale updater: {e}")
                if 'conn' in locals():
                    conn.close()
        
        socketio.sleep(10)  # Attendre 10 secondes entre les mises à jour
@app.route('/graph')
def performance_graph():
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Récupération des trades
        cursor.execute("""
            SELECT t.*, c.Price as current_price_at_time
            FROM trades t
            JOIN crypto_data c ON t.crypto = c.Name AND DATE(t.date) = DATE(c.last_updated)
            WHERE t.user_id = %s
            ORDER BY t.date
        """, (user_id,))
        trades = cursor.fetchall()
        
        # Récupération de l'historique du solde
        cursor.execute("""
            SELECT date, solde 
            FROM solde_history 
            WHERE user_id = %s
            ORDER BY date
        """, (user_id,))
        solde_history = cursor.fetchall()
        
        conn.close()
        
        if not trades:
            return render_template("graph.html", plot_div="<p>Aucune donnée de trading disponible</p>")
        
        # Préparation des données pour le graphique
        data = []
        
        # Graphique de la valeur du portefeuille dans le temps
        dates = []
        portfolio_values = []
        current_holdings = {}
        current_solde = Decimal('10000')  # Valeur initiale
        
        for trade in trades:
            date = trade['date']
            crypto = trade['crypto']
            action = trade['action']
            quantite = Decimal(str(trade['quantite']))
            prix = Decimal(str(trade['prix']))
            
            if crypto not in current_holdings:
                current_holdings[crypto] = Decimal('0')
            
            if action == 'buy':
                current_holdings[crypto] += quantite
                current_solde -= Decimal(str(trade['montant']))
            else:
                current_holdings[crypto] -= quantite
                current_solde += Decimal(str(trade['montant']))
            
            # Calcul de la valeur actuelle du portefeuille
            portfolio_value = current_solde
            for crypto_name, qty in current_holdings.items():
                if qty > 0:
                    portfolio_value += qty * Decimal(str(trade['current_price_at_time']))
            
            dates.append(date)
            portfolio_values.append(float(portfolio_value))
        
        # Ajout de la courbe de performance
        performance_trace = go.Scatter(
            x=dates,
            y=portfolio_values,
            mode='lines+markers',
            name='Valeur du portefeuille',
            line=dict(color='green')
        )
        data.append(performance_trace)
        
        # Graphique du solde dans le temps
        if solde_history:
            solde_dates = [item['date'] for item in solde_history]
            solde_values = [float(Decimal(str(item['solde']))) for item in solde_history]
            
            solde_trace = go.Scatter(
                x=solde_dates,
                y=solde_values,
                mode='lines',
                name='Solde USD',
                line=dict(color='blue')
            )
            data.append(solde_trace)
        
        layout = go.Layout(
            title="Performance du Portefeuille",
            xaxis=dict(title="Date"),
            yaxis=dict(title="Valeur (USD)"),
            hovermode='closest'
        )
        
        fig = go.Figure(data=data, layout=layout)
        plot_div = pio.to_html(fig, full_html=False)
        
        return render_template("graph.html", plot_div=plot_div)
    
    except Exception as e:
        return render_template("graph.html", plot_div="<p>Erreur lors du chargement du graphique</p>")
    
def check_and_close_liquidated_positions(user_id):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Récupérer toutes les positions ouvertes avec leur prix actuel
        cursor.execute("""
            SELECT fp.id, fp.position_type, fp.quantity, fp.entry_price, 
                   fp.liquidation_price, fp.margin, fp.leverage,
                   c.Price as current_price
            FROM futures_positions fp
            JOIN crypto_data c ON fp.crypto = c.Name
            WHERE fp.user_id = %s AND fp.status = 'open'
            FOR UPDATE
        """, (user_id,))
        positions = cursor.fetchall()
        
        for position in positions:
            current_price = Decimal(str(position['current_price']))
            liquidation_price = Decimal(str(position['liquidation_price']))
            
            # Vérifier si la position doit être liquidée
            if ((position['position_type'] == 'long' and current_price <= liquidation_price) or
                (position['position_type'] == 'short' and current_price >= liquidation_price)):
                
                # Calcul du PnL
                if position['position_type'] == 'long':
                    pnl = (current_price - Decimal(str(position['entry_price']))) * Decimal(str(position['quantity']))
                else:
                    pnl = (Decimal(str(position['entry_price'])) - current_price) * Decimal(str(position['quantity']))
                
                # Fermeture de la position
                cursor.execute("""
                    UPDATE futures_positions 
                    SET status = 'liquidated', 
                        closed_at = NOW(), 
                        pnl = %s,
                        close_price = %s
                    WHERE id = %s
                """, (str(pnl), str(current_price), position['id']))
                
                # Enregistrement dans l'historique
                cursor.execute("""
                    INSERT INTO futures_history 
                    (position_id, user_id, action, price, quantity, leverage, pnl)
                    VALUES (%s, %s, 'liquidate', %s, %s, %s, %s)
                """, (
                    position['id'], 
                    user_id, 
                    str(current_price * position['leverage']),
                    str(position['quantity']),
                    position['leverage'],
                    str(pnl)
                ))
                
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()
    
@app.route('/futures')
def futures_trading():
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    check_and_close_liquidated_positions(user_id)
    
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    # Récupération des cryptos disponibles
    cursor.execute("SELECT Name,Symbol, Price FROM crypto_data LIMIT 50")
    cryptos = cursor.fetchall()
    
    # Crypto sélectionnée (par défaut la première)
    selected_crypto = request.args.get('crypto', cryptos[0]['Symbol'] if cryptos else 'BTC')
    
    # Récupération des positions ouvertes avec calcul précis du PnL
    cursor.execute("""
    SELECT fp.*, c.Price as current_price,
           CASE 
               WHEN fp.position_type = 'long' THEN (c.Price - fp.entry_price) * fp.quantity
               ELSE (fp.entry_price - c.Price) * fp.quantity
           END as unrealized_pnl,
           fp.entry_price * fp.leverage as display_entry_price,
           c.Price * fp.leverage as display_current_price,
           fp.liquidation_price * fp.leverage as display_liquidation_price
    FROM futures_positions fp
    JOIN crypto_data c ON fp.crypto = c.Name
    WHERE fp.user_id = %s AND fp.status = 'open'
    """, (user_id,))
    positions = cursor.fetchall()
    
    # Calcul du solde et de la marge utilisée
    cursor.execute("SELECT solde FROM solde WHERE user_id = %s", (user_id,))
    solde_result = cursor.fetchone()
    solde = Decimal(str(solde_result['solde'])) if solde_result else Decimal('0')
    
    cursor.execute("""
        SELECT COALESCE(SUM(margin), 0) as used_margin 
        FROM futures_positions 
        WHERE user_id = %s AND status = 'open'
    """, (user_id,))
    used_margin = Decimal(str(cursor.fetchone()['used_margin']))
    
    conn.close()
    
    return render_template(
        "futures.html",
        cryptos=cryptos,
        positions=positions,
        solde=solde,
        used_margin=used_margin,
        available_margin=solde - used_margin,
        selected_crypto=selected_crypto
    )
@app.route('/check_liquidations')
def check_liquidations():
    # Cette route peut être appelée par un cron job toutes les minutes
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Récupérer tous les utilisateurs avec des positions ouvertes
        cursor.execute("SELECT DISTINCT user_id FROM futures_positions WHERE status = 'open'")
        users = cursor.fetchall()
        
        for user in users:
            check_and_close_liquidated_positions(user['user_id'])
            
        return "Liquidations vérifiées", 200
    except Exception as e:
        return f"Erreur: {str(e)}", 500
    finally:
        conn.close()

# WebSocket handlers pour futures
# WebSocket handlers pour futures
@socketio.on('connect', namespace='/futures')
def handle_futures_connect():
    if 'user_id' not in session:
        return False
    user_id = session['user_id']
    join_room(str(user_id))
    print(f"Client futures connecté (user_id: {user_id})")
    emit('futures_connected', {'status': 'connected'})

# Tâche en arrière-plan pour les mises à jour futures
def background_futures_updater():
    while True:
        with app.app_context():
            try:
                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                
                # Récupérer tous les utilisateurs avec des positions futures
                cursor.execute("SELECT DISTINCT user_id FROM futures_positions WHERE status = 'open'")
                users = cursor.fetchall()
                
                for user in users:
                    user_id = user['user_id']
                    try:
                        # 1. Récupération du solde et marge
                        cursor.execute("SELECT solde FROM solde WHERE user_id = %s", (user_id,))
                        solde_result = cursor.fetchone()
                        solde = float(solde_result['solde']) if solde_result else 0.0

                        cursor.execute("""
                            SELECT COALESCE(SUM(margin), 0) as used_margin 
                            FROM futures_positions 
                            WHERE user_id = %s AND status = 'open'
                        """, (user_id,))
                        used_margin = float(cursor.fetchone()['used_margin'])
                        
                        # 2. Récupération des positions
                        cursor.execute("""
                            SELECT fp.*, c.Price as current_price,
                                   CASE 
                                       WHEN fp.position_type = 'long' THEN (c.Price - fp.entry_price) * fp.quantity
                                       ELSE (fp.entry_price - c.Price) * fp.quantity
                                   END as unrealized_pnl
                            FROM futures_positions fp
                            JOIN crypto_data c ON fp.crypto = c.Name
                            WHERE fp.user_id = %s AND fp.status = 'open'
                        """, (user_id,))
                        positions = cursor.fetchall()
                        
                        # Conversion pour sérialisation
                        positions_serializable = []
                        for pos in positions:
                            positions_serializable.append({
                                'id': pos['id'],
                                'crypto': pos['crypto'],
                                'position_type': pos['position_type'],
                                'quantity': float(pos['quantity']),
                                'entry_price': float(pos['entry_price']),
                                'current_price': float(pos['current_price']),
                                'leverage': pos['leverage'],
                                'unrealized_pnl': float(pos['unrealized_pnl']),
                                'liquidation_price': float(pos['liquidation_price'])
                            })
                        
                        # Envoi des données
                        socketio.emit('futures_update', {
                            'positions': positions_serializable,
                            'solde': solde,
                            'used_margin': used_margin,
                            'available_margin': solde - used_margin
                        }, namespace='/futures', room=str(user_id))
                        
                    except Exception as e:
                        print(f"Erreur futures user {user_id}: {e}")
                        continue
                
                conn.close()
            except Exception as e:
                print(f"Erreur globale futures updater: {e}")
                if 'conn' in locals():
                    conn.close()
        
        socketio.sleep(5)  # Mise à jour toutes les 5 secondes
@app.route('/open_future', methods=['POST'])
def open_future_position():
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    data = request.form
    
    try:
        crypto = data['crypto']
        quantity = Decimal(data['quantity'])
        position_type = data['position_type']
        leverage = int(data['leverage'])
        
        if quantity <= 0:
            flash("La quantité doit être positive", 'danger')
            return redirect('/futures')
        
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Récupération du prix actuel
        cursor.execute("SELECT Price FROM crypto_data WHERE Name = %s", (crypto,))
        result = cursor.fetchone()
        if not result:
            conn.close()
            flash("Cryptomonnaie introuvable", 'danger')
            return redirect('/futures')
        
        current_price = Decimal(str(result['Price']))
        margin = (quantity * current_price) / Decimal(leverage)
        
        # 2. Vérification du solde
        cursor.execute("SELECT solde FROM solde WHERE user_id = %s FOR UPDATE", (user_id,))
        solde_result = cursor.fetchone()
        if not solde_result:
            conn.close()
            flash("Solde introuvable", 'danger')
            return redirect('/futures')
            
        solde = Decimal(str(solde_result['solde']))
        
        cursor.execute("""
            SELECT COALESCE(SUM(margin), 0) as used_margin 
            FROM futures_positions 
            WHERE user_id = %s AND status = 'open'
        """, (user_id,))
        used_margin = Decimal(str(cursor.fetchone()['used_margin']))
        
        if margin > (solde - used_margin):
            conn.close()
            flash("Marge insuffisante pour ouvrir cette position", 'danger')
            return redirect('/futures')
        
        # 3. Calcul du prix de liquidation
        if position_type == 'long':
            liquidation_price = current_price * (Decimal('1') - (Decimal('1') / Decimal(leverage)))
        else:
            liquidation_price = current_price * (Decimal('1') + (Decimal('1') / Decimal(leverage)))
        
        # 4. Ouverture de la position (avec commit immédiat)
        cursor.execute("""
            INSERT INTO futures_positions 
            (user_id, crypto, quantity, entry_price, position_type, leverage, liquidation_price, margin)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            user_id, 
            crypto, 
            str(quantity),
            str(current_price),
            position_type,
            leverage,
            str(liquidation_price),
            str(margin)
        ))
        
        # Important: Récupération de l'ID avant commit
        position_id = cursor.lastrowid
        conn.commit()  # Premier commit pour s'assurer que la position existe
        
        # 5. Maintenant insertion dans l'historique
        try:
            cursor.execute("""
                INSERT INTO futures_history 
                (position_id, user_id, action, price, quantity, leverage)
                VALUES (%s, %s, 'open', %s, %s, %s)
            """, (
                position_id, 
                user_id, 
                str(current_price), 
                str(quantity),
                leverage
            ))
            
            # 6. Débit du solde
            cursor.execute("""
                UPDATE solde 
                SET solde = solde - %s 
                WHERE user_id = %s
            """, (str(margin), user_id))
            
            conn.commit()
            flash(f"Position {position_type} ouverte avec succès (levier: {leverage}x)", 'success')
            
        except Exception as e:
            conn.rollback()
            # Si l'insertion dans l'historique échoue, supprimer la position créée
            cursor.execute("DELETE FROM futures_positions WHERE id = %s", (position_id,))
            conn.commit()
            raise e
            
        return redirect('/futures')
    
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        flash(f"Erreur lors de l'ouverture de la position: {str(e)}", 'danger')
        return redirect('/futures')
@app.route('/close_future/<int:position_id>')
def close_future_position(position_id):
    if 'user_id' not in session:
        return redirect('/login')
    
    user_id = session['user_id']
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        
        # Récupération de la position avec verrouillage
        cursor.execute("""
            SELECT fp.*, c.Price as current_price
            FROM futures_positions fp
            JOIN crypto_data c ON fp.crypto = c.Name
            WHERE fp.id = %s AND fp.user_id = %s AND fp.status = 'open'
            FOR UPDATE
        """, (position_id, user_id))
        position = cursor.fetchone()
        
        if not position:
            conn.close()
            flash("Position introuvable ou déjà fermée", 'danger')
            return redirect('/futures')
        
        # Conversion en Decimal pour les calculs précis
        current_price = Decimal(str(position['current_price']))
        entry_price = Decimal(str(position['entry_price']))
        quantity = Decimal(str(position['quantity']))
        leverage = position['leverage']
        margin = Decimal(str(position['margin']))
        
        # Calcul précis du PnL (sans levier)
        if position['position_type'] == 'long':
            pnl = (current_price - entry_price) * quantity
        else:
            pnl = (entry_price - current_price) * quantity
        
        # Application du levier uniquement pour l'affichage et le crédit
        #pnl_with_leverage = pnl * leverage
        
        # Fermeture de la position
        cursor.execute("""
            UPDATE futures_positions 
            SET status = 'closed', 
                closed_at = NOW(), 
                pnl = %s,
                close_price = %s
            WHERE id = %s
        """, (str(pnl), str(current_price), position_id))
        
        # Mise à jour du solde
        cursor.execute("""
            UPDATE solde 
            SET solde = solde + %s 
            WHERE user_id = %s
        """, (str(pnl + margin), user_id))
        
        # Enregistrement dans l'historique
        cursor.execute("""
            INSERT INTO futures_history 
            (position_id, user_id, action, price, quantity, leverage, pnl)
            VALUES (%s, %s, 'close', %s, %s, %s, %s)
        """, (
            position_id, 
            user_id, 
            str(current_price * leverage),  # Prix affiché avec levier
            str(quantity),
            leverage,
            str(pnl)
        ))
        
        conn.commit()
        conn.close()
        
        flash(f"Position fermée. PnL: {pnl:.2f}$ ({'Profit' if pnl >= 0 else 'Perte'})", 'success')
        return redirect('/futures')
    
    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        flash(f"Erreur lors de la fermeture de la position: {str(e)}", 'danger')
        return redirect('/futures')
    
@app.route('/vid', methods=['GET', 'POST'])
def vid():
    video_url = None
    script = None
    prompt = None
    
    if request.method == 'POST':
        prompt = request.form.get('prompt')
        if prompt:
            # Générer le script
            script = generate_script(prompt)
            
            # Obtenir l'URL de la vidéo
            video_url = generate_video_from_prompt(prompt)
    
    return render_template(
        'vid.html',
        video_url=video_url,
        script=script,
        prompt=prompt
    )



# 🚀 Fonction pour déclencher le DAG Airflow
def trigger_airflow_dag(pdf_content):
    data = {"conf": {"pdf_file": pdf_content}}
    response = requests.post(
        AIRFLOW_API_URL,
        json=data,
        auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
        headers={"Content-Type": "application/json"},
    )
    return response

# Démarrer l'application Flask
if __name__ == "__main__":
    # Créer le dossier des sessions si inexistant
    if not os.path.exists(app.config['SESSION_FILE_DIR']):
        os.makedirs(app.config['SESSION_FILE_DIR'])
    socketio.start_background_task(update_crypto_prices)
    socketio.start_background_task(background_portfolio_updater)
    socketio.start_background_task(background_futures_updater)



    app.run(host="0.0.0.0", port=5000, debug=True)
   
