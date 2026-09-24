import matplotlib.pyplot as plt
from io import BytesIO
import base64
from datetime import datetime, timedelta
import random
import mysql.connector
import numpy as np
import os
import pandas as pd
from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM, Dropout
from keras.callbacks import EarlyStopping, ModelCheckpoint
import keras_tuner
from keras_tuner import RandomSearch
from pycoingecko import CoinGeckoAPI
import time
from sklearn.metrics import mean_absolute_error

# Initialisation de l'API CoinGecko
cg = CoinGeckoAPI()

def generate_crypto_chart():
    """Génère le graphique de répartition crypto"""
    conn = None
    cursor = None
    try:
        # 1. Connexion à MySQL
        conn = mysql.connector.connect(
            host="mysql", 
            user="root",
            password="root",
            database="crypto_db"
        )
        cursor = conn.cursor(dictionary=True)
        
        # 2. Récupération des top 5 cryptos
        cursor.execute("""
            SELECT 
                Name, 
                Symbol, 
                MarketCap,
                ROUND((MarketCap / (SELECT SUM(MarketCap) FROM crypto_data WHERE MarketCap IS NOT NULL)) * 100, 1) as Percentage
            FROM crypto_data 
            WHERE MarketCap IS NOT NULL
            ORDER BY MarketCap DESC 
            LIMIT 5
        """)
        cryptos = cursor.fetchall()
        
        if not cryptos:
            print("Aucune donnée trouvée dans crypto_data")
            return None

        # 3. Préparation données pour le pie chart
        names = [f"{crypto['Name']} ({crypto['Symbol']})" for crypto in cryptos]
        percentages = [crypto['Percentage'] for crypto in cryptos]
        colors = ['#F7931A', '#627EEA', '#23292F', '#F0B90B', '#00FFA3']
        
        # 4. Création du graphique
        plt.style.use('seaborn-v0_8')
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 12))
        
        # - Graphique 1: Parts de marché
        bars = ax1.bar(names, percentages, color=colors)
        ax1.set_title('TOP 5 CRYPTOMONNAIES PAR PART DE MARCHÉ', fontsize=14, pad=20, fontweight='bold')
        ax1.set_ylabel('Part de marché (%)', fontsize=10)
        
        # Ajout des valeurs sur les barres
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2, height,
                    f'{height:.1f}%', 
                    ha='center', va='bottom', fontweight='bold')

        # - Graphique 2: Évolution simulée
        ax2.set_title('ÉVOLUTION SIMULÉE DU MARCHÉ (30 JOURS)', fontsize=14, pad=20, fontweight='bold')
        ax2.set_ylabel('Capitalisation ($ Milliards)', fontsize=10)
        
        # Génération de données simulées plus réalistes
        end_value = cryptos[0]['MarketCap'] / 1e9  # Utilise le market cap de la première crypto comme référence
        start_value = end_value * 0.8  # -20%
        volatility = end_value * 0.05  # 5% de volatilité
        
        dates = [datetime.now() - timedelta(days=i) for i in range(30, -1, -1)]
        values = np.linspace(start_value, end_value, 31)
        values = values + np.random.uniform(-volatility, volatility, size=31)
        
        ax2.plot(dates, values, color='#F7931A', linewidth=2.5, marker='o', markersize=4)
        ax2.grid(True, linestyle='--', alpha=0.7)
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        # 5. Conversion en image base64
        buffer = BytesIO()
        plt.savefig(buffer, format='png', dpi=120, bbox_inches='tight')
        buffer.seek(0)
        image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
        plt.close()  # Important pour libérer la mémoire
        return image_base64
        
    except Exception as e:
        print(f"ERREUR GRAPHIQUE: {str(e)}", flush=True)
        return None
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()

# Configuration
BASE_DAYS = 100  # Fenêtre de 365 jours pour le LSTM
N_FUTURE = 30    # Nombre de jours à prédire dans le futur
BOOTSTRAP_SAMPLES = 20  # Nombre d'échantillons pour l'intervalle de confiance

class CryptoAnalyzer:
    def __init__(self, upload_folder='static/images'):
        # Mapping des symboles vers les IDs CoinGecko
        self.CRYPTOS = {
            'BTC-USD': 'bitcoin',
            'XRP-USD': 'ripple',
            'SOL-USD': 'solana',
            'HBAR-USD': 'hedera-hashgraph'
        }
        self.upload_folder = upload_folder
        self.data_cache = {}  # Cache pour stocker les données téléchargées
        self.model_cache = {}  # Cache pour stocker les modèles entraînés
        os.makedirs(self.upload_folder, exist_ok=True)
        os.makedirs('keras_tuner', exist_ok=True)
        
    def get_crypto_data(self, crypto, days=364, force_update=True):
        """Récupère les données historiques depuis CoinGecko"""
        if not force_update and crypto in self.data_cache:
            return self.data_cache[crypto]
            
        try:
            crypto_id = self.CRYPTOS.get(crypto, crypto.lower())
            
            # Récupération des données avec taux limite
            time.sleep(1)  # Respecter les limites de l'API
            data = cg.get_coin_market_chart_by_id(
                id=crypto_id,
                vs_currency='usd',
                days=days
            )
            
            # Conversion en DataFrame
            df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('date', inplace=True)
            df = df[['price']].rename(columns={'price': 'Close'})
            
            self.data_cache[crypto] = df
            return df
            
        except Exception as e:
            raise ValueError(f"Erreur CoinGecko pour {crypto}: {str(e)}")
    
    def generate_plots(self, crypto):
        """Génère les graphiques de base (prix et moyennes mobiles)"""
        data = self.get_crypto_data(crypto)
        closing_price = data[['Close']]
        
        # Création des deux graphiques dans une seule figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
        
        # Graphique des prix de clôture
        ax1.plot(closing_price.index, closing_price['Close'], label='Close Price', color='blue', linewidth=2)
        ax1.set_title(f"Close price of {crypto} over time", fontsize=16)
        ax1.set_xlabel("Date", fontsize=14)
        ax1.set_ylabel('Price (USD)', fontsize=14)
        ax1.grid(alpha=0.3)
        ax1.legend(fontsize=12)
        
        # Graphique des moyennes mobiles
        ma_365 = closing_price['Close'].rolling(window=365).mean()
        ma_100 = closing_price['Close'].rolling(window=100).mean()
        
        ax2.plot(closing_price.index, closing_price['Close'], label='Close Price', color='blue', linewidth=2)
        ax2.plot(closing_price.index, ma_365, label='365 Days MA', color='red', linestyle="--", linewidth=2)
        ax2.plot(closing_price.index, ma_100, label='100 Days MA', color='green', linestyle="--", linewidth=2)
        ax2.set_title(f"Moving averages for {crypto}", fontsize=16)
        ax2.set_xlabel("Date", fontsize=14)
        ax2.set_ylabel('Price (USD)', fontsize=14)
        ax2.grid(alpha=0.3)
        ax2.legend(fontsize=12)
        
        # Sauvegarde des graphiques
        close_plot_path = os.path.join(self.upload_folder, f'{crypto}_close.png')
        ma_plot_path = os.path.join(self.upload_folder, f'{crypto}_ma.png')
        
        plt.tight_layout()
        fig.savefig(close_plot_path)
        ax1.clear()
        fig.savefig(ma_plot_path)
        plt.close(fig)
        
        return {
            'close_plot': close_plot_path.replace('static/', ''),
            'ma_plot': ma_plot_path.replace('static/', '')
        }
    
    def prepare_lstm_data(self, crypto):
        """Prépare les données pour le modèle LSTM"""
        if crypto in self.model_cache and 'prepared_data' in self.model_cache[crypto]:
            return self.model_cache[crypto]['prepared_data']
            
        data = self.get_crypto_data(crypto)
        closing_price = data[['Close']].dropna()
        
        # Vérification des données
        if len(closing_price) < BASE_DAYS + 1:
            raise ValueError(f"Pas assez de données pour {crypto}. Requis: {BASE_DAYS + 1}, Disponible: {len(closing_price)}")
        
        # Normalisation des données
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(closing_price[['Close']])
        
        # Préparation des séquences
        x_data = np.zeros((len(scaled_data) - BASE_DAYS, BASE_DAYS, 1))
        y_data = np.zeros((len(scaled_data) - BASE_DAYS, 1))
        
        for i in range(BASE_DAYS, len(scaled_data)):
            x_data[i-BASE_DAYS] = scaled_data[i-BASE_DAYS:i]
            y_data[i-BASE_DAYS] = scaled_data[i]
        
        result = (x_data, y_data, scaler, closing_price)
        
        if crypto not in self.model_cache:
            self.model_cache[crypto] = {}
        self.model_cache[crypto]['prepared_data'] = result
        
        return result
    
    def build_lstm_model(self, hp):
        """Construction optimisée du modèle LSTM"""
        model = Sequential()
        model.add(LSTM(
            units=hp.Int('units_1', min_value=64, max_value=128, step=32),
            return_sequences=True,
            input_shape=(BASE_DAYS, 1)
        ))
        model.add(Dropout(rate=hp.Float('dropout_1', min_value=0.2, max_value=0.4, step=0.1)))
        model.add(LSTM(
            units=hp.Int('units_2', min_value=32, max_value=64, step=32),
            return_sequences=False)
        )
        model.add(Dropout(rate=hp.Float('dropout_2', min_value=0.2, max_value=0.4, step=0.1)))
        model.add(Dense(25, activation='relu'))
        model.add(Dense(1))
        
        model.compile(optimizer="adam", loss="mse")
        return model
    
    def train_and_predict(self, crypto):
        """Entraîne le modèle et fait des prédictions avec intervalle de confiance"""
        # Vérifier si le modèle est déjà en cache
        if crypto in self.model_cache and 'model' in self.model_cache[crypto]:
            best_model = self.model_cache[crypto]['model']
            x_data, y_data, scaler, closing_price = self.model_cache[crypto]['prepared_data']
        else:
            x_data, y_data, scaler, closing_price = self.prepare_lstm_data(crypto)
            
            # Division des données
            train_size = int(len(x_data) * 0.9)
            x_train, y_train = x_data[:train_size], y_data[:train_size]
            x_test, y_test = x_data[train_size:], y_data[train_size:]
            
            # Optimisation des hyperparamètres
            tuner = RandomSearch(
                self.build_lstm_model,
                objective='val_loss',
                max_trials=3,
                executions_per_trial=2,
                directory='keras_tuner',
                project_name=f'{crypto}_tuning'
            )
            
            tuner.search(
                x_train, y_train,
                epochs=20,
                validation_data=(x_test, y_test),
                batch_size=64,
                callbacks=[EarlyStopping(monitor='val_loss', patience=3)]
            )
            
            # Récupération du meilleur modèle
            best_model = tuner.get_best_models(num_models=1)[0]
            
            # Entraînement final
            history = best_model.fit(
                x_train, y_train,
                batch_size=64,
                epochs=30,
                validation_data=(x_test, y_test),
                callbacks=[
                    EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True),
                    ModelCheckpoint(f'best_model_{crypto}.h5', monitor='val_loss', save_best_only=True)
                ],
                verbose=0
            )
            
            # Mise en cache du modèle
            if crypto not in self.model_cache:
                self.model_cache[crypto] = {}
            self.model_cache[crypto]['model'] = best_model
            
        # Prédictions sur l'ensemble de test
        train_size = int(len(x_data) * 0.9)
        x_test = x_data[train_size:]
        y_test = y_data[train_size:]
        
        predictions = best_model.predict(x_test, batch_size=64)
        inv_predictions = scaler.inverse_transform(predictions)
        inv_y_test = scaler.inverse_transform(y_test)
        
        # Calcul de l'erreur absolue moyenne (MAE)
        mae = mean_absolute_error(inv_y_test, inv_predictions)
        
        # Graphique des prédictions
        plotting_data = pd.DataFrame(
            {
                'Original': inv_y_test.flatten(),
                'Prediction': inv_predictions.flatten(),
            }, index=closing_price.index[train_size + BASE_DAYS:]
        )
        
        # Création des deux graphiques dans une seule figure
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 12))
        
        # Graphique des prédictions
        ax1.plot(plotting_data.index, plotting_data['Original'], label='Original', color='blue', linewidth=2)
        ax1.plot(plotting_data.index, plotting_data['Prediction'], label='Prediction', color='red', linewidth=2)
        ax1.fill_between(plotting_data.index, 
                        plotting_data['Prediction'] - mae, 
                        plotting_data['Prediction'] + mae,
                        color='red', alpha=0.2, label=f'Intervalle d\'erreur (±{mae:.2f})')
        ax1.set_title(f"LSTM Predictions for {crypto}", fontsize=16)
        ax1.set_xlabel("Date", fontsize=14)
        ax1.set_ylabel('Price (USD)', fontsize=14)
        ax1.grid(alpha=0.3)
        ax1.legend(fontsize=12)
        
        # Prédictions futures avec intervalle de confiance
        last_data = scaler.transform(closing_price[['Close']].dropna())[-BASE_DAYS:]
        future_predictions = []
        
        for _ in range(N_FUTURE):
            x_future = last_data[-BASE_DAYS:].reshape(1, BASE_DAYS, 1)
            future_price = best_model.predict(x_future, verbose=0, batch_size=1)
            future_predictions.append(future_price[0, 0])
            last_data = np.append(last_data, future_price, axis=0)
        
        future_predictions = scaler.inverse_transform(np.array(future_predictions).reshape(-1, 1))
        
        # Calcul des bornes supérieures et inférieures
        upper_bounds = future_predictions + mae
        lower_bounds = future_predictions - mae
        
        future_dates = pd.date_range(start=closing_price.index[-1], periods=N_FUTURE + 1, freq='B')[1:]
        future_df = pd.DataFrame({
            'Predicted_Close': future_predictions.flatten(),
            'Upper_Bound': upper_bounds.flatten(),
            'Lower_Bound': lower_bounds.flatten()
        }, index=future_dates)
        
        # Graphique des prédictions futures
        ax2.plot(closing_price.index[-100:], closing_price['Close'][-100:], label='Historical', color='blue', linewidth=2)
        ax2.plot(future_df.index, future_df['Predicted_Close'], label='Predicted', color='red', linewidth=2)
        ax2.fill_between(future_df.index, 
                        future_df['Lower_Bound'], 
                        future_df['Upper_Bound'],
                        color='red', alpha=0.2, label=f'Intervalle d\'erreur (±{mae:.2f})')
        ax2.set_title(f"Future Price Predictions for {crypto}", fontsize=16)
        ax2.set_xlabel("Date", fontsize=14)
        ax2.set_ylabel('Price (USD)', fontsize=14)
        ax2.grid(alpha=0.3)
        ax2.legend(fontsize=12)
        
        # Sauvegarde des graphiques
        pred_plot_path = os.path.join(self.upload_folder, f'{crypto}_predictions.png')
        future_plot_path = os.path.join(self.upload_folder, f'{crypto}_future.png')
        
        plt.tight_layout()
        fig.savefig(pred_plot_path)
        ax1.clear()
        fig.savefig(future_plot_path)
        plt.close(fig)
        
        # Formatage des prédictions avec intervalle d'erreur
        future_df['Formatted'] = future_df.apply(
            lambda row: f"{row['Predicted_Close']:.4f} ± {mae:.4f}",
            axis=1
        )
        
        return {
            'pred_plot': pred_plot_path.replace('static/', ''),
            'future_plot': future_plot_path.replace('static/', ''),
            'future_predictions': future_df[['Formatted']].tail(10).rename(
                columns={'Formatted': 'Prediction (USD)'}
            ).to_html(classes='table table-striped'),
            'last_update': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'mae': mae
        }