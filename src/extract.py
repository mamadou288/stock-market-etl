import os
import requests
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

API_KEY = os.getenv("API_KEY")

def fetch_stock_data(symbol, interval="5min"):
    """Récupère les données de stock depuis Alpha Vantage."""
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": API_KEY  # Utilisation de la clé API depuis .env
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Afficher toute la réponse de l'API pour le débogage
    print("Données reçues : ", data)

    if "Time Series (5min)" in data:
        return data["Time Series (5min)"]
    else:
        print(f"❌ Erreur lors de la récupération des données pour {symbol}")
        return None
