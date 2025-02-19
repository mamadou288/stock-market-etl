import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from extract import fetch_stock_data
from transform import transform_stock_data
import time
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta

# Charger les variables d'environnement
load_dotenv()

# Configuration
ALPHA_VANTAGE_CONFIG = {
    "requests_per_minute": 15,
    "pause_time": 5  # secondes entre chaque requête
}

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# Configuration des horaires de mise à jour
SCHEDULE_CONFIG = {
    "market_hours": {
        "start": "00:00",  # Pour test: toute la journée
        "end": "23:59",
    },
    "update_interval": 5,
}

def create_connection():
    """Cree une connexion a la base de donnees."""
    try:
        # Utiliser les variables d'environnement de DB_CONFIG
        conn = psycopg2.connect(
            **DB_CONFIG,
            client_encoding='utf8'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    except Exception as e:
        print(f"Erreur de connexion a la base de donnees : {str(e)}")
        return None

def save_to_postgresql(df, symbol):
    """Sauvegarde les donnees transformees dans la table stock_data."""
    conn = None
    cur = None
    try:
        # Connexion a PostgreSQL
        conn = create_connection()
        if not conn:
            return False
            
        cur = conn.cursor()

        # Preparation de la requete d'insertion
        insert_query = """
        INSERT INTO stock_data (timestamp, symbol, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (timestamp, symbol) 
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
        """

        # Preparation des donnees pour l'insertion
        records = [
            (
                index,
                symbol,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"])
            )
            for index, row in df.iterrows()
        ]

        # Execution de l'insertion par lots
        cur.executemany(insert_query, records)
        
        # Commit et fermeture des connexions
        conn.commit()
        print(f"+ {len(records)} lignes inserees/mises a jour pour {symbol}")
        return True

    except Exception as e:
        print(f"- Erreur lors de l'insertion des donnees : {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def load_multiple_stocks(symbols):
    """Charge les données pour plusieurs symboles boursiers."""
    successful_loads = 0
    failed_loads = 0
    
    for symbol in symbols:
        try:
            print(f"+ Chargement des donnees pour {symbol}...")
            raw_data = fetch_stock_data(symbol)
            
            if isinstance(raw_data, dict) and 'Information' in raw_data:
                # Limite d'API atteinte
                print("- Limite d'API Alpha Vantage atteinte")
                print("  Pour augmenter la limite, visitez: https://www.alphavantage.co/premium/")
                break
            
            if raw_data:
                df = transform_stock_data(raw_data)
                if df is not None and not df.empty:
                    save_to_postgresql(df, symbol)
                    successful_loads += 1
                    print(f"+ Donnees chargees avec succes pour {symbol}")
                else:
                    failed_loads += 1
                    print(f"- Echec de la transformation pour {symbol}")
            else:
                failed_loads += 1
                print(f"- Echec de l'extraction pour {symbol}")
                
            # Pause plus courte entre les requêtes (15 requêtes par minute maximum)
            time.sleep(5)
            
        except Exception as e:
            failed_loads += 1
            print(f"- Erreur lors du traitement de {symbol}: {e}")
            continue

    # Résumé final
    print("\nRésumé du chargement:")
    print(f"+ Symboles chargés avec succès: {successful_loads}")
    print(f"- Symboles en échec: {failed_loads}")

# Liste des symboles à charger
SYMBOLS = [
    "AAPL",   # Apple
    "MSFT",   # Microsoft
    "GOOGL",  # Google
    "AMZN",   # Amazon
    "META"    # Meta (Facebook)
]

def is_market_open():
    """Vérifie si le marché est ouvert."""
    now = datetime.now()
    
    # Vérifie si c'est un jour de semaine (0 = lundi, 4 = vendredi)
    if now.weekday() > 4:
        return False
    
    # Convertit les heures de marché en objets datetime
    market_start = datetime.strptime(SCHEDULE_CONFIG["market_hours"]["start"], "%H:%M").time()
    market_end = datetime.strptime(SCHEDULE_CONFIG["market_hours"]["end"], "%H:%M").time()
    current_time = now.time()
    
    return market_start <= current_time <= market_end

def scheduled_update():
    """Fonction exécutée à chaque mise à jour planifiée."""
    if is_market_open():
        print(f"\n=== Mise à jour du {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
        try:
            load_multiple_stocks(SYMBOLS)
        except Exception as e:
            print(f"- Erreur lors de la mise à jour automatique: {e}")
    else:
        print(f"= Marché fermé ({datetime.now().strftime('%H:%M')})")

if __name__ == "__main__":
    print("+ Démarrage du service de mise à jour automatique...")
    
    # Création du planificateur
    scheduler = BlockingScheduler()
    
    # Planification des mises à jour pendant les heures de marché
    scheduler.add_job(
        scheduled_update,
        trigger=CronTrigger(
            day_of_week='mon-fri',
            hour=f"{SCHEDULE_CONFIG['market_hours']['start'].split(':')[0]}-{SCHEDULE_CONFIG['market_hours']['end'].split(':')[0]}",
            minute=f"*/{SCHEDULE_CONFIG['update_interval']}"
        ),
        name='stock_market_update'
    )
    
    try:
        # Exécution immédiate d'une première mise à jour
        scheduled_update()
        
        # Démarrage du planificateur
        print("+ Service démarré - En attente des mises à jour planifiées")
        print(f"  Intervalle de mise à jour: {SCHEDULE_CONFIG['update_interval']} minutes")
        print(f"  Heures de marché: {SCHEDULE_CONFIG['market_hours']['start']} - {SCHEDULE_CONFIG['market_hours']['end']} EST")
        scheduler.start()
    except KeyboardInterrupt:
        print("\n! Service interrompu par l'utilisateur")
        scheduler.shutdown()
    except Exception as e:
        print(f"- Erreur du service: {e}")
        scheduler.shutdown()
