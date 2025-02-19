import pandas as pd

def transform_stock_data(raw_data):
    """Transforme les données brutes en DataFrame."""
    try:
        # Debug: afficher la structure des données reçues
        print("\nDébug - Structure des données reçues:")
        print(f"Type: {type(raw_data)}")
        
        # Si les données sont déjà une série temporelle, on les utilise directement
        if isinstance(raw_data, dict) and all(isinstance(k, str) and ':' in k for k in raw_data.keys()):
            time_series = raw_data
        # Sinon, on cherche la clé 'Time Series (5min)'
        elif isinstance(raw_data, dict) and 'Time Series (5min)' in raw_data:
            time_series = raw_data['Time Series (5min)']
        else:
            print("❌ Format de données invalide")
            print(f"Clés disponibles: {raw_data.keys() if isinstance(raw_data, dict) else 'Pas un dictionnaire'}")
            return None
        
        # Debug: afficher un exemple de données temporelles
        print("\nDébug - Exemple de données temporelles:")
        first_key = next(iter(time_series))
        print(f"Premier timestamp: {first_key}")
        print(f"Données: {time_series[first_key]}")
        
        # Convertir en DataFrame
        df = pd.DataFrame.from_dict(time_series, orient='index')
        
        # Debug: afficher la structure du DataFrame
        print("\nDébug - Structure du DataFrame:")
        print(f"Colonnes: {df.columns.tolist()}")
        print(f"Premières lignes:\n{df.head()}")
        
        # Renommer les colonnes pour plus de clarté
        df.columns = [col.split('. ')[1] for col in df.columns]
        
        # Convertir les types de données en float
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            df[col] = df[col].astype(float)
        
        # L'index est déjà la date/heure, on s'assure juste qu'il est au bon format
        df.index = pd.to_datetime(df.index)
        
        print(f"✅ Données transformées avec succès ({len(df)} entrées)")
        return df

    except Exception as e:
        print(f"❌ Erreur de transformation : {str(e)}")
        import traceback
        print(f"Traceback:\n{traceback.format_exc()}")
        return None
