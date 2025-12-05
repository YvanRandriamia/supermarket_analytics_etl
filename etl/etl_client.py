import pandas as pd 
import os
from datetime import datetime
import psycopg2
import psycopg2.extras
from db import get_connection, get_cursor


# Chemins des fichiers
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
REJECTED_DIR = "data/rejected"

CSV_FILENAME = "dim_clients.csv"  # nom du fichier source

def etl_clients():
    # Créer les dossiers si n'existent pas
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    raw_path = os.path.join(RAW_DIR, CSV_FILENAME)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    try:
        # ----------------------
        # 1️⃣ Extraction
        # ----------------------
        df = pd.read_csv(raw_path)
        print(f"[INFO] {len(df)} lignes lues depuis {raw_path}")

        # ----------------------
        # 2️⃣ Transformation / nettoyage
        # ----------------------
        required_cols = ['client_id', 'nom', 'age', 'ville', 'categorie_client']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

        df['nom'] = df['nom'].astype(str).str.strip()
        df['ville'] = df['ville'].astype(str).str.strip()
        df['age'] = pd.to_numeric(df['age'], errors='coerce')

        # Normaliser les catégories pour respecter la contrainte CHECK
        df['categorie_client'] = df['categorie_client'].astype(str).str.upper()
        df['categorie_client'] = df['categorie_client'].replace({
            'VIP': 'VIP',
            'PARTICULIER': 'Particulier',
            'ENTREPRISE': 'Entreprise'
        })

        # Filtrer les lignes invalides
        valid_categories = ['VIP', 'Particulier', 'Entreprise']
        df_valid = df.dropna(subset=['client_id', 'nom', 'age', 'categorie_client'])
        df_valid = df_valid[df_valid['categorie_client'].isin(valid_categories)]
        df_invalid = df[~df.index.isin(df_valid.index)]

        if not df_invalid.empty:
            rejected_file = os.path.join(REJECTED_DIR, f"clients_rejected_{timestamp}.csv")
            df_invalid.to_csv(rejected_file, index=False)
            print(f"[WARN] {len(df_invalid)} lignes rejetées sauvegardées dans {rejected_file}")

        # ----------------------
        # 3️⃣ Chargement en staging
        # ----------------------
        conn = get_connection()
        cur = get_cursor(conn)

        cur.execute("""
            CREATE TEMP TABLE clients_staging (
                client_id INT,
                nom VARCHAR(255),
                age INT,
                ville VARCHAR(100),
                categorie_client VARCHAR(50)
            );
        """)
        conn.commit()

        extras = [(row.client_id, row.nom, int(row.age), row.ville, row.categorie_client) 
                  for idx, row in df_valid.iterrows()]
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO clients_staging (client_id, nom, age, ville, categorie_client) VALUES %s",
            extras
        )
        conn.commit()

        # ----------------------
        # 4️⃣ Merge staging -> table principale
        # ----------------------
        cur.execute("""
            INSERT INTO clients (client_id, nom, age, ville, categorie_client)
            SELECT s.client_id, s.nom, s.age, s.ville, s.categorie_client
            FROM clients_staging s
            ON CONFLICT (client_id)
            DO UPDATE SET
                nom = EXCLUDED.nom,
                age = EXCLUDED.age,
                ville = EXCLUDED.ville,
                categorie_client = EXCLUDED.categorie_client;
        """)
        conn.commit()

        # ----------------------
        # 5️⃣ Export processed
        # ----------------------
        processed_file = os.path.join(PROCESSED_DIR, f"clients_processed_{timestamp}.csv")
        df_valid.to_csv(processed_file, index=False)
        print(f"[INFO] {len(df_valid)} lignes chargées dans la DB et sauvegardées dans {processed_file}")

    except Exception as e:
        print(f"[ERROR] ETL clients échoué : {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    etl_clients()
