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

CSV_FILENAME = "dim_magasin.csv"  # nom du fichier source

def etl_magasins():
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
        required_cols = ['magasin_id', 'ville', 'region', 'surface_m2']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

        df['magasin_id'] = df['magasin_id'].astype(str).str.strip()
        df['ville'] = df['ville'].astype(str).str.strip()
        df['region'] = df['region'].astype(str).str.strip()
        df['surface_m2'] = pd.to_numeric(df['surface_m2'], errors='coerce')

        # Filtrer les lignes invalides
        df_valid = df.dropna(subset=['magasin_id', 'ville', 'region', 'surface_m2'])
        df_invalid = df[~df.index.isin(df_valid.index)]

        if not df_invalid.empty:
            rejected_file = os.path.join(REJECTED_DIR, f"magasins_rejected_{timestamp}.csv")
            df_invalid.to_csv(rejected_file, index=False)
            print(f"[WARN] {len(df_invalid)} lignes rejetées sauvegardées dans {rejected_file}")

        # ----------------------
        # 3️⃣ Chargement en staging
        # ----------------------
        conn = get_connection()
        cur = get_cursor(conn)

        cur.execute("""
            CREATE TEMP TABLE magasins_staging (
                magasin_id VARCHAR(50),
                ville VARCHAR(100),
                region VARCHAR(100),
                surface_m2 NUMERIC
            );
        """)
        conn.commit()

        extras = [(row.magasin_id, row.ville, row.region, row.surface_m2)
                  for idx, row in df_valid.iterrows()]
        if extras:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO magasins_staging (magasin_id, ville, region, surface_m2) VALUES %s",
                extras
            )
            conn.commit()

        # ----------------------
        # 4️⃣ Merge staging -> table principale
        # ----------------------
        cur.execute("""
            INSERT INTO localisation (magasin_id, ville, region, surface_m2)
            SELECT s.magasin_id, s.ville, s.region, s.surface_m2
            FROM magasins_staging s
            ON CONFLICT (magasin_id)
            DO UPDATE SET
                ville = EXCLUDED.ville,
                region = EXCLUDED.region,
                surface_m2 = EXCLUDED.surface_m2;
        """)
        conn.commit()

        # ----------------------
        # 5️⃣ Export processed
        # ----------------------
        processed_file = os.path.join(PROCESSED_DIR, f"magasins_processed_{timestamp}.csv")
        df_valid.to_csv(processed_file, index=False)
        print(f"[INFO] {len(df_valid)} lignes chargées dans la DB et sauvegardées dans {processed_file}")

    except Exception as e:
        print(f"[ERROR] ETL magasins échoué : {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    etl_magasins()
