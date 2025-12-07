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

CSV_FILENAME = "dim_temps.csv"

def etl_temps():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(REJECTED_DIR, exist_ok=True)

    raw_path = os.path.join(RAW_DIR, CSV_FILENAME)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    try:
        # 1️⃣ Extraction
        df = pd.read_csv(raw_path)
        print(f"[INFO] {len(df)} lignes lues depuis {raw_path}")

        # 2️⃣ Transformation / nettoyage
        required_cols = ['date_id', 'jour', 'semaine', 'mois', 'annee']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

        df['date_id'] = pd.to_datetime(df['date_id'], errors='coerce').dt.date
        df['jour'] = pd.to_numeric(df['jour'], errors='coerce')
        df['semaine'] = pd.to_numeric(df['semaine'], errors='coerce')
        df['annee'] = pd.to_numeric(df['annee'], errors='coerce')
        df['mois'] = df['mois'].astype(str).str.strip()
        df['mois_num'] = pd.to_datetime(df['date_id']).dt.month

        # Filtrer lignes invalides
        df_valid = df.dropna(subset=['date_id', 'jour', 'semaine', 'mois', 'annee', 'mois_num'])
        df_invalid = df[~df.index.isin(df_valid.index)]

        if not df_invalid.empty:
            rejected_file = os.path.join(REJECTED_DIR, f"temps_rejected_{timestamp}.csv")
            df_invalid.to_csv(rejected_file, index=False)
            print(f"[WARN] {len(df_invalid)} lignes rejetées sauvegardées dans {rejected_file}")

        # 3️⃣ Staging
        conn = get_connection()
        cur = get_cursor(conn)

        cur.execute("""
            CREATE TEMP TABLE temps_staging (
                date_id DATE,
                jour INT,
                semaine INT,
                mois VARCHAR(20),
                annee INT,
                mois_num INT
            );
        """)
        conn.commit()

        extras = [
            (row.date_id, int(row.jour), int(row.semaine), row.mois, int(row.annee), int(row.mois_num))
            for idx, row in df_valid.iterrows()
        ]
        if extras:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO temps_staging (date_id, jour, semaine, mois, annee, mois_num) VALUES %s",
                extras
            )
            conn.commit()

        # 4️⃣ Merge staging -> table principale
        cur.execute("""
            INSERT INTO temps (date_id, jour, semaine, mois, annee, mois_num)
            SELECT s.date_id, s.jour, s.semaine, s.mois, s.annee, s.mois_num
            FROM temps_staging s
            ON CONFLICT (date_id)
            DO UPDATE SET
                jour = EXCLUDED.jour,
                semaine = EXCLUDED.semaine,
                mois = EXCLUDED.mois,
                annee = EXCLUDED.annee,
                mois_num = EXCLUDED.mois_num;
        """)
        conn.commit()

        # 5️⃣ Export processed
        processed_file = os.path.join(PROCESSED_DIR, f"temps_processed_{timestamp}.csv")
        df_valid.to_csv(processed_file, index=False)
        print(f"[INFO] {len(df_valid)} lignes chargées dans la DB et sauvegardées dans {processed_file}")

    except Exception as e:
        print(f"[ERROR] ETL temps échoué : {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    etl_temps()
