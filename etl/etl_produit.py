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

CSV_FILENAME = "dim_produits.csv"  # nom du fichier source

def etl_produits():
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
        required_cols = ['produit_id', 'designation', 'categorie', 'prix_ht', 'fournisseur']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

        # Nettoyage
        df['produit_id'] = df['produit_id'].astype(str).str.strip()  # s'assurer que c'est une string
        df['designation'] = df['designation'].astype(str).str.strip()
        df['categorie'] = df['categorie'].astype(str).str.strip()
        df['fournisseur'] = df['fournisseur'].astype(str).str.strip()
        df['prix_ht'] = pd.to_numeric(df['prix_ht'], errors='coerce')

        # Filtrer les lignes invalides
        df_valid = df.dropna(subset=['produit_id', 'designation', 'categorie', 'prix_ht', 'fournisseur'])
        df_invalid = df[~df.index.isin(df_valid.index)]

        if not df_invalid.empty:
            rejected_file = os.path.join(REJECTED_DIR, f"produits_rejected_{timestamp}.csv")
            df_invalid.to_csv(rejected_file, index=False)
            print(f"[WARN] {len(df_invalid)} lignes rejetées sauvegardées dans {rejected_file}")

        # ----------------------
        # 3️⃣ Chargement en staging
        # ----------------------
        conn = get_connection()
        cur = get_cursor(conn)

        cur.execute("""
            CREATE TEMP TABLE produits_staging (
                produit_id VARCHAR(50),
                designation VARCHAR(255),
                categorie VARCHAR(100),
                prix_ht NUMERIC,
                fournisseur VARCHAR(255)
            );
        """)
        conn.commit()

        extras = [(row.produit_id, row.designation, row.categorie, row.prix_ht, row.fournisseur)
                  for idx, row in df_valid.iterrows()]
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO produits_staging (produit_id, designation, categorie, prix_ht, fournisseur) VALUES %s",
            extras
        )
        conn.commit()

        # ----------------------
        # 4️⃣ Merge staging -> table principale
        # ----------------------
        cur.execute("""
            INSERT INTO produits (produit_id, designation, categorie, prix_ht, fournisseur)
            SELECT s.produit_id, s.designation, s.categorie, s.prix_ht, s.fournisseur
            FROM produits_staging s
            ON CONFLICT (produit_id)
            DO UPDATE SET
                designation = EXCLUDED.designation,
                categorie = EXCLUDED.categorie,
                prix_ht = EXCLUDED.prix_ht,
                fournisseur = EXCLUDED.fournisseur;
        """)
        conn.commit()

        # ----------------------
        # 5️⃣ Export processed
        # ----------------------
        processed_file = os.path.join(PROCESSED_DIR, f"produits_processed_{timestamp}.csv")
        df_valid.to_csv(processed_file, index=False)
        print(f"[INFO] {len(df_valid)} lignes chargées dans la DB et sauvegardées dans {processed_file}")

    except Exception as e:
        print(f"[ERROR] ETL produits échoué : {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    etl_produits()
