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

CSV_FILENAME = "ventes.csv"  # nom du fichier source

def etl_ventes():
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
        required_cols = ['date_id', 'produit_id', 'client_id', 'magasin_id', 'quantite', 'montant']
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes: {missing_cols}")

        # Nettoyage simple
        df['quantite'] = pd.to_numeric(df['quantite'], errors='coerce')
        df['montant'] = pd.to_numeric(df['montant'], errors='coerce')
        df['date_id'] = pd.to_datetime(df['date_id'], errors='coerce').dt.date  # ⚡ conversion date
        df['produit_id'] = df['produit_id'].astype(str).str.strip()
        df['client_id'] = df['client_id'].astype(int)
        df['magasin_id'] = df['magasin_id'].astype(str).str.strip()

        # Filtrer les lignes invalides (valeurs manquantes)
        df_valid = df.dropna(subset=required_cols)
        df_invalid = df[~df.index.isin(df_valid.index)]

        # ----------------------
        # 3️⃣ Vérification FK
        # ----------------------
        conn = get_connection()
        cur = get_cursor(conn)

        # Récupérer les valeurs existantes dans les dimensions
        cur.execute("SELECT date_id FROM temps")
        dates_exist = set([row[0] for row in cur.fetchall()])

        cur.execute("SELECT produit_id FROM produits")
        produits_exist = set([row[0] for row in cur.fetchall()])

        cur.execute("SELECT client_id FROM clients")
        clients_exist = set([row[0] for row in cur.fetchall()])

        cur.execute("SELECT magasin_id FROM localisation")
        magasins_exist = set([row[0] for row in cur.fetchall()])

        # Filtrer les lignes qui respectent les FK
        mask_fk = (
            df_valid['date_id'].isin(dates_exist) &
            df_valid['produit_id'].isin(produits_exist) &
            df_valid['client_id'].isin(clients_exist) &
            df_valid['magasin_id'].isin(magasins_exist)
        )

        df_fk_valid = df_valid[mask_fk]
        df_fk_invalid = df_valid[~mask_fk]

        # Combiner les lignes invalides
        df_invalid = pd.concat([df_invalid, df_fk_invalid], ignore_index=True)

        # Sauvegarder les lignes rejetées
        if not df_invalid.empty:
            rejected_file = os.path.join(REJECTED_DIR, f"ventes_rejected_{timestamp}.csv")
            df_invalid.to_csv(rejected_file, index=False)
            print(f"[WARN] {len(df_invalid)} lignes rejetées sauvegardées dans {rejected_file}")

        # ----------------------
        # 4️⃣ Chargement en staging
        # ----------------------
        cur.execute("""
            CREATE TEMP TABLE ventes_staging (
                date_id DATE,
                produit_id VARCHAR(50),
                client_id INT,
                magasin_id VARCHAR(50),
                quantite INT,
                montant NUMERIC
            );
        """)
        conn.commit()

        extras = [
            (row.date_id, row.produit_id, row.client_id, row.magasin_id, row.quantite, row.montant)
            for idx, row in df_fk_valid.iterrows()
        ]
        # Par ceci ✅ : on charge uniquement la staging
        if extras:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO ventes_staging (date_id, produit_id, client_id, magasin_id, quantite, montant) VALUES %s",
                extras
            )
            conn.commit()

        # ----------------------
        # 5️⃣ Merge staging -> table principale
        # ----------------------
        cur.execute("""
            INSERT INTO ventes (date_id, produit_id, client_id, magasin_id, quantite, montant)
            SELECT s.date_id, s.produit_id, s.client_id, s.magasin_id, s.quantite, s.montant
            FROM ventes_staging s
            ON CONFLICT (date_id, produit_id, client_id, magasin_id)
            DO UPDATE SET
                quantite = EXCLUDED.quantite,
                montant = EXCLUDED.montant;
        """)
        conn.commit()

        # ----------------------
        # 6️⃣ Export processed
        # ----------------------
        processed_file = os.path.join(PROCESSED_DIR, f"ventes_processed_{timestamp}.csv")
        df_fk_valid.to_csv(processed_file, index=False)
        print(f"[INFO] {len(df_fk_valid)} lignes chargées dans la DB et sauvegardées dans {processed_file}")

    except Exception as e:
        print(f"[ERROR] ETL ventes échoué : {e}")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    etl_ventes()
