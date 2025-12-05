import psycopg2
from psycopg2 import sql, extras

# Paramètres de connexion
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'supermarche_etl',
    'user': 'yvanrandriamia',
    'password': '205290yvan'
}

def get_connection():
    """Créer et retourner une connexion PostgreSQL"""
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

def get_cursor(conn):
    """Créer un curseur avec dictionnaire pour PostgreSQL"""
    return conn.cursor(cursor_factory=extras.DictCursor)
