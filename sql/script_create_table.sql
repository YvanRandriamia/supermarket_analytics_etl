-- =========================================================
-- Script SQL pour créer toutes les tables du projet
-- ETL Supermarket Analytics
-- PostgreSQL 15
-- =========================================================

-- 1️⃣ Table Produits
CREATE TABLE IF NOT EXISTS produits (
    produit_id VARCHAR(50) PRIMARY KEY,
    designation VARCHAR(255) NOT NULL,
    categorie VARCHAR(100),
    prix_ht NUMERIC,
    fournisseur VARCHAR(255)
);

-- 2️⃣ Table Clients
CREATE TABLE IF NOT EXISTS clients (
    client_id INT PRIMARY KEY,
    nom VARCHAR(255) NOT NULL,
    age INT CHECK (age >= 0),
    ville VARCHAR(100),
    categorie_client VARCHAR(50) CHECK (categorie_client IN ('VIP','Particulier','Entreprise'))
);

-- 3️⃣ Table Temps
CREATE TABLE IF NOT EXISTS temps (
    date_id DATE PRIMARY KEY,
    jour INT CHECK (jour BETWEEN 1 AND 31),
    semaine INT CHECK (semaine BETWEEN 1 AND 53),
    mois VARCHAR(20),
    annee INT
);

-- 4️⃣ Table Localisation
CREATE TABLE IF NOT EXISTS localisation (
    magasin_id VARCHAR(10) PRIMARY KEY,
    ville VARCHAR(100),
    region VARCHAR(100),
    surface_m2 INT
);

-- 5️⃣ Table Ventes
CREATE TABLE IF NOT EXISTS ventes (
    date_id DATE NOT NULL,
    produit_id VARCHAR(50) NOT NULL,
    client_id INT NOT NULL,
    magasin_id VARCHAR(10) NOT NULL,
    quantite INT CHECK (quantite > 0),
    montant NUMERIC(12,2) CHECK (montant >= 0),
    PRIMARY KEY (date_id, produit_id, client_id, magasin_id),
    CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES temps(date_id),
    CONSTRAINT fk_produit FOREIGN KEY (produit_id) REFERENCES produits(produit_id),
    CONSTRAINT fk_client FOREIGN KEY (client_id) REFERENCES clients(client_id),
    CONSTRAINT fk_magasin FOREIGN KEY (magasin_id) REFERENCES localisation(magasin_id)
);
