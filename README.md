# Supermarket Analytics – ETL & Business Intelligence

## 📌 Description

Ce projet met en place une **architecture complète de traitement et d’analyse des données de vente d’un supermarché**, allant de la donnée brute (CSV / sources transactionnelles) jusqu’aux **indicateurs décisionnels (KPI)** utilisés par la direction commerciale.

En tant que **Data Engineer & Business Analyst**, l’objectif est de transformer des données opérationnelles en **informations stratégiques exploitables** permettant d’optimiser :

* La performance commerciale
* La gestion des stocks
* La stratégie marketing
* La connaissance client

---

## 🎯 Objectifs du projet

* Construire un **pipeline ETL automatisé** fiable et flexible
* Intégrer les données dans un **Data Warehouse (PostgreSQL)**
* Assurer la qualité, la traçabilité et la cohérence des données
* Fournir des indicateurs clés (KPI) pour l’aide à la décision
* Permettre l’analyse :

  * Du chiffre d’affaires (par jour, mois, magasin, catégorie…)
  * Du comportement d’achat client
  * De la saisonnalité des ventes
  * De la performance produit et magasin

---

## 🏗️ Architecture du projet

Source data (CSV / fichiers bruts)
  ↓
**ETL Python (pandas, psycopg2)**
  ↓
**PostgreSQL – Data Warehouse**
  ↓
**SQL / Power BI / Dashboard / Rapports**

---

## 📁 Structure du projet

```
supermarket-analytics-etl/
│
├── etl/
│   ├── data/
│   │   ├── raw/            # Fichiers sources (CSV)
│   │   ├── processed/      # Données nettoyées
│   │   └── rejected/       # Données rejetées (erreurs)
│   │
│   │── db.py            
|   |-- etl_client.py       # Extraction des données
│   │── etl_magasin.py      # Transformation
│   │── etl_produit.py      # Chargement vers PostgreSQL
│
├── sql/
│   ├── script_create_table.sql           # Création des tables
│   └── olap_queries.sql         # Requêtes analytiques
│
├── metabase/               #  visualisations
├── docs/                   # Rapports
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🛢️ Modèle de données (Data Warehouse)

### Tables principales

* `fact_ventes`
* `dim_clients`
* `dim_produits`
* `dim_magasins`
* `dim_temps`

### Mesures clés

* Chiffre d’affaires (CA)
* Quantité vendue
* Fréquence d’achat
* Panier moyen
* Taux de croissance

---

## ⚙️ Technologies utilisées

* **Python** (pandas, psycopg2)
* **PostgreSQL**
* **Git & GitHub**
* **Docker (optionnel)**
* **Power BI / Tableau / Matplotlib / Seaborn**
* **SQL avancé (GROUP BY, CTE, Window functions)**

---

## 📊 Exemples d’analyses

### Chiffre d'affaires par magasin

```
SELECT 
    m.nom_magasin,
    SUM(v.quantite * p.prix) AS chiffre_affaires
FROM fact_ventes v
JOIN dim_produits p ON v.produit_id = p.produit_id
JOIN dim_magasins m ON v.magasin_id = m.magasin_id
GROUP BY m.nom_magasin
ORDER BY chiffre_affaires DESC;
```

### Saisonnalité (ordre des mois)

```
ORDER BY 
   CASE t.mois
      WHEN 'January' THEN 1
      WHEN 'February' THEN 2
      WHEN 'March' THEN 3
      WHEN 'April' THEN 4
      WHEN 'May' THEN 5
      WHEN 'June' THEN 6
      WHEN 'July' THEN 7
      WHEN 'August' THEN 8
      WHEN 'September' THEN 9
      WHEN 'October' THEN 10
      WHEN 'November' THEN 11
      WHEN 'December' THEN 12
   END
```

---

## 📈 Valeur métier (Apport stratégique)

Grâce à ce projet, l’entreprise peut désormais :

✅ Identifier les magasins les plus rentables
✅ Détecter les produits vedettes et les produits faibles
✅ Optimiser les stocks selon la saison
✅ Personnaliser les campagnes marketing
✅ Améliorer la prise de décision via la donnée

---

## 🚫 .gitignore recommandé

```
# Environnements virtuels
etl/venv/
env/
.venv/

# Cache
__pycache__/
*.pyc

# Data
etl/data/raw/
etl/data/processed/
etl/data/rejected/

# Environnement local
.env

# IDE
.vscode/
.idea/
```

---

## ✅ Conclusion

Ce projet démontre une **maîtrise complète de la chaîne décisionnelle** :

* Data Engineering
* Modélisation BI
* Analyse stratégique
* Prise de décision orientée données

Il constitue une **base solide pour un système d’aide à la décision en environnement réel d’entreprise**.

---

**Auteur : [Votre Nom]**
**Rôle : Data Engineer / Business Analyst**
**Projet : Supermarket Business Intelligence**
