-- Top 10 des produits par chiffre d’affaires mensuel
SELECT 
    p.designation,
    t.mois,
    t.annee,
    SUM(v.montant) AS chiffre_affaires
FROM ventes v
JOIN produits p ON v.produit_id = p.produit_id
JOIN temps t ON v.date_id = t.date_id
GROUP BY p.designation, t.mois, t.annee
ORDER BY chiffre_affaires DESC
LIMIT 10;

-- Répartition des clients par âge et type
SELECT 
    categorie_client,
    CASE 
        WHEN age < 25 THEN 'Jeunes (<25)'
        WHEN age BETWEEN 25 AND 40 THEN 'Adultes (25-40)'
        ELSE 'Seniors (40+)'
    END AS tranche_age,
    COUNT(*) AS total
FROM clients
GROUP BY categorie_client, tranche_age
ORDER BY total DESC;

-- Analyse des ventes par région
SELECT 
    l.region,
    SUM(v.montant) AS chiffre_affaires
FROM ventes v
JOIN localisation l ON v.magasin_id = l.magasin_id
GROUP BY l.region
ORDER BY chiffre_affaires DESC;

-- Saisonnalité (produits × mois)
SELECT 
    p.categorie,
    t.mois,
    SUM(v.quantite) AS total_ventes
FROM ventes v
JOIN produits p ON v.produit_id = p.produit_id
JOIN temps t ON v.date_id = t.date_id
GROUP BY p.categorie, t.mois, t.mois_num
ORDER BY t.mois, total_ventes DESC;

-- Evolution CA
SELECT
    t.annee,
    t.mois,
    SUM(v.montant) AS chiffre_affaires
FROM ventes v
JOIN temps t ON v.date_id = t.date_id
GROUP BY t.annee, t.mois, t.mois_num
ORDER BY t.annee, t.mois_num;

-- Correlation categorie produits x Quantite d'achats
SELECT 
    p.categorie,
    COUNT(v.produit_id) AS nombre_achats
FROM ventes v
JOIN produits p ON v.produit_id = p.produit_id
GROUP BY p.categorie
ORDER BY nombre_achats DESC;

-- Jour avec le plus de ventes 
SELECT t.jour, SUM(v.montant) AS total_ca
FROM ventes v
JOIN temps t ON v.date_id = t.date_id
GROUP BY t.jour
ORDER BY total_ca DESC;

