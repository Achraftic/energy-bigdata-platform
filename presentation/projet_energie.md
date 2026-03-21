# Présentation du Projet : Plateforme Big Data Énergie ⚡

## 1. Introduction (30s)
*   **Objectif** : Créer une plateforme capable de gérer des millions de données énergétiques en temps réel.
*   **Problème** : Comment surveiller la consommation, détecter les pannes et prévoir la demande future ?
*   **Solution** : Une architecture "Plug-and-Play" basée sur les meilleures technologies Big Data.

---

## 2. L'Architecture Technique (1 min)
Nous utilisons une pile technologique moderne (SMACK Stack) :
1.  **Ingestion** : Apache Kafka (le facteur qui transporte les messages).
2.  **Traitement** : Apache Spark (le cerveau qui calcule).
3.  **Stockage** : 
    *   Cassandra (pour la rapidité, les données vivantes).
    *   HDFS (pour le stockage de masse, l'historique).
4.  **Visualisation** : Streamlit (un dashboard interactif en Français).

---

## 3. Le Processus Étape par Étape (2 min)

### Étape 1 : Génération des Données
*   Des capteurs simulés génèrent 12 relevés par seconde (un par région du Maroc).
*   Données : Tension, Fréquence, Consommation, Production.

### Étape 2 : Ingestion en Temps Réel
*   Le producteur envoie les données vers **Kafka**.
*   **Spark Streaming** lit ces données instantanément.

### Étape 3 : Analyse et Détection
*   Spark vérifie si la tension ou la fréquence est anormale.
*   Si oui, une **anomalie** est enregistrée immédiatement dans Cassandra.

### Étape 4 : Machine Learning (IA)
*   Spark analyse l'historique pour **prédire la consommation** des prochaines 24 heures.
*   Le modèle est sauvegardé et réutilisé pour les prévisions.

---

## 4. Démonstration du Dashboard (1 min)
*   **Vue d'Ensemble** : Production vs Consommation.
*   **Analyses Techniques** : Stabilité du réseau (Tension/Fréquence).
*   **Surveillance** : Liste des alertes et anomalies.
*   **Prévisions** : Courbe de demande future générée par l'IA.

---

## 5. Le Flux Détaillé (Le Pipeline) 🛠️
Pour les plus techniques, voici le parcours d'une donnée :
1.  **Source (Producteur)** : Script Python simulant 12 régions (1 msg/sec/région).
2.  **Transport (Kafka)** : Les messages JSON sont stockés dans le topic `energy_topic`.
3.  **Traitement Temps Réel (Spark Streaming)** : 
    - Lecture micro-batch.
    - Filtrage et calcul des anomalies.
    - Écriture dans **Cassandra**.
4.  **Traitement Batch (Historique)** :
    - Données brutes sur **HDFS**.
    - Chargement massif vers Cassandra.
5.  **Intelligence Artificielle (Spark ML)** :
    - Entraînement sur les données historiques.
    - Calcul des prévisions de consommation.
6.  **Distribution (API FastAPI)** : Sert de pont entre la base de données et l'interface.
7.  **Dashboard (Streamlit)** : Visualisation interactive en temps réel.

---

## 6. Le Voyage du Stockage 💾
Où sont stockées vos données à chaque étape ?
1.  **Fichier CSV (Source)** : Le point de départ sur le disque local.
2.  **Kafka (Tampon)** : Stockage temporaire ultra-rapide pour ne perdre aucun message.
3.  **HDFS (Archive)** : Notre "Data Lake". Il stocke des téraoctets de données historiques en toute sécurité.
4.  **Cassandra (Base de Données)** : Le cœur du système. Utilisé par Spark et l'API pour un accès instantané.
5.  **Modèles ML (Disque)** : Les cerveaux de l'IA sont sauvegardés sous forme de fichiers pour éviter de ré-entraîner à chaque fois.

---

## 7. Conclusion (30s)
*   **Puissance** : La plateforme peut traiter des milliers de lignes par seconde.
*   **Flexibilité** : Facile à déployer avec Docker.
*   **Résultat** : Un outil complet pour la gestion intelligente de l'énergie.
