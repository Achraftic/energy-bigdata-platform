# Présentation : Plateforme Nationale d'Analyse Énergétique (Maroc)
**Durée estimée : 8 minutes**

---

## 1. Introduction (1 min)
*   **Sujet** : Analyse et monitoring en temps réel de la consommation énergétique nationale (12 régions).
*   **Objectif** : Ingestion de flux massifs de données (compteurs intelligents) pour détecter les anomalies et prévoir la charge.
*   **Enjeu** : Stabilité du réseau électrique et optimisation de la production (éolien, solaire, thermique).

---

## 2. Architecture du Système (2 mins)
*   **Modèle Lambda** :
    *   **Speed Layer (Real-time)** : Kafka -> Spark Streaming -> Cassandra. Utile pour les alertes immédiates (sur-consommation, pannes).
    *   **Batch Layer** : HDFS -> Spark Batch -> Cassandra. Utile pour les rapports historiques et le machine learning.
*   **Infrastructure** : Entièrement conteneurisée via Docker pour la portabilité et l'agilité.

---

## 3. Stack Technique (1.5 mins)
*   **Ingestion** : Confluent Kafka 7.0.1 (multi-listeners internal/external).
*   **Stockage Distribué** : Hadoop HDFS 3.2.1 & Cassandra 4.0 (NoSQL orienté écritures rapides).
*   **Traitement** : Apache Spark 3.5.0 (PySpark, SQL, Streaming).
*   **Backend & Dashboard** : FastAPI & Streamlit pour la visualisation des métriques.

---

## 4. Défis Techniques & Solutions (2.5 mins)
*   **Orchestration Réseau** : Résolution du problème de métadonnées Kafka (`LEADER_NOT_AVAILABLE`) via une configuration fine des `ADVERTISED_LISTENERS`.
*   **Synchronisation Schéma** : Alignement dynamique entre le simulateur JSON, les types Spark (`StructType`) et le modèle Cassandra (CQL).
*   **Performance Spark** : Optimisation de la gestion des ressources (exécuteurs/cœurs) dans un environnement Docker contraint.
*   **Connectivité NoSQL** : Mise en place d'un `dict_factory` pour l'API afin de transformer efficacement les données Cassandra en JSON consommable par le Dashboard.

---

## 5. Démonstration & Résultats (1 min)
*   **Ingestion Live** : Flux constant de 12 régions marocaines.
*   **Analyse en Temps Réel** : Détection automatique des sur-consommations (>1.5x la production).
*   **API Metrics** : Disponibilité des données via REST.
*   **Vérification** : Script de test automatisé validant chaque étape du pipeline.

---

## 6. Conclusion
*   Une plateforme robuste, scalable et prête pour une mise en production simulée.
*   **Prochaines étapes** : Intégration de modèles prédictifs plus avancés (LSTM) et ajout de Neo4j pour l'analyse des graphes de distribution.

---
**Merci de votre attention ! Questions ?**
