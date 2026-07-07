# crunch — Rapport

*Pipeline batch Spark et observabilité — M2 Big Data · IPSSI*
*Nom : … · Date : …*

> Trame à compléter avec vos mesures réelles, puis à exporter en PDF.

## 1. Contexte et objectif

Traiter 7 jours de pageviews Wikipédia (168 fichiers gzip, ≈ 5 Go compressés),
produire trois agrégats (top100, monument, circadian), exécuter la même campagne
sur 1, 2 puis 4 workers, et prouver le comportement observé avec Grafana,
la Spark UI et le manifeste `runs.csv`.

## 2. Architecture

- Schéma de la stack : master, workers, prometheus, cadvisor, grafana, history server.
- Chaîne de mesure : cgroups → cAdvisor → Prometheus (scrape 10 s) → Grafana.
- Où tourne le driver, où tournent les executors, où est la donnée.

## 3. Les jobs

Pour chaque job : filtres appliqués, plan (stages, shuffle), forme de la sortie.

| Job | Entrée | Filtres | Agrégat | Sortie |
|-----|--------|---------|---------|--------|
| top100 | 168 fichiers | fr + fr.m, pages spéciales exclues | somme par page, top 100 | 100 lignes |
| monument | 168 fichiers | fr + fr.m, titre exact | somme par jour | 7 lignes |
| circadian | 168 fichiers | fr + fr.m | somme par heure (0-23) | 24 lignes |

## 4. Résultats de la campagne (12 runs)

Coller ici `data/out/runs.csv` et un tableau de synthèse :

| Run | Variante | Job | duration_s | rows_out |
|-----|----------|-----|-----------|----------|
| 1 | 1w | top100 | … | … |
| … | | | | |

### Analyse du passage à l'échelle

- Accélération 1w → 2w → 4w sur top100 : mesurée vs idéale (×2, ×4).
- Pourquoi ce n'est pas linéaire : fichiers gzip non découpables (le parallélisme
  vient du nombre de fichiers), coût fixe du driver, shuffle, contention disque.
- Variance : écart entre les deux runs top100 d'une même variante.

## 5. Preuves d'observabilité

À insérer (captures) :

1. **Grafana** — les 5 panels pendant un run top100, sur chaque variante.
   Commenter : plateaux CPU (cœurs saturés ?), bosses réseau (shuffle), mémoire.
2. **Spark UI / History Server** — DAG et onglet Stages d'un run :
   nombre de tasks par stage, tailles de partitions, shuffle write.
3. **Prometheus** — une requête testée dans :9090 → Graph avant Grafana.

Corrélation attendue : les vagues de tasks dans la Spark UI correspondent aux
bosses de CPU dans Grafana ; le shuffle apparaît dans `container_network_*`.

## 6. Le manifeste comme filet de sécurité

- Rôle de `rows_out` : un job « qui marche » avec rows_out = 0 est une panne
  silencieuse (ex : titre inexact pour monument).
- Interdiction d'écraser une sortie : traçabilité des runs horodatés.

## 7. Conclusion

Ce qui limite ce pipeline (gzip, une seule action, driver co-localisé),
ce qu'on ferait en production (Parquet, partitionnement, cache, alerting).
