# crunch — Pipeline batch Spark et observabilité

Traitement de 7 jours de pageviews Wikipédia sur un cluster Spark dockerisé
(1, 2 puis 4 workers), avec monitoring Prometheus / Grafana et manifeste de runs.

## Arborescence

```
TP1/
├── download.py                  # téléchargement des 168 fichiers horaires
├── run_job.py                   # soumission depuis l'hôte + commande check
├── docker-compose.1w.yml        # 1 worker  · 2 cœurs  (référence)
├── docker-compose.2w.yml        # 2 workers · 4 cœurs
├── docker-compose.4w.yml        # 4 workers · 8 cœurs
├── jobs/
│   ├── lib.py                   # bibliothèque commune (lecture, filtres, manifeste)
│   └── main.py                  # jobs top100 / monument / circadian
├── spark/conf/                  # spark-defaults.conf + metrics.properties (Prometheus)
├── monitoring/
│   ├── prometheus.yml           # cibles : master, workers, driver, cAdvisor
│   └── grafana/                 # provisioning datasource + dashboard crunch.json
├── data/
│   ├── raw/<mois>/              # .gz téléchargés (hors git)
│   ├── monuments.csv            # titres exacts valides
│   ├── spark-events/            # journaux pour le History Server
│   └── out/                     # runs.csv + un dossier horodaté par run
└── rapport/rapport.md           # trame du rapport PDF
```

## 1. Récupérer les données

```bash
python download.py 2026-06 7
```

168 fichiers (~5 Go compressés) dans `data/raw/2026-06/`. Le script temporise
entre deux fichiers (service public) et reprend là où il s'est arrêté.

## 2. Démarrer une variante du cluster

Le dossier `data/spark-events` doit exister avant le premier démarrage
(présent dans le dépôt via `.gitkeep`).

```bash
docker compose -f docker-compose.1w.yml up -d
python run_job.py check        # conteneurs + cibles Prometheus + master Spark
```

| Interface                    | URL              |
|------------------------------|------------------|
| Spark master                 | localhost:8080   |
| Spark UI (app en cours)      | localhost:4040   |
| History Server               | localhost:18080  |
| Prometheus                   | localhost:9090   |
| Grafana (admin/admin)        | localhost:3000   |
| cAdvisor                     | localhost:8085   |

Arrêt : **toujours avec le même fichier** que le démarrage :

```bash
docker compose -f docker-compose.1w.yml down
```

## 3. Lancer les jobs

```bash
python run_job.py top100 2026-06
python run_job.py monument 2026-06 Tour_Eiffel
python run_job.py circadian 2026-06
```

`run_job.py` fait un `docker exec spark-master spark-submit
--master spark://spark-master:7077 ...` : le driver tourne dans le conteneur
master, jamais en `local[*]`.

Chaque run écrit son résultat dans `data/out/<job>_<timestampUTC>_p<parallélisme>/result.csv`
et ajoute une ligne au manifeste `data/out/runs.csv`
(run_id, ts_utc, job, month, parallelism, duration_s, rows_in, rows_out).
`rows_in` vaut -1 par défaut ; ajoutez `--count-input` pour le compter
(action supplémentaire qui relit les fichiers).

## 4. La campagne des 12 runs

Pour chaque variante, dans l'ordre : `up -d` → `check` → les 4 runs → `down`.

| Variante | Runs |
|----------|------|
| `docker-compose.1w.yml` | top100 · monument Tour_Eiffel · monument Sagrada_Família · top100 |
| `docker-compose.2w.yml` | top100 · monument Sagrada_Família · monument Mont-Saint-Michel · top100 |
| `docker-compose.4w.yml` | top100 · monument Mont-Saint-Michel · monument Tour_Eiffel · top100 |

## 5. Le dashboard Grafana

Provisionné automatiquement (`crunch — Spark & conteneurs`) : workers vivants,
applications en cours au master, CPU / mémoire / réseau par conteneur.
Les compteurs passent par `rate()` ; chaque requête est testable dans
Prometheus (localhost:9090 → Graph) avant Grafana.
