"""Bibliothèque commune des jobs crunch.

Tout tourne côté cluster : les chemins sont ceux vus par les conteneurs
(volume ./data monté sur /data, volume ./jobs monté sur /app).
"""

import csv
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/data"
RAW_DIR = f"{DATA_DIR}/raw"
OUT_DIR = f"{DATA_DIR}/out"
MANIFEST = f"{OUT_DIR}/runs.csv"
MANIFEST_COLUMNS = [
    "run_id", "ts_utc", "job", "month",
    "parallelism", "duration_s", "rows_in", "rows_out",
]

# Préfixes de namespaces à exclure du comptage éditorial (top100).
# Un titre encyclopédique ne contient pas de préfixe « Namespace: ».
NAMESPACE_PREFIXES = (
    "Spécial:", "Special:", "Wikipédia:", "Wikipedia:", "Catégorie:", "Category:",
    "Discussion:", "Discussion_", "Aide:", "Help:", "Fichier:", "File:",
    "Modèle:", "Template:", "Portail:", "Portal:", "Projet:", "Project:",
    "Utilisateur:", "User:", "MediaWiki:", "Module:", "Référence:", "Sujet:",
)

MAIN_PAGES = ("Wikipédia:Accueil_principal", "Accueil", "Main_Page", "-")


def get_spark(app_name: str) -> SparkSession:
    """Session Spark. Le master vient de spark-defaults.conf : jamais local[*]."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def read_pageviews(spark: SparkSession, month: str) -> DataFrame:
    """Lit data/raw/<mois>/*.gz et retourne (project, page, views, date, hour).

    La date et l'heure viennent du NOM du fichier (pageviews-AAAAMMJJ-HH0000.gz),
    pas du contenu : chaque fichier gzip couvre exactement une heure.
    """
    lines = spark.read.text(f"{RAW_DIR}/{month}/*.gz")

    parts = F.split(F.col("value"), " ")
    fname = F.input_file_name()

    return (
        lines
        .select(
            parts.getItem(0).alias("project"),
            parts.getItem(1).alias("page"),
            parts.getItem(2).cast("long").alias("views"),
            # le 4e champ (octets) vaut toujours 0 : ignoré
            F.to_date(F.regexp_extract(fname, r"pageviews-(\d{8})-", 1), "yyyyMMdd").alias("date"),
            F.regexp_extract(fname, r"pageviews-\d{8}-(\d{2})0000", 1).cast("int").alias("hour"),
        )
        .where(F.col("views").isNotNull())  # écarte les lignes malformées
    )


def filter_fr_projects(df: DataFrame) -> DataFrame:
    """Ne garde que fr.wikipedia.org : fr (desktop) et fr.m (mobile).

    Exclut fr.b (wikibooks), fr.d (wiktionary), fr.m.b, etc.
    """
    return df.where(F.col("project").isin("fr", "fr.m"))


def filter_clean_pages(df: DataFrame) -> DataFrame:
    """Écarte le titre '-', les pages d'accueil et les namespaces techniques."""
    cond = ~F.col("page").isin(*MAIN_PAGES)
    for prefix in NAMESPACE_PREFIXES:
        cond = cond & ~F.col("page").startswith(prefix)
    return df.where(cond)


def make_output_dir(job: str, ts_utc: datetime, parallelism: int) -> str:
    """data/out/<job>_<timestampUTC>_p<parallélisme>/ — jamais écrasé."""
    stamp = ts_utc.strftime("%Y%m%dT%H%M%SZ")
    path = f"{OUT_DIR}/{job}_{stamp}_p{parallelism}"
    os.makedirs(path, exist_ok=False)  # écraser une sortie précédente est interdit
    return path


def write_result_csv(result: DataFrame, out_dir: str) -> int:
    """Écrit le résultat (petit : 7 à 100 lignes) en un seul CSV. Retourne rows_out."""
    rows = result.collect()
    columns = result.columns
    with open(f"{out_dir}/result.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for row in rows:
            writer.writerow([row[c] for c in columns])
    return len(rows)


def append_manifest(run: dict) -> None:
    """Ajoute une ligne à data/out/runs.csv (créé avec entête au premier run)."""
    os.makedirs(OUT_DIR, exist_ok=True)
    new_file = not os.path.exists(MANIFEST)
    with open(MANIFEST, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerow(run)


def run_and_record(job_name: str, month: str, build_result, count_input: bool = False) -> None:
    """Trame commune : lire, agréger, écrire la sortie horodatée + le manifeste.

    build_result(df) reçoit le DataFrame brut parsé et retourne le DataFrame résultat.
    count_input=True ajoute une action count() sur l'entrée (relit les fichiers).
    """
    ts_utc = datetime.now(timezone.utc)
    t0 = ts_utc.timestamp()

    spark = get_spark(f"crunch-{job_name}")
    parallelism = spark.sparkContext.defaultParallelism

    df = read_pageviews(spark, month)
    result = build_result(df)

    out_dir = make_output_dir(job_name, ts_utc, parallelism)
    rows_out = write_result_csv(result, out_dir)
    rows_in = df.count() if count_input else -1

    duration_s = round(datetime.now(timezone.utc).timestamp() - t0, 1)
    append_manifest({
        "run_id": f"{job_name}_{ts_utc.strftime('%Y%m%dT%H%M%SZ')}",
        "ts_utc": ts_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "job": job_name,
        "month": month,
        "parallelism": parallelism,
        "duration_s": duration_s,
        "rows_in": rows_in,
        "rows_out": rows_out,
    })

    print(f"[crunch] {job_name} : {rows_out} lignes -> {out_dir} "
          f"({duration_s}s, parallelism={parallelism}, rows_in={rows_in})")
    if rows_out == 0:
        print("[crunch] ATTENTION : rows_out = 0, panne silencieuse probable "
              "(titre inexistant ? mauvais mois ? filtres trop agressifs ?)")

    spark.stop()
