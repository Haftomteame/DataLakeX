"""Point d'entrée unique soumis via spark-submit (voir run_job.py côté hôte).

Usage (dans le conteneur spark-master) :
    spark-submit --py-files /app/lib.py /app/main.py top100 2026-06
    spark-submit --py-files /app/lib.py /app/main.py monument 2026-06 Tour_Eiffel
    spark-submit --py-files /app/lib.py /app/main.py circadian 2026-06

Option : --count-input pour renseigner rows_in dans le manifeste
(action count() supplémentaire, relit les fichiers ; sinon rows_in = -1).
"""

import sys

from pyspark.sql import functions as F

import lib


def top100(df):
    """Somme des vues par page sur les 7 jours, tri décroissant, 100 premières."""
    return (
        lib.filter_clean_pages(lib.filter_fr_projects(df))
        .groupBy("page")
        .agg(F.sum("views").alias("views"))
        .orderBy(F.desc("views"))
        .limit(100)
    )


def monument(df, title):
    """Vues quotidiennes du titre exact demandé (desktop + mobile), triées par jour."""
    return (
        lib.filter_fr_projects(df)
        .where(F.col("page") == title)  # titre exact : underscores et accents compris
        .groupBy("date")
        .agg(F.sum("views").alias("views"))
        .orderBy("date")
    )


def circadian(df):
    """Profil circadien : total des vues fr + fr.m par heure de la journée (0-23)."""
    return (
        lib.filter_fr_projects(df)
        .groupBy("hour")
        .agg(F.sum("views").alias("views"))
        .orderBy("hour")
    )


def main(argv):
    count_input = "--count-input" in argv
    argv = [a for a in argv if a != "--count-input"]

    if len(argv) < 2:
        print(__doc__)
        return 2

    job, month = argv[0], argv[1]

    if job == "top100":
        lib.run_and_record("top100", month, top100, count_input)
    elif job == "monument":
        if len(argv) != 3:
            print("usage : monument <mois> <Titre_Exact>")
            return 2
        title = argv[2]
        lib.run_and_record(f"monument_{title}", month,
                           lambda df: monument(df, title), count_input)
    elif job == "circadian":
        lib.run_and_record("circadian", month, circadian, count_input)
    else:
        print(f"job inconnu : {job} (attendu : top100 | monument | circadian)")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
