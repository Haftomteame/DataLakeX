#!/usr/bin/env python3
"""Soumission des jobs crunch depuis la machine hôte.

Usage :
    python run_job.py top100 2026-06
    python run_job.py monument 2026-06 Tour_Eiffel
    python run_job.py circadian 2026-06
    python run_job.py check          # état de la stack (conteneurs, Prometheus, master)

La soumission construit un `docker exec spark-master spark-submit ...` :
le driver tourne dans le conteneur master (Spark UI sur localhost:4040),
le mode local[*] est interdit.
"""

import json
import subprocess
import sys
import urllib.request

MASTER_URL = "spark://spark-master:7077"
CONTAINER = "spark-master"
JOBS = ("top100", "monument", "circadian")


def submit(job: str, args: list[str]) -> int:
    """Construit et exécute la commande docker exec ... spark-submit ... ."""
    cmd = [
        "docker", "exec", CONTAINER,
        "/opt/spark/bin/spark-submit",
        "--master", MASTER_URL,
        "--py-files", "/app/lib.py",
        "/app/main.py",
        job, *args,
    ]
    print("[run_job] " + " ".join(cmd))
    return subprocess.run(cmd).returncode


def _http_json(url: str, timeout: int = 5):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def check() -> int:
    """Vérifie conteneurs, cibles Prometheus et master Spark. Retourne 0 si tout va bien."""
    ok = True

    # 1. Conteneurs en cours d'exécution
    print("== Conteneurs ==")
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Status}}"],
            capture_output=True, text=True, check=True,
        ).stdout
    except (subprocess.CalledProcessError, FileNotFoundError) as exc:
        print(f"  ERREUR docker ps : {exc}")
        return 1

    names = []
    for line in sorted(out.strip().splitlines()):
        name, status = line.split("\t", 1)
        names.append(name)
        print(f"  {name:20s} {status}")
    workers = [n for n in names if n.startswith("spark-worker")]
    for required in ("spark-master", "prometheus", "grafana", "cadvisor", "spark-history"):
        if required not in names:
            print(f"  MANQUANT : {required}")
            ok = False
    print(f"  Workers démarrés : {len(workers)}")

    # 2. Cibles Prometheus
    print("== Cibles Prometheus (localhost:9090) ==")
    try:
        targets = _http_json("http://localhost:9090/api/v1/targets")["data"]["activeTargets"]
        for t in sorted(targets, key=lambda t: (t["labels"]["job"], t["scrapeUrl"])):
            state = t["health"]
            print(f"  [{state:4s}] {t['labels']['job']:15s} {t['scrapeUrl']}")
        up_workers = sum(1 for t in targets
                         if t["labels"]["job"] == "spark-workers" and t["health"] == "up")
        if up_workers != len(workers):
            print(f"  ATTENTION : {up_workers} worker(s) up côté Prometheus, "
                  f"{len(workers)} conteneur(s) démarré(s)")
    except Exception as exc:
        print(f"  ERREUR : Prometheus injoignable ({exc})")
        ok = False

    # 3. Master Spark
    print("== Master Spark (localhost:8080) ==")
    try:
        info = _http_json("http://localhost:8080/json/")
        alive = [w for w in info.get("workers", []) if w.get("state") == "ALIVE"]
        cores = sum(w.get("cores", 0) for w in alive)
        print(f"  status={info.get('status')} | workers ALIVE={len(alive)} | "
              f"cœurs totaux={cores} | apps en cours={len(info.get('activeapps', []))}")
        if not alive:
            print("  ATTENTION : aucun worker enregistré auprès du master")
            ok = False
    except Exception as exc:
        print(f"  ERREUR : master injoignable ({exc})")
        ok = False

    print("== " + ("STACK OK" if ok else "STACK DEGRADEE") + " ==")
    return 0 if ok else 1


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 2
    cmd, args = argv[0], argv[1:]
    if cmd == "check":
        return check()
    if cmd in JOBS:
        return submit(cmd, args)
    print(f"commande inconnue : {cmd} (attendu : {' | '.join(JOBS)} | check)")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
