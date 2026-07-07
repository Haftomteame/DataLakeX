#!/usr/bin/env python3
"""Télécharge les pageviews horaires Wikimédia dans data/raw/<mois>/.

Usage :
    python download.py 2026-06 7

Télécharge <jours> jours complets (24 fichiers/jour) depuis le 1er du mois.
Un fichier déjà présent et non vide n'est pas retéléchargé (reprise possible).
Le script temporise entre deux fichiers : l'endpoint est un service public,
ne modifiez pas ce comportement.
"""

import sys
import time
import urllib.request
from datetime import date, timedelta
from pathlib import Path

BASE_URL = "https://dumps.wikimedia.org/other/pageviews"
PAUSE_S = 3  # temporisation entre deux GET — ne pas modifier
USER_AGENT = "crunch-tp-ipssi/1.0 (usage pedagogique)"


def download(month: str, days: int) -> None:
    year, mon = month.split("-")
    out_dir = Path("data") / "raw" / month
    out_dir.mkdir(parents=True, exist_ok=True)

    start = date(int(year), int(mon), 1)
    total = days * 24
    done = 0

    for d in range(days):
        day = start + timedelta(days=d)
        for hour in range(24):
            fname = f"pageviews-{day:%Y%m%d}-{hour:02d}0000.gz"
            dest = out_dir / fname
            done += 1
            if dest.exists() and dest.stat().st_size > 0:
                print(f"[{done:3d}/{total}] {fname} déjà présent, ignoré")
                continue

            url = f"{BASE_URL}/{year}/{year}-{mon}/{fname}"
            print(f"[{done:3d}/{total}] GET {url}")
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            tmp = dest.with_suffix(".gz.part")
            try:
                with urllib.request.urlopen(req, timeout=120) as resp, open(tmp, "wb") as f:
                    while True:
                        chunk = resp.read(1 << 20)
                        if not chunk:
                            break
                        f.write(chunk)
                tmp.rename(dest)
            except Exception as exc:
                tmp.unlink(missing_ok=True)
                print(f"    ECHEC : {exc} — relancez le script pour reprendre", file=sys.stderr)
                sys.exit(1)

            time.sleep(PAUSE_S)

    print(f"Terminé : {total} fichiers dans {out_dir}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    download(sys.argv[1], int(sys.argv[2]))
