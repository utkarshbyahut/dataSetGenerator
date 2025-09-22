#!/usr/bin/env python3
"""
generate_study_researchers.py

Create synthetic StudyResearcher rows (which researchers belong to which study):
- study_id (FK)
- researcher_id (FK)
- role: one of {"PI","coordinator","RA"}

Behavior:
- Uses ./study.csv (or ./studies.csv) to load study_id values when available; otherwise synthesizes a pool.
- Uses ./researchers.csv to load researcher_id values when available; otherwise synthesizes a pool.
- Ensures at least one PI per study.
- Adds 0–2 coordinators per study (weighted toward 0–1).
- Adds 1–3 RAs per study by default.
- If --n is set and exceeds the baseline count, it will add extra edges (mostly RAs) until >= n rows.

Usage:
  python3 generate_study_researchers.py --n 1500 --outfile study_researchers.csv --seed 42
  python3 generate_study_researchers.py --json --outfile study_researchers.json --seed 7
"""

import argparse
import csv
import json
import random
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------
# Config
# ---------------------------
FALLBACK_STUDIES = 300
FALLBACK_RESEARCHERS = 500

COORD_CHOICES = [0, 1, 2]
COORD_WEIGHTS = [0.60, 0.30, 0.10]

RA_MIN_DEFAULT = 1
RA_MAX_DEFAULT = 3

TOPUP_ROLE_WEIGHTS = {"RA": 0.80, "coordinator": 0.15, "PI": 0.05}

# ---------------------------
# Readers
# ---------------------------
def read_study_ids(primary: Path, alternate: Path) -> List[str]:
    path = primary if primary.exists() else (alternate if alternate.exists() else None)
    if path is None:
        return []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not data:
            return []
        if isinstance(data, list) and data and isinstance(data[0], dict):
            return [row["study_id"] for row in data if row.get("study_id")]
        return [str(x) for x in data]
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "study_id" not in r.fieldnames:
            return []
        return [row["study_id"] for row in r if row.get("study_id")]

def read_researcher_ids(path: Path) -> List[str]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not data:
            return []
        if isinstance(data, list) and data and isinstance(data[0], dict):
            key = "researcher_id" if "researcher_id" in data[0] else None
            if key:
                return [row[key] for row in data if row.get(key)]
            return []
        return [str(x) for x in data]
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "researcher_id" not in r.fieldnames:
            return []
        return [row["researcher_id"] for row in r if row.get("researcher_id")]

# ---------------------------
# Core generation
# ---------------------------
def pick_unique(researchers: List[str], forbid: Set[str], k: int) -> List[str]:
    choices = [rid for rid in researchers if rid not in forbid]
    random.shuffle(choices)
    return choices[:k]

def baseline_assignments(
    studies: List[str],
    researchers: List[str],
    pi_per_study: int,
    coord_weights: Tuple[List[int], List[float]],
    ra_min: int,
    ra_max: int,
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for sid in studies:
        used_here: Set[str] = set()

        pis = pick_unique(researchers, used_here, max(1, pi_per_study))
        for rid in pis:
            rows.append({"study_id": sid, "researcher_id": rid, "role": "PI"})
            used_here.add(rid)

        k_coord = random.choices(coord_weights[0], weights=coord_weights[1], k=1)[0]
        coords = pick_unique(researchers, used_here, k_coord)
        for rid in coords:
            rows.append({"study_id": sid, "researcher_id": rid, "role": "coordinator"})
            used_here.add(rid)

        k_ra = random.randint(ra_min, ra_max)
        ras = pick_unique(researchers, used_here, k_ra)
        for rid in ras:
            rows.append({"study_id": sid, "researcher_id": rid, "role": "RA"})
            used_here.add(rid)

    return rows

def top_up_to_target(
    rows: List[Dict[str, str]],
    target_n: int,
    studies: List[str],
    researchers: List[str],
    role_weights: Dict[str, float],
) -> List[Dict[str, str]]:
    if target_n <= 0 or len(rows) >= target_n:
        return rows

    used_pairs: Set[Tuple[str, str]] = {(r["study_id"], r["researcher_id"]) for r in rows}
    role_labels = list(role_weights.keys())
    role_probs = [role_weights[k] for k in role_labels]

    attempts = 0
    max_attempts = (target_n - len(rows)) * 20
    while len(rows) < target_n and attempts < max_attempts:
        attempts += 1
        sid = random.choice(studies)
        rid = random.choice(researchers)
        if (sid, rid) in used_pairs:
            continue
        role = random.choices(role_labels, weights=role_probs, k=1)[0]
        rows.append({"study_id": sid, "researcher_id": rid, "role": role})
        used_pairs.add((sid, rid))
    return rows

# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate synthetic StudyResearcher edges.")
    ap.add_argument("--outfile", type=str, default="study_researchers.csv", help="Output path")
    ap.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")

    ap.add_argument("--n", type=int, default=0, help="Target at least N rows (0 = per-study baseline only)")
    ap.add_argument("--pi-per-study", type=int, default=1, help="Number of PIs per study (default: 1)")
    ap.add_argument("--ra-min", type=int, default=RA_MIN_DEFAULT, help="Min RAs per study (default: 1)")
    ap.add_argument("--ra-max", type=int, default=RA_MAX_DEFAULT, help="Max RAs per study (default: 3)")

    ap.add_argument("--studies-file", default="study.csv", help="Path to studies CSV/JSON (default: ./study.csv)")
    ap.add_argument("--alt-studies-file", default="studies.csv", help="Alternate studies path (default: ./studies.csv)")
    ap.add_argument("--researchers-file", default="researchers.csv", help="Path to researchers CSV/JSON (default: ./researchers.csv)")

    ap.add_argument("--study-pool", type=int, default=FALLBACK_STUDIES, help="Synthesized study pool size")
    ap.add_argument("--researcher-pool", type=int, default=FALLBACK_RESEARCHERS, help="Synthesized researcher pool size")

    args = ap.parse_args()
    random.seed(args.seed)

    # Load pools
    studies = read_study_ids(Path(args.studies_file), Path(args.alt_studies_file))
    if not studies:
        studies = [str(uuid.uuid4()) for _ in range(args.study_pool)]

    researchers = read_researcher_ids(Path(args.researchers_file))
    if not researchers:
        researchers = [str(uuid.uuid4()) for _ in range(args.researcher_pool)]

    # Guardrails (FIXED: use underscore, not hyphen)
    if args.pi_per_study < 1:
        args.pi_per_study = 1
    if args.ra_min < 0:
        args.ra_min = 0
    if args.ra_max < args.ra_min:
        args.ra_max = max(args.ra_min, 1)

    # Build baseline and top up if needed
    rows = baseline_assignments(
        studies=studies,
        researchers=researchers,
        pi_per_study=args.pi_per_study,
        coord_weights=(COORD_CHOICES, COORD_WEIGHTS),
        ra_min=args.ra_min,
        ra_max=args.ra_max,
    )

    if args.n and len(rows) < args.n:
        rows = top_up_to_target(
            rows=rows,
            target_n=args.n,
            studies=studies,
            researchers=researchers,
            role_weights=TOPUP_ROLE_WEIGHTS,
        )

    # Write
    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "study_researchers.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} study_researchers rows to JSON: {out}")
    else:
        fieldnames = ["study_id","researcher_id","role"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} study_researchers rows to CSV: {args.outfile}")

if __name__ == "__main__":
    main()
