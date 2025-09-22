#!/usr/bin/env python3
"""
generate_participant_consents.py

Generate ParticipantConsent rows with ONLY:
- participant_consent_id (UUID)
- participant_id (FK)
- consent_version_id (FK)
- signedAt (ISO8601)
- withdrawnAt (ISO8601 or blank)

Signed times fall within each consent version's [effectiveFrom, effectiveTo or today].
At most one row per (participant_id, consent_version_id) unless --allow-duplicates is set.

Usage:
  python3 generate_participant_consents.py \
    --participants-file participants.csv \
    --consents-file ConsentVersions.csv \
    --n 1500 --withdraw-rate 0.12 \
    --outfile ParticipantConsents.csv --seed 42
"""

import argparse
import csv
import json
import random
import sys
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

TODAY = date(2025, 9, 21)  # keep dates stable for reproducibility

# ---------------------------
# Helpers
# ---------------------------
def parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def today_end_dt() -> datetime:
    return datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=23, minutes=59, seconds=59)

# ---------------------------
# Input readers
# ---------------------------
def read_participant_ids(path: str) -> List[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"participants file not found: {path}")
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        if not data:
            return []
        if isinstance(data[0], dict):
            key = "participant_id" if "participant_id" in data[0] else "id"
            return [row[key] for row in data if row.get(key)]
        return [str(x) for x in data]
    with open(p, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or ("participant_id" not in r.fieldnames and "id" not in r.fieldnames):
            raise ValueError("Participants CSV must include 'participant_id' (or 'id').")
        key = "participant_id" if "participant_id" in r.fieldnames else "id"
        return [row[key] for row in r if row.get(key)]

def read_consent_versions(path: str) -> List[Dict[str, Any]]:
    """
    Expect columns: consent_version_id, effectiveFrom, effectiveTo
    (effectiveTo may be blank to mean 'open/current')
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"consent versions file not found: {path}")
    rows: List[Dict[str, Any]] = []
    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        for row in data:
            rows.append({
                "consent_version_id": row.get("consent_version_id") or row.get("id"),
                "_from": parse_dt(row.get("effectiveFrom", "")),
                "_to": parse_dt(row.get("effectiveTo", "")),
            })
    else:
        with open(p, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            need = {"consent_version_id", "effectiveFrom", "effectiveTo"}
            if not r.fieldnames or not need.issubset(set(r.fieldnames)):
                raise ValueError("Consent versions CSV must include: consent_version_id,effectiveFrom,effectiveTo")
            for row in r:
                rows.append({
                    "consent_version_id": row["consent_version_id"],
                    "_from": parse_dt(row["effectiveFrom"]),
                    "_to": parse_dt(row.get("effectiveTo", "")),
                })
    end_today = today_end_dt()
    # keep only versions whose effectiveFrom has begun
    return [r for r in rows if r["_from"] and r["_from"] <= end_today]

# ---------------------------
# Core generation
# ---------------------------
def pick_signed(eff_from: datetime, eff_to: Optional[datetime]) -> Optional[datetime]:
    end = min(eff_to or today_end_dt(), today_end_dt())
    if end < eff_from:
        return None
    delta = int((end - eff_from).total_seconds())
    return eff_from + timedelta(seconds=random.randint(0, max(0, delta)))

def maybe_withdraw(signed_at: datetime, eff_to: Optional[datetime], rate: float) -> str:
    if random.random() >= rate:
        return ""
    last = min(eff_to or today_end_dt(), today_end_dt())
    if last <= signed_at:
        return ""
    delta = int((last - signed_at).total_seconds())
    when = signed_at + timedelta(seconds=random.randint(60, max(60, delta)))
    if when > last:
        when = last
    return when.isoformat(timespec="seconds")

def make_rows(
    participants: List[str],
    versions: List[Dict[str, Any]],
    n: int,
    withdraw_rate: float,
    allow_duplicates: bool,
) -> List[Dict[str, Any]]:
    if not participants or not versions:
        return []
    out: List[Dict[str, Any]] = []
    used = set()  # (participant_id, consent_version_id)
    attempts = 0
    max_attempts = n * 10
    while len(out) < n and attempts < max_attempts:
        attempts += 1
        pid = random.choice(participants)
        v = random.choice(versions)
        cvid = v["consent_version_id"]
        if not allow_duplicates and (pid, cvid) in used:
            continue
        signed = pick_signed(v["_from"], v["_to"])
        if not signed:
            continue
        out.append({
            "participant_consent_id": str(uuid.uuid4()),
            "participant_id": pid,
            "consent_version_id": cvid,
            "signedAt": signed.isoformat(timespec="seconds"),
            "withdrawnAt": maybe_withdraw(signed, v["_to"], withdraw_rate),
        })
        used.add((pid, cvid))
    return out

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate ParticipantConsent rows (minimal schema).")
    parser.add_argument("--participants-file", required=True, help="CSV/JSON with 'participant_id' or 'id'")
    parser.add_argument("--consents-file", required=True, help="CSV/JSON with consent_version_id + effectiveFrom/To")
    parser.add_argument("--n", type=int, default=1000, help="Number of rows (default: 1000)")
    parser.add_argument("--withdraw-rate", type=float, default=0.15, help="Probability withdrawnAt is set")
    parser.add_argument("--allow-duplicates", action="store_true", help="Allow multiple rows per (participant, consent_version_id)")
    parser.add_argument("--outfile", type=str, default="ParticipantConsents.csv", help="Output CSV path (or .json)")
    parser.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)

    participants = read_participant_ids(args.participants_file)
    if not participants:
        raise ValueError("No participants found.")
    versions = read_consent_versions(args.consents_file)
    if not versions:
        raise ValueError("No consent versions found (or all are future-only).")

    rows = make_rows(participants, versions, args.n, args.withdraw_rate, args.allow_duplicates)

    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "ParticipantConsents.json"
        Path(out).write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {len(rows)} ParticipantConsent rows to JSON: {out}")
    else:
        fieldnames = ["participant_consent_id","participant_id","consent_version_id","signedAt","withdrawnAt"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} ParticipantConsent rows to CSV: {args.outfile}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
