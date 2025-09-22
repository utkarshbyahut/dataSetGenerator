#!/usr/bin/env python3
"""
generate_participant_consents.py

Create synthetic ParticipantConsent rows (no external inputs):
- participant_consent_id (UUID)
- participant_id (UUID; synthesized)
- consent_version_id (UUID; synthesized)
- signedAt (ISO8601; recent timestamp)
- withdrawnAt (ISO8601 or blank; optional)

Usage examples:
  python3 generate_participant_consents.py --n 1500 --outfile ParticipantConsents.csv --seed 42
  python3 generate_participant_consents.py --n 1500 --json --outfile ParticipantConsents.json --seed 42
"""

import argparse
import csv
import json
import random
import uuid
from datetime import date, datetime, timedelta

# ---------------------------
# Config
# ---------------------------
TODAY = date(2025, 9, 21)             # keep dates stable for reproducibility
SIGNED_WINDOW_DAYS = 540              # how far back signedAt can be
WITHDRAW_MAX_DAYS_AFTER_SIGN = 240    # max days after signedAt for withdrawnAt

# ---------------------------
# Helpers
# ---------------------------
def today_start() -> datetime:
    return datetime.combine(TODAY, datetime.min.time())

def today_end() -> datetime:
    return datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=23, minutes=59, seconds=59)

def rand_dt_between(a: datetime, b: datetime) -> datetime:
    if b < a:
        a, b = b, a
    delta = int((b - a).total_seconds())
    return a + timedelta(seconds=random.randint(0, max(0, delta)))

def make_id() -> str:
    return str(uuid.uuid4())

# ---------------------------
# Row factory
# ---------------------------
def make_participant_consent_row(pid: str, cvid: str, withdraw_rate: float) -> dict:
    # signed within the last SIGNED_WINDOW_DAYS
    signed_at = rand_dt_between(today_start() - timedelta(days=SIGNED_WINDOW_DAYS), today_end())

    # maybe produce a withdrawnAt after signedAt (and not in the future)
    withdrawn_at = ""
    if random.random() < withdraw_rate:
        latest = min(signed_at + timedelta(days=WITHDRAW_MAX_DAYS_AFTER_SIGN), today_end())
        if latest > signed_at:
            withdrawn_at = rand_dt_between(signed_at + timedelta(minutes=1), latest).isoformat(timespec="seconds")

    return {
        "participant_consent_id": make_id(),
        "participant_id": pid,
        "consent_version_id": cvid,
        "signedAt": signed_at.isoformat(timespec="seconds"),
        "withdrawnAt": withdrawn_at,
    }

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate synthetic ParticipantConsent data (standalone).")
    parser.add_argument("--n", type=int, default=1000, help="Number of rows (default: 1000)")
    parser.add_argument("--participants", type=int, default=500, help="Size of participant_id pool (default: 500)")
    parser.add_argument("--versions", type=int, default=300, help="Size of consent_version_id pool (default: 300)")
    parser.add_argument("--withdraw-rate", type=float, default=0.15, help="Probability a row has withdrawnAt (default: 0.15)")
    parser.add_argument("--allow-duplicates", action="store_true", help="Allow multiple rows per (participant, consent_version_id)")
    parser.add_argument("--outfile", type=str, default="ParticipantConsents.csv", help="Output file path")
    parser.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)

    # Make ID pools
    participant_ids = [make_id() for _ in range(args.participants)]
    consent_version_ids = [make_id() for _ in range(args.versions)]

    rows = []
    used_pairs = set()
    attempts = 0
    max_attempts = args.n * 10  # safety to avoid infinite loops if not allowing duplicates

    while len(rows) < args.n and attempts < max_attempts:
        attempts += 1
        pid = random.choice(participant_ids)
        cvid = random.choice(consent_version_ids)
        pair = (pid, cvid)

        if not args.allow_duplicates and pair in used_pairs:
            continue

        row = make_participant_consent_row(pid, cvid, args.withdraw_rate)
        rows.append(row)
        used_pairs.add(pair)

    # Write output
    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "ParticipantConsents.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} ParticipantConsent rows to JSON: {out}")
    else:
        fieldnames = ["participant_consent_id","participant_id","consent_version_id","signedAt","withdrawnAt"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} ParticipantConsent rows to CSV: {args.outfile}")

if __name__ == "__main__":
    main()
