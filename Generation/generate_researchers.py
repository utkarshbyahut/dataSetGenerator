#!/usr/bin/env python3
"""
generate_participant_consents.py

Create synthetic ParticipantConsent rows (no external inputs):
- participant_consent_id (UUID)
- participant_id (UUID; synthesized)
- consent_version_id (UUID; synthesized)
- signedAt (ISO8601; random time within recent days)
- withdrawnAt (ISO8601 or blank; optional after signedAt)

Usage examples:
  python generate_participant_consents.py --n 1500 --outfile ParticipantConsents.csv --seed 42

Options:
  --participants N : size of the participant_id pool (default: 500)
  --versions N     : size of the consent_version_id pool (default: 300)
  --withdraw-rate  : probability a row has withdrawnAt (default: 0.15)
  --allow-duplicates : allow multiple rows per (participant_id, consent_version_id) pair
  --json           : write JSON instead of CSV
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
TODAY = date(2025, 9, 21)  # keep dates stable for reproducibility

# Signed times in the last this-many days
SIGNED_WINDOW_DAYS = 540
# If withdrawn, must occur within this many days after signedAt (and not after "today")
WITHDRAW_MAX_DAYS_AFTER_SIGN = 240

# ---------------------------
# Helpers
# ---------------------------
def rand_dt_between(start_dt: datetime, end_dt: datetime) -> datetime:
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt
    delta = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=random.randint(0, max(0, delta)))

def today_start() -> datetime:
    return datetime.combine(TODAY, datetime.min.time())

def today_end() -> datetime:
    return datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=23, minutes=59, seconds=59)

def make_id() -> str:
    return str(uuid.uuid4())

# ---------------------------
# Row factory
# ---------------------------
def make_row(pid: str, cvid: str, withdraw_rate: float) -> dict:
    # signedAt somewhere in the last SIGNED_WINDOW_DAYS
    start = today_start() - timedelta(days=SIGNED_WINDOW_DAYS)
    signed_at = rand_dt_between(start, today_end())

    # maybe withdrawn after signedAt, but not beyond today or WITHDRAW_MAX_DAYS_AFTER_SIGN
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
    parser.add_argument("--outfile", type=str, default="ParticipantConsents.csv", help="Output path")
    parser.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)

    # Build pools of participant_ids and consent_version_ids
    participants = [make_id() for _ in range(args.participants)]
    versions = [make_id() for _ in range(args.versions)]

    rows = []
    used_pairs = set()
    attempts = 0
    max_attempts = args.n * 10

    while len(rows) < args.n and attempts < max_attempts:
        attempts += 1
        pid = random.choice(participants)
        cvid = random.choice(versions)
        pair = (pid, cvid)

        if not args.allow_duplicates and pair in used_pairs:
            continue

        row = make_row(pid, cvid, args.withdraw_rate)
        rows.append(row)
        used_pairs.add(pair)

    # Write out
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
