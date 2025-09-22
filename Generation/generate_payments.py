#!/usr/bin/env python3
"""
generate_payments.py

Create synthetic Payment rows (enrollment-level):
- participant_id (FK to enrollment)
- session_id     (FK to enrollment)
- amount         (numeric; USD-like)
- method         (e.g., gift_card, cash, credit_card, paypal, venmo, none)
- status         (paid, pending, failed, refunded, waived, void)

Behavior:
- If ./enrollments.csv exists, generates a payment row per sampled enrollment
  and maps payment status sensibly from enrollment.status.
- If not present, synthesizes an enrollment pool.

Usage examples:
  python3 generate_payments.py --n 1500 --outfile payments.csv --seed 42
  python3 generate_payments.py --n 1500 --json --outfile payments.json --seed 7
"""

import argparse
import csv
import json
import random
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------
# Config
# ---------------------------
# Fallback pool sizes (if no enrollments.csv)
FALLBACK_PARTICIPANTS = 1200
FALLBACK_SESSIONS = 400

# Incentive buckets (USD-like)
AMOUNT_BUCKETS = [10, 15, 20, 25, 30, 40, 50]
AMOUNT_WEIGHTS = [10, 22, 28, 20, 12, 5, 3]  # favors $15–$25

# Payment methods and weights
METHODS = ["gift_card", "cash", "credit_card", "paypal", "venmo"]
METHOD_WEIGHTS = [45, 20, 15, 10, 10]  # %

# ---------------------------
# Helpers
# ---------------------------
def pick_amount() -> int:
    return random.choices(AMOUNT_BUCKETS, weights=AMOUNT_WEIGHTS, k=1)[0]

def pick_method() -> str:
    return random.choices(METHODS, weights=METHOD_WEIGHTS, k=1)[0]

def read_enrollments(path: Path) -> List[Dict[str, Any]]:
    """
    Expects CSV/JSON with participant_id, session_id, status (other fields ignored).
    Returns a list of dicts with those keys (missing entries skipped).
    """
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        for r in data:
            pid, sid = r.get("participant_id"), r.get("session_id")
            st = (r.get("status") or "").strip().lower()
            if pid and sid:
                rows.append({"participant_id": pid, "session_id": sid, "status": st})
        return rows
    # CSV
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        flds = set(r.fieldnames or [])
        need = {"participant_id", "session_id"}
        if not need.issubset(flds):
            return []
        for row in r:
            pid, sid = row.get("participant_id"), row.get("session_id")
            st = (row.get("status") or "").strip().lower()
            if pid and sid:
                rows.append({"participant_id": pid, "session_id": sid, "status": st})
    return rows

def synth_enrollments(n_pairs: int) -> List[Dict[str, Any]]:
    """Create a synthetic pool of (participant_id, session_id, status)."""
    participants = [str(uuid.uuid4()) for _ in range(FALLBACK_PARTICIPANTS)]
    sessions = [str(uuid.uuid4()) for _ in range(FALLBACK_SESSIONS)]
    statuses = ["enrolled", "waitlisted", "cancelled", "attended", "no_show"]
    weights  = [0.45,       0.10,         0.10,        0.28,      0.07]
    rows = []
    used = set()
    attempts = 0
    while len(rows) < n_pairs and attempts < n_pairs * 10:
        attempts += 1
        pid = random.choice(participants)
        sid = random.choice(sessions)
        if (pid, sid) in used:
            continue
        rows.append({"participant_id": pid, "session_id": sid,
                     "status": random.choices(statuses, weights=weights, k=1)[0]})
        used.add((pid, sid))
    return rows

def map_payment_status(enrollment_status: str) -> str:
    """Map enrollment.status to a plausible payment.status."""
    s = (enrollment_status or "").lower()
    if s == "attended":
        return random.choices(["paid", "pending", "refunded", "failed"], [82, 8, 5, 5], k=1)[0]
    if s == "no_show":
        return random.choices(["waived", "pending", "paid", "refunded", "failed"], [70, 10, 5, 5, 10], k=1)[0]
    if s == "cancelled":
        return random.choices(["refunded", "void", "failed", "waived"], [60, 35, 3, 2], k=1)[0]
    if s == "waitlisted":
        return "void"
    # default: enrolled / unknown
    return random.choices(["pending", "paid", "failed", "refunded", "waived"], [85, 5, 3, 2, 5], k=1)[0]

def amount_for_status(pay_status: str) -> int:
    """Zero out amounts for statuses where no money changes hands."""
    if pay_status in ("void", "waived"):
        return 0
    return pick_amount()

def method_for_status(pay_status: str) -> str:
    """Use 'none' method when no payment is processed."""
    if pay_status in ("void", "waived"):
        return "none"
    return pick_method()

# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate synthetic Payment data (enrollment-level).")
    ap.add_argument("--n", type=int, default=1000, help="Number of payment rows (default: 1000)")
    ap.add_argument("--outfile", type=str, default="payments.csv", help="Output path")
    ap.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")
    ap.add_argument("--enrollments-file", default="enrollments.csv", help="Optional path to enrollments CSV/JSON")
    # When enrollments are missing, we synthesize a pool this big to draw from:
    ap.add_argument("--fallback-pool", type=int, default=2000, help="Synthetic enrollment pool size if file missing")
    args = ap.parse_args()

    random.seed(args.seed)

    # Load enrollments or synthesize
    enrollments = read_enrollments(Path(args.enrollments_file))
    if not enrollments:
        enrollments = synth_enrollments(args.fallback_pool)

    # Build payments
    rows: List[Dict[str, Any]] = []
    used_pairs = set()  # prevent duplicate payments for same (participant_id, session_id)
    attempts = 0
    max_attempts = args.n * 10
    while len(rows) < args.n and attempts < max_attempts:
        attempts += 1
        e = random.choice(enrollments)
        pair = (e["participant_id"], e["session_id"])
        if pair in used_pairs:
            continue
        pay_status = map_payment_status(e.get("status", ""))
        amount = amount_for_status(pay_status)
        method = method_for_status(pay_status)
        rows.append({
            "participant_id": e["participant_id"],
            "session_id": e["session_id"],
            "amount": amount,
            "method": method,
            "status": pay_status,
        })
        used_pairs.add(pair)

    # Write out
    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "payments.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} payments to JSON: {out}")
    else:
        fieldnames = ["participant_id","session_id","amount","method","status"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} payments to CSV: {args.outfile}")

if __name__ == "__main__":
    main()
