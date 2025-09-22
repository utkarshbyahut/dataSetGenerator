#!/usr/bin/env python3
"""
generate_enrollments.py

Create synthetic Enrollment rows:
- participant_id (FK)
- session_id     (FK)
- status         (enrolled | waitlisted | cancelled | attended | no_show)
- created_at     (ISO8601)
- updated_at     (ISO8601)

Behavior:
- If ./participants.csv exists, uses its 'participant_id' (or 'id') values. Otherwise synthesizes a pool.
- If ./sessions.csv exists, reads startTs/endTs/capacity and derives a stable session_id if missing.
- Respects room/session capacity when available (enrolled/attended/no_show occupy seats; cancelled/waitlisted do not).
- Timestamps align with session times (enroll before start; attended/no_show update after end).

Usage examples:
  python3 generate_enrollments.py --n 1500 --outfile enrollments.csv --seed 42
  python3 generate_enrollments.py --n 1500 --json --outfile enrollments.json --seed 42
"""

import argparse
import csv
import json
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ---------------------------
# Config
# ---------------------------
TODAY = date(2025, 9, 21)  # stable for reproducibility

# How far before a session enrollments can start appearing
ENROLL_OPEN_DAYS = 90

# Status distribution when a seat is available (future sessions)
STATUS_WEIGHTS_FUTURE = {
    "enrolled": 0.70,
    "cancelled": 0.12,
    "waitlisted": 0.08,
    "no_show": 0.00,   # not applicable before session
    "attended": 0.00,  # not applicable before session
}

# Status distribution when the session is in the past (seat was taken)
STATUS_WEIGHTS_PAST = {
    "attended": 0.55,
    "no_show": 0.12,
    "cancelled": 0.08,
    "enrolled": 0.25,  # enrolled but no final outcome recorded
    "waitlisted": 0.00,
}

# If no sessions/participants files, fallback pool sizes
FALLBACK_PARTICIPANTS = 1200
FALLBACK_SESSIONS = 400

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

def today_start() -> datetime:
    return datetime.combine(TODAY, datetime.min.time())

def today_end() -> datetime:
    return datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=23, minutes=59, seconds=59)

def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")

def uuid5_for_session(study_id: str, room_id: str, start_ts: str) -> str:
    basis = f"{(study_id or '').strip()}::{(room_id or '').strip()}::{(start_ts or '').strip()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, basis))

def rand_dt_between(a: datetime, b: datetime) -> datetime:
    if b < a:
        a, b = b, a
    delta = int((b - a).total_seconds())
    return a + timedelta(seconds=random.randint(0, max(0, delta)))

def pick_weighted(weights: Dict[str, float]) -> str:
    items = list(weights.items())
    labels, probs = zip(*items)
    return random.choices(labels, weights=probs, k=1)[0]

# ---------------------------
# Readers (optional local files)
# ---------------------------
def read_participant_ids(path: Path) -> List[str]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not data:
            return []
        if isinstance(data, list) and data and isinstance(data[0], dict):
            key = "participant_id" if "participant_id" in data[0] else "id"
            return [row[key] for row in data if row.get(key)]
        return [str(x) for x in data]
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or ("participant_id" not in reader.fieldnames and "id" not in reader.fieldnames):
            return []
        key = "participant_id" if "participant_id" in reader.fieldnames else "id"
        return [row[key] for row in reader if row.get(key)]

def read_sessions(path: Path) -> List[Dict[str, Any]]:
    """
    Returns dicts with at least: session_id, startTs, endTs, capacity (optional)
    If session_id missing, derives stable UUID5 from (study_id, room_id, startTs)
    """
    if not path.exists():
        return []
    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        for row in data:
            sid = row.get("session_id")
            if not sid:
                sid = uuid5_for_session(row.get("study_id", ""), row.get("room_id", ""), row.get("startTs", ""))
            cap = row.get("capacity", None)
            rows.append({
                "session_id": sid,
                "startTs": row.get("startTs"),
                "endTs": row.get("endTs"),
                "capacity": int(cap) if isinstance(cap, int) or (isinstance(cap, str) and cap.isdigit()) else None,
            })
        return rows
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        has_session_id = r.fieldnames and "session_id" in r.fieldnames
        for row in r:
            sid = row.get("session_id") if has_session_id else uuid5_for_session(row.get("study_id",""), row.get("room_id",""), row.get("startTs",""))
            cap = None
            if "capacity" in (r.fieldnames or []):
                try:
                    cap = int(row["capacity"])
                except Exception:
                    cap = None
            rows.append({
                "session_id": sid,
                "startTs": row.get("startTs"),
                "endTs": row.get("endTs"),
                "capacity": cap,
            })
    return rows

# ---------------------------
# Row factory
# ---------------------------
def build_enrollment_for(
    participant_id: str,
    sess: Dict[str, Any],
    used_pairs: set,
    session_used: Dict[str, int],
) -> Optional[Dict[str, Any]]:
    session_id = sess["session_id"]
    if (participant_id, session_id) in used_pairs:
        return None

    start = parse_dt(sess.get("startTs", "")) or today_start() + timedelta(days=7)
    end = parse_dt(sess.get("endTs", "")) or (start + timedelta(hours=1))
    now = today_end()

    # Determine if seat available
    cap = sess.get("capacity")
    taken = session_used.get(session_id, 0)
    seat_available = (cap is None) or (taken < max(0, cap))

    # Choose status based on timing and seat
    if seat_available:
        if end <= now:  # session in the past
            status = pick_weighted(STATUS_WEIGHTS_PAST)
        else:
            status = pick_weighted(STATUS_WEIGHTS_FUTURE)
    else:
        status = "waitlisted"

    # created_at before session start (or before now if start in past)
    open_from = start - timedelta(days=ENROLL_OPEN_DAYS)
    open_to = min(start - timedelta(hours=1), now)
    if open_to <= open_from:
        open_to = start - timedelta(minutes=30)
        open_from = open_to - timedelta(days=1)
    created_at = rand_dt_between(open_from, open_to)

    # updated_at depends on status
    if status == "cancelled":
        # cancel between created and (start - 10 min) or now
        last = min(start - timedelta(minutes=10), now)
        if last <= created_at:
            last = created_at + timedelta(minutes=5)
        updated_at = rand_dt_between(created_at + timedelta(minutes=1), last)
    elif status in ("attended", "no_show"):
        # update shortly after session end (if in the past)
        base = end if end <= now else start
        updated_at = rand_dt_between(base, min(base + timedelta(hours=3), now))
    else:
        # enrolled/waitlisted: updated sometime after created, but not past now/start
        last = min(now, start)
        if last <= created_at:
            last = created_at + timedelta(minutes=5)
        updated_at = rand_dt_between(created_at, last)

    # Count seat usage if it occupies capacity
    if seat_available and status in ("enrolled", "attended", "no_show"):
        session_used[session_id] = taken + 1

    used_pairs.add((participant_id, session_id))
    return {
        "participant_id": participant_id,
        "session_id": session_id,
        "status": status,
        "created_at": iso(created_at),
        "updated_at": iso(updated_at),
    }

# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate synthetic Enrollment data.")
    ap.add_argument("--n", type=int, default=1000, help="Number of enrollment rows (default: 1000)")
    ap.add_argument("--outfile", type=str, default="enrollments.csv", help="Output path")
    ap.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")

    # Optional local inputs; defaults to current directory
    ap.add_argument("--participants-file", default="participants.csv", help="Optional: path to participants CSV/JSON")
    ap.add_argument("--sessions-file", default="sessions.csv", help="Optional: path to sessions CSV/JSON")
    # If files missing, synthesize pools:
    ap.add_argument("--participant-pool", type=int, default=FALLBACK_PARTICIPANTS, help="Fallback participant pool size")
    ap.add_argument("--session-pool", type=int, default=FALLBACK_SESSIONS, help="Fallback session pool size")
    args = ap.parse_args()

    random.seed(args.seed)

    # Load participants
    participants = read_participant_ids(Path(args.participants_file))
    if not participants:
        participants = [str(uuid.uuid4()) for _ in range(args.participant_pool)]

    # Load sessions
    sessions = read_sessions(Path(args.sessions_file))
    if not sessions:
        # synthesize sessions (without times, but give a plausible spread)
        sessions = []
        base = today_start()
        for i in range(args.session_pool):
            start = base + timedelta(days=random.randint(-10, 60), hours=random.randint(8, 19), minutes=random.choice([0, 15, 30, 45]))
            end = start + timedelta(minutes=random.randint(45, 120))
            sessions.append({
                "session_id": str(uuid.uuid4()),
                "startTs": iso(start),
                "endTs": iso(end),
                "capacity": random.randint(18, 60),
            })

    # Build enrollments
    used_pairs: set = set()
    session_used: Dict[str, int] = {}
    rows: List[Dict[str, Any]] = []

    attempts = 0
    max_attempts = args.n * 15
    while len(rows) < args.n and attempts < max_attempts:
        attempts += 1
        pid = random.choice(participants)
        sess = random.choice(sessions)
        row = build_enrollment_for(pid, sess, used_pairs, session_used)
        if row:
            rows.append(row)

    # Write out
    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "enrollments.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} enrollments to JSON: {out}")
    else:
        fieldnames = ["participant_id","session_id","status","created_at","updated_at"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} enrollments to CSV: {args.outfile}")

if __name__ == "__main__":
    main()
