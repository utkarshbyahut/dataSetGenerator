#!/usr/bin/env python3
"""
generate_sessions.py

Create synthetic Session rows:
- study_id (FK)
- room_id (FK)
- startTs (ISO8601)
- endTs   (ISO8601)
- capacity (int)

Behavior:
- If ./study.csv exists, uses its 'study_id' values. Otherwise synthesizes a pool.
- If ./rooms.csv exists, tries 'room_id' column; if missing, hashes (building:name) to a stable UUID.
  Uses room capacity to bound session capacity when available.
- Schedules sessions across a realistic calendar window with variable durations.
- Avoids overlapping sessions within the same room (best-effort).

Usage examples:
  python3 generate_sessions.py --n 1500 --outfile sessions.csv --seed 42
  python3 generate_sessions.py --n 1500 --json --outfile sessions.json --seed 42
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

# Time window (relative to TODAY) for scheduling sessions
DAYS_PAST = 10
DAYS_FUTURE = 60

# Sessions run between these hours (24h)
START_HOUR_MIN = 8
START_HOUR_MAX = 19

# Session duration (minutes)
DURATION_MIN = 45
DURATION_MAX = 120

# If no room file, fallback room capacity range
ROOM_CAP_FALLBACK = (18, 60)

# ---------------------------
# Helpers
# ---------------------------
def today_at(h: int, m: int = 0) -> datetime:
    return datetime.combine(TODAY, datetime.min.time()) + timedelta(hours=h, minutes=m)

def random_start_dt() -> datetime:
    # choose a day offset and a minute slot
    day_offset = random.randint(-DAYS_PAST, DAYS_FUTURE)
    hour = random.randint(START_HOUR_MIN, START_HOUR_MAX)
    minute = random.choice([0, 15, 30, 45])
    return datetime.combine(TODAY, datetime.min.time()) + timedelta(days=day_offset, hours=hour, minutes=minute)

def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")

def derive_room_id_from_name(building: str, name: str) -> str:
    # Stable UUID5 based on (building:name) so joins are reproducible
    basis = f"{(building or '').strip()}::{(name or '').strip()}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, basis))

# ---------------------------
# Readers (optional local files)
# ---------------------------
def read_study_ids(path: Path) -> List[str]:
    if not path.exists():
        return []  # synthesize later
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        if not data:
            return []
        if isinstance(data[0], dict) and "study_id" in data[0]:
            return [row["study_id"] for row in data if row.get("study_id")]
        # Accept raw list
        return [str(x) for x in data]
    # CSV
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "study_id" not in r.fieldnames:
            return []
        return [row["study_id"] for row in r if row.get("study_id")]

def read_rooms(path: Path) -> List[Dict[str, Any]]:
    """
    Returns list of dicts with:
      room_id (from file or derived), capacity (int or None)
    """
    if not path.exists():
        return []  # synthesize later
    rows: List[Dict[str, Any]] = []
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        for row in data:
            rid = row.get("room_id")
            name = row.get("name", "")
            building = row.get("building", "")
            if not rid:
                if name:
                    rid = derive_room_id_from_name(building, name)
                else:
                    rid = str(uuid.uuid4())
            cap_raw = row.get("capacity")
            cap = int(cap_raw) if isinstance(cap_raw, int) or (isinstance(cap_raw, str) and cap_raw.isdigit()) else None
            rows.append({"room_id": rid, "capacity": cap})
        return rows
    # CSV
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        has_room_id = r.fieldnames and "room_id" in r.fieldnames
        has_name = r.fieldnames and "name" in r.fieldnames
        has_building = r.fieldnames and "building" in r.fieldnames
        has_capacity = r.fieldnames and "capacity" in r.fieldnames
        for row in r:
            if has_room_id:
                rid = row.get("room_id") or str(uuid.uuid4())
            else:
                nm = row.get("name", "") if has_name else ""
                bldg = row.get("building", "") if has_building else ""
                rid = derive_room_id_from_name(bldg, nm) if nm else str(uuid.uuid4())
            cap = None
            if has_capacity:
                try:
                    cap = int(row["capacity"])
                except Exception:
                    cap = None
            rows.append({"room_id": rid, "capacity": cap})
    return rows

# ---------------------------
# Non-overlap scheduling per room
# ---------------------------
def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return not (a_end <= b_start or b_end <= a_start)

def pick_times_for_room(existing: List[Tuple[datetime, datetime]]) -> Tuple[datetime, datetime]:
    """
    Pick a start/end that doesn't overlap existing intervals for the room.
    Try a handful of times before giving up overlap avoidance.
    """
    for _ in range(20):
        start = random_start_dt()
        duration = random.randint(DURATION_MIN, DURATION_MAX)
        end = start + timedelta(minutes=duration)
        if all(not overlaps(start, end, s, e) for (s, e) in existing):
            return start, end
    # Fallback: return the last attempt even if overlapping
    return start, end

# ---------------------------
# Row factory
# ---------------------------
def make_row(study_id: str, room: Dict[str, Any], room_schedules: Dict[str, List[Tuple[datetime, datetime]]]) -> Dict[str, Any]:
    rid = room["room_id"]
    sched = room_schedules.setdefault(rid, [])
    start, end = pick_times_for_room(sched)
    sched.append((start, end))

    # Session capacity: respect room capacity when available
    if room.get("capacity"):
        cap_hi = max(2, room["capacity"])
        capacity = random.randint(max(2, min(6, cap_hi)), cap_hi)
    else:
        capacity = random.randint(*ROOM_CAP_FALLBACK)

    return {
        "study_id": study_id,
        "room_id": rid,
        "startTs": iso(start),
        "endTs": iso(end),
        "capacity": capacity,
    }

# ---------------------------
# Main
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Generate synthetic Session data.")
    ap.add_argument("--n", type=int, default=500, help="Number of sessions (default: 500)")
    ap.add_argument("--outfile", type=str, default="sessions.csv", help="Output path")
    ap.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    ap.add_argument("--seed", type=int, default=1337, help="Random seed")
    # Optional local files (auto-detected if present)
    ap.add_argument("--studies-file", default="study.csv", help="Optional path to studies CSV/JSON (default: ./study.csv)")
    ap.add_argument("--rooms-file", default="rooms.csv", help="Optional path to rooms CSV/JSON (default: ./rooms.csv)")
    # If no files, synthesize pools:
    ap.add_argument("--study-pool", type=int, default=200, help="Synthesized study_id pool size (fallback)")
    ap.add_argument("--room-pool", type=int, default=80, help="Synthesized room_id pool size (fallback)")
    args = ap.parse_args()

    random.seed(args.seed)

    # Load or synthesize studies
    study_ids = read_study_ids(Path(args.studies_file))
    if not study_ids:
        study_ids = [str(uuid.uuid4()) for _ in range(args.study_pool)]

    # Load or synthesize rooms
    rooms = read_rooms(Path(args.rooms_file))
    if not rooms:
        rooms = [{"room_id": str(uuid.uuid4()), "capacity": random.randint(*ROOM_CAP_FALLBACK)} for _ in range(args.room_pool)]

    # Build rows
    room_schedules: Dict[str, List[Tuple[datetime, datetime]]] = {}
    rows: List[Dict[str, Any]] = []
    for _ in range(args.n):
        study_id = random.choice(study_ids)
        room = random.choice(rooms)
        rows.append(make_row(study_id, room, room_schedules))

    # Write
    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "sessions.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} sessions to JSON: {out}")
    else:
        fieldnames = ["study_id","room_id","startTs","endTs","capacity"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} sessions to CSV: {args.outfile}")

if __name__ == "__main__":
    main()
