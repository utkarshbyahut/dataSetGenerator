#!/usr/bin/env python3
"""
generate_rooms.py

Create synthetic Room rows:
- name, building, capacity

Usage examples:
  python3 generate_rooms.py --n 200 --outfile rooms.csv --seed 42
  python3 generate_rooms.py --n 500 --json --outfile rooms.json --seed 7
"""

import argparse
import csv
import json
import random

# ---------------------------
# Vocabularies
# ---------------------------
BUILDINGS = [
    "Engineering Center", "Science Hall", "Computer Science Building",
    "Chemistry Complex", "Physics Pavilion", "Biology Annex",
    "Mathematics Tower", "Business Center", "Humanities Hall",
    "Library West", "Art & Design Studios", "Music & Performing Arts",
    "Psychology Building", "Health Sciences Center", "Education Hall",
    "Athletics Complex", "Innovation Hub", "Data Science Institute",
    "Law School", "Medical Research Building"
]

ROOM_TYPES = [
    "lecture", "lecture", "lecture",  # weight toward lecture rooms
    "lab", "lab", "lab",
    "seminar", "seminar",
    "studio", "room", "room"
]

LETTERS = list("ABCDEFGHJKMNPQRSTUVWXZ")  # omit confusing I/O/Y/Z sometimes

# ---------------------------
# Helpers
# ---------------------------
def pick_room_name(rtype: str) -> str:
    # Generate a plausible room name based on type
    if rtype == "lecture":
        # Lecture Hall 1..300
        return f"Lecture Hall {random.randint(1, 300)}"
    if rtype == "lab":
        # Lab 1..80 + optional letter
        base = f"Lab {random.randint(1, 80)}"
        return base + random.choice(["", f"{random.choice(LETTERS)}"])
    if rtype == "seminar":
        # Seminar 100..599
        return f"Seminar {random.randint(100, 599)}"
    if rtype == "studio":
        # Studio A..Z + 1..20
        return f"Studio {random.choice(LETTERS)}-{random.randint(1, 20)}"
    # generic room: floor + number (e.g., 2-14, 3-08)
    floor = random.randint(1, 6)
    num = random.randint(1, 35)
    return f"{floor}-{num:02d}"

def pick_capacity(rtype: str) -> int:
    if rtype == "lecture":
        return random.randint(80, 300)
    if rtype == "lab":
        return random.randint(12, 30)
    if rtype == "seminar":
        return random.randint(10, 24)
    if rtype == "studio":
        return random.randint(15, 35)
    # generic rooms
    return random.randint(18, 45)

def make_row(existing_pairs) -> dict:
    building = random.choice(BUILDINGS)
    # ensure (building, name) uniqueness
    for _ in range(20):  # try a few times to avoid collisions
        rtype = random.choice(ROOM_TYPES)
        name = pick_room_name(rtype)
        if (building, name) not in existing_pairs:
            existing_pairs.add((building, name))
            return {
                "name": name,
                "building": building,
                "capacity": pick_capacity(rtype),
            }
    # fallback: force unique by appending a suffix
    suffix = random.randint(1000, 9999)
    rtype = random.choice(ROOM_TYPES)
    name = f"{pick_room_name(rtype)}-{suffix}"
    existing_pairs.add((building, name))
    return {
        "name": name,
        "building": building,
        "capacity": pick_capacity(rtype),
    }

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Room data.")
    parser.add_argument("--n", type=int, default=200, help="Number of rows (default: 200)")
    parser.add_argument("--outfile", type=str, default="rooms.csv", help="Output path")
    parser.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)

    rows = []
    seen = set()  # (building, name)
    for _ in range(args.n):
        rows.append(make_row(seen))

    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "rooms.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} rooms to JSON: {out}")
    else:
        fieldnames = ["name", "building", "capacity"]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} rooms to CSV: {args.outfile}")

if __name__ == "__main__":
    main()
