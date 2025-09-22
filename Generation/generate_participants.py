#!/usr/bin/env python3
"""
generate_participants.py

Create synthetic Participant rows with profile, demographics, GPA, and status.

Usage:
  python generate_participants.py --n 60 --outfile participants.csv --seed 42
  python generate_participants.py --json participants.json
"""

import argparse
import csv
import json
import random
import string
import sys
import uuid
from datetime import date, datetime, timedelta

# ---------------------------
# Configurable vocabularies
# ---------------------------
FIRST_NAMES = [
    "Avery","Jordan","Taylor","Riley","Quinn","Hayden","Peyton","Logan","Casey","Kai",
    "Maya","Aria","Noah","Liam","Olivia","Emma","Sophia","Isabella","Ethan","Aiden",
    "Amara","Sofia","Zoe","Leo","Mila","Ishan","Anika","Diego","Lucia"
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez",
    "Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin",
    "Lee","Perez","Thompson","White","Harris","Sanchez","Clark","Ramirez","Lewis","Walker"
]
MAJORS = [
    "Computer Science","Biology","Psychology","Economics","Mechanical Engineering",
    "Electrical Engineering","Sociology","Nursing","Business","Mathematics","Chemistry",
    "Political Science","Art History","English","Statistics","Data Science","Neuroscience"
]
GENDERS = ["Female","Male","Nonbinary","Prefer not to say"]
ETHNICITIES = [
    "Asian","Black or African American","Hispanic or Latino","Middle Eastern or North African",
    "Native American or Alaska Native","Native Hawaiian or Other Pacific Islander",
    "White","Two or More Races","Prefer not to say"
]
STATUSES = ["active","paused","ineligible","banned"]
PAYLOAD_STATUS_WEIGHTS = [0.78, 0.12, 0.08, 0.02]  # realistic distribution
EMAIL_DOMAINS = ["example.edu","university.edu","mail.edu","campus.edu"]

# Academic year range for undergrads; adjust if needed
CLASS_YEARS = list(range(2025, 2031))

TODAY = date(2025, 9, 21)  # keep dates stable for reproducibility

# ---------------------------
# Helpers
# ---------------------------
def slugify(s: str) -> str:
    s = s.lower()
    return "".join(ch if ch.isalnum() else "-" for ch in s).strip("-")

def random_phone() -> str:
    # Simple US-style phone; avoids NANP invalids like 0/1 starts
    def block(n, allow_leading_zero=False):
        first = random.randint(2 if not allow_leading_zero else 0, 9)
        rest = "".join(str(random.randint(0,9)) for _ in range(n-1))
        return f"{first}{rest}"
    return f"{block(3)}-{block(3)}-{''.join(str(random.randint(0,9)) for _ in range(4))}"

def random_dob_age(min_age=18, max_age=65):
    age = random.randint(min_age, max_age)
    # Random birthday within that age
    start = TODAY.replace(year=TODAY.year - age) - timedelta(days=365)
    end = TODAY.replace(year=TODAY.year - (age - 1))
    # Clamp for leap years and ordering
    if start > end:
        start, end = end, start
    delta_days = (end - start).days
    dob = start + timedelta(days=random.randint(0, max(delta_days, 0)))
    # Recompute age precisely
    computed_age = TODAY.year - dob.year - ((TODAY.month, TODAY.day) < (dob.month, dob.day))
    return dob, computed_age

def weighted_choice(options, weights):
    return random.choices(options, weights=weights, k=1)[0]

def random_gpa(min_gpa=2.0, max_gpa=4.0):
    # Beta distribution to cluster around 3.2–3.6
    g = random.betavariate(6, 3)  # skew high
    val = min_gpa + g * (max_gpa - min_gpa)
    return round(val, 2)

def random_bio(major):
    templates = [
        "Interested in {major}, research participation, and campus volunteering.",
        "Enjoys intramural sports, hackathons, and learning more about {major}.",
        "Looking to gain exposure to human subjects research related to {major}.",
        "Works part-time, balances coursework in {major} with community projects.",
        "Exploring career paths that connect {major} with real-world impact."
    ]
    return random.choice(templates).format(major=major)

def make_email(first, last):
    handle_variants = [
        f"{first}.{last}",
        f"{first}{last[0]}",
        f"{first[0]}{last}",
        f"{first}{last}{random.randint(1,99)}",
    ]
    handle = slugify(random.choice(handle_variants))
    domain = random.choice(EMAIL_DOMAINS)
    return f"{handle}@{domain}"

def rand_ts_between(days_back=180):
    # Random timestamp within last N days
    seconds_back = random.randint(0, days_back * 24 * 3600)
    dt = datetime.combine(TODAY, datetime.min.time()) + timedelta(seconds=seconds_back)
    return dt.isoformat()

# ---------------------------
# Row factory
# ---------------------------
def make_participant_row(i: int):
    pid = str(uuid.uuid4())
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)
    email = make_email(first, last)
    phone = random_phone()
    gender = random.choice(GENDERS)
    ethnicity = random.choice(ETHNICITIES)
    major = random.choice(MAJORS)
    class_year = random.choice(CLASS_YEARS)

    dob, age = random_dob_age(18, 65)
    gpa = random_gpa(2.0, 4.0)
    status = weighted_choice(STATUSES, PAYLOAD_STATUS_WEIGHTS)

    created_at = rand_ts_between(365)
    # updated_at at or after created_at
    updated_at = datetime.fromisoformat(created_at) + timedelta(days=random.randint(0, 120), seconds=random.randint(0, 86400))
    bio = random_bio(major)

    return {
        "participant_id": pid,
        "first_name": first,
        "last_name": last,
        "email": email,
        "phone": phone,
        "date_of_birth": dob.isoformat(),
        "age": age,
        "gender": gender,
        "ethnicity": ethnicity,
        "major": major,
        "class_year": class_year,
        "gpa": gpa,
        "status": status,
        "bio": bio,
        "created_at": created_at,
        "updated_at": updated_at.isoformat(timespec="seconds"),
    }

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Participant data.")
    parser.add_argument("--n", type=int, default=60, help="Number of rows to generate (default: 60)")
    parser.add_argument("--outfile", type=str, default="participants.csv", help="Output file path")
    parser.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)

    rows = [make_participant_row(i) for i in range(args.n)]

    if args.json_out or args.outfile.lower().endswith(".json"):
        with open(args.outfile if not args.outfile.endswith(".csv") else "participants.json", "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} participants to JSON.")
    else:
        fieldnames = list(rows[0].keys())
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} participants to CSV: {args.outfile}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
