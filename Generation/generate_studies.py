#!/usr/bin/env python3
"""
generate_studies.py

Create synthetic Study rows:
- title, description, minAge, maxAge, minGPA, cooldownDays, active
(+ study_id, created_at, updated_at for convenience)

Usage:
  python generate_studies.py --n 40 --outfile studies.csv --seed 7
  python generate_studies.py --n 80 --json --outfile studies.json
"""

import argparse
import csv
import json
import random
import uuid
from datetime import date, datetime, timedelta

TODAY = date(2025, 9, 21)  # stable for reproducibility

# ---------------------------
# Vocabularies
# ---------------------------
DOMAINS = [
    "Cognitive Psychology", "Human-Computer Interaction", "Sleep & Memory",
    "Nutrition & Performance", "Exercise Science", "Social Behavior",
    "Learning & Education", "Attention & Perception", "Language Processing",
    "Decision Making", "Neuroscience", "Mental Health", "Usability Testing",
    "Human Factors", "Affect & Emotion", "Motivation & Goals",
]
METHODS = [
    "online survey", "lab-based task", "EEG session", "eye-tracking study",
    "VR interaction task", "mobile app diary", "A/B usability test",
    "behavioral game", "reaction-time task", "interview session",
]
POPULATIONS = [
    "undergraduates", "graduate students", "general adults", "bilingual speakers",
    "habitual nappers", "competitive athletes", "night owls", "early risers",
    "heavy social media users", "first-year students", "STEM majors",
]
INCENTIVES = [
    "$10 gift card", "$15 gift card", "$20 gift card", "course credit",
    "$25 gift card", "snacks + course credit"
]
GOALS = [
    "measure short-term memory accuracy", "quantify decision speed under time pressure",
    "evaluate UI learnability", "assess the impact of sleep duration on recall",
    "study effects of nutrition on reaction time", "model social conformity behavior",
    "compare different feedback strategies on learning", "analyze eye movements during reading",
    "test usability of a new mobile interface", "understand bilingual lexical access",
]

# ---------------------------
# Helpers
# ---------------------------
def sentence_case(s: str) -> str:
    return s[0].upper() + s[1:] if s else s

def make_title():
    domain = random.choice(DOMAINS)
    method = random.choice(METHODS)
    goal = random.choice(GOALS)
    # Short, plausible study title
    variants = [
        f"{domain}: {sentence_case(goal)}",
        f"{domain} via {method.title()}",
        f"{sentence_case(goal)} ({domain})",
        f"{domain} – {sentence_case(goal)}",
    ]
    return random.choice(variants)

def make_description():
    domain = random.choice(DOMAINS)
    method = random.choice(METHODS)
    pop = random.choice(POPULATIONS)
    incentive = random.choice(INCENTIVES)
    goal = random.choice(GOALS)
    est = random.choice([20, 30, 35, 45, 60, 75, 90])
    return (
        f"This {domain.lower()} study uses a {method} with {pop}. "
        f"It aims to {goal}. Approx. {est} minutes. Compensation: {incentive}. "
        "Participation is voluntary; you may withdraw at any time."
    )

def bounded_age_pair():
    # Typical IRB ranges; allow 18–65, with some broader
    min_age = random.choice([18, 18, 18, 21])  # skew to 18+
    max_age = random.choice([45, 55, 60, 65])
    if max_age < min_age:
        max_age = min_age + random.randint(1, 5)
    return min_age, max_age

def random_min_gpa():
    # Many studies accept wide range; skew toward 2.0–3.0
    choices = [2.0, 2.3, 2.5, 2.7, 3.0, 3.2, 3.5]
    weights =  [30,  15,  20,  12,  15,   6,   2]  # favors lower thresholds
    return random.choices(choices, weights=weights, k=1)[0]

def random_cooldown_days():
    # Cooldown between enrollments in the SAME study
    return random.choice([0, 7, 14, 21, 30])

def random_active_flag():
    # ~70% active by default
    return random.random() < 0.7

def rand_ts_between(days_back=365):
    seconds_back = random.randint(0, days_back * 24 * 3600)
    dt = datetime.combine(TODAY, datetime.min.time()) + timedelta(seconds=seconds_back)
    return dt

def make_row():
    study_id = str(uuid.uuid4())
    title = make_title()
    description = make_description()
    min_age, max_age = bounded_age_pair()
    min_gpa = random_min_gpa()
    cooldown = random_cooldown_days()
    active = random_active_flag()

    created_at = rand_ts_between(540)
    updated_at = created_at + timedelta(days=random.randint(0, 180), seconds=random.randint(0, 86400))

    return {
        "study_id": study_id,
        "title": title,
        "description": description,
        "minAge": min_age,
        "maxAge": max_age,
        "minGPA": round(min_gpa, 2),
        "cooldownDays": cooldown,
        "active": active,
        "created_at": created_at.isoformat(timespec="seconds"),
        "updated_at": updated_at.isoformat(timespec="seconds"),
    }

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser(description="Generate synthetic Study data.")
    parser.add_argument("--n", type=int, default=40, help="Number of rows (default: 40)")
    parser.add_argument("--outfile", type=str, default="studies.csv", help="Output path")
    parser.add_argument("--json", dest="json_out", action="store_true", help="Write JSON instead of CSV")
    parser.add_argument("--seed", type=int, default=1337, help="Random seed")
    args = parser.parse_args()

    random.seed(args.seed)

    rows = [make_row() for _ in range(args.n)]

    # CSV or JSON
    if args.json_out or args.outfile.lower().endswith(".json"):
        out = args.outfile if args.outfile.lower().endswith(".json") else "studies.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print(f"Wrote {len(rows)} studies to JSON: {out}")
    else:
        fieldnames = [
            "study_id","title","description","minAge","maxAge",
            "minGPA","cooldownDays","active","created_at","updated_at"
        ]
        with open(args.outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Wrote {len(rows)} studies to CSV: {args.outfile}")

if __name__ == "__main__":
    main()