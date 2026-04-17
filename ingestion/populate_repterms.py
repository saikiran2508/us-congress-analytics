"""
Populate RepTerms table from existing Reps table.

Creates one record per congress term per representative.
Handles all chamber variants: House, Senate, Delegate, Resident Commissioner, etc.

Run after clearing the RepTerms table.

Usage:
    python populate_repterms.py
"""

import boto3
import uuid
from decimal import Decimal

dynamodb    = boto3.resource("dynamodb", region_name="us-east-2")
reps_table  = dynamodb.Table("Reps")
terms_table = dynamodb.Table("RepTerms")


def scan_all_reps():
    items = []
    kwargs = {}
    while True:
        resp = reps_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


# Only include these two chambers — everything else is noise
VALID_CHAMBERS = {"Senate", "House"}


def normalize_chamber(chamber):
    """
    Normalize chamber variants. Returns None for non House/Senate roles.
    Filters out: Delegate, Resident Commissioner, President,
    Vice President, Speaker Of The House, etc.
    """
    c = chamber.lower().strip()
    if c in ("senate", "senator"):
        return "Senate"
    if c in ("house", "representative", "house of representatives"):
        return "House"
    # Everything else is filtered out
    return None


def clear_table():
    """Delete all existing items from RepTerms before repopulating."""
    print("Clearing existing RepTerms records...")
    items = []
    kwargs = {}
    while True:
        resp = terms_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    deleted = 0
    with terms_table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={"termId": item["termId"]})
            deleted += 1
    print(f"  Deleted {deleted} existing records.")


def populate():
    # Clear existing data first
    clear_table()

    print("\nScanning Reps table...")
    reps = scan_all_reps()
    print(f"Found {len(reps)} reps. Populating RepTerms...")

    inserted = 0
    skipped  = 0
    errors   = 0
    chamber_counts = {}

    with terms_table.batch_writer() as batch:
        for rep in reps:
            bioguideid = rep.get("bioguideId")
            terms = rep.get("terms", [])

            if not isinstance(terms, list):
                skipped += 1
                continue

            seen = set()

            for t in terms:
                if not isinstance(t, dict):
                    continue

                congress = t.get("congress")
                chamber  = t.get("chamber", "").strip()

                if not congress or not chamber:
                    continue

                chamber_norm = normalize_chamber(chamber)

                # Skip non House/Senate chambers
                if chamber_norm not in VALID_CHAMBERS:
                    continue

                congress_int = int(congress) if isinstance(congress, Decimal) else int(congress)

                # Track unique congress+chamber per rep
                combo_key = f"{congress_int}#{chamber_norm}"
                if combo_key in seen:
                    continue
                seen.add(combo_key)

                # Track chamber distribution
                chamber_counts[chamber_norm] = chamber_counts.get(chamber_norm, 0) + 1

                try:
                    batch.put_item(Item={
                        "termId":     str(uuid.uuid4()),
                        "congress":   congress_int,
                        "chamber":    chamber_norm,
                        "bioguideId": bioguideid,
                    })
                    inserted += 1
                    if inserted % 1000 == 0:
                        print(f"  Inserted {inserted} terms...")
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  Error for {bioguideid}: {e}")

    print(f"\nDone!")
    print(f"  Inserted: {inserted} term records")
    print(f"  Skipped:  {skipped} reps (no terms)")
    print(f"  Errors:   {errors}")
    print(f"\nChamber distribution:")
    for chamber, count in sorted(chamber_counts.items()):
        print(f"  {chamber}: {count}")
    print(f"\nRepTerms table is ready!")


if __name__ == "__main__":
    populate()