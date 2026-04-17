## @package ingestion.populate_repterms
#  Populates the RepTerms DynamoDB table from the existing Reps table.
#
#  Creates one record per congress term per representative, enabling fast
#  filtered queries by congress number and chamber without scanning the
#  full 12,310-record Reps table.
#
#  Key data structures:
#    - rep:   dict - a single Reps table record with a "terms" list
#    - term:  dict - one congress term with congress, chamber, bioguideId
#    - combo_key: str - "119#Senate" deduplication key per rep per congress
#
#  Run this script once after clearing the RepTerms table, or whenever
#  the Reps table has been significantly updated.
#
#  Usage:
#    python populate_repterms.py

import uuid
from typing import Optional

import boto3

# ---------------------------------------------------------------------------
# DynamoDB table handles
# ---------------------------------------------------------------------------

## Shared DynamoDB resource for us-east-2.
dynamodb = boto3.resource("dynamodb", region_name="us-east-2")

## Source table — contains full representative biographical records.
reps_table = dynamodb.Table("Reps")

## Target table — one record per congress term per representative.
terms_table = dynamodb.Table("RepTerms")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

## Set of canonical chamber names that are valid for the RepTerms table.
#  All other chamber variants (Delegate, Resident Commissioner, etc.) are filtered out.
VALID_CHAMBERS = {"Senate", "House"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

## Performs a full paginated scan of the Reps table.
#
#  DynamoDB scan returns at most 1 MB per request. This function continues
#  issuing requests using LastEvaluatedKey until all pages are exhausted.
#
#  @return  list - all records from the Reps table
def scan_all_reps() -> list:
    items: list = []
    kwargs: dict = {}
    while True:
        resp = reps_table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


## Normalizes chamber name variants to canonical "Senate" or "House".
#
#  The Reps table contains mixed chamber labels from different data sources.
#  This function maps all known variants to a canonical form, and returns
#  None for non-legislative roles that should be excluded from the graph
#  (e.g. Delegate, Resident Commissioner, Vice President).
#
#  @param chamber  str - raw chamber string from a Reps table term record
#  @return         Optional[str] - "Senate", "House", or None if not applicable
def normalize_chamber(chamber: str) -> Optional[str]:
    c = chamber.lower().strip()
    if c in ("senate", "senator"):
        return "Senate"
    if c in ("house", "representative", "house of representatives"):
        return "House"
    # All other roles (Delegate, Resident Commissioner, etc.) are excluded
    return None


## Deletes all existing records from the RepTerms table before repopulating.
#
#  Uses batch_writer for efficient bulk deletion. This ensures the table
#  is in a clean state before the populate step writes new records.
def clear_table() -> None:
    print("Clearing existing RepTerms records...")
    items: list = []
    kwargs: dict = {}
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


## Main population function — reads Reps and writes one RepTerms record per term.
#
#  Algorithm:
#    1. Clear all existing RepTerms records
#    2. Scan all 12,310 Reps records
#    3. For each rep, walk their terms list
#    4. Normalize and validate the chamber name
#    5. Deduplicate by congress + chamber per rep (a rep may have duplicate terms)
#    6. Write one RepTerms record per unique congress + chamber combination
#
#  Prints a summary of inserted records, skipped reps, errors, and the
#  distribution of records across Senate and House chambers.
def populate() -> None:
    clear_table()

    print("\nScanning Reps table...")
    reps = scan_all_reps()
    print(f"Found {len(reps)} reps. Populating RepTerms...")

    inserted = 0
    skipped = 0
    errors = 0
    chamber_counts: dict = {}

    with terms_table.batch_writer() as batch:
        for rep in reps:
            bioguideid = rep.get("bioguideId")
            terms = rep.get("terms", [])

            if not isinstance(terms, list):
                skipped += 1
                continue

            # Track seen congress+chamber combos to avoid duplicate term records
            seen: set = set()

            for t in terms:
                if not isinstance(t, dict):
                    continue

                congress = t.get("congress")
                chamber = t.get("chamber", "").strip()

                if not congress or not chamber:
                    continue

                chamber_norm = normalize_chamber(chamber)

                # Skip non-House/Senate chambers
                if chamber_norm not in VALID_CHAMBERS:
                    continue

                congress_int = int(congress)

                # Deduplicate: one record per rep per congress per chamber
                combo_key = f"{congress_int}#{chamber_norm}"
                if combo_key in seen:
                    continue
                seen.add(combo_key)

                chamber_counts[chamber_norm] = chamber_counts.get(chamber_norm, 0) + 1

                try:
                    batch.put_item(Item={
                        "termId": str(uuid.uuid4()),
                        "congress": congress_int,
                        "chamber": chamber_norm,
                        "bioguideId": bioguideid,
                    })
                    inserted += 1
                    if inserted % 1000 == 0:
                        print(f"  Inserted {inserted} terms...")
                except Exception as e:
                    errors += 1
                    if errors <= 5:
                        print(f"  Error for {bioguideid}: {e}")

    print("\nDone!")
    print(f"  Inserted: {inserted} term records")
    print(f"  Skipped:  {skipped} reps (no terms)")
    print(f"  Errors:   {errors}")
    print("\nChamber distribution:")
    for chamber, count in sorted(chamber_counts.items()):
        print(f"  {chamber}: {count}")
    print("\nRepTerms table is ready!")


if __name__ == "__main__":
    populate()
