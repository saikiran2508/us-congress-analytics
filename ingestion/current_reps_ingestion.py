## @package ingestion.current_reps_ingestion
#  Ingests current US Congressional member data from the Congress.gov API.
#
#  Fetches member records for a specified congress number and writes them
#  to a DynamoDB table. Uses an incremental update strategy — existing
#  fields are never overwritten, only missing fields are filled in.
#
#  Key data structures:
#    - member:     dict - raw member object from Congress.gov API
#    - term:       dict - one congress term with chamber, party, state, dates
#    - existing:   dict - current DynamoDB record for the same bioguideId
#    - updates:    dict - fields to write (only those missing in DynamoDB)
#
#  Usage:
#    python current_reps_ingestion.py --congress 119 --table Reps
#    python current_reps_ingestion.py --congress 119 --table Reps --dry-run

import os
import re
import json
import argparse
import datetime as dt
from decimal import Decimal
from typing import Optional, Iterator

import requests
import boto3
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

## Base URL for the Congress.gov REST API v3.
API_ROOT = "https://api.congress.gov/v3"

## Congress.gov API key — loaded from environment variable for security.
API_KEY = os.getenv("CONGRESS_API_KEY", "")

## Default request headers for Congress.gov API calls.
HEADERS = {"X-Api-Key": API_KEY, "Accept": "application/json"}

## Browser-like User-Agent string used for Bioguide fallback requests.
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

## Base headers for Bioguide.congress.gov requests to avoid bot detection.
HEADERS_BASE = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

## Recursively converts Python floats to Decimal for DynamoDB compatibility.
#
#  DynamoDB does not accept Python float values. This function walks the
#  entire data structure and converts every float to Decimal.
#
#  @param val  any - dict, list, float, or any other Python value
#  @return     any - same structure with all floats replaced by Decimal
def to_dynamo(val: any) -> any:
    if isinstance(val, dict):
        return {k: to_dynamo(v) for k, v in val.items()}
    if isinstance(val, list):
        return [to_dynamo(v) for v in val]
    if isinstance(val, float):
        return Decimal(str(val))
    return val


## Returns the current Eastern Time as a formatted string.
#
#  Tries zoneinfo (Python 3.9+), then dateutil, then falls back to UTC-5.
#
#  @return str - formatted datetime string e.g. "2024-01-15 - 10:30:00 ET"
def now_et_string() -> str:
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
    except Exception:
        try:
            from dateutil.tz import gettz
            et = gettz("America/New_York")
        except Exception:
            et = dt.timezone(dt.timedelta(hours=-5))
    d = dt.datetime.now(et)
    return f"{d:%Y-%m-%d} - {d:%H:%M:%S} ET"


## Converts a display name to a URL-friendly slug.
#
#  Used to construct Congress.gov member profile URLs.
#  Example: "Joaquin Castro" -> "joaquin-castro"
#
#  @param name  str - representative display name
#  @return      str - lowercase hyphenated slug safe for use in URLs
def name_slug(name: str) -> str:
    if not name:
        return "member"
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9 -]", "", slug)
    slug = slug.replace(" ", "-")
    return slug


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

## Fetches an existing representative record from DynamoDB by bioguide ID.
#
#  Returns an empty dict if the record does not exist or if the request fails.
#
#  @param table        any - boto3 DynamoDB Table resource
#  @param bioguide_id  str - bioguide ID to look up (e.g. "C000127")
#  @return             dict - existing DynamoDB item, or empty dict if not found
def get_existing_item(table: any, bioguide_id: str) -> dict:
    try:
        r = table.get_item(Key={"bioguideId": bioguide_id})
        return r.get("Item") or {}
    except Exception:
        return {}


## Updates only the missing fields in an existing DynamoDB record.
#
#  Never overwrites fields that already have values. Builds a DynamoDB
#  UpdateExpression dynamically from the provided updates dict, skipping
#  any keys with None values. Always sets updateDate on every write.
#
#  @param table        any  - boto3 DynamoDB Table resource
#  @param bioguide_id  str  - primary key of the record to update
#  @param updates      dict - field names and new values to write if missing
def update_missing_fields(table: any, bioguide_id: str, updates: dict) -> None:
    if not updates:
        return

    expr_parts: list = []
    expr_values: dict = {}
    expr_names: dict = {}

    for key, value in updates.items():
        if value is None:
            continue
        safe_key = f"#f_{key}"
        val_key = f":v_{key}"
        expr_parts.append(f"{safe_key} = {val_key}")
        expr_values[val_key] = to_dynamo(value)
        expr_names[safe_key] = key

    if not expr_parts:
        return

    expr_parts.append("#f_updateDate = :v_updateDate")
    expr_values[":v_updateDate"] = now_et_string()
    expr_names["#f_updateDate"] = "updateDate"

    table.update_item(
        Key={"bioguideId": bioguide_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names,
    )


# ---------------------------------------------------------------------------
# Congress.gov API helpers
# ---------------------------------------------------------------------------

## Extracts the members list from a Congress.gov API response payload.
#
#  Handles two response formats returned by different API versions:
#  direct "members" key at root level, or nested under a "data" key.
#
#  @param payload  dict - raw JSON response from the Congress.gov API
#  @return         list - list of member dicts, or empty list if not found
def extract_members(payload: dict) -> list:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("members"), list):
        return payload["members"]
    data = payload.get("data") or {}
    if isinstance(data.get("members"), list):
        return data["members"]
    return []


## Paginates through all members of a given congress from Congress.gov API.
#
#  Yields one member dict at a time. Automatically handles pagination using
#  offset increments until fewer results than the page limit are returned.
#
#  @param congress      int              - congress number to fetch (e.g. 119)
#  @param limit         int              - page size, max 250 (default 250)
#  @param current_only  bool             - filter to current members only
#  @param sess          requests.Session - optional shared session (default None)
#  @return              Iterator[dict]   - yields one member dict per iteration
def list_members(
    congress: int,
    limit: int = 250,
    current_only: bool = True,
    sess: Optional[requests.Session] = None
) -> Iterator[dict]:
    s = sess or requests.Session()
    offset = 0
    limit = max(1, min(limit, 250))
    url = f"{API_ROOT}/member/congress/{congress}"
    while True:
        params = {"limit": limit, "offset": offset, "format": "json"}
        if current_only:
            params["currentMember"] = "true"
        r = s.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        members = extract_members(data)
        if not members:
            break
        for m in members:
            yield m
        if len(members) < limit:
            break
        offset += limit


## Fetches the detailed member record from Congress.gov API by bioguide ID.
#
#  Returns an empty dict if the member is not found (HTTP 404).
#
#  @param bioguide_id  str              - bioguide ID to look up (e.g. "C000127")
#  @param sess         requests.Session - active HTTP session
#  @return             dict             - full member detail payload, or empty dict
def get_member_detail(bioguide_id: str, sess: requests.Session) -> dict:
    url = f"{API_ROOT}/member/{bioguide_id}"
    r = sess.get(url, headers=HEADERS, params={"format": "json"}, timeout=30)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()


## Fetches biographical data from the Bioguide website as a fallback source.
#
#  Used to fill in the Bio field when it is missing from the Congress.gov API.
#  Returns an empty dict on any error or non-JSON response.
#
#  @param sess         requests.Session - active HTTP session
#  @param bioguide_id  str              - bioguide ID to look up
#  @return             dict             - "data" object from Bioguide JSON response,
#                                         or empty dict on failure
def fetch_bioguide_json(sess: requests.Session, bioguide_id: str) -> dict:
    url = f"https://bioguide.congress.gov/search/bio/{bioguide_id}.json"
    try:
        r = sess.get(url, timeout=25, allow_redirects=True, headers={
            **HEADERS_BASE,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://bioguide.congress.gov/search/bio/{bioguide_id}",
            "Origin": "https://bioguide.congress.gov",
        })
        ct = (r.headers.get("Content-Type") or "").lower()
        if r.status_code == 200 and "json" in ct:
            js = r.json()
            data = js.get("data") or {}
            return data if isinstance(data, dict) else {}
        else:
            print(f"  [bioguide] {bioguide_id} status={r.status_code}")
    except Exception as e:
        print(f"  [bioguide] {bioguide_id} error: {e}")
    return {}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

## Builds a display name from a Congress.gov member dict.
#
#  Tries firstName + lastName + middleName first, falls back to displayName.
#
#  @param m  dict - member object from Congress.gov API response
#  @return   Optional[str] - full display name, or None if unavailable
def build_name_from_member(m: dict) -> Optional[str]:
    first = (m.get("firstName") or m.get("givenName") or "").strip()
    last = (m.get("lastName") or m.get("familyName") or "").strip()
    middle = (m.get("middleName") or m.get("middleInitial") or "").strip()
    display = (m.get("name") or m.get("displayName") or "").strip()
    parts = [p for p in [first, last, middle] if p]
    return " ".join(parts) if parts else (display or None)


## Parses all congressional terms from a Congress.gov member dict.
#
#  Builds a flat list of term dicts from the "terms" array in the member
#  object. Normalizes chamber names and constructs ISO date strings from
#  separate year/month/day fields. Applies latest party from partyHistory
#  to any terms that are missing a party value.
#
#  @param m  dict - member object from Congress.gov API response
#  @return   list - sorted list of term dicts, each containing:
#                   congress, chamber, district, state, party, start, departure
def parse_terms_from_member(m: dict) -> list:
    terms_src = m.get("terms") or []
    terms = []
    for t in terms_src:
        if not isinstance(t, dict):
            continue
        congress = t.get("congress")
        chamber = t.get("chamber")
        if isinstance(chamber, str):
            c = chamber.lower()
            if "senate" in c:
                chamber = "Senate"
            elif "house" in c or "representative" in c:
                chamber = "House"

        def iso(prefix: str) -> Optional[str]:
            y = t.get(prefix + "Year")
            mo = t.get(prefix + "Month")
            d = t.get(prefix + "Day")
            try:
                if y and mo and d:
                    return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                if y:
                    return f"{int(y):04d}-01-01"
            except Exception:
                return None
            return None

        terms.append({
            "congress": congress,
            "chamber": chamber,
            "district": t.get("district") or t.get("regionType") or None,
            "state": t.get("stateCode") or t.get("stateAbbrev") or t.get("state"),
            "party": None,
            "start": iso("start"),
            "departure": iso("end"),
        })

    # Remove terms with no identifying date or congress number
    terms = [
        x for x in terms
        if x.get("congress") or x.get("start") or x.get("departure")
    ]
    terms.sort(key=lambda x: (
        x.get("start") or "0000-00-00",
        x.get("congress") or 0
    ))

    # Apply latest party from partyHistory to terms missing a party value
    ph = m.get("partyHistory") or []
    party_latest = None
    if isinstance(ph, list) and ph:
        pe = ph[-1]
        if isinstance(pe, dict):
            party_latest = pe.get("partyName") or pe.get("party")
    if party_latest:
        for t in terms:
            if not t.get("party"):
                t["party"] = party_latest

    return terms


## Resolves the representative's photo URL from the depiction field.
#
#  Falls back to a standard placeholder image if no photo is available.
#
#  @param m  dict - member object from Congress.gov API response
#  @return   str  - absolute URL to the representative's photo
def image_from_member(m: dict) -> str:
    placeholder = "https://bioguide.congress.gov/assets/placeholder_square.png"
    dep = m.get("depiction")
    if isinstance(dep, dict):
        img_url = dep.get("imageUrl") or dep.get("thumbnail")
        if img_url:
            return img_url
    return placeholder


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

## Entry point — parses CLI arguments and runs the full ingestion pipeline.
#
#  Fetches all members for the specified congress from the Congress.gov API,
#  compares each record against the existing DynamoDB entry, and writes only
#  the missing or changed fields. Supports a dry-run mode that previews
#  changes locally without writing to DynamoDB.
#
#  CLI arguments:
#    --congress  int   - congress number to ingest (default 119)
#    --table     str   - DynamoDB table name (required)
#    --region    str   - AWS region (default from AWS_REGION env var)
#    --limit     int   - API page size, max 250 (default 250)
#    --total     int   - max members to process, 0 = all (default 545)
#    --dry-run         - preview changes without writing to DynamoDB
#    --out       str   - output file path for dry-run preview JSON
def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Ingest members from Congress.gov API. "
            "Updates only missing fields - never overwrites existing data."
        )
    )
    ap.add_argument("--congress", type=int, default=119)
    ap.add_argument("--table", required=True, help="DynamoDB table name")
    ap.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-2"))
    ap.add_argument("--limit", type=int, default=250)
    ap.add_argument("--total", type=int, default=545,
                    help="Max members to process. 0 = all.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Preview locally without writing to DynamoDB.")
    ap.add_argument("--out", default="members_preview.json")
    args = ap.parse_args()

    if not API_KEY:
        raise SystemExit("Set CONGRESS_API_KEY environment variable.")

    sess = requests.Session()
    sess.headers.update(HEADERS_BASE)

    ddb = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    if args.dry_run:
        print("DRY RUN - nothing will be written to DynamoDB\n")

    seen: set = set()
    total = 0
    updated = 0
    all_items: list = []

    for m in list_members(args.congress, limit=args.limit,
                          current_only=True, sess=sess):
        # Extract bioguide ID from multiple possible field names
        bioguide = (
            m.get("bioguideId") or m.get("bioguideID") or m.get("bioguide") or ""
        )
        if not bioguide:
            ids = m.get("ids") or {}
            bioguide = ids.get("bioguideId") or ids.get("bioguide") or ""
        if not bioguide or bioguide in seen:
            continue

        cg_detail = get_member_detail(bioguide, sess) or {}
        api_member = cg_detail.get("member") or {}
        existing = {} if args.dry_run else get_existing_item(table, bioguide)

        updates: dict = {}
        new_name = build_name_from_member(api_member)
        new_image = image_from_member(api_member)
        new_terms = parse_terms_from_member(api_member)

        if new_name:
            updates["name"] = new_name
        if new_image:
            updates["image"] = new_image
        if new_terms:
            updates["terms"] = new_terms

        # Only update birth if currently missing in DynamoDB
        if not existing.get("birth"):
            birth = api_member.get("birthYear") or api_member.get("birthDate")
            if birth:
                updates["birth"] = birth

        # Only update Bio if currently missing — fetch from Bioguide as fallback
        if not existing.get("Bio"):
            bg = fetch_bioguide_json(sess, bioguide)
            bio = (
                bg.get("profileText") or bg.get("biographyText") or
                bg.get("profile") or None
            )
            if bio:
                updates["Bio"] = bio

        if args.dry_run:
            print(f"\n{'=' * 60}")
            print(f"[{total + 1}] {bioguide} - {new_name}")
            print(f"  birth: {existing.get('birth')} -> "
                  f"{updates.get('birth', '(keep)')}")
            print(f"  Bio:   {'yes' if existing.get('Bio') else 'missing'} -> "
                  f"{'found' if updates.get('Bio') else '(still missing)'}")
            print(f"  Terms: {len(new_terms)}")
            all_items.append({**existing, **updates, "bioguideId": bioguide})
        else:
            if existing:
                update_missing_fields(table, bioguide, updates)
                updated += 1
                print(f"Updated {bioguide} (total={total + 1}) | "
                      f"{list(updates.keys())}")
            else:
                item = {
                    "bioguideId": bioguide,
                    "name": new_name,
                    "image": new_image,
                    "birth": updates.get("birth"),
                    "Bio": updates.get("Bio"),
                    "terms": new_terms,
                    "updateDate": now_et_string(),
                    "url": (
                        f"https://www.congress.gov/member/"
                        f"{name_slug(new_name)}/{bioguide}"
                    ),
                }
                table.put_item(Item=to_dynamo(
                    {k: v for k, v in item.items() if v is not None}
                ))
                updated += 1
                print(f"Inserted {bioguide} (total={total + 1}) | new record")

        seen.add(bioguide)
        total += 1

        if args.total and total >= args.total:
            break

    if args.dry_run:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(all_items, f, indent=2, default=str)
        print("DRY RUN complete - nothing was written to DynamoDB")
        print(f"Saved to: {args.out}")
    else:
        print("\nIngest complete:")
        print(f"  Processed: {total}")
        print(f"  Updated:   {updated}")


if __name__ == "__main__":
    main()
