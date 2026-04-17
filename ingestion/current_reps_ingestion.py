import os
import re
import json
import argparse
import datetime as dt
import requests
import boto3
from decimal import Decimal

API_ROOT = "https://api.congress.gov/v3"
API_KEY  = "F8GstZbcQIB090NSZ38eEEmsNvaMZtJXuMSXALIX"
HEADERS  = {"X-Api-Key": API_KEY, "Accept": "application/json"}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")
HEADERS_BASE = {
    "User-Agent": UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

def to_dynamo(val):
    if isinstance(val, dict):
        return {k: to_dynamo(v) for k, v in val.items()}
    if isinstance(val, list):
        return [to_dynamo(v) for v in val]
    if isinstance(val, float):
        return Decimal(str(val))
    return val


def now_et_string():
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


def name_slug(name: str) -> str:
    """Convert name to URL slug e.g. 'Joaquin Castro' -> 'joaquin-castro'"""
    if not name:
        return "member"
    slug = name.lower().strip()
    slug = re.sub(r"[^a-z0-9 -]", "", slug)
    slug = slug.replace(" ", "-")
    return slug


# ─────────────────────────────────────────────
# DynamoDB helpers
# ─────────────────────────────────────────────

def get_existing_item(table, bioguide_id: str) -> dict:
    """Fetch existing record from DynamoDB."""
    try:
        r = table.get_item(Key={"bioguideId": bioguide_id})
        return r.get("Item") or {}
    except Exception:
        return {}


def update_missing_fields(table, bioguide_id: str, updates: dict):
    """
    Update only the fields that are missing (None/empty) in DynamoDB.
    Never overwrites existing data.
    """
    if not updates:
        return

    # Build UpdateExpression only for fields that have new values
    expr_parts = []
    expr_values = {}
    expr_names  = {}

    for key, value in updates.items():
        if value is None:
            continue
        safe_key = f"#f_{key}"
        val_key  = f":v_{key}"
        expr_parts.append(f"{safe_key} = {val_key}")
        expr_values[val_key] = to_dynamo(value)
        expr_names[safe_key] = key

    if not expr_parts:
        return

    expr_parts.append("#f_updateDate = :v_updateDate")
    expr_values[":v_updateDate"] = now_et_string()
    expr_names["#f_updateDate"]  = "updateDate"

    table.update_item(
        Key={"bioguideId": bioguide_id},
        UpdateExpression="SET " + ", ".join(expr_parts),
        ExpressionAttributeValues=expr_values,
        ExpressionAttributeNames=expr_names,
    )


# ─────────────────────────────────────────────
# Congress.gov API helpers
# ─────────────────────────────────────────────

def extract_members(payload: dict):
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("members"), list):
        return payload["members"]
    data = payload.get("data") or {}
    if isinstance(data.get("members"), list):
        return data["members"]
    return []


def list_members(congress: int, limit: int = 250,
                 current_only: bool = True, sess=None):
    s = sess or requests.Session()
    offset = 0
    limit  = max(1, min(limit, 250))
    url    = f"{API_ROOT}/member/congress/{congress}"
    while True:
        params = {"limit": limit, "offset": offset, "format": "json"}
        if current_only:
            params["currentMember"] = "true"
        r = s.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        data    = r.json()
        members = extract_members(data)
        if not members:
            break
        for m in members:
            yield m
        if len(members) < limit:
            break
        offset += limit


def get_member_detail(bioguide_id: str, sess: requests.Session) -> dict:
    url = f"{API_ROOT}/member/{bioguide_id}"
    r   = sess.get(url, headers=HEADERS, params={"format": "json"}, timeout=30)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json()


def fetch_bioguide_json(sess: requests.Session, bioguide_id: str) -> dict:
    url = f"https://bioguide.congress.gov/search/bio/{bioguide_id}.json"
    try:
        r  = sess.get(url, timeout=25, allow_redirects=True, headers={
            **HEADERS_BASE,
            "Accept": "application/json, text/plain, */*",
            "Referer": f"https://bioguide.congress.gov/search/bio/{bioguide_id}",
            "Origin": "https://bioguide.congress.gov",
        })
        ct = (r.headers.get("Content-Type") or "").lower()
        if r.status_code == 200 and "json" in ct:
            js   = r.json()
            data = js.get("data") or {}
            return data if isinstance(data, dict) else {}
        else:
            print(f"  [bioguide] {bioguide_id} status={r.status_code}")
    except Exception as e:
        print(f"  [bioguide] {bioguide_id} error: {e}")
    return {}


# ─────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────

def build_name_from_member(m: dict):
    first   = (m.get("firstName")  or m.get("givenName")    or "").strip()
    last    = (m.get("lastName")   or m.get("familyName")   or "").strip()
    middle  = (m.get("middleName") or m.get("middleInitial") or "").strip()
    display = (m.get("name")       or m.get("displayName")  or "").strip()
    parts   = [p for p in [first, last, middle] if p]
    return " ".join(parts) if parts else (display or None)


def parse_terms_from_member(m: dict):
    terms_src = m.get("terms") or []
    terms     = []
    for t in terms_src:
        if not isinstance(t, dict):
            continue
        congress = t.get("congress")
        chamber  = t.get("chamber")
        if isinstance(chamber, str):
            c = chamber.lower()
            if "senate" in c:                            chamber = "Senate"
            elif "house" in c or "representative" in c: chamber = "House"
        def iso(prefix):
            y, mo, d = t.get(prefix+"Year"), t.get(prefix+"Month"), t.get(prefix+"Day")
            try:
                if y and mo and d: return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
                if y:              return f"{int(y):04d}-01-01"
            except Exception:
                return None
            return None
        start    = iso("start")
        end      = iso("end")
        state    = t.get("stateCode") or t.get("stateAbbrev") or t.get("state")
        district = t.get("district") or t.get("regionType") or None
        terms.append({
            "congress":  congress,
            "chamber":   chamber,
            "district":  district,
            "state":     state,
            "party":     None,
            "start":     start,
            "departure": end,
        })
    terms = [x for x in terms
             if x.get("congress") or x.get("start") or x.get("departure")]
    terms.sort(key=lambda x: (x.get("start") or "0000-00-00",
                               x.get("congress") or 0))
    ph           = m.get("partyHistory") or []
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


def image_from_member(m: dict):
    placeholder = "https://bioguide.congress.gov/assets/placeholder_square.png"
    dep = m.get("depiction")
    if isinstance(dep, dict):
        img_url = dep.get("imageUrl") or dep.get("thumbnail")
        if img_url:
            return img_url
    return placeholder


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Ingest members from Congress.gov API. "
                    "Updates only missing fields — never overwrites existing data."
    )
    ap.add_argument("--congress", type=int, default=119)
    ap.add_argument("--table",    required=True, help="DynamoDB table name")
    ap.add_argument("--region",   default="us-east-2")
    ap.add_argument("--limit",    type=int, default=250)
    ap.add_argument("--total",    type=int, default=545,
                    help="Max members to process. 0 = all.")
    ap.add_argument("--dry-run",  action="store_true",
                    help="Preview locally without writing to DynamoDB.")
    ap.add_argument("--out",      default="members_preview.json")
    args = ap.parse_args()

    if not API_KEY:
        raise SystemExit("Set CONGRESS_API_KEY environment variable.")

    sess = requests.Session()
    sess.headers.update(HEADERS_BASE)

    ddb   = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    if args.dry_run:
        print(f"DRY RUN — nothing will be written to DynamoDB\n")

    seen      = set()
    total     = 0
    updated   = 0
    skipped   = 0
    all_items = []

    for m in list_members(args.congress, limit=args.limit,
                          current_only=True, sess=sess):
        bioguide = (
            m.get("bioguideId") or m.get("bioguideID") or m.get("bioguide") or ""
        )
        if not bioguide:
            ids      = m.get("ids") or {}
            bioguide = ids.get("bioguideId") or ids.get("bioguide") or ""
        if not bioguide or bioguide in seen:
            continue

        # Fetch latest data from Congress.gov API
        cg_detail = get_member_detail(bioguide, sess) or {}
        api_member = cg_detail.get("member") or {}

        # Get existing record from DynamoDB
        existing  = {} if args.dry_run else get_existing_item(table, bioguide)

        # Determine what's missing and needs updating
        updates = {}

        # Always update these from latest API
        new_name  = build_name_from_member(api_member)
        new_image = image_from_member(api_member)
        new_terms = parse_terms_from_member(api_member)

        if new_name:
            updates["name"] = new_name
        if new_image:
            updates["image"] = new_image
        if new_terms:
            updates["terms"] = new_terms

        # Only update birth if missing
        if not existing.get("birth"):
            birth = (
                api_member.get("birthYear") or
                api_member.get("birthDate") or
                None
            )
            if birth:
                updates["birth"] = birth

        # Only update Bio if missing (Bioguide data)
        if not existing.get("Bio"):
            bg  = fetch_bioguide_json(sess, bioguide)
            bio = (
                bg.get("profileText")   or
                bg.get("biographyText") or
                bg.get("profile")       or
                None
            )
            if bio:
                updates["Bio"] = bio

        if args.dry_run:
            print(f"\n{'='*60}")
            print(f"[{total+1}] {bioguide} — {new_name}")
            print(f"  Existing birth: {existing.get('birth')} → New: {updates.get('birth','(keep)')}")
            print(f"  Existing Bio:   {'yes' if existing.get('Bio') else 'missing'} → {'found' if updates.get('Bio') else '(still missing)'}")
            print(f"  Terms:          {len(new_terms)}")
            print(f"  Image:          {new_image[:60] if new_image else 'none'}")
            all_items.append({**existing, **updates, "bioguideId": bioguide})
        else:
            if existing:
                # Record exists — update only missing/changed fields
                update_missing_fields(table, bioguide, updates)
                updated += 1
                print(f"Updated {bioguide} (total={total+1}) | fields: {list(updates.keys())}")
            else:
                # New record — full insert
                item = {
                    "bioguideId": bioguide,
                    "name":       new_name,
                    "image":      new_image,
                    "birth":      updates.get("birth"),
                    "Bio":        updates.get("Bio"),
                    "terms":      new_terms,
                    "updateDate": now_et_string(),
                    "url":        f"https://www.congress.gov/member/{name_slug(new_name)}/{bioguide}",
                }
                table.put_item(Item=to_dynamo(
                    {k: v for k, v in item.items() if v is not None}
                ))
                updated += 1
                print(f"Inserted {bioguide} (total={total+1}) | new record")

        seen.add(bioguide)
        total += 1

        if args.total and total >= args.total:
            break

    if args.dry_run:
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(all_items, f, indent=2, default=str)
        print(f"\nDRY RUN complete: previewed {total} member(s)")
        print(f"Saved to: {args.out}")
    else:
        print(f"\nIngest complete:")
        print(f"  Processed: {total}")
        print(f"  Updated:   {updated}")


if __name__ == "__main__":
    main()