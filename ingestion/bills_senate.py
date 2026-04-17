import os
import json
import argparse
import datetime as dt
import requests
import boto3
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

API_ROOT = "https://api.congress.gov/v3"
API_KEY  = os.getenv("CONGRESS_API_KEY", "F8GstZbcQIB090NSZ38eEEmsNvaMZtJXuMSXALIX")
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
    """Recursively convert floats to Decimal for DynamoDB."""
    if isinstance(val, dict):
        return {k: to_dynamo(v) for k, v in val.items() if v is not None}
    if isinstance(val, list):
        return [to_dynamo(v) for v in val if v is not None]
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


# ─────────────────────────────────────────────
# Congress.gov API helpers
# ─────────────────────────────────────────────

def get_last_run_timestamp(table) -> str | None:
    """
    Get the most recent Introduced date from the bills table.
    This tells us the latest bill we already have so we only
    fetch newer bills on the next run.
    """
    try:
        items = []
        kwargs = {"ProjectionExpression": "LatestActionDate"}
        while True:
            r = table.scan(**kwargs)
            items.extend(r.get("Items", []))
            if not r.get("LastEvaluatedKey"):
                break
            kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]

        # Filter out metadata records and None values
        # Use LatestActionDate — the most recent real activity date
        dates = [
            i["LatestActionDate"] for i in items
            if i.get("LatestActionDate")
        ]
        if not dates:
            return None

        latest = max(dates)
        # Convert to Congress.gov API datetime format
        # e.g. "2025-03-15" -> "2025-03-15T00:00:00Z"
        if "T" not in latest:
            latest = f"{latest}T00:00:00Z"
        return latest

    except Exception as e:
        print(f"  [warn] could not read latest date from table: {e}")
        return None


def list_bills(congress: int, bill_type: str = "s", limit: int = 250,
               total: int = 0, from_date: str = None, sess=None):
    s = sess or requests.Session()
    limit = max(1, min(limit, 250))
    url = f"{API_ROOT}/bill/{congress}/{bill_type.lower()}"
    offset = 0
    yielded = 0

    while True:
        params = {"limit": limit, "offset": offset,
                  "format": "json", "sort": "updateDate+desc"}
        if from_date:
            params["fromDateTime"] = from_date

        r = api_get(s, url, params)
        r.raise_for_status()
        data = r.json()
        bills = data.get("bills") or []
        if not bills:
            break
        for b in bills:
            yield b
            yielded += 1
            if total and yielded >= total:
                return
        if len(bills) < limit:
            break
        offset += limit


def api_get(sess, url, params, retries=3, timeout=30):
    """GET with retry logic for timeouts and rate limits."""
    import time
    for attempt in range(retries):
        try:
            r = sess.get(url, headers=HEADERS, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 5))
                print(f"  [rate limit] waiting {wait}s...")
                time.sleep(wait)
                continue
            return r
        except requests.exceptions.Timeout:
            wait = 2 ** attempt
            print(f"  [timeout] attempt {attempt+1}/{retries}, retrying in {wait}s...")
            time.sleep(wait)
        except requests.exceptions.ConnectionError:
            wait = 2 ** attempt
            print(f"  [connection error] retrying in {wait}s...")
            time.sleep(wait)
    raise requests.exceptions.Timeout(f"Failed after {retries} retries: {url}")


def get_bill_detail(congress: int, bill_type: str, number: int,
                    sess: requests.Session) -> dict:
    url = f"{API_ROOT}/bill/{congress}/{bill_type.lower()}/{number}"
    r = api_get(sess, url, {"format": "json"})
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    return r.json().get("bill") or {}


def get_cosponsors(congress: int, bill_type: str, number: int,
                   sess: requests.Session) -> list:
    url = f"{API_ROOT}/bill/{congress}/{bill_type.lower()}/{number}/cosponsors"
    offset, limit, cosponsors = 0, 250, []
    while True:
        r = api_get(sess, url, {"format": "json", "limit": limit, "offset": offset})
        if r.status_code == 404:
            break
        r.raise_for_status()
        page = r.json().get("cosponsors") or []
        cosponsors.extend(page)
        if len(page) < limit:
            break
        offset += limit
    return cosponsors


def get_subjects(congress: int, bill_type: str, number: int,
                 sess: requests.Session) -> dict:
    url = f"{API_ROOT}/bill/{congress}/{bill_type.lower()}/{number}/subjects"
    r = api_get(sess, url, {"format": "json"})
    if r.status_code == 404:
        return {"policyArea": None, "legislativeSubjects": []}
    r.raise_for_status()
    data = r.json().get("subjects") or {}

    pa = data.get("policyArea")
    policy_area = pa.get("name") if isinstance(pa, dict) else (pa or None)

    leg_subjects = []
    for s in (data.get("legislativeSubjects") or []):
        name = s.get("name") if isinstance(s, dict) else s
        if name:
            leg_subjects.append(name)

    return {"policyArea": policy_area, "legislativeSubjects": leg_subjects}


# ─────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────

def parse_sponsor(detail: dict) -> dict | None:
    sponsors = detail.get("sponsors") or []
    if not sponsors:
        return None
    s = sponsors[0]
    first  = (s.get("firstName")  or "").strip()
    last   = (s.get("lastName")   or "").strip()
    middle = (s.get("middleName") or "").strip()
    parts  = [p for p in [first, middle, last] if p]
    name   = " ".join(parts) if parts else (s.get("fullName") or "").strip()
    return {
        "name":       name or None,
        "bioguideId": s.get("bioguideId") or s.get("bioguideID") or None,
        "party":      s.get("party") or None,
        "state":      s.get("state") or None,
    }


def parse_cosponsors(raw: list) -> list:
    out = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        first  = (c.get("firstName")  or "").strip()
        last   = (c.get("lastName")   or "").strip()
        middle = (c.get("middleName") or "").strip()
        parts  = [p for p in [first, middle, last] if p]
        name   = " ".join(parts) if parts else (c.get("fullName") or "").strip()
        out.append({
            "name":               name or None,
            "bioguideId":         c.get("bioguideId") or c.get("bioguideID") or None,
            "party":              c.get("party") or None,
            "state":              c.get("state") or None,
            "sponsorshipDate":    c.get("sponsorshipDate") or None,
            "isOriginalCosponsor": c.get("isOriginalCosponsor"),
        })
    return out


def parse_bill_status(detail: dict) -> dict:
    laws         = detail.get("laws") or []
    latest       = detail.get("latestAction") or {}
    became_law   = len(laws) > 0
    action_text  = latest.get("text") or None
    action_date  = latest.get("actionDate") or None
    text_lower   = (action_text or "").lower()

    if became_law:                                              status = "Enacted"
    elif "vetoed" in text_lower:                               status = "Vetoed"
    elif "passed senate" in text_lower and \
         "passed house"  in text_lower:                        status = "Passed Both Chambers"
    elif "passed senate"  in text_lower:                       status = "Passed Senate"
    elif "passed house"   in text_lower:                       status = "Passed House"
    elif "referred to"    in text_lower:                       status = "Referred to Committee"
    elif "introduced"     in text_lower:                       status = "Introduced"
    else:                                                       status = "In Progress"

    return {
        "status":           status,
        "becameLaw":        became_law,
        "latestAction":     action_text,
        "latestActionDate": action_date,
    }


# ─────────────────────────────────────────────
# Item builder
# ─────────────────────────────────────────────

def build_item(bill_summary: dict, detail: dict,
               cosponsors_raw: list, subjects: dict) -> dict:
    congress   = detail.get("congress")      or bill_summary.get("congress")
    number_raw = detail.get("number")        or bill_summary.get("number")
    bill_type  = (detail.get("type") or bill_summary.get("type") or "S").upper()
    title      = detail.get("title")         or bill_summary.get("title") or None
    introduced = detail.get("introducedDate") or bill_summary.get("introducedDate") or None
    origin     = detail.get("originChamber") or bill_summary.get("originChamber") or "Senate"

    try:
        number = int(number_raw)
    except (TypeError, ValueError):
        number = number_raw

    bill_id        = f"{congress}-{bill_type}-{number}"
    sponsor        = parse_sponsor(detail)
    cosponsor_list = parse_cosponsors(cosponsors_raw)
    bill_status    = parse_bill_status(detail)

    return {
        "billId":           bill_id,
        "Number":           number,
        "Type":             bill_type,
        "Congress":         congress,
        "Chamber":          origin,
        "Title":            title,
        "Introduced":       introduced,
        "Sponsor":          sponsor,
        "SponsorCount":     1 if sponsor else 0,
        "Subject":          subjects.get("policyArea"),
        "Keywords":         subjects.get("legislativeSubjects", []),
        "Status":           bill_status["status"],
        "BecameLaw":        bill_status["becameLaw"],
        "LatestAction":     bill_status["latestAction"],
        "LatestActionDate": bill_status["latestActionDate"],
        "CosponsorCount":   len(cosponsor_list),
        "Cosponsors":       cosponsor_list,
        "url":              f"https://www.congress.gov/bill/{congress}th-congress/senate-bill/{number}",
        "updateDate":       now_et_string(),
    }


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Ingest Senate bills into DynamoDB. Supports incremental updates."
    )
    ap.add_argument("--congress",    type=int, default=119)
    ap.add_argument("--bill-type",   default="s")
    ap.add_argument("--table",       default=os.getenv("SENATE_BILLS_TABLE", "bills"),
                    help="DynamoDB table name (default: bills)")
    ap.add_argument("--region",      default="us-east-2")
    ap.add_argument("--limit",       type=int, default=250,
                    help="Page size per API request (max 250)")
    ap.add_argument("--total",       type=int, default=10,
                    help="Max bills to ingest. 0 = all.")
    ap.add_argument("--from-date",   default=None,
                    help="Only fetch bills updated after this date. "
                         "Format: 2025-01-01T00:00:00Z. "
                         "If not set, uses last run timestamp from DynamoDB.")
    ap.add_argument("--full",        action="store_true",
                    help="Force full ingestion, ignore last run timestamp.")
    args = ap.parse_args()

    if not API_KEY:
        raise SystemExit("Set CONGRESS_API_KEY env variable.")

    sess = requests.Session()
    sess.headers.update(HEADERS_BASE)

    ddb   = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    # Determine from_date for incremental ingestion
    from_date = None
    if not args.full:
        if args.from_date:
            from_date = args.from_date
            print(f"Incremental mode: fetching bills updated after {from_date}")
        else:
            from_date = get_last_run_timestamp(table)
            if from_date:
                print(f"Incremental mode: last run was {from_date}")
                print(f"Only fetching bills updated since then...")
            else:
                print("No previous run found — running full ingestion...")
    else:
        print("Full ingestion mode...")

    seen  = set()
    total = 0

    with table.batch_writer() as writer:
        for b in list_bills(args.congress, bill_type=args.bill_type,
                            limit=args.limit, total=args.total,
                            from_date=from_date, sess=sess):

            number_raw = b.get("number")
            try:
                number = int(number_raw)
            except (TypeError, ValueError):
                print(f"[skip] bad number: {number_raw}")
                continue

            key = f"{args.congress}-{args.bill_type.upper()}-{number}"
            if key in seen:
                continue

            try:
                detail         = get_bill_detail(args.congress, args.bill_type, number, sess)
                cosponsors_raw = get_cosponsors(args.congress, args.bill_type, number, sess)
                subjects       = get_subjects(args.congress, args.bill_type, number, sess)
                item           = build_item(b, detail, cosponsors_raw, subjects)

                writer.put_item(Item=to_dynamo(item))
                seen.add(key)
                total += 1
                print(f"Upserted {key} (total={total}) | {(item.get('Title') or '')[:60]}")

            except Exception as e:
                print(f"[ERROR] {key}: {e}")

            if args.total and total >= args.total:
                break

    # No need to save timestamp — next run reads latest Introduced date from table

    mode = "incremental" if from_date else "full"
    print(f"\nIngest complete ({mode}): wrote {total} bill(s) to {args.table}")


if __name__ == "__main__":
    main()