## @package ingestion.bioguide_members
#  Scrapes US Congressional biographical data from bioguide.congress.gov.
#
#  Uses Playwright (a real browser) to bypass Cloudflare bot detection on the
#  Bioguide website. Fetches JSON records for each representative identified
#  by a bioguide ID (e.g. "L000603") and writes the parsed records to a
#  DynamoDB table.
#
#  Key data structures:
#    - bioguideId: str  - unique identifier e.g. "A000001"
#    - record:     dict - parsed representative record with name, terms, image
#    - term:       dict - one congress term with chamber, party, state, dates
#
#  Usage:
#    python bioguide_members.py --letters A-Z --table Reps --region us-east-2

import argparse
import json
import random
import time
import re
import datetime as dt
from decimal import Decimal
from typing import Optional

import boto3
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

## Base URL for individual bioguide JSON records.
BASE = "https://bioguide.congress.gov/search/bio"

## Home URL used for browser warmup to obtain session cookies.
HOME = "https://bioguide.congress.gov/search"

# Resolve Eastern Time zone — tries zoneinfo (Python 3.9+), then dateutil,
# then falls back to a fixed UTC-5 offset.
try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    try:
        from dateutil.tz import gettz
        ET_TZ = gettz("America/New_York")
    except Exception:
        ET_TZ = dt.timezone(dt.timedelta(hours=-5))


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

## Returns the current Eastern Time as a formatted string.
#
#  @return str - formatted datetime string e.g. "2024-01-15 - 10:30:00 ET"
def now_et_string() -> str:
    d = dt.datetime.now(ET_TZ)
    return f"{d:%Y-%m-%d} - {d:%H:%M:%S} ET"


## Constructs a bioguide ID from a letter prefix and sequential number.
#
#  Bioguide IDs follow the format "L000603" — one uppercase letter followed
#  by a zero-padded 6-digit number.
#
#  @param letter  str - single uppercase letter prefix (e.g. "L")
#  @param n       int - sequential number (e.g. 603)
#  @return        str - formatted bioguide ID (e.g. "L000603")
def bid(letter: str, n: int) -> str:
    return f"{letter}{n:06d}"


## Sleeps for a random duration between a and b seconds.
#
#  Used to add human-like delays between browser requests to avoid
#  triggering rate limiting or bot detection on the Bioguide website.
#
#  @param a  float - minimum sleep duration in seconds
#  @param b  float - maximum sleep duration in seconds
def jsleep(a: float, b: float) -> None:
    time.sleep(random.uniform(a, b))


## Recursively converts Python floats to Decimal for DynamoDB compatibility.
#
#  DynamoDB does not accept Python float values — they must be converted to
#  Decimal before writing. This function walks the entire data structure and
#  converts every float it finds.
#
#  @param item  any - dict, list, float, or any other Python value
#  @return      any - same structure with all floats replaced by Decimal
def to_dynamo(item: any) -> any:
    if isinstance(item, dict):
        return {k: to_dynamo(v) for k, v in item.items()}
    if isinstance(item, list):
        return [to_dynamo(v) for v in item]
    if isinstance(item, float):
        return Decimal(str(item))
    return item


# ---------------------------------------------------------------------------
# Playwright fetch
# ---------------------------------------------------------------------------

## Fetches a bioguide JSON record using a real browser to bypass Cloudflare.
#
#  Attempts to load the JSON endpoint for the given bioguide ID up to
#  `retries` times. Handles HTTP 403 (Cloudflare challenge) by visiting the
#  home page to warm up cookies, and HTTP 429 (rate limit) by waiting 30s.
#
#  @param page     Playwright Page - active browser page instance
#  @param b        str             - bioguide ID to fetch (e.g. "L000603")
#  @param retries  int             - maximum number of retry attempts (default 3)
#  @param timeout  int             - page load timeout in milliseconds (default 30000)
#  @return         tuple           - ("ok", data_dict) on success,
#                                    ("notfound", None) for HTTP 404,
#                                    ("error", None) on failure
def fetch_bioguide(
    page: any,
    b: str,
    retries: int = 3,
    timeout: int = 30000
) -> tuple:
    url = f"{BASE}/{b}.json"

    for attempt in range(retries):
        try:
            response = page.goto(url, timeout=timeout, wait_until="domcontentloaded")

            if response is None:
                jsleep(2, 4)
                continue

            status = response.status

            if status == 200:
                # Attempt to extract JSON from page content
                content = page.content()
                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    try:
                        data = json.loads(match.group())
                        if "data" in data:
                            return ("ok", data)
                    except json.JSONDecodeError:
                        pass
                # Fallback: read raw text directly from the DOM
                try:
                    raw = page.evaluate("() => document.body.innerText")
                    data = json.loads(raw)
                    if "data" in data:
                        return ("ok", data)
                except Exception:
                    pass
                return ("error", None)

            elif status == 404:
                return ("notfound", None)

            elif status == 403:
                # Cloudflare challenge - warm up cookies and retry
                print(f"  [{b}] 403 on attempt {attempt + 1}, waiting...")
                page.goto(HOME, timeout=30000, wait_until="domcontentloaded")
                jsleep(3, 6)
                continue

            elif status == 429:
                print(f"  [{b}] 429 rate limited, waiting 30s...")
                time.sleep(30)
                continue

            else:
                jsleep(2, 4)
                continue

        except Exception as e:
            print(f"  [{b}] error attempt {attempt + 1}: {e}")
            jsleep(2, 4)

    return ("error", None)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

## Extracts a canonical chamber name from a job position title.
#
#  @param name  Optional[str] - job title string from the Bioguide API
#  @return      Optional[str] - "Senate", "House", original name, or None
def chamber_from_job(name: Optional[str]) -> Optional[str]:
    if not name:
        return None
    n = name.lower()
    if re.search(r'\bsenator\b', n):
        return "Senate"
    if re.search(r'\brepresentative\b', n):
        return "House"
    return name


## Extracts the party name from a congress affiliation dict.
#
#  Tries partyAffiliation first, then falls back to caucusAffiliation.
#  Handles both list and dict formats found in different API record versions.
#
#  @param ca  dict - congressAffiliation dict from the Bioguide API
#  @return    Optional[str] - party name string, or None if not found
def party_from_affils(ca: dict) -> Optional[str]:
    pa = ca.get("partyAffiliation")
    if isinstance(pa, list) and pa:
        p = ((pa[0] or {}).get("party") or {}).get("name")
        if p:
            return p
    if isinstance(pa, dict):
        p = (pa.get("party") or {}).get("name")
        if p:
            return p
    caul = ca.get("caucusAffiliation")
    if isinstance(caul, list) and caul:
        p = ((caul[0] or {}).get("party") or {}).get("name")
        if p:
            return p
    if isinstance(caul, dict):
        p = (caul.get("party") or {}).get("name")
        if p:
            return p
    return None


## Parses all congressional terms from a representative's job positions.
#
#  Walks the jobPositions array in the Bioguide data structure and builds
#  a flat list of term dicts. Each term captures one congress session that
#  the representative served in, including chamber, party, state, and dates.
#
#  @param d  dict - the "data" object from a Bioguide API response
#  @return   list - sorted list of term dicts, each containing:
#                   congress, chamber, district, state, party, start, departure
def terms_from_jobpositions(d: dict) -> list:
    terms = []
    for jp in (d.get("jobPositions") or []):
        chamber = chamber_from_job((jp.get("job") or {}).get("name"))
        affs = []
        if isinstance(jp.get("congressAffiliations"), list):
            affs = jp["congressAffiliations"]
        elif isinstance(jp.get("congressAffiliation"), list):
            affs = jp["congressAffiliation"]
        elif isinstance(jp.get("congressAffiliation"), dict):
            affs = [jp["congressAffiliation"]]
        for ca in affs:
            cong = ca.get("congress") or {}
            rep = ca.get("represents") or {}
            terms.append({
                "congress": cong.get("congressNumber"),
                "chamber": chamber,
                "district": rep.get("regionType"),
                "state": rep.get("regionCode") or None,
                "party": party_from_affils(ca),
                "start": cong.get("startDate"),
                "departure": cong.get("endDate"),
            })

    # Remove terms with no congress number and no dates (unusable records)
    terms = [t for t in terms if t["congress"] is not None or t["start"] or t["departure"]]
    terms.sort(key=lambda t: (t["start"] or "0000-00-00", t["congress"] or 0))
    return terms


## Builds a display name from a representative's name components.
#
#  Combines givenName, familyName, and middleName/additionalName fields.
#  Falls back to displayName if individual components are missing.
#
#  @param d  dict - the "data" object from a Bioguide API response
#  @return   Optional[str] - full display name, or None if unavailable
def build_name(d: dict) -> Optional[str]:
    given = (d.get("givenName") or "").strip()
    family = (d.get("familyName") or "").strip()
    middle = (d.get("middleName") or d.get("additionalName") or "").strip()
    parts = [p for p in [given, family, middle] if p]
    return " ".join(parts) or (d.get("displayName") or None)


## Resolves the representative's photo URL from the assets field.
#
#  Falls back to a placeholder image if no photo URL is found in the
#  API response. Handles relative URLs by constructing the full photo path.
#
#  @param d  dict - the "data" object from a Bioguide API response
#  @param b  str  - bioguide ID used to construct the fallback photo URL
#  @return   str  - absolute URL to the representative's photo
def image_url_from_assets(d: dict, b: str = None) -> str:
    placeholder = "https://bioguide.congress.gov/assets/placeholder_square.png"
    imgs = d.get("image") or []
    if isinstance(imgs, dict):
        imgs = [imgs]
    for im in imgs:
        u = (im.get("contentUrl") or "").strip()
        if u:
            if u.startswith("/"):
                return f"https://bioguide.congress.gov/photo/{b}.jpg"
            return u
    return placeholder


## Assembles a complete DynamoDB-ready record from a Bioguide API response.
#
#  @param b     str  - bioguide ID (e.g. "L000603")
#  @param data  dict - full parsed JSON response from the Bioguide API
#  @return      dict - flat record ready for DynamoDB PutItem, containing:
#                      bioguideId, image, name, birth, death, Bio, terms,
#                      updateDate, url
def build_record(b: str, data: dict) -> dict:
    d = data.get("data") or {}
    return {
        "bioguideId": b,
        "image": image_url_from_assets(d, b),
        "name": build_name(d),
        "birth": d.get("birthDate"),
        "death": d.get("deathDate") or None,
        "Bio": (
            d.get("profileText") or d.get("biographyText") or
            d.get("profile") or None
        ),
        "terms": terms_from_jobpositions(d),
        "updateDate": now_et_string(),
        "url": f"https://bioguide.congress.gov/search/bio/{b}",
    }


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

## Scans all bioguide IDs for a single letter prefix and saves to DynamoDB.
#
#  Iterates over numeric suffixes starting from args.start, stopping either
#  when the cap is reached or when stop_after_misses consecutive non-OK
#  responses are seen beyond the last successful hit.
#
#  Sparse letters (X, Q, U, Y) use a lower cap and shorter timeouts since
#  very few historical members have IDs starting with those letters.
#
#  @param L     str            - single uppercase letter to scan (e.g. "A")
#  @param args  argparse.Namespace - parsed CLI arguments
#  @param page  Playwright Page    - active browser page instance
#  @param table boto3 Table        - DynamoDB table resource for writing
#  @return      int                - number of records successfully saved
def scan_letter(L: str, args: any, page: any, table: any) -> int:
    saved = 0
    last_hit = 0
    misses_since_hit = 0
    NONOK = {"notfound", "blocked", "error"}
    n = max(1, args.start)

    # Letters with very few historical members — stop early if no hits
    SPARSE_LETTERS = {"X", "Q", "U", "Y"}
    sparse_cap = 500 if L in SPARSE_LETTERS else args.cap

    with table.batch_writer() as writer:
        while n <= sparse_cap:
            b = bid(L, n)

            fetch_timeout = 15000 if L in SPARSE_LETTERS else 30000
            st, js = fetch_bioguide(page, b, timeout=fetch_timeout)

            if st == "ok":
                rec = build_record(b, js)
                saved += 1
                last_hit = n
                misses_since_hit = 0
                writer.put_item(Item=to_dynamo(rec))
                print(f"  [{b}] saved={saved} | {rec.get('name')} | terms={len(rec['terms'])}")

            elif st in NONOK:
                if last_hit > 0:
                    misses_since_hit += 1
                    print(f"  [{b}] {st} | misses_since_hit={misses_since_hit}")
                    if misses_since_hit >= args.stop_after_misses:
                        print(
                            f"  [{L}] stopping after {misses_since_hit} misses "
                            f"beyond last hit {last_hit:06d}"
                        )
                        break
                else:
                    # No hits yet - stop earlier for sparse letters
                    early_stop = 100 if L in SPARSE_LETTERS else 2500
                    if n - args.start > early_stop:
                        print(f"  [{L}] no hits through {n:06d}, stopping early")
                        break

            jsleep(*args.sleep)
            n += 1

    return saved


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

## Entry point — parses CLI arguments and runs the full ingestion pipeline.
#
#  Launches a Playwright browser, warms up session cookies by visiting the
#  Bioguide home page, then scans each requested letter prefix sequentially.
#  All records are written to the specified DynamoDB table.
#
#  CLI arguments:
#    --letters           str   - letters or ranges e.g. "A-Z" or "A,B,C"
#    --start             int   - starting numeric suffix (default 1)
#    --cap               int   - maximum numeric suffix to try (default 999999)
#    --sleep             float - min and max sleep seconds between requests
#    --table             str   - DynamoDB table name (required)
#    --region            str   - AWS region (default us-east-2)
#    --stop-after-misses int   - stop after N consecutive misses (default 3)
#    --headless                - run browser without visible window
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Collect Bioguide records using Playwright and save to DynamoDB."
    )
    ap.add_argument("--letters", default="A",
                    help="Letters or ranges e.g. 'A-Z' or 'A,B,C'")
    ap.add_argument("--start", type=int, default=1)
    ap.add_argument("--cap", type=int, default=999_999)
    ap.add_argument("--sleep", nargs=2, type=float,
                    default=[2, 5], metavar=("MIN", "MAX"))
    ap.add_argument("--table", required=True,
                    help="DynamoDB table name")
    ap.add_argument("--region", default="us-east-2")
    ap.add_argument("--stop-after-misses", type=int, default=3,
                    help="Stop after N consecutive misses beyond last hit")
    ap.add_argument("--headless", action="store_true",
                    help="Run browser in headless mode (no visible window)")
    args = ap.parse_args()

    # Parse letter specification into an ordered list of unique letters
    spec = (args.letters or "").upper().replace(" ", "")
    letters = []
    seen: set = set()
    for part in spec.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            for code in range(ord(a[0]), ord(b[0]) + 1):
                ch = chr(code)
                if ch not in seen:
                    seen.add(ch)
                    letters.append(ch)
        else:
            ch = part[0]
            if ch not in seen:
                seen.add(ch)
                letters.append(ch)
    letters = letters or ["A"]
    print(f"Letters: {''.join(letters)}")

    ddb = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)
    total_saved = 0

    with sync_playwright() as p:
        # Launch real Chrome browser to bypass Cloudflare bot detection
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="en-US",
        )
        page = context.new_page()

        # Warmup - visit home page to obtain session cookies before scraping
        print("Warming up browser...")
        page.goto(HOME, timeout=30000, wait_until="domcontentloaded")
        jsleep(2, 4)
        print("Browser ready!\n")

        for L in letters:
            print(f"\n=== Scanning letter {L} ===")
            saved = scan_letter(L, args, page, table)
            total_saved += saved
            print(f"--- {L}: saved {saved} ---")

        browser.close()

    print(f"\nIngest complete: wrote {total_saved} record(s) to {args.table}")


if __name__ == "__main__":
    main()
