import argparse
import json
import random
import time
import re
import datetime as dt
import boto3
from decimal import Decimal
from playwright.sync_api import sync_playwright

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BASE = "https://bioguide.congress.gov/search/bio"
HOME = "https://bioguide.congress.gov/search"

try:
    from zoneinfo import ZoneInfo
    ET_TZ = ZoneInfo("America/New_York")
except Exception:
    try:
        from dateutil.tz import gettz
        ET_TZ = gettz("America/New_York")
    except Exception:
        ET_TZ = dt.timezone(dt.timedelta(hours=-5))


# ─────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────

def now_et_string() -> str:
    d = dt.datetime.now(ET_TZ)
    return f"{d:%Y-%m-%d} - {d:%H:%M:%S} ET"


def bid(letter: str, n: int) -> str:
    return f"{letter}{n:06d}"


def jsleep(a, b):
    time.sleep(random.uniform(a, b))


def to_dynamo(item):
    if isinstance(item, dict):
        return {k: to_dynamo(v) for k, v in item.items()}
    if isinstance(item, list):
        return [to_dynamo(v) for v in item]
    if isinstance(item, float):
        return Decimal(str(item))
    return item


# ─────────────────────────────────────────────
# Playwright fetch — bypasses Cloudflare
# ─────────────────────────────────────────────

def fetch_bioguide(page, b: str, retries: int = 3, timeout: int = 30000):
    """
    Fetch bioguide JSON using a real browser (Playwright).
    Bypasses Cloudflare bot detection.
    """
    url = f"{BASE}/{b}.json"

    for attempt in range(retries):
        try:
            # Navigate to the JSON endpoint
            response = page.goto(url, timeout=timeout, wait_until="domcontentloaded")

            if response is None:
                jsleep(2, 4)
                continue

            status = response.status

            if status == 200:
                # Get page content and parse JSON
                content = page.content()
                # Extract JSON from the page body
                match = re.search(r'\{.*\}', content, re.DOTALL)
                if match:
                    try:
                        data = json.loads(match.group())
                        if "data" in data:
                            return ("ok", data)
                    except json.JSONDecodeError:
                        pass
                # Try getting raw text directly
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
                # Cloudflare challenge — wait and retry
                print(f"  [{b}] 403 on attempt {attempt+1}, waiting...")
                # Visit home page first to warm up cookies
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
            print(f"  [{b}] error attempt {attempt+1}: {e}")
            jsleep(2, 4)

    return ("error", None)


# ─────────────────────────────────────────────
# Parsers
# ─────────────────────────────────────────────

def chamber_from_job(name: str | None):
    if not name:
        return None
    n = name.lower()
    if re.search(r'\bsenator\b', n):
        return "Senate"
    if re.search(r'\brepresentative\b', n):
        return "House"
    return name


def party_from_affils(ca: dict) -> str | None:
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
            rep  = ca.get("represents") or {}
            terms.append({
                "congress":  cong.get("congressNumber"),
                "chamber":   chamber,
                "district":  rep.get("regionType"),
                "state":     rep.get("regionCode") or None,
                "party":     party_from_affils(ca),
                "start":     cong.get("startDate"),
                "departure": cong.get("endDate"),
            })
    terms = [t for t in terms if t["congress"] is not None or t["start"] or t["departure"]]
    terms.sort(key=lambda t: (t["start"] or "0000-00-00", t["congress"] or 0))
    return terms


def build_name(d: dict) -> str | None:
    given  = (d.get("givenName")      or "").strip()
    family = (d.get("familyName")     or "").strip()
    middle = (d.get("middleName") or d.get("additionalName") or "").strip()
    parts  = [p for p in [given, family, middle] if p]
    return " ".join(parts) or (d.get("displayName") or None)


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


def build_record(b: str, data: dict) -> dict:
    d = data.get("data") or {}
    return {
        "bioguideId": b,
        "image":      image_url_from_assets(d, b),
        "name":       build_name(d),
        "birth":      d.get("birthDate"),
        "death":      d.get("deathDate") or None,
        "Bio":        (d.get("profileText") or d.get("biographyText") or
                       d.get("profile") or None),
        "terms":      terms_from_jobpositions(d),
        "updateDate": now_et_string(),
        "url":        f"https://bioguide.congress.gov/search/bio/{b}",
    }


# ─────────────────────────────────────────────
# Scanner
# ─────────────────────────────────────────────

def scan_letter(L, args, page, table):
    saved            = 0
    last_hit         = 0
    misses_since_hit = 0
    NONOK            = {"notfound", "blocked", "error"}
    n                = max(1, args.start)

    # Letters with very few historical members — stop early if no hits
    SPARSE_LETTERS   = {"X", "Q", "U", "Y"}
    sparse_cap       = 500 if L in SPARSE_LETTERS else args.cap

    with table.batch_writer() as writer:
        while n <= sparse_cap:
            b       = bid(L, n)

            # Short timeout for sparse letters to avoid getting stuck
            fetch_timeout = 15000 if L in SPARSE_LETTERS else 30000
            st, js  = fetch_bioguide(page, b, timeout=fetch_timeout)

            if st == "ok":
                rec              = build_record(b, js)
                saved           += 1
                last_hit         = n
                misses_since_hit = 0
                writer.put_item(Item=to_dynamo(rec))
                print(f"  [{b}] ✓ saved={saved} | {rec.get('name')} | terms={len(rec['terms'])}")

            elif st in NONOK:
                if last_hit > 0:
                    misses_since_hit += 1
                    print(f"  [{b}] {st} | misses_since_hit={misses_since_hit}")
                    if misses_since_hit >= args.stop_after_misses:
                        print(f"  [{L}] stopping after {misses_since_hit} misses beyond last hit {last_hit:06d}")
                        break
                else:
                    # No hits yet — stop earlier for sparse letters
                    early_stop = 100 if L in SPARSE_LETTERS else 2500
                    if n - args.start > early_stop:
                        print(f"  [{L}] no hits through {n:06d}, stopping early")
                        break

            jsleep(*args.sleep)
            n += 1

    return saved


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Collect Bioguide records using Playwright and save to DynamoDB."
    )
    ap.add_argument("--letters",           default="A",
                    help="Letters or ranges e.g. 'A-Z' or 'A,B,C'")
    ap.add_argument("--start",             type=int,   default=1)
    ap.add_argument("--cap",               type=int,   default=999_999)
    ap.add_argument("--sleep",             nargs=2,    type=float,
                    default=[2, 5],        metavar=("MIN", "MAX"))
    ap.add_argument("--table",             required=True,
                    help="DynamoDB table name")
    ap.add_argument("--region",            default="us-east-2")
    ap.add_argument("--stop-after-misses", type=int,   default=3,
                    help="Stop after N consecutive misses beyond last hit")
    ap.add_argument("--headless",          action="store_true",
                    help="Run browser in headless mode (no visible window)")
    args = ap.parse_args()

    # Parse letters
    spec = (args.letters or "").upper().replace(" ", "")
    letters = []
    seen = set()
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

    # DynamoDB
    ddb   = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    total_saved = 0

    with sync_playwright() as p:
        # Launch real Chrome browser
        browser = p.chromium.launch(headless=args.headless)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            locale="en-US",
        )
        page = context.new_page()

        # Warmup — visit home page to get cookies
        print("Warming up browser...")
        page.goto(HOME, timeout=30000, wait_until="domcontentloaded")
        jsleep(2, 4)
        print("Browser ready!\n")

        for L in letters:
            print(f"\n=== Scanning letter {L} ===")
            saved        = scan_letter(L, args, page, table)
            total_saved += saved
            print(f"--- {L}: saved {saved} ---")

        browser.close()

    print(f"\nIngest complete: wrote {total_saved} record(s) to {args.table}")


if __name__ == "__main__":
    main()