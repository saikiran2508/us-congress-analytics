import os
import re
import json
import traceback
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Attr, Key
from flask import Flask, request, Response
from flask_cors import CORS

# ---------------- INIT ---------------- #
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, allow_headers=["Content-Type"])

dynamodb    = boto3.resource("dynamodb", region_name="us-east-2")
reps_table  = dynamodb.Table(os.getenv("REPS_TABLE",  "Reps"))
terms_table = dynamodb.Table(os.getenv("TERMS_TABLE", "RepTerms"))


# ---------------- HELPERS ---------------- #
def convert_decimals(obj):
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def normalize_chamber(chamber):
    c = chamber.lower()
    if c in ("senate", "senator"):
        return "Senate"
    if c in ("house", "representative"):
        return "House"
    if c == "delegate":
        return "Delegate"
    return chamber.strip().title()


def scan_all(table, filter_expression=None):
    kwargs = {}
    if filter_expression is not None:
        kwargs["FilterExpression"] = filter_expression
    items = []
    while True:
        r = table.scan(**kwargs)
        items.extend(r.get("Items", []))
        if not r.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
    return items


def get_terms(item):
    t = item.get("terms")
    return [x for x in t if isinstance(x, dict)] if isinstance(t, list) else []


def deduplicate(items):
    seen = set()
    result = []
    for item in items:
        bid = item.get("bioguideId")
        if bid not in seen:
            seen.add(bid)
            result.append(item)
    return result


def make_json(data, status=200):
    return Response(
        json.dumps(data),
        status=status,
        mimetype="application/json",
        headers={
            "Access-Control-Allow-Origin":  "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    )


# ---------------- ROUTES ---------------- #
@app.route("/", methods=["GET"])
def index():
    return "Reps API is live!"


@app.route("/reps/map", methods=["GET"])
def get_map_reps():
    try:
        congress = request.args.get("congress", "").strip()
        chamber  = request.args.get("chamber",  "").strip()
        party    = request.args.get("party",    "").strip()

        if not congress or not chamber:
            return make_json({
                "error": "Both 'congress' and 'chamber' are required.",
                "example": "/reps/map?congress=118&chamber=senate"
            }, 400)

        chamber_normalized = normalize_chamber(chamber)
        congress_int = int(congress)

        print(f"[MAP] congress={congress_int} chamber={chamber_normalized}")

        # Step 1: Scan RepTerms
        filter_expr = (
            Attr("congress").eq(congress_int) &
            Attr("chamber").eq(chamber_normalized)
        )
        term_records = scan_all(terms_table, filter_expr)
        term_records = convert_decimals(term_records)
        print(f"[MAP] Found {len(term_records)} term records")

        if not term_records:
            return make_json({
                "count": 0,
                "filters": {"congress": congress, "chamber": chamber_normalized, "party": party or None},
                "data": []
            })

        # Step 2: BatchGet from Reps
        # Use a set to deduplicate bioguideIds — a senator may appear in
        # multiple RepTerms rows (e.g. served as both Delegate and Senator
        # in the same congress, or filled a vacancy mid-congress)
        bioguideid_list = list({t["bioguideId"] for t in term_records
                                 if t.get("bioguideId")})
        print(f"[MAP] Fetching {len(bioguideid_list)} unique reps via BatchGet")

        reps = []
        for i in range(0, len(bioguideid_list), 100):
            batch_keys = bioguideid_list[i:i+100]
            response = dynamodb.batch_get_item(
                RequestItems={
                    "Reps": {
                        "Keys": [{"bioguideId": bid} for bid in batch_keys]
                    }
                }
            )
            reps.extend(response["Responses"].get("Reps", []))

        reps = convert_decimals(reps)

        # Final deduplication by bioguideId — belt and suspenders
        seen_ids = set()
        unique_reps = []
        for rep in reps:
            bid = rep.get("bioguideId")
            if bid and bid not in seen_ids:
                seen_ids.add(bid)
                unique_reps.append(rep)
        reps = unique_reps
        print(f"[MAP] Got {len(reps)} unique reps")

        # Optional party filter
        if party:
            filtered = []
            for rep in reps:
                try:
                    if any(t.get("party", "").lower() == party.lower() for t in get_terms(rep)):
                        filtered.append(rep)
                except Exception:
                    pass
            reps = filtered

        return make_json({
            "count": len(reps),
            "filters": {
                "congress": congress,
                "chamber":  chamber_normalized,
                "party":    party or None,
            },
            "data": reps,
        })

    except Exception as e:
        error_msg = traceback.format_exc()
        print(f"[MAP ERROR] {error_msg}")
        return make_json({"error": str(e), "trace": error_msg}, 500)


# Cache congress list in Lambda memory (never changes)
_congresses_cache = None

@app.route("/reps/congresses", methods=["GET"])
def get_congresses():
    global _congresses_cache
    try:
        if _congresses_cache is not None:
            print("[CONGRESSES] Returning cached result")
            return make_json({"congresses": _congresses_cache})

        print("[CONGRESSES] Building congress list from RepTerms...")
        items = scan_all(terms_table, filter_expression=None)
        congresses = set()
        for item in items:
            c = item.get("congress")
            if c is not None:
                congresses.add(int(c) if isinstance(c, Decimal) else int(c))
        _congresses_cache = sorted(list(congresses), reverse=True)
        print(f"[CONGRESSES] Found {len(_congresses_cache)} unique congresses, cached.")
        return make_json({"congresses": _congresses_cache})
    except Exception as e:
        print(f"[CONGRESSES ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


@app.route("/reps", methods=["GET"])
def get_reps():
    try:
        name     = request.args.get("name",     "").strip()
        chamber  = request.args.get("chamber",  "").strip()
        party    = request.args.get("party",    "").strip()
        congress = request.args.get("congress", "").strip()
        limit    = request.args.get("limit", type=int)

        fe = Attr("name").contains(name) if name else None
        items = scan_all(reps_table, fe)
        items = convert_decimals(items)

        if name:
            p = re.compile(re.escape(name), re.IGNORECASE)
            items = [i for i in items if p.search(i.get("name", ""))]

        if chamber:
            ch_norm = normalize_chamber(chamber)
            items = [i for i in items if any(
                normalize_chamber(t.get("chamber", "")) == ch_norm
                for t in get_terms(i)
            )]

        if party:
            filtered = []
            for i in items:
                try:
                    if any(t.get("party", "").lower() == party.lower() for t in get_terms(i)):
                        filtered.append(i)
                except Exception:
                    pass
            items = filtered

        if congress:
            items = [i for i in items if any(
                str(t.get("congress", "")) == congress
                for t in get_terms(i)
            )]

        items = deduplicate(items)

        if limit:
            items = items[:limit]

        return make_json({
            "count": len(items),
            "filters": {
                "name":     name     or None,
                "chamber":  chamber  or None,
                "party":    party    or None,
                "congress": congress or None,
            },
            "data": items,
        })
    except Exception as e:
        print(f"[REPS ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


@app.route("/reps/<string:bioguideid>", methods=["GET"])
def get_rep_by_id(bioguideid):
    try:
        r = reps_table.get_item(Key={"bioguideId": bioguideid})
        item = r.get("Item")
        if not item:
            return make_json({"error": f"Rep '{bioguideid}' not found."}, 404)
        return make_json(convert_decimals(item))
    except Exception as e:
        print(f"[REP ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)