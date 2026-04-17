## @package api.main
#  REST API for US Congressional Representatives.
#
#  Provides endpoints to query representative data stored in AWS DynamoDB.
#  Uses a two-table design (Reps + RepTerms) for fast filtered queries
#  by congress number and chamber without full-table scans.
#
#  Tables:
#    - Reps:     12,310 records - one per person, full biographical data
#    - RepTerms: 49,778 records - one per congress term, used for fast lookups
#
#  Endpoints:
#    GET /                                          -> health check
#    GET /reps/map?congress=119&chamber=senate      -> members for map display
#    GET /reps/congresses                           -> list of congress numbers
#    GET /reps?name=&chamber=&party=&congress=      -> search with filters
#    GET /reps/<bioguide_id>                        -> single rep by ID

import os
import re
import json
import traceback
from decimal import Decimal
from typing import Any, Optional

import boto3
from boto3.dynamodb.conditions import Attr
from flask import Flask, request, Response
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Flask app and CORS configuration
# ---------------------------------------------------------------------------

## Flask application instance for the Representatives API.
app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}}, allow_headers=["Content-Type"])

# ---------------------------------------------------------------------------
# DynamoDB lazy initialization
#
# Tables are initialized on first access rather than at module load time.
# This allows moto to intercept boto3 calls during testing before any
# real AWS connections are attempted.
# ---------------------------------------------------------------------------

## Cached DynamoDB resource - initialized on first use.
_dynamodb = None

## Cached Reps table handle - initialized on first use.
_reps_table = None

## Cached RepTerms table handle - initialized on first use.
_terms_table = None


## Returns the shared DynamoDB resource, creating it on first call.
#
#  Uses environment variable AWS_REGION if set, defaults to us-east-2.
#
#  @return Any - boto3 DynamoDB ServiceResource
def get_dynamodb() -> Any:
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb", region_name="us-east-2")
    return _dynamodb


## Returns the Reps table handle, creating it on first call.
#
#  Table name is read from the REPS_TABLE environment variable,
#  defaulting to "Reps" if not set.
#
#  @return Any - boto3 DynamoDB Table resource for the Reps table
def get_reps_table() -> Any:
    global _reps_table
    if _reps_table is None:
        _reps_table = get_dynamodb().Table(os.getenv("REPS_TABLE", "Reps"))
    return _reps_table


## Returns the RepTerms table handle, creating it on first call.
#
#  Table name is read from the TERMS_TABLE environment variable,
#  defaulting to "RepTerms" if not set.
#
#  @return Any - boto3 DynamoDB Table resource for the RepTerms table
def get_terms_table() -> Any:
    global _terms_table
    if _terms_table is None:
        _terms_table = get_dynamodb().Table(os.getenv("TERMS_TABLE", "RepTerms"))
    return _terms_table


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

## Recursively converts DynamoDB types to JSON-serializable Python types.
#
#  DynamoDB returns numbers as Decimal and sets as Python set, neither of
#  which are JSON-serializable. This function walks the entire data structure
#  and converts every Decimal to int or float, and every set to list.
#
#  @param obj  Any - dict, list, set, Decimal, or any other Python value
#  @return     Any - same structure with Decimal and set values converted
def convert_decimals(obj: Any) -> Any:
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    if isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


## Normalizes legacy chamber naming conventions to canonical form.
#
#  The US Congress dataset uses inconsistent chamber labels across time.
#  Older records use "Senator" and "Representative" while newer records
#  use "Senate" and "House". This function maps all variants to a
#  consistent canonical label.
#
#  @param chamber  str - raw chamber string from DynamoDB record
#  @return         str - "Senate", "House", "Delegate", or title-cased original
def normalize_chamber(chamber: str) -> str:
    c = chamber.lower()
    if c in ("senate", "senator"):
        return "Senate"
    if c in ("house", "representative"):
        return "House"
    if c == "delegate":
        return "Delegate"
    return chamber.strip().title()


## Performs a full DynamoDB table scan with automatic pagination.
#
#  DynamoDB scan returns at most 1 MB of data per request. This function
#  continues issuing scan requests using LastEvaluatedKey until all pages
#  are exhausted, then returns the complete result set.
#
#  @param table              Any - boto3 DynamoDB Table resource to scan
#  @param filter_expression  Optional[Any] - boto3 ConditionBase filter (default None)
#  @return                   list - all matching DynamoDB item dicts
def scan_all(table: Any, filter_expression: Optional[Any] = None) -> list:
    kwargs: dict = {}
    if filter_expression is not None:
        kwargs["FilterExpression"] = filter_expression

    items: list = []
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get("Items", []))
        if not response.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
    return items


## Extracts the list of term dicts from a representative record.
#
#  Safely handles cases where the terms field is missing, None, or
#  contains non-dict entries from legacy data formats.
#
#  @param item  dict - a single DynamoDB Reps table record
#  @return      list - list of term dicts, or empty list if none found
def get_terms(item: dict) -> list:
    terms = item.get("terms")
    if isinstance(terms, list):
        return [t for t in terms if isinstance(t, dict)]
    return []


## Removes duplicate representatives by bioguideId, keeping first occurrence.
#
#  Used after BatchGetItem responses which may occasionally return
#  duplicate records for representatives who served in multiple roles.
#
#  @param items  list - list of representative dicts
#  @return       list - deduplicated list preserving original order
def deduplicate(items: list) -> list:
    seen: set = set()
    result: list = []
    for item in items:
        bid = item.get("bioguideId")
        if bid not in seen:
            seen.add(bid)
            result.append(item)
    return result


## Builds a JSON HTTP response with CORS headers.
#
#  @param data    Any - Python dict or list to serialize as JSON
#  @param status  int - HTTP status code (default 200)
#  @return        Response - Flask Response with application/json content type
def make_json(data: Any, status: int = 200) -> Response:
    return Response(
        json.dumps(data),
        status=status,
        mimetype="application/json",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        }
    )


# ---------------------------------------------------------------------------
# In-memory cache for congress list
#
# The list of available congress numbers never changes at runtime so it is
# cached in Lambda memory after the first request to avoid repeated full
# table scans on a hot Lambda instance.
# ---------------------------------------------------------------------------

## Cached list of congress numbers - populated on first request to /reps/congresses.
_congresses_cache: Optional[list] = None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

## Health-check endpoint.
#
#  @return  str - plain text confirmation that the API is running
@app.route("/", methods=["GET"])
def index() -> str:
    return "Reps API is live!"


## Returns congressional representatives optimized for US map display.
#
#  Uses the two-table design for fast lookup:
#    Step 1 - Scan RepTerms filtered by congress + chamber (~437 records)
#    Step 2 - BatchGetItem from Reps using the collected bioguideIds
#
#  This avoids a full 12,310-record scan of the Reps table and reduces
#  response time from ~7 seconds to under 1 second.
#
#  Query parameters:
#    congress  int  - Congress number (e.g. 119). Required.
#    chamber   str  - "senate" or "house" (case-insensitive). Required.
#    party     str  - optional party filter (e.g. "Democrat", "Republican")
#
#  @return  Response - JSON object with count, filters, and data array,
#                      or a JSON error object with HTTP 400/500
@app.route("/reps/map", methods=["GET"])
def get_map_reps() -> Response:
    try:
        congress = request.args.get("congress", "").strip()
        chamber = request.args.get("chamber", "").strip()
        party = request.args.get("party", "").strip()

        if not congress or not chamber:
            return make_json({
                "error": "Both 'congress' and 'chamber' are required.",
                "example": "/reps/map?congress=118&chamber=senate"
            }, 400)

        chamber_normalized = normalize_chamber(chamber)
        congress_int = int(congress)

        # Step 1: find all bioguide IDs active in this congress + chamber
        filter_expr = (
            Attr("congress").eq(congress_int) &
            Attr("chamber").eq(chamber_normalized)
        )
        term_records = scan_all(get_terms_table(), filter_expr)
        term_records = convert_decimals(term_records)

        if not term_records:
            return make_json({
                "count": 0,
                "filters": {
                    "congress": congress,
                    "chamber": chamber_normalized,
                    "party": party or None
                },
                "data": []
            })

        # Deduplicate bioguideIds - a senator may appear in multiple
        # RepTerms rows if they filled a vacancy or changed roles mid-congress
        bioguide_ids = list({
            t["bioguideId"] for t in term_records if t.get("bioguideId")
        })

        # Step 2: batch-fetch full biographical records in chunks of 100
        # (DynamoDB BatchGetItem limit is 100 keys per request)
        reps: list = []
        for i in range(0, len(bioguide_ids), 100):
            batch_keys = bioguide_ids[i:i + 100]
            response = get_dynamodb().batch_get_item(
                RequestItems={
                    "Reps": {
                        "Keys": [{"bioguideId": bid} for bid in batch_keys]
                    }
                }
            )
            reps.extend(response["Responses"].get("Reps", []))

        reps = deduplicate(convert_decimals(reps))

        # Optional party filter applied after fetching full records
        if party:
            reps = [
                rep for rep in reps
                if any(
                    t.get("party", "").lower() == party.lower()
                    for t in get_terms(rep)
                )
            ]

        return make_json({
            "count": len(reps),
            "filters": {
                "congress": congress,
                "chamber": chamber_normalized,
                "party": party or None,
            },
            "data": reps,
        })

    except Exception as e:
        print(f"[MAP ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


## Returns the list of distinct congress numbers available in the dataset.
#
#  Scans the RepTerms table once and caches the result in Lambda memory.
#  The list is sorted in descending order (most recent first) for use
#  in frontend dropdown menus.
#
#  @return  Response - JSON object with a "congresses" key containing a
#                      sorted list of integers e.g. {"congresses": [119, 118, ...]}
@app.route("/reps/congresses", methods=["GET"])
def get_congresses() -> Response:
    global _congresses_cache
    try:
        if _congresses_cache is not None:
            return make_json({"congresses": _congresses_cache})

        items = scan_all(get_terms_table())
        congresses: set = set()
        for item in items:
            c = item.get("congress")
            if c is not None:
                congresses.add(int(c))

        _congresses_cache = sorted(list(congresses), reverse=True)
        return make_json({"congresses": _congresses_cache})

    except Exception as e:
        print(f"[CONGRESSES ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


## Searches and filters representatives with optional query parameters.
#
#  Performs a full scan of the Reps table. Supports filtering by name
#  substring, chamber, party, and congress number. All filters are
#  applied in-memory after the initial DynamoDB scan.
#
#  Query parameters (all optional):
#    name     str  - case-insensitive substring match on representative name
#    chamber  str  - filter by chamber ("senate" or "house")
#    party    str  - filter by party (e.g. "Democrat", "Republican")
#    congress int  - filter by congress number
#    limit    int  - maximum number of results to return
#
#  @return  Response - JSON object with count, filters, and data array
@app.route("/reps", methods=["GET"])
def get_reps() -> Response:
    try:
        name = request.args.get("name", "").strip()
        chamber = request.args.get("chamber", "").strip()
        party = request.args.get("party", "").strip()
        congress = request.args.get("congress", "").strip()
        limit = request.args.get("limit", type=int)

        # Use DynamoDB filter for name to reduce data transfer
        filter_expr = Attr("name").contains(name) if name else None
        items = scan_all(get_reps_table(), filter_expr)
        items = convert_decimals(items)

        # Apply additional in-memory filters
        if name:
            pattern = re.compile(re.escape(name), re.IGNORECASE)
            items = [i for i in items if pattern.search(i.get("name", ""))]

        if chamber:
            ch_norm = normalize_chamber(chamber)
            items = [
                i for i in items
                if any(
                    normalize_chamber(t.get("chamber", "")) == ch_norm
                    for t in get_terms(i)
                )
            ]

        if party:
            items = [
                i for i in items
                if any(
                    t.get("party", "").lower() == party.lower()
                    for t in get_terms(i)
                )
            ]

        if congress:
            items = [
                i for i in items
                if any(
                    str(t.get("congress", "")) == congress
                    for t in get_terms(i)
                )
            ]

        items = deduplicate(items)

        if limit:
            items = items[:limit]

        return make_json({
            "count": len(items),
            "filters": {
                "name": name or None,
                "chamber": chamber or None,
                "party": party or None,
                "congress": congress or None,
            },
            "data": items,
        })

    except Exception as e:
        print(f"[REPS ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


## Returns a single representative record by bioguide ID.
#
#  Performs a direct DynamoDB GetItem call - O(1) lookup by primary key.
#
#  @param bioguideid  str - the unique bioguide identifier (e.g. "L000603")
#  @return            Response - JSON object for the representative,
#                                or HTTP 404 if not found
@app.route("/reps/<string:bioguideid>", methods=["GET"])
def get_rep_by_id(bioguideid: str) -> Response:
    try:
        response = get_reps_table().get_item(Key={"bioguideId": bioguideid})
        item = response.get("Item")
        if not item:
            return make_json({"error": f"Rep '{bioguideid}' not found."}, 404)
        return make_json(convert_decimals(item))

    except Exception as e:
        print(f"[REP ERROR] {traceback.format_exc()}")
        return make_json({"error": str(e)}, 500)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)
