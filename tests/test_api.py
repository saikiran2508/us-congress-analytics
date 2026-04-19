## @package tests.test_api
#  Unit tests for the iCitizen Representatives REST API.
#
#  Uses moto to mock AWS DynamoDB so tests run without any real AWS
#  credentials or network access. Tests cover all helper functions
#  and all four HTTP route handlers.
#
#  Run with:
#    python -m pytest tests/test_api.py -v

import json
import pytest
import boto3
from moto import mock_aws
from decimal import Decimal


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

## Creates mocked DynamoDB Reps and RepTerms tables with seeded test data.
#
#  Resets the module-level boto3 handles before each test so moto can
#  intercept all DynamoDB calls within the mock_aws context.
#
#  @return  Flask test client configured against the mocked environment
@pytest.fixture
def client():
    with mock_aws():
        # Reset lazy-initialized module-level DynamoDB handles so they are
        # re-created inside this mock_aws context
        import api.main as main_module
        main_module._dynamodb = None
        main_module._reps_table = None
        main_module._terms_table = None

        db = boto3.resource("dynamodb", region_name="us-east-2")

        db.create_table(
            TableName="Reps",
            KeySchema=[{"AttributeName": "bioguideId", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "bioguideId", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        db.create_table(
            TableName="RepTerms",
            KeySchema=[{"AttributeName": "termId", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "termId", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST",
        )

        # Seed representative data
        reps_table = db.Table("Reps")
        reps_table.put_item(Item={
            "bioguideId": "W000817",
            "name": "Elizabeth Warren",
            "state": "MA",
            "party": "Democrat",
            "terms": [
                {"congress": Decimal("119"), "chamber": "Senate",
                 "party": "Democrat", "state": "MA"}
            ],
        })
        reps_table.put_item(Item={
            "bioguideId": "S000148",
            "name": "Chuck Schumer",
            "state": "NY",
            "party": "Democrat",
            "terms": [
                {"congress": Decimal("119"), "chamber": "Senate",
                 "party": "Democrat", "state": "NY"}
            ],
        })

        # Seed term data
        terms_table = db.Table("RepTerms")
        terms_table.put_item(Item={
            "termId": "term-001",
            "bioguideId": "W000817",
            "congress": Decimal("119"),
            "chamber": "Senate",
        })
        terms_table.put_item(Item={
            "termId": "term-002",
            "bioguideId": "S000148",
            "congress": Decimal("119"),
            "chamber": "Senate",
        })

        from api.main import app
        app.config["TESTING"] = True
        with app.test_client() as test_client:
            yield test_client


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------

## Tests convert_decimals converts whole-number Decimals to int.
def test_convert_decimals_whole_number():
    from api.main import convert_decimals
    result = convert_decimals({"congress": Decimal("119")})
    assert result["congress"] == 119
    assert isinstance(result["congress"], int)


## Tests convert_decimals converts fractional Decimals to float.
def test_convert_decimals_fractional():
    from api.main import convert_decimals
    result = convert_decimals(Decimal("3.14"))
    assert abs(result - 3.14) < 0.001
    assert isinstance(result, float)


## Tests convert_decimals handles nested structures correctly.
def test_convert_decimals_nested():
    from api.main import convert_decimals
    data = {"terms": [{"congress": Decimal("118")}, {"congress": Decimal("119")}]}
    result = convert_decimals(data)
    assert result["terms"][0]["congress"] == 118
    assert result["terms"][1]["congress"] == 119


## Tests convert_decimals converts Python sets to lists.
def test_convert_decimals_set():
    from api.main import convert_decimals
    result = convert_decimals({"states": {"MA", "NY"}})
    assert isinstance(result["states"], list)


## Tests normalize_chamber maps legacy "Senator" to "Senate".
def test_normalize_chamber_senator():
    from api.main import normalize_chamber
    assert normalize_chamber("Senator") == "Senate"


## Tests normalize_chamber maps legacy "Representative" to "House".
def test_normalize_chamber_representative():
    from api.main import normalize_chamber
    assert normalize_chamber("Representative") == "House"


## Tests normalize_chamber passes through canonical labels unchanged.
def test_normalize_chamber_canonical():
    from api.main import normalize_chamber
    assert normalize_chamber("Senate") == "Senate"
    assert normalize_chamber("House") == "House"


## Tests normalize_chamber maps "Delegate" correctly.
def test_normalize_chamber_delegate():
    from api.main import normalize_chamber
    assert normalize_chamber("Delegate") == "Delegate"


## Tests get_terms returns empty list for missing terms field.
def test_get_terms_missing():
    from api.main import get_terms
    assert get_terms({}) == []


## Tests get_terms returns empty list when terms is not a list.
def test_get_terms_not_list():
    from api.main import get_terms
    assert get_terms({"terms": "invalid"}) == []


## Tests get_terms filters out non-dict entries.
def test_get_terms_filters_non_dict():
    from api.main import get_terms
    item = {"terms": [{"congress": 119}, "bad", None]}
    result = get_terms(item)
    assert len(result) == 1
    assert result[0]["congress"] == 119


## Tests deduplicate removes duplicate bioguideIds keeping first occurrence.
def test_deduplicate():
    from api.main import deduplicate
    items = [
        {"bioguideId": "A001", "name": "Alice"},
        {"bioguideId": "B001", "name": "Bob"},
        {"bioguideId": "A001", "name": "Alice Duplicate"},
    ]
    result = deduplicate(items)
    assert len(result) == 2
    assert result[0]["name"] == "Alice"


# ---------------------------------------------------------------------------
# Route tests
# ---------------------------------------------------------------------------

## Tests the health check index route returns HTTP 200.
def test_index(client):
    response = client.get("/")
    assert response.status_code == 200


## Tests /reps/map returns Senate members for congress 119.
def test_map_returns_senate_members(client):
    response = client.get("/reps/map?congress=119&chamber=senate")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "data" in data
    assert isinstance(data["data"], list)
    bioguide_ids = {m["bioguideId"] for m in data["data"]}
    assert "W000817" in bioguide_ids
    assert "S000148" in bioguide_ids


## Tests /reps/map returns 400 when required parameters are missing.
def test_map_missing_params(client):
    response = client.get("/reps/map")
    assert response.status_code == 400
    data = json.loads(response.data)
    assert "error" in data


## Tests /reps/map returns empty data for a congress with no members.
def test_map_empty_result(client):
    response = client.get("/reps/map?congress=1&chamber=senate")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["count"] == 0
    assert data["data"] == []


## Tests /reps/congresses returns a sorted list of congress numbers.
def test_congresses_returns_sorted_list(client):
    response = client.get("/reps/congresses")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert "congresses" in data
    assert 119 in data["congresses"]
    assert data["congresses"] == sorted(data["congresses"], reverse=True)


## Tests /reps search returns matching results for a valid name query.
def test_search_returns_results(client):
    response = client.get("/reps?name=Warren")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert any(m["bioguideId"] == "W000817" for m in data["data"])


## Tests /reps search returns all members when no filters are applied.
def test_search_no_filters(client):
    response = client.get("/reps")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["count"] >= 2


## Tests /reps/<id> returns the correct representative for a known ID.
def test_get_rep_by_id_found(client):
    response = client.get("/reps/W000817")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["bioguideId"] == "W000817"
    assert data["name"] == "Elizabeth Warren"


## Tests /reps/<id> returns 404 for an unknown bioguide ID.
def test_get_rep_by_id_not_found(client):
    response = client.get("/reps/UNKNOWN999")
    assert response.status_code == 404
    data = json.loads(response.data)
    assert "error" in data
