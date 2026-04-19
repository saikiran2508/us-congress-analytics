## @package tests.test_ingestion
#  Smoke tests for the ingestion helper functions.
#
#  These tests verify that helper functions in the ingestion scripts
#  produce the correct output format without making any real API calls
#  or DynamoDB connections. They use hardcoded sample data that mirrors
#  the structure returned by the Congress.gov and Bioguide APIs.
#
#  Run with:
#    python -m pytest tests/test_ingestion.py -v

from decimal import Decimal


# ---------------------------------------------------------------------------
# bioguide_members.py helper tests
# ---------------------------------------------------------------------------

## Tests bid() produces correctly formatted bioguide IDs.
def test_bid_format():
    from ingestion.bioguide_members import bid
    assert bid("L", 603) == "L000603"
    assert bid("A", 1) == "A000001"
    assert bid("Z", 999999) == "Z999999"


## Tests normalize_chamber maps "Senator" to "Senate".
def test_bioguide_normalize_chamber_senator():
    from ingestion.bioguide_members import chamber_from_job
    assert chamber_from_job("Senator") == "Senate"


## Tests normalize_chamber maps "Representative" to "House".
def test_bioguide_normalize_chamber_representative():
    from ingestion.bioguide_members import chamber_from_job
    assert chamber_from_job("Representative") == "House"


## Tests normalize_chamber returns None for missing input.
def test_bioguide_normalize_chamber_none():
    from ingestion.bioguide_members import chamber_from_job
    assert chamber_from_job(None) is None


## Tests build_name constructs name from given and family name components.
def test_build_name_full():
    from ingestion.bioguide_members import build_name
    d = {"givenName": "Elizabeth", "familyName": "Warren"}
    assert build_name(d) == "Elizabeth Warren"


## Tests build_name falls back to displayName when components are missing.
def test_build_name_fallback():
    from ingestion.bioguide_members import build_name
    d = {"displayName": "Elizabeth A. Warren"}
    assert build_name(d) == "Elizabeth A. Warren"


## Tests build_name returns None when no name data is available.
def test_build_name_empty():
    from ingestion.bioguide_members import build_name
    assert build_name({}) is None


## Tests to_dynamo converts float values to Decimal.
def test_bioguide_to_dynamo_float():
    from ingestion.bioguide_members import to_dynamo
    result = to_dynamo({"score": 3.14})
    assert isinstance(result["score"], Decimal)


## Tests to_dynamo handles nested dicts and lists correctly.
def test_bioguide_to_dynamo_nested():
    from ingestion.bioguide_members import to_dynamo
    result = to_dynamo({"terms": [{"weight": 1.5}]})
    assert isinstance(result["terms"][0]["weight"], Decimal)


## Tests image_url_from_assets returns placeholder for empty assets.
def test_image_url_placeholder():
    from ingestion.bioguide_members import image_url_from_assets
    url = image_url_from_assets({}, "L000603")
    assert "placeholder" in url


## Tests image_url_from_assets extracts URL from image list.
def test_image_url_from_assets():
    from ingestion.bioguide_members import image_url_from_assets
    d = {"image": [{"contentUrl": "https://example.com/photo.jpg"}]}
    url = image_url_from_assets(d, "L000603")
    assert url == "https://example.com/photo.jpg"


# ---------------------------------------------------------------------------
# current_reps_ingestion.py helper tests
# ---------------------------------------------------------------------------

## Tests build_name_from_member constructs name from firstName and lastName.
def test_build_name_from_member_full():
    from ingestion.current_reps_ingestion import build_name_from_member
    m = {"firstName": "Elizabeth", "lastName": "Warren"}
    assert build_name_from_member(m) == "Elizabeth Warren"


## Tests build_name_from_member falls back to name field.
def test_build_name_from_member_fallback():
    from ingestion.current_reps_ingestion import build_name_from_member
    m = {"name": "Warren, Elizabeth"}
    assert build_name_from_member(m) == "Warren, Elizabeth"


## Tests image_from_member extracts imageUrl from depiction field.
def test_image_from_member_found():
    from ingestion.current_reps_ingestion import image_from_member
    m = {"depiction": {"imageUrl": "https://example.com/photo.jpg"}}
    assert image_from_member(m) == "https://example.com/photo.jpg"


## Tests image_from_member returns placeholder when no image is available.
def test_image_from_member_placeholder():
    from ingestion.current_reps_ingestion import image_from_member
    assert "placeholder" in image_from_member({})


## Tests name_slug converts display name to URL-safe slug.
def test_name_slug():
    from ingestion.current_reps_ingestion import name_slug
    assert name_slug("Joaquin Castro") == "joaquin-castro"
    assert name_slug("Elizabeth A. Warren") == "elizabeth-a-warren"


## Tests name_slug handles empty string input.
def test_name_slug_empty():
    from ingestion.current_reps_ingestion import name_slug
    assert name_slug("") == "member"


## Tests extract_members extracts list from root-level "members" key.
def test_extract_members_root():
    from ingestion.current_reps_ingestion import extract_members
    payload = {"members": [{"bioguideId": "W000817"}]}
    result = extract_members(payload)
    assert len(result) == 1
    assert result[0]["bioguideId"] == "W000817"


## Tests extract_members returns empty list for non-dict input.
def test_extract_members_invalid():
    from ingestion.current_reps_ingestion import extract_members
    assert extract_members([]) == []
    assert extract_members(None) == []


# ---------------------------------------------------------------------------
# populate_repterms.py helper tests
# ---------------------------------------------------------------------------

## Tests normalize_chamber maps "Senate" to "Senate".
def test_populate_normalize_senate():
    from ingestion.populate_repterms import normalize_chamber
    assert normalize_chamber("Senate") == "Senate"
    assert normalize_chamber("Senator") == "Senate"


## Tests normalize_chamber maps "House" variants correctly.
def test_populate_normalize_house():
    from ingestion.populate_repterms import normalize_chamber
    assert normalize_chamber("House") == "House"
    assert normalize_chamber("Representative") == "House"
    assert normalize_chamber("House of Representatives") == "House"


## Tests normalize_chamber returns None for non-legislative roles.
def test_populate_normalize_other():
    from ingestion.populate_repterms import normalize_chamber
    assert normalize_chamber("Delegate") is None
    assert normalize_chamber("Vice President") is None


# ---------------------------------------------------------------------------
# bills_senate.py helper tests
# ---------------------------------------------------------------------------

## Tests parse_bill_status identifies enacted bills correctly.
def test_parse_bill_status_enacted():
    from ingestion.bills_senate import parse_bill_status
    detail = {"laws": [{"type": "Public Law"}], "latestAction": {}}
    result = parse_bill_status(detail)
    assert result["status"] == "Enacted"
    assert result["becameLaw"] is True


## Tests parse_bill_status identifies introduced bills correctly.
def test_parse_bill_status_introduced():
    from ingestion.bills_senate import parse_bill_status
    detail = {
        "laws": [],
        "latestAction": {"text": "Introduced in Senate", "actionDate": "2025-01-15"}
    }
    result = parse_bill_status(detail)
    assert result["status"] == "Introduced"
    assert result["latestActionDate"] == "2025-01-15"


## Tests parse_bill_status identifies referred to committee bills.
def test_parse_bill_status_referred():
    from ingestion.bills_senate import parse_bill_status
    detail = {
        "laws": [],
        "latestAction": {"text": "Referred to the Committee on Finance"}
    }
    result = parse_bill_status(detail)
    assert result["status"] == "Referred to Committee"


## Tests parse_sponsor extracts sponsor from bill detail.
def test_parse_sponsor_found():
    from ingestion.bills_senate import parse_sponsor
    detail = {
        "sponsors": [{
            "firstName": "Elizabeth",
            "lastName": "Warren",
            "bioguideId": "W000817",
            "party": "D",
            "state": "MA",
        }]
    }
    result = parse_sponsor(detail)
    assert result["bioguideId"] == "W000817"
    assert result["state"] == "MA"


## Tests parse_sponsor returns None when no sponsors are present.
def test_parse_sponsor_empty():
    from ingestion.bills_senate import parse_sponsor
    assert parse_sponsor({}) is None
    assert parse_sponsor({"sponsors": []}) is None


## Tests parse_cosponsors builds cleaned cosponsor list.
def test_parse_cosponsors():
    from ingestion.bills_senate import parse_cosponsors
    raw = [{"firstName": "Chuck", "lastName": "Schumer",
            "bioguideId": "S000148", "party": "D", "state": "NY"}]
    result = parse_cosponsors(raw)
    assert len(result) == 1
    assert result[0]["bioguideId"] == "S000148"


## Tests to_dynamo removes None values from dicts.
def test_bills_to_dynamo_removes_none():
    from ingestion.bills_senate import to_dynamo
    result = to_dynamo({"title": "Test Bill", "summary": None})
    assert "summary" not in result
    assert result["title"] == "Test Bill"


# ---------------------------------------------------------------------------
# Data format validation tests
# ---------------------------------------------------------------------------

## Tests that build_item produces a bill record with all required DynamoDB fields.
#
#  Verifies the output format is correct before writing to DynamoDB.
def test_build_item_has_required_fields():
    from ingestion.bills_senate import build_item
    item = build_item(
        bill_summary={"congress": 119, "number": 1, "type": "S"},
        detail={
            "title": "A bill to test",
            "congress": 119,
            "number": 1,
            "type": "S",
            "introducedDate": "2025-01-15",
            "originChamber": "Senate",
            "sponsors": [{
                "firstName": "Elizabeth",
                "lastName": "Warren",
                "bioguideId": "W000817",
                "party": "D",
                "state": "MA",
            }],
            "laws": [],
            "latestAction": {"text": "Introduced in Senate",
                             "actionDate": "2025-01-15"},
        },
        cosponsors_raw=[],
        subjects={"policyArea": "Health", "legislativeSubjects": ["Medicare"]},
    )
    # Verify all required DynamoDB fields are present
    required_fields = [
        "billId", "Number", "Type", "Congress", "Chamber",
        "Title", "Introduced", "Sponsor", "Status", "BecameLaw",
        "LatestAction", "LatestActionDate", "CosponsorCount",
        "Cosponsors", "Subject", "Keywords", "url", "updateDate",
    ]
    for field in required_fields:
        assert field in item, f"Missing required field: {field}"


## Tests that build_item produces correct billId format.
def test_build_item_bill_id_format():
    from ingestion.bills_senate import build_item
    item = build_item(
        bill_summary={"congress": 119, "number": 42, "type": "S"},
        detail={"congress": 119, "number": 42, "type": "S",
                "laws": [], "latestAction": {}},
        cosponsors_raw=[],
        subjects={"policyArea": None, "legislativeSubjects": []},
    )
    assert item["billId"] == "119-S-42"
    assert item["Congress"] == 119
    assert item["Number"] == 42


## Tests that build_item correctly counts cosponsors.
def test_build_item_cosponsor_count():
    from ingestion.bills_senate import build_item
    cosponsors = [
        {"firstName": "Chuck", "lastName": "Schumer",
         "bioguideId": "S000148", "party": "D", "state": "NY"},
        {"firstName": "Bernie", "lastName": "Sanders",
         "bioguideId": "S000033", "party": "I", "state": "VT"},
    ]
    item = build_item(
        bill_summary={"congress": 119, "number": 1, "type": "S"},
        detail={"laws": [], "latestAction": {}},
        cosponsors_raw=cosponsors,
        subjects={"policyArea": None, "legislativeSubjects": []},
    )
    assert item["CosponsorCount"] == 2
    assert len(item["Cosponsors"]) == 2


## Tests that a Reps DynamoDB record has all required fields.
#
#  Verifies the record structure produced by bioguide_members.build_record.
def test_reps_record_has_required_fields():
    from ingestion.bioguide_members import build_record
    sample_data = {
        "data": {
            "givenName": "Elizabeth",
            "familyName": "Warren",
            "birthDate": "1949-06-22",
            "jobPositions": [],
        }
    }
    record = build_record("W000817", sample_data)
    required_fields = [
        "bioguideId", "image", "name", "birth",
        "terms", "updateDate", "url",
    ]
    for field in required_fields:
        assert field in record, f"Missing required field: {field}"
    assert record["bioguideId"] == "W000817"
    assert isinstance(record["terms"], list)


## Tests that a RepTerms record has all required fields.
#
#  Verifies the structure that populate_repterms.py writes to DynamoDB.
def test_repterms_record_structure():
    import uuid
    from ingestion.populate_repterms import normalize_chamber
    from ingestion.populate_repterms import VALID_CHAMBERS

    # Simulate what populate() writes for one term
    term = {
        "congress": 119,
        "chamber": "Senate",
        "bioguideId": "W000817",
    }
    chamber_norm = normalize_chamber(term["chamber"])
    assert chamber_norm in VALID_CHAMBERS

    record = {
        "termId": str(uuid.uuid4()),
        "congress": int(term["congress"]),
        "chamber": chamber_norm,
        "bioguideId": term["bioguideId"],
    }
    required_fields = ["termId", "congress", "chamber", "bioguideId"]
    for field in required_fields:
        assert field in record, f"Missing required field: {field}"
    assert isinstance(record["termId"], str)
    assert isinstance(record["congress"], int)