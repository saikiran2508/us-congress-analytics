## @package tests.test_senator_graph
#  Smoke tests for the senator_graph helper functions.
#
#  Tests verify that graph construction, clustering, and analysis helper
#  functions produce correct output formats using small in-memory sample
#  data. No real DynamoDB, Neo4j, or file system access is required.
#
#  Run with:
#    python -m pytest tests/test_senator_graph.py -v

import json
import networkx as nx


# ---------------------------------------------------------------------------
# build_graph.py tests
# ---------------------------------------------------------------------------

## Tests extract_senator_id returns bioguideId from a person dict.
def test_extract_senator_id_found():
    from senator_graph.build_graph import extract_senator_id
    person = {"bioguideId": "W000817", "name": "Elizabeth Warren"}
    assert extract_senator_id(person) == "W000817"


## Tests extract_senator_id handles alternative casing bioguideID.
def test_extract_senator_id_alt_casing():
    from senator_graph.build_graph import extract_senator_id
    person = {"bioguideID": "S000148"}
    assert extract_senator_id(person) == "S000148"


## Tests extract_senator_id returns None when no ID is present.
def test_extract_senator_id_missing():
    from senator_graph.build_graph import extract_senator_id
    assert extract_senator_id({}) is None
    assert extract_senator_id({"name": "Unknown"}) is None


## Tests build_cosponsorship_graph creates correct nodes and edges.
#
#  Uses a minimal sample of 3 bills with 3 senators to verify the
#  graph structure — node count, edge count, and edge weight calculation.
def test_build_cosponsorship_graph_basic():
    from senator_graph.build_graph import build_cosponsorship_graph

    # Sample bill data with sponsor and co-sponsors
    bills = [
        {
            "billId": "119-S-1",
            "Sponsor": {"bioguideId": "W000817", "name": "Warren",
                        "party": "Democrat", "state": "MA"},
            "Cosponsors": [
                {"bioguideId": "S000148", "name": "Schumer",
                 "party": "Democrat", "state": "NY"},
            ],
        },
        {
            "billId": "119-S-2",
            "Sponsor": {"bioguideId": "W000817", "name": "Warren",
                        "party": "Democrat", "state": "MA"},
            "Cosponsors": [
                {"bioguideId": "S000148", "name": "Schumer",
                 "party": "Democrat", "state": "NY"},
                {"bioguideId": "M000133", "name": "Markey",
                 "party": "Democrat", "state": "MA"},
            ],
        },
    ]

    G, senator_meta, senator_bills = build_cosponsorship_graph(bills)

    # Should have 3 senator nodes
    assert G.number_of_nodes() == 3

    # Warren and Schumer co-sponsor 2 bills together
    assert G.has_edge("W000817", "S000148")
    assert G["W000817"]["S000148"]["raw_count"] == 2

    # Warren and Markey co-sponsor 1 bill together
    assert G.has_edge("W000817", "M000133")
    assert G["W000817"]["M000133"]["raw_count"] == 1

    # Edge weights should be positive floats
    for _, _, d in G.edges(data=True):
        assert d["weight"] > 0


## Tests build_cosponsorship_graph skips bills with missing billId.
def test_build_cosponsorship_graph_skips_invalid():
    from senator_graph.build_graph import build_cosponsorship_graph
    bills = [{"Sponsor": {"bioguideId": "W000817"}, "Cosponsors": []}]
    G, _, _ = build_cosponsorship_graph(bills)
    assert G.number_of_nodes() == 0


## Tests scan_all_bills returns an empty list for an empty table.
def test_scan_all_bills_empty():
    from senator_graph.build_graph import scan_all_bills

    class MockTable:
        def scan(self, **kwargs):
            return {"Items": [], "LastEvaluatedKey": None}

    result = scan_all_bills(MockTable())
    assert result == []


# ---------------------------------------------------------------------------
# run_clustering_v2.py tests
# ---------------------------------------------------------------------------

## Tests filter_graph removes edges below the percentile threshold.
def test_filter_graph_removes_weak_edges():
    from senator_graph.run_clustering_v2 import filter_graph

    G = nx.Graph()
    G.add_edge("A", "B", weight=0.1)
    G.add_edge("B", "C", weight=0.5)
    G.add_edge("A", "C", weight=0.9)

    # p50 should keep top 50% of edges (2 out of 3)
    filtered, isolated, threshold = filter_graph(G, percentile=50)
    assert filtered.number_of_edges() < G.number_of_edges()
    assert threshold > 0


## Tests modularity_score returns a float between -1 and 1.
def test_modularity_score_range():
    from senator_graph.run_clustering_v2 import modularity_score

    G = nx.karate_club_graph()
    for u, v in G.edges():
        G[u][v]["weight"] = 1.0
    communities = [set(range(17)), set(range(17, 34))]
    score = modularity_score(G, communities)
    assert -1.0 <= score <= 1.0


## Tests communities_to_mapping builds correct bioguideId to community dict.
def test_communities_to_mapping():
    from senator_graph.run_clustering_v2 import communities_to_mapping
    communities = [{"A001", "A002"}, {"B001", "B002"}]
    mapping = communities_to_mapping(communities)
    assert mapping["A001"] == 0
    assert mapping["B001"] == 1
    assert len(mapping) == 4


## Tests community_party_breakdown computes correct party counts.
def test_community_party_breakdown():
    from senator_graph.run_clustering_v2 import community_party_breakdown

    G = nx.Graph()
    G.add_node("W000817", party="Democrat", name="Warren", state="MA", bill_count=100)
    G.add_node("S000148", party="Democrat", name="Schumer", state="NY", bill_count=80)
    G.add_node("M000355", party="Republican", name="McConnell", state="KY", bill_count=60)
    G.add_edge("W000817", "S000148", weight=0.5)

    communities = [{"W000817", "S000148"}, {"M000355"}]
    breakdown = community_party_breakdown(G, communities)

    assert len(breakdown) == 2
    # Find the Democrat community
    dem_comm = next(b for b in breakdown if b["dominant_party"] == "Democrat")
    assert dem_comm["size"] == 2


# ---------------------------------------------------------------------------
# analyze_clusters.py tests
# ---------------------------------------------------------------------------

## Tests party_alignment_score returns 1.0 for a single-party community.
def test_party_alignment_score_pure():
    from senator_graph.analyze_clusters import party_alignment_score
    breakdown = {"Democrat": 10}
    assert party_alignment_score(breakdown) == 1.0


## Tests party_alignment_score returns 0.0 for an equal split.
def test_party_alignment_score_mixed():
    from senator_graph.analyze_clusters import party_alignment_score
    breakdown = {"Democrat": 5, "Republican": 5}
    score = party_alignment_score(breakdown)
    assert abs(score) < 0.01


## Tests party_alignment_score returns 0.0 for empty breakdown.
def test_party_alignment_score_empty():
    from senator_graph.analyze_clusters import party_alignment_score
    assert party_alignment_score({}) == 0.0


# ---------------------------------------------------------------------------
# identify_clusters.py tests
# ---------------------------------------------------------------------------

## Tests get_best_result returns the correct algorithm result.
def test_get_best_result_found():
    from senator_graph.identify_clusters import get_best_result
    results = {
        "louvain_res0.5": {
            "modularity": 0.387,
            "senators": [{"bioguideId": "W000817", "community_id": 0,
                          "name": "Warren", "party": "Democrat",
                          "state": "MA", "bill_count": 100}]
        }
    }
    result = get_best_result(results, "louvain_res0.5")
    assert result is not None
    assert result["modularity"] == 0.387


## Tests get_best_result falls back to first key when algo not found.
def test_get_best_result_fallback():
    from senator_graph.identify_clusters import get_best_result
    results = {
        "some_algo": {
            "modularity": 0.3,
            "senators": []
        }
    }
    result = get_best_result(results, "nonexistent_key")
    assert result is not None


## Tests get_best_result returns None for empty results dict.
def test_get_best_result_empty():
    from senator_graph.identify_clusters import get_best_result
    result = get_best_result({})
    assert result is None


# ---------------------------------------------------------------------------
# Data format validation tests
# ---------------------------------------------------------------------------

## Tests that graph nodes have all required attributes after construction.
#
#  Verifies the node attribute format is correct before loading into Neo4j.
def test_graph_nodes_have_required_attributes():
    from senator_graph.build_graph import build_cosponsorship_graph
    bills = [
        {
            "billId": "119-S-1",
            "Sponsor": {"bioguideId": "W000817", "name": "Warren",
                        "party": "Democrat", "state": "MA"},
            "Cosponsors": [
                {"bioguideId": "S000148", "name": "Schumer",
                 "party": "Democrat", "state": "NY"},
            ],
        }
    ]
    G, senator_meta, senator_bills = build_cosponsorship_graph(bills)

    # Every node must have name, party, state, bill_count attributes
    required_attrs = ["name", "party", "state", "bill_count"]
    for node_id, attrs in G.nodes(data=True):
        for attr in required_attrs:
            assert attr in attrs, f"Node {node_id} missing attribute: {attr}"


## Tests that graph edges have required weight attributes.
#
#  Verifies edge attributes are correct before loading into Neo4j.
def test_graph_edges_have_required_attributes():
    from senator_graph.build_graph import build_cosponsorship_graph
    bills = [
        {
            "billId": "119-S-1",
            "Sponsor": {"bioguideId": "W000817", "name": "Warren",
                        "party": "Democrat", "state": "MA"},
            "Cosponsors": [
                {"bioguideId": "S000148", "name": "Schumer",
                 "party": "Democrat", "state": "NY"},
            ],
        }
    ]
    G, _, _ = build_cosponsorship_graph(bills)

    # Every edge must have weight and raw_count
    for u, v, attrs in G.edges(data=True):
        assert "weight" in attrs, f"Edge {u}-{v} missing weight"
        assert "raw_count" in attrs, f"Edge {u}-{v} missing raw_count"
        assert attrs["weight"] > 0
        assert attrs["raw_count"] > 0


## Tests that build_result produces correctly structured clustering output.
#
#  Verifies the JSON format written to cluster_results_v2.json is correct.
def test_build_result_structure():
    from senator_graph.run_clustering_v2 import build_result

    G = nx.Graph()
    G.add_node("W000817", name="Warren", party="Democrat",
               state="MA", bill_count=100)
    G.add_node("S000148", name="Schumer", party="Democrat",
               state="NY", bill_count=80)
    G.add_edge("W000817", "S000148", weight=0.5, raw_count=10)

    communities = [{"W000817", "S000148"}]
    result = build_result(G, "test_algo", communities, 0.35)

    # Verify required top-level keys
    required_keys = [
        "algorithm", "modularity", "num_communities",
        "senators", "community_summary"
    ]
    for key in required_keys:
        assert key in result, f"Missing key: {key}"

    # Verify senator records have required fields
    for senator in result["senators"]:
        required_senator_fields = [
            "bioguideId", "name", "party", "state",
            "bill_count", "community_id"
        ]
        for field in required_senator_fields:
            assert field in senator, f"Senator missing field: {field}"


## Tests that community_summary records have required fields.
def test_community_summary_structure():
    from senator_graph.run_clustering_v2 import community_party_breakdown

    G = nx.Graph()
    G.add_node("W000817", party="Democrat", name="Warren",
               state="MA", bill_count=100)
    G.add_node("S000148", party="Democrat", name="Schumer",
               state="NY", bill_count=80)
    G.add_edge("W000817", "S000148", weight=0.5)

    communities = [{"W000817", "S000148"}]
    breakdown = community_party_breakdown(G, communities)

    required_fields = [
        "community_id", "size", "party_breakdown",
        "dominant_party", "dominant_pct", "bipartisan"
    ]
    for record in breakdown:
        for field in required_fields:
            assert field in record, f"Community summary missing field: {field}"