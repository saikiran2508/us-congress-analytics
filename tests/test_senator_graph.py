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
