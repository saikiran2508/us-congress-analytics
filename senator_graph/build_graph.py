## @package senator_graph.build_graph
#  Builds a weighted Senate co-sponsorship graph from DynamoDB bill data.
#
#  Reads all Senate bills from DynamoDB, extracts sponsor and co-sponsor
#  relationships, and constructs a weighted undirected NetworkX graph where
#  each node is a senator and each edge represents the number of bills they
#  co-sponsored together, normalized using a Jaccard-like formula.
#
#  Edge weight formula:
#    weight(A, B) = shared_bills(A, B) / sqrt(total_bills(A) * total_bills(B))
#
#  This normalization accounts for highly active senators who co-sponsor many
#  bills — a senator who co-sponsors 500 bills is not necessarily closer to
#  their partner than one who co-sponsors 50 targeted bills.
#
#  Outputs:
#    senate_graph.graphml  — full graph for use in clustering scripts
#    senator_stats.json    — per-senator metadata (party, state, bill count)
#
#  Usage:
#    python build_graph.py --table bills --region us-east-2

import os
import json
import math
import argparse
from collections import defaultdict
from typing import Optional

import boto3
import networkx as nx
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# DynamoDB scan
# ---------------------------------------------------------------------------

## Performs a full paginated scan of the bills DynamoDB table.
#
#  DynamoDB scan returns at most 1 MB per request. This function continues
#  issuing requests using LastEvaluatedKey until all pages are exhausted.
#
#  @param table  any - boto3 DynamoDB Table resource for the bills table
#  @return       list - all bill records from the table
def scan_all_bills(table: any) -> list:
    items: list = []
    kwargs: dict = {}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    print(f"Scanned {len(items):,} bills from DynamoDB")
    return items


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

## Extracts the canonical senator ID (bioguideId) from a sponsor or cosponsor dict.
#
#  Tries both "bioguideId" and "bioguideID" field names to handle inconsistent
#  casing in the DynamoDB records.
#
#  @param person  dict - sponsor or cosponsor dict from a bill record
#  @return        Optional[str] - bioguide ID string, or None if not found
def extract_senator_id(person: dict) -> Optional[str]:
    bid = person.get("bioguideId") or person.get("bioguideID")
    return bid if bid else None


## Builds a weighted co-sponsorship graph from a list of bill records.
#
#  Algorithm:
#    1. Walk every bill and collect the sponsor + all co-sponsors
#    2. For each pair of participants in the same bill, increment their
#       shared bill count
#    3. Normalize each edge weight using the Jaccard-like formula:
#       weight = shared / sqrt(total_A * total_B)
#    4. Add senator nodes with metadata and weighted edges to a NetworkX graph
#
#  @param bills  list - list of bill dicts from DynamoDB
#  @return       tuple - (G, senator_meta, senator_bills) where:
#                  G:             nx.Graph with senator nodes and weighted edges
#                  senator_meta:  dict mapping bioguideId to name/party/state
#                  senator_bills: dict mapping bioguideId to set of bill IDs
def build_cosponsorship_graph(bills: list) -> tuple:
    # senator_meta: bioguideId -> {name, party, state}
    senator_meta: dict = {}

    # senator_bills: bioguideId -> set of billIds they participated in
    senator_bills: dict = defaultdict(set)

    # cosponsor_pairs: (bioguideId_A, bioguideId_B) -> shared bill count
    cosponsor_pairs: dict = defaultdict(int)

    skipped = 0

    for bill in bills:
        bill_id = bill.get("billId")
        sponsor_raw = bill.get("Sponsor")
        cosponsors_raw = bill.get("Cosponsors", [])

        if not bill_id:
            skipped += 1
            continue

        # Normalize sponsor field — sometimes stored as a JSON string
        if isinstance(sponsor_raw, str):
            try:
                sponsor_raw = json.loads(sponsor_raw)
            except Exception:
                sponsor_raw = None

        sponsor_id = (
            extract_senator_id(sponsor_raw)
            if isinstance(sponsor_raw, dict)
            else None
        )

        # Collect all bill participants: sponsor + co-sponsors
        participants: list = []

        if sponsor_id:
            senator_bills[sponsor_id].add(bill_id)
            if sponsor_id not in senator_meta:
                senator_meta[sponsor_id] = {
                    "name": sponsor_raw.get("name") or sponsor_id,
                    "party": sponsor_raw.get("party") or "Unknown",
                    "state": sponsor_raw.get("state") or "Unknown",
                }
            participants.append(sponsor_id)

        for c in (cosponsors_raw or []):
            if isinstance(c, str):
                try:
                    c = json.loads(c)
                except Exception:
                    continue
            if not isinstance(c, dict):
                continue
            cid = extract_senator_id(c)
            if not cid:
                continue
            senator_bills[cid].add(bill_id)
            if cid not in senator_meta:
                senator_meta[cid] = {
                    "name": c.get("name") or cid,
                    "party": c.get("party") or "Unknown",
                    "state": c.get("state") or "Unknown",
                }
            participants.append(cid)

        # Build undirected pairs from all participants in this bill
        unique = list(set(participants))
        for i in range(len(unique)):
            for j in range(i + 1, len(unique)):
                a, b = sorted([unique[i], unique[j]])
                cosponsor_pairs[(a, b)] += 1

    print(f"Found {len(senator_meta):,} unique senators")
    print(f"Found {len(cosponsor_pairs):,} co-sponsorship pairs")
    if skipped:
        print(f"Skipped {skipped:,} bills (missing billId)")

    # Build NetworkX graph with senator nodes and normalized edge weights
    G = nx.Graph()

    for sid, meta in senator_meta.items():
        G.add_node(
            sid,
            name=meta["name"],
            party=meta["party"],
            state=meta["state"],
            bill_count=len(senator_bills[sid]),
        )

    for (a, b), shared in cosponsor_pairs.items():
        total_a = len(senator_bills[a])
        total_b = len(senator_bills[b])
        denom = math.sqrt(total_a * total_b) if (total_a and total_b) else 1
        weight = shared / denom
        G.add_edge(a, b, weight=weight, raw_count=shared)

    print("\nGraph summary:")
    print(f"  Nodes (senators): {G.number_of_nodes():,}")
    print(f"  Edges (pairs):    {G.number_of_edges():,}")
    if G.number_of_nodes():
        print(f"  Density:          {nx.density(G):.4f}")

    return G, senator_meta, senator_bills


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

## Entry point — scans DynamoDB bills table and saves the co-sponsorship graph.
#
#  Reads all bills from the specified DynamoDB table, builds the weighted
#  co-sponsorship graph, and saves it in two formats:
#    - GraphML file for use in clustering and Neo4j loading scripts
#    - JSON file with per-senator metadata (party, state, bill count, degree)
#
#  CLI arguments:
#    --table      str - DynamoDB bills table name (default from SENATE_BILLS_TABLE env)
#    --region     str - AWS region (default us-east-2)
#    --out-graph  str - output GraphML file path (default senate_graph.graphml)
#    --out-stats  str - output JSON stats file path (default senator_stats.json)
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Build Senate co-sponsorship graph from DynamoDB"
    )
    ap.add_argument("--table", default=os.getenv("SENATE_BILLS_TABLE", "bills"))
    ap.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-2"))
    ap.add_argument("--out-graph", default="senate_graph.graphml")
    ap.add_argument("--out-stats", default="senator_stats.json")
    args = ap.parse_args()

    ddb = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    bills = scan_all_bills(table)
    if not bills:
        raise SystemExit("No bills found in table. Check table name and region.")

    G, senator_meta, senator_bills = build_cosponsorship_graph(bills)

    # Save graph in GraphML format for use in clustering and Neo4j scripts
    nx.write_graphml(G, args.out_graph)
    print(f"\nSaved graph -> {args.out_graph}")

    # Save per-senator stats including degree centrality from the graph
    stats = {}
    for sid, meta in senator_meta.items():
        stats[sid] = {
            **meta,
            "bill_count": len(senator_bills[sid]),
            "degree": G.degree(sid) if sid in G else 0,
        }
    with open(args.out_stats, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Saved senator stats -> {args.out_stats}")


if __name__ == "__main__":
    main()
