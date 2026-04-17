"""
build_graph.py
--------------
Reads all Senate bills from DynamoDB and constructs a weighted
co-sponsorship graph using NetworkX.

Edge weight between senator A and senator B =
    shared_bills(A, B) / sqrt(total_bills(A) * total_bills(B))
    (Jaccard-like normalization — accounts for highly active senators)

Saves:
    senate_graph.graphml   — full graph for clustering
    senator_stats.json     — per-senator metadata (party, state, bill count)
"""

import os
import json
import math
import argparse
from collections import defaultdict

import boto3
import networkx as nx
from dotenv import load_dotenv

load_dotenv()


# ─────────────────────────────────────────────
# DynamoDB scan
# ─────────────────────────────────────────────

def scan_all_bills(table) -> list[dict]:
    """Full table scan — returns all bill items."""
    items = []
    kwargs = {}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        if not resp.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    print(f"Scanned {len(items):,} bills from DynamoDB")
    return items


# ─────────────────────────────────────────────
# Graph construction
# ─────────────────────────────────────────────

def extract_senator_id(person: dict) -> str | None:
    """Use bioguideId as the canonical senator ID."""
    bid = person.get("bioguideId") or person.get("bioguideID")
    return bid if bid else None


def build_cosponsorship_graph(bills: list[dict]) -> nx.Graph:
    """
    Returns a weighted undirected graph where:
        nodes  = senators (bioguideId)
        edges  = co-sponsorship pairs
        weight = Jaccard-like normalized co-sponsorship count
    """
    # Track senator metadata and bill counts
    senator_meta: dict[str, dict] = {}      # bioguideId -> {name, party, state}
    senator_bills: dict[str, set] = defaultdict(set)   # bioguideId -> set of billIds
    cosponsor_pairs: dict[tuple, int] = defaultdict(int)  # (A, B) -> count

    skipped = 0

    for bill in bills:
        bill_id = bill.get("billId")
        sponsor_raw = bill.get("Sponsor")
        cosponsors_raw = bill.get("Cosponsors", [])

        if not bill_id:
            skipped += 1
            continue

        # Normalize sponsor
        if isinstance(sponsor_raw, str):
            # sometimes stored as JSON string
            try:
                sponsor_raw = json.loads(sponsor_raw)
            except Exception:
                sponsor_raw = None

        sponsor_id = extract_senator_id(sponsor_raw) if isinstance(sponsor_raw, dict) else None

        # Collect all participants: sponsor + cosponsors
        participants = []

        if sponsor_id:
            senator_bills[sponsor_id].add(bill_id)
            if sponsor_id not in senator_meta:
                senator_meta[sponsor_id] = {
                    "name":  sponsor_raw.get("name") or sponsor_id,
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
                    "name":  c.get("name") or cid,
                    "party": c.get("party") or "Unknown",
                    "state": c.get("state") or "Unknown",
                }
            participants.append(cid)

        # Build pairs (undirected)
        unique = list(set(participants))
        for i in range(len(unique)):
            for j in range(i + 1, len(unique)):
                a, b = sorted([unique[i], unique[j]])
                cosponsor_pairs[(a, b)] += 1

    print(f"Found {len(senator_meta):,} unique senators")
    print(f"Found {len(cosponsor_pairs):,} co-sponsorship pairs")
    if skipped:
        print(f"Skipped {skipped:,} bills (missing billId)")

    # ── Build NetworkX graph ──────────────────────────────────────
    G = nx.Graph()

    # Add nodes with metadata
    for sid, meta in senator_meta.items():
        G.add_node(
            sid,
            name=meta["name"],
            party=meta["party"],
            state=meta["state"],
            bill_count=len(senator_bills[sid]),
        )

    # Add edges with normalized weight
    for (a, b), shared in cosponsor_pairs.items():
        total_a = len(senator_bills[a])
        total_b = len(senator_bills[b])
        denom = math.sqrt(total_a * total_b) if (total_a and total_b) else 1
        weight = shared / denom

        G.add_edge(a, b,
                   weight=weight,
                   raw_count=shared)

    print(f"\nGraph summary:")
    print(f"  Nodes (senators): {G.number_of_nodes():,}")
    print(f"  Edges (pairs):    {G.number_of_edges():,}")
    if G.number_of_nodes():
        density = nx.density(G)
        print(f"  Density:          {density:.4f}")

    return G, senator_meta, senator_bills


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Build Senate co-sponsorship graph from DynamoDB")
    ap.add_argument("--table",   default=os.getenv("SENATE_BILLS_TABLE", "bills"))
    ap.add_argument("--region",  default="us-east-2")
    ap.add_argument("--out-graph",  default="senate_graph.graphml")
    ap.add_argument("--out-stats",  default="senator_stats.json")
    args = ap.parse_args()

    ddb   = boto3.resource("dynamodb", region_name=args.region)
    table = ddb.Table(args.table)

    bills = scan_all_bills(table)
    if not bills:
        raise SystemExit("No bills found in table. Check table name and region.")

    G, senator_meta, senator_bills = build_cosponsorship_graph(bills)

    # Save graph
    nx.write_graphml(G, args.out_graph)
    print(f"\nSaved graph → {args.out_graph}")

    # Save per-senator stats
    stats = {}
    for sid, meta in senator_meta.items():
        stats[sid] = {
            **meta,
            "bill_count":  len(senator_bills[sid]),
            "degree":      G.degree(sid) if sid in G else 0,
        }
    with open(args.out_stats, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"Saved senator stats → {args.out_stats}")


if __name__ == "__main__":
    main()
