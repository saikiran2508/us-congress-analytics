"""
analyze_clusters.py
-------------------
Reads cluster_results.json and prints human-readable analysis:
    - Community composition per algorithm
    - Party alignment (how partisan are the clusters?)
    - Cross-party senators (bipartisan connectors)
    - Top co-sponsors per cluster
    - Algorithm comparison table

Usage:
    python analyze_clusters.py
    python analyze_clusters.py --algo louvain
    python analyze_clusters.py --algo louvain --top-senators 5
"""

import json
import argparse
from collections import Counter, defaultdict


def load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def print_separator(char="─", width=60):
    print(char * width)


def party_alignment_score(breakdown: dict) -> float:
    """
    Returns 0.0 (perfectly mixed) to 1.0 (pure single party).
    Uses normalized Herfindahl-Hirschman Index.
    """
    total = sum(breakdown.values())
    if total == 0:
        return 0.0
    shares = [v / total for v in breakdown.values()]
    n = len(shares)
    hhi = sum(s ** 2 for s in shares)
    # Normalize: 1/n (perfect mix) to 1 (pure)
    if n == 1:
        return 1.0
    return (hhi - 1 / n) / (1 - 1 / n)


def print_algo_report(algo_name: str, result: dict, top_senators: int = 5):
    print_separator("═")
    print(f"  {algo_name.upper().replace('_', ' ')}")
    print(f"  Modularity: {result['modularity']:.4f}   "
          f"Communities: {result['num_communities']}")
    print_separator("═")

    # Build lookup: community_id -> list of senators
    comm_senators = defaultdict(list)
    for s in result["senators"]:
        comm_senators[s["community_id"]].append(s)

    for summary in sorted(result["community_summary"], key=lambda x: x["size"], reverse=True):
        cid = summary["community_id"]
        size = summary["size"]
        dominant = summary["dominant_party"]
        breakdown = summary["party_breakdown"]
        alignment = party_alignment_score(breakdown)
        bipartisan = summary["bipartisan"]

        # Party bar
        total = sum(breakdown.values())
        bar_parts = []
        for party, count in sorted(breakdown.items(), key=lambda x: x[1], reverse=True):
            pct = count / total * 100
            symbol = {"R": "R", "D": "D", "ID": "I"}.get(party, party[:2])
            bar_parts.append(f"{symbol}: {count} ({pct:.0f}%)")
        party_str = "  |  ".join(bar_parts)

        bipartisan_tag = " [bipartisan]" if bipartisan else ""
        print(f"\nCommunity {cid}  ({size} senators){bipartisan_tag}")
        print(f"  {party_str}")
        print(f"  Alignment score: {alignment:.3f}  "
              f"(0=mixed, 1=uniform)  |  Dominant: {dominant}")

        # Top senators by degree (most connected in this community)
        senators_in_comm = comm_senators[cid]
        # Sort by bill_count as proxy for activity
        senators_sorted = sorted(senators_in_comm, key=lambda x: x["bill_count"], reverse=True)
        top = senators_sorted[:top_senators]
        names = [f"{s['name']} ({s['party']}-{s['state']})" for s in top]
        print(f"  Top senators: {', '.join(names)}")

    print()


def cross_party_analysis(result: dict):
    """Find senators in communities dominated by the opposite party."""
    comm_dominant = {}
    for s in result["community_summary"]:
        comm_dominant[s["community_id"]] = s["dominant_party"]

    outliers = []
    for s in result["senators"]:
        cid = s["community_id"]
        dominant = comm_dominant.get(cid, "Unknown")
        if s["party"] != dominant and s["party"] not in ("Unknown", "") \
                and dominant not in ("Unknown", ""):
            outliers.append({
                "name":          s["name"],
                "party":         s["party"],
                "state":         s["state"],
                "community_id":  cid,
                "dominant":      dominant,
                "bill_count":    s["bill_count"],
            })

    if outliers:
        print_separator()
        print("Cross-party senators (placed in opposite-party community)")
        print_separator()
        for o in sorted(outliers, key=lambda x: x["bill_count"], reverse=True):
            print(f"  {o['name']:<30} {o['party']}-{o['state']:<4}  "
                  f"→ community {o['community_id']} (dom: {o['dominant']})")
    else:
        print("No cross-party placements detected for this algorithm.")


def comparison_table(results: dict):
    print_separator("═")
    print("  ALGORITHM COMPARISON")
    print_separator("═")
    print(f"  {'Algorithm':<24} {'Modularity':>11} {'Communities':>13}")
    print_separator()
    rows = sorted(results.items(), key=lambda x: x[1]["modularity"], reverse=True)
    for algo, res in rows:
        print(f"  {algo.replace('_',' ').title():<24} "
              f"{res['modularity']:>11.4f} "
              f"{res['num_communities']:>13}")
    print_separator("═")
    best_algo, best_res = rows[0]
    print(f"  Best modularity: {best_algo.replace('_',' ').title()} "
          f"({best_res['modularity']:.4f})")
    print_separator("═")
    print()


def main():
    ap = argparse.ArgumentParser(description="Analyze Senate clustering results")
    ap.add_argument("--results",      default="cluster_results.json")
    ap.add_argument("--algo",         default=None,
                    help="Run report for one algorithm only (e.g. louvain)")
    ap.add_argument("--top-senators", type=int, default=5)
    ap.add_argument("--cross-party",  action="store_true",
                    help="Show cross-party senator analysis")
    args = ap.parse_args()

    results = load_results(args.results)

    # Summary table
    comparison_table(results)

    # Per-algorithm reports
    algos = [args.algo] if args.algo else list(results.keys())
    for algo in algos:
        if algo not in results:
            print(f"Algorithm '{algo}' not found. Available: {list(results.keys())}")
            continue
        print_algo_report(algo, results[algo], top_senators=args.top_senators)

        if args.cross_party:
            cross_party_analysis(results[algo])
            print()


if __name__ == "__main__":
    main()
