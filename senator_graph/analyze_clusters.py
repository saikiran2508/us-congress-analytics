## @package senator_graph.analyze_clusters
#  Analyzes and prints human-readable reports from cluster results JSON.
#
#  Reads the cluster_results_v2.json file produced by run_clustering_v2.py
#  and generates detailed reports including:
#    - Community composition per algorithm (party breakdown, top senators)
#    - Party alignment scores (how partisan are the clusters?)
#    - Cross-party senators (bipartisan connectors placed in opposite-party communities)
#    - Algorithm comparison table sorted by modularity score
#
#  Usage:
#    python analyze_clusters.py
#    python analyze_clusters.py --algo louvain_res0.5
#    python analyze_clusters.py --algo louvain_res0.5 --top-senators 5 --cross-party

import json
import argparse
from collections import defaultdict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

## Loads cluster results from a JSON file.
#
#  @param path  str - path to the cluster results JSON file
#  @return      dict - full results dict with one entry per algorithm
def load_results(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


## Prints a separator line of repeated characters.
#
#  @param char   str - character to repeat (default "─")
#  @param width  int - number of characters to print (default 60)
def print_separator(char: str = "─", width: int = 60) -> None:
    print(char * width)


## Computes a party alignment score using the normalized Herfindahl-Hirschman Index.
#
#  Returns a score from 0.0 (perfectly mixed parties) to 1.0 (single party only).
#  Uses HHI normalization so the score is comparable across communities of
#  different sizes and numbers of parties.
#
#  @param breakdown  dict - party name to count mapping for a community
#  @return           float - alignment score between 0.0 and 1.0
def party_alignment_score(breakdown: dict) -> float:
    total = sum(breakdown.values())
    if total == 0:
        return 0.0
    shares = [v / total for v in breakdown.values()]
    n = len(shares)
    hhi = sum(s ** 2 for s in shares)
    if n == 1:
        return 1.0
    # Normalize HHI: maps range [1/n, 1] to [0, 1]
    return (hhi - 1 / n) / (1 - 1 / n)


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

## Prints a full community breakdown report for a single algorithm result.
#
#  For each community, shows size, party composition, alignment score,
#  bipartisan flag, and the top N most active senators by bill count.
#  Communities are sorted by size (largest first).
#
#  @param algo_name    str - display name for the algorithm
#  @param result       dict - result dict for this algorithm from the JSON file
#  @param top_senators int  - number of top senators to show per community (default 5)
def print_algo_report(algo_name: str, result: dict, top_senators: int = 5) -> None:
    print_separator("=")
    print(f"  {algo_name.upper().replace('_', ' ')}")
    print(f"  Modularity: {result['modularity']:.4f}   "
          f"Communities: {result['num_communities']}")
    print_separator("=")

    # Build community_id -> list of senators lookup
    comm_senators: dict = defaultdict(list)
    for s in result["senators"]:
        comm_senators[s["community_id"]].append(s)

    for summary in sorted(
        result["community_summary"], key=lambda x: x["size"], reverse=True
    ):
        cid = summary["community_id"]
        size = summary["size"]
        dominant = summary["dominant_party"]
        breakdown = summary["party_breakdown"]
        alignment = party_alignment_score(breakdown)
        bipartisan = summary["bipartisan"]

        # Build party composition string
        total = sum(breakdown.values())
        bar_parts = []
        for party, count in sorted(
            breakdown.items(), key=lambda x: x[1], reverse=True
        ):
            pct = count / total * 100
            bar_parts.append(f"{party}: {count} ({pct:.0f}%)")
        party_str = "  |  ".join(bar_parts)

        bipartisan_tag = " [bipartisan]" if bipartisan else ""
        print(f"\nCommunity {cid}  ({size} senators){bipartisan_tag}")
        print(f"  {party_str}")
        print(f"  Alignment score: {alignment:.3f}  "
              f"(0=mixed, 1=uniform)  |  Dominant: {dominant}")

        # Show top senators by bill count as proxy for activity level
        senators_sorted = sorted(
            comm_senators[cid], key=lambda x: x["bill_count"], reverse=True
        )
        top = senators_sorted[:top_senators]
        names = [f"{s['name']} ({s['party']}-{s['state']})" for s in top]
        print(f"  Top senators: {', '.join(names)}")

    print()


## Finds and prints senators placed in communities dominated by the opposite party.
#
#  These are bipartisan connectors — senators who co-sponsor more heavily
#  with members of the other party than their own. Useful for identifying
#  cross-aisle relationships in the Senate network.
#
#  @param result  dict - result dict for a single algorithm from the JSON file
def cross_party_analysis(result: dict) -> None:
    # Build community_id -> dominant party mapping
    comm_dominant: dict = {}
    for s in result["community_summary"]:
        comm_dominant[s["community_id"]] = s["dominant_party"]

    outliers: list = []
    for s in result["senators"]:
        cid = s["community_id"]
        dominant = comm_dominant.get(cid, "Unknown")
        if (
            s["party"] != dominant
            and s["party"] not in ("Unknown", "")
            and dominant not in ("Unknown", "")
        ):
            outliers.append({
                "name": s["name"],
                "party": s["party"],
                "state": s["state"],
                "community_id": cid,
                "dominant": dominant,
                "bill_count": s["bill_count"],
            })

    if outliers:
        print_separator()
        print("Cross-party senators (placed in opposite-party community)")
        print_separator()
        for o in sorted(outliers, key=lambda x: x["bill_count"], reverse=True):
            print(
                f"  {o['name']:<30} {o['party']}-{o['state']:<4}  "
                f"-> community {o['community_id']} (dom: {o['dominant']})"
            )
    else:
        print("No cross-party placements detected for this algorithm.")


## Prints a comparison table of all algorithms sorted by modularity score.
#
#  Shows algorithm name, modularity, and number of communities for each
#  result in the JSON file. Highlights the best-performing algorithm.
#
#  @param results  dict - full results dict with one entry per algorithm
def comparison_table(results: dict) -> None:
    print_separator("=")
    print("  ALGORITHM COMPARISON")
    print_separator("=")
    print(f"  {'Algorithm':<24} {'Modularity':>11} {'Communities':>13}")
    print_separator()
    rows = sorted(results.items(), key=lambda x: x[1]["modularity"], reverse=True)
    for algo, res in rows:
        print(
            f"  {algo.replace('_', ' ').title():<24} "
            f"{res['modularity']:>11.4f} "
            f"{res['num_communities']:>13}"
        )
    print_separator("=")
    best_algo, best_res = rows[0]
    print(
        f"  Best modularity: {best_algo.replace('_', ' ').title()} "
        f"({best_res['modularity']:.4f})"
    )
    print_separator("=")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

## Entry point — loads cluster results and prints analysis reports.
#
#  Prints the algorithm comparison table first, then per-algorithm community
#  breakdowns. If --algo is specified, only that algorithm is reported.
#  If --cross-party is specified, also prints the bipartisan connector analysis.
#
#  CLI arguments:
#    --results      str  - path to cluster results JSON (default cluster_results.json)
#    --algo         str  - report for one algorithm only (default all)
#    --top-senators int  - number of top senators to show per community (default 5)
#    --cross-party       - show cross-party senator analysis
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Analyze Senate clustering results"
    )
    ap.add_argument("--results", default="cluster_results.json")
    ap.add_argument("--algo", default=None,
                    help="Run report for one algorithm only (e.g. louvain_res0.5)")
    ap.add_argument("--top-senators", type=int, default=5)
    ap.add_argument("--cross-party", action="store_true",
                    help="Show cross-party senator analysis")
    args = ap.parse_args()

    results = load_results(args.results)

    # Print algorithm comparison table first
    comparison_table(results)

    # Print per-algorithm community breakdowns
    algos = [args.algo] if args.algo else list(results.keys())
    for algo in algos:
        if algo not in results:
            print(
                f"Algorithm '{algo}' not found. "
                f"Available: {list(results.keys())}"
            )
            continue
        print_algo_report(algo, results[algo], top_senators=args.top_senators)

        if args.cross_party:
            cross_party_analysis(results[algo])
            print()


if __name__ == "__main__":
    main()
