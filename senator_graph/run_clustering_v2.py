## @package senator_graph.run_clustering_v2
#  Runs multiple community detection algorithms on the Senate co-sponsorship graph.
#
#  Improves on v1 by filtering weak edges before clustering, which prevents
#  the high-density graph from flattening all senators into one community.
#  Runs four algorithms and saves all results for comparison:
#
#    1. Louvain         — at multiple resolutions (0.5, 1.0, 1.5, 2.0)
#    2. Label Propagation — fast, scalable community detection
#    3. Spectral Clustering — matrix-based, multiple k values (2-5)
#    4. Greedy Modularity (CNM) — good alternative to Louvain
#
#  Key improvement over v1:
#    Edge filtering keeps only the top 25% strongest co-sponsorship pairs
#    (p75 threshold) before running algorithms. This removes noise and
#    reveals clearer community structure.
#
#  Outputs:
#    cluster_results_v2.json — all algorithm results for comparison
#
#  Usage:
#    python run_clustering_v2.py --graph senate_graph.graphml
#    python run_clustering_v2.py --graph senate_graph.graphml --percentile 90

import json
import argparse
import time
import warnings
from collections import defaultdict, Counter
import networkx as nx
import networkx.algorithms.community as nx_comm
import numpy as np
from sklearn.cluster import SpectralClustering
from sklearn.preprocessing import normalize

warnings.filterwarnings("ignore")


# ---------------------------------------------------------------------------
# Edge filtering
# ---------------------------------------------------------------------------

## Filters a graph to keep only edges above a weight percentile threshold.
#
#  Removes weak co-sponsorship edges to reduce noise before clustering.
#  Also removes any nodes that become isolated after edge removal.
#
#  @param G           nx.Graph - the full co-sponsorship graph
#  @param percentile  float    - keep edges above this percentile (default 75)
#  @return            tuple    - (filtered_graph, isolated_nodes, threshold) where:
#                       filtered_graph: nx.Graph with weak edges removed
#                       isolated_nodes: list of node IDs that were removed
#                       threshold:      float weight cutoff value used
def filter_graph(G: nx.Graph, percentile: float = 75) -> tuple:
    weights = [d["weight"] for _, _, d in G.edges(data=True)]
    threshold = float(np.percentile(weights, percentile))
    filtered = G.copy()

    # Remove edges below the threshold
    weak_edges = [
        (u, v) for u, v, d in G.edges(data=True)
        if d["weight"] < threshold
    ]
    filtered.remove_edges_from(weak_edges)

    # Remove nodes that are now isolated after edge removal
    isolated = list(nx.isolates(filtered))
    filtered.remove_nodes_from(isolated)

    print(
        f"  Edge filter (p{percentile:.0f}): {G.number_of_edges()} -> "
        f"{filtered.number_of_edges()} edges | "
        f"{len(isolated)} isolated nodes removed | "
        f"threshold weight: {threshold:.4f}"
    )
    return filtered, isolated, threshold


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

## Computes the weighted modularity score for a set of communities.
#
#  Modularity measures how well a partition separates the graph into
#  communities. Values above 0.3 indicate strong community structure.
#
#  @param G            nx.Graph - the graph
#  @param communities  list     - list of sets of node IDs
#  @return             float    - modularity score, or 0.0 on error
def modularity_score(G: nx.Graph, communities: list) -> float:
    try:
        return nx_comm.modularity(G, communities, weight="weight")
    except Exception:
        return 0.0


## Computes the party composition breakdown for each community.
#
#  For each community, counts how many senators belong to each party,
#  identifies the dominant party, and flags bipartisan communities.
#
#  @param G            nx.Graph - the graph with senator node attributes
#  @param communities  list     - list of sets of node IDs
#  @return             list     - list of dicts with community_id, size,
#                                 party_breakdown, dominant_party, dominant_pct,
#                                 and bipartisan flag
def community_party_breakdown(G: nx.Graph, communities: list) -> list:
    results = []
    for i, comm in enumerate(communities):
        party_counts: Counter = Counter()
        for node in comm:
            party = G.nodes[node].get("party", "Unknown")
            party_counts[party] += 1
        total = sum(party_counts.values())
        dominant = party_counts.most_common(1)[0][0] if party_counts else "Unknown"
        dominant_pct = (party_counts[dominant] / total * 100) if total else 0
        results.append({
            "community_id": i,
            "size": len(comm),
            "party_breakdown": dict(party_counts),
            "dominant_party": dominant,
            "dominant_pct": round(dominant_pct, 1),
            "bipartisan": len([p for p, c in party_counts.items() if c > 0]) > 1,
        })
    return results


## Converts a list of community sets to a bioguideId-to-community mapping.
#
#  @param communities  list - list of sets of node IDs
#  @return             dict - maps str(node_id) to int community index
def communities_to_mapping(communities: list) -> dict:
    mapping: dict = {}
    for i, comm in enumerate(communities):
        for node in comm:
            mapping[str(node)] = i
    return mapping


## Builds a standardized result dict for a single algorithm run.
#
#  @param G            nx.Graph - the filtered graph used for clustering
#  @param algo_name    str      - name of the algorithm
#  @param communities  list     - list of sets of node IDs
#  @param modularity   float    - modularity score for this result
#  @return             dict     - standardized result with algorithm, modularity,
#                                 num_communities, senators, community_summary
def build_result(
    G: nx.Graph,
    algo_name: str,
    communities: list,
    modularity: float
) -> dict:
    mapping = communities_to_mapping(communities)
    breakdown = community_party_breakdown(G, communities)
    senator_data: list = []
    for node in G.nodes():
        meta = G.nodes[node]
        senator_data.append({
            "bioguideId": str(node),
            "name": meta.get("name", node),
            "party": meta.get("party", "Unknown"),
            "state": meta.get("state", "Unknown"),
            "bill_count": meta.get("bill_count", 0),
            "community_id": mapping.get(str(node), -1),
        })
    return {
        "algorithm": algo_name,
        "modularity": round(modularity, 6),
        "num_communities": len(communities),
        "senators": senator_data,
        "community_summary": breakdown,
    }


# ---------------------------------------------------------------------------
# Algorithms
# ---------------------------------------------------------------------------

## Runs Louvain community detection at multiple resolution values.
#
#  Lower resolution values produce fewer, larger communities.
#  Higher resolution values produce more, smaller communities.
#  Resolution=0.5 tends to give the best modularity for Senate data.
#
#  @param G            nx.Graph - the filtered co-sponsorship graph
#  @param resolutions  tuple    - resolution values to try (default 0.5-2.0)
#  @param seed         int      - random seed for reproducibility (default 42)
#  @return             list     - list of (resolution, communities, modularity) tuples
def run_louvain_multi_resolution(
    G: nx.Graph,
    resolutions: tuple = (0.5, 1.0, 1.5, 2.0),
    seed: int = 42
) -> list:
    print("\n[1] Louvain (multi-resolution)")
    results: list = []
    for res in resolutions:
        try:
            communities = nx_comm.louvain_communities(
                G, weight="weight", resolution=res, seed=seed
            )
            mod = modularity_score(G, communities)
            print(f"  resolution={res:.1f}  ->  k={len(communities)}  mod={mod:.4f}")
            results.append((res, communities, mod))
        except Exception as e:
            print(f"  resolution={res:.1f}  ->  error: {e}")
    return results


## Runs Label Propagation community detection.
#
#  Fast and scalable — spreads community labels through the network.
#  Non-deterministic but usually consistent on dense graphs.
#
#  @param G  nx.Graph - the filtered co-sponsorship graph
#  @return   tuple    - (communities, modularity) where communities is a
#                       list of sets of node IDs
def run_label_propagation(G: nx.Graph) -> tuple:
    print("\n[2] Label Propagation")
    t0 = time.time()
    communities = list(nx_comm.label_propagation_communities(G))
    mod = modularity_score(G, communities)
    print(f"  k={len(communities)}  mod={mod:.4f}  time={time.time() - t0:.2f}s")
    return communities, mod


## Runs Spectral Clustering for multiple k values.
#
#  Uses the normalized adjacency matrix as the affinity matrix.
#  Tries multiple k values and returns all results for comparison.
#
#  @param G         nx.Graph - the filtered co-sponsorship graph
#  @param k_values  tuple    - number of clusters to try (default 2-5)
#  @param seed      int      - random seed for reproducibility (default 42)
#  @return          list     - list of (k, communities, modularity) tuples
def run_spectral(
    G: nx.Graph,
    k_values: tuple = (2, 3, 4, 5),
    seed: int = 42
) -> list:
    print("\n[3] Spectral Clustering (multi-k)")
    nodes = list(G.nodes())
    A = nx.to_numpy_array(G, nodelist=nodes, weight="weight")
    A_norm = normalize(A, norm="l1")

    results: list = []
    for k in k_values:
        try:
            t0 = time.time()
            sc = SpectralClustering(
                n_clusters=k, affinity="precomputed",
                random_state=seed, n_init=10
            )
            labels = sc.fit_predict(A_norm)
            community_sets: dict = defaultdict(set)
            for node, label in zip(nodes, labels):
                community_sets[int(label)].add(node)
            communities = list(community_sets.values())
            mod = modularity_score(G, communities)
            print(f"  k={k}  mod={mod:.4f}  time={time.time() - t0:.2f}s")
            results.append((k, communities, mod))
        except Exception as e:
            print(f"  k={k}  error: {e}")
    return results


## Runs Clauset-Newman-Moore greedy modularity community detection.
#
#  A deterministic alternative to Louvain that maximizes modularity
#  by greedily merging communities. Slower than Louvain but reproducible.
#
#  @param G  nx.Graph - the filtered co-sponsorship graph
#  @return   tuple    - (communities, modularity) where communities is a
#                       list of frozensets of node IDs
def run_greedy_modularity(G: nx.Graph) -> tuple:
    print("\n[4] Greedy Modularity (CNM)")
    t0 = time.time()
    communities = list(nx_comm.greedy_modularity_communities(G, weight="weight"))
    mod = modularity_score(G, communities)
    print(f"  k={len(communities)}  mod={mod:.4f}  time={time.time() - t0:.2f}s")
    return communities, mod


# ---------------------------------------------------------------------------
# Summary printer
# ---------------------------------------------------------------------------

## Prints a formatted party composition table for a set of communities.
#
#  @param G            nx.Graph - the graph with senator node attributes
#  @param communities  list     - list of sets of node IDs
#  @param label        str      - header label for the table
def print_community_detail(G: nx.Graph, communities: list, label: str = "") -> None:
    breakdown = community_party_breakdown(G, communities)
    print(f"\n  {'─' * 50}")
    print(f"  {label}")
    print(f"  {'─' * 50}")
    for b in sorted(breakdown, key=lambda x: x["size"], reverse=True):
        party_str = "  ".join(
            f"{p}:{c}" for p, c in
            sorted(b["party_breakdown"].items(), key=lambda x: x[1], reverse=True)
        )
        bipartisan = " [bipartisan]" if b["bipartisan"] else ""
        print(
            f"  Community {b['community_id']:>2} | size={b['size']:>3} | "
            f"{party_str:<30} | dominant={b['dominant_party']} "
            f"({b['dominant_pct']:.0f}%){bipartisan}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

## Entry point — runs all clustering algorithms and saves results to JSON.
#
#  Loads the GraphML graph, filters weak edges, runs four community detection
#  algorithms, prints a summary of each, and saves all results to a JSON file
#  for use by load_neo4j.py and identify_clusters.py.
#
#  CLI arguments:
#    --graph       str   - GraphML input file (default senate_graph.graphml)
#    --out         str   - output JSON file (default cluster_results_v2.json)
#    --percentile  float - edge weight percentile filter (default 75)
#    --seed        int   - random seed for reproducibility (default 42)
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run community detection on the Senate co-sponsorship graph"
    )
    ap.add_argument("--graph", default="senate_graph.graphml")
    ap.add_argument("--out", default="cluster_results_v2.json")
    ap.add_argument("--percentile", type=float, default=75,
                    help="Keep edges above this weight percentile (default: 75)")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    print("Loading graph...")
    G_full = nx.read_graphml(args.graph)
    for u, v, d in G_full.edges(data=True):
        G_full[u][v]["weight"] = float(d.get("weight", 1.0))
    print(
        f"  Full graph: {G_full.number_of_nodes()} nodes, "
        f"{G_full.number_of_edges()} edges, "
        f"density={nx.density(G_full):.4f}"
    )

    # Print weight distribution to help choose percentile threshold
    weights = sorted([d["weight"] for _, _, d in G_full.edges(data=True)])
    print("\n  Weight distribution:")
    for p in [25, 50, 75, 90, 95]:
        print(f"    p{p:>2}: {np.percentile(weights, p):.4f}")
    print(f"    max: {max(weights):.4f}")

    print(f"\nFiltering to top {100 - args.percentile:.0f}% strongest edges...")
    G, isolated_nodes, threshold = filter_graph(G_full, percentile=args.percentile)
    print(
        f"  Filtered graph: {G.number_of_nodes()} nodes, "
        f"{G.number_of_edges()} edges, "
        f"density={nx.density(G):.4f}"
    )

    all_results: dict = {}
    best_overall: dict = {"mod": -1, "label": "", "communities": None}

    # Run Louvain at multiple resolutions
    louvain_runs = run_louvain_multi_resolution(G, seed=args.seed)
    best_louvain = max(louvain_runs, key=lambda x: x[2])
    res, communities, mod = best_louvain
    print_community_detail(
        G, communities,
        f"Louvain best (res={res}, k={len(communities)}, mod={mod:.4f})"
    )
    all_results["louvain_best"] = build_result(G, f"louvain_res{res}", communities, mod)
    if mod > best_overall["mod"]:
        best_overall = {
            "mod": mod, "label": f"Louvain res={res}", "communities": communities
        }
    for res, communities, mod in louvain_runs:
        all_results[f"louvain_res{res}"] = build_result(
            G, f"louvain_res{res}", communities, mod
        )

    # Run Label Propagation
    lp_communities, lp_mod = run_label_propagation(G)
    print_community_detail(
        G, lp_communities,
        f"Label Propagation (k={len(lp_communities)}, mod={lp_mod:.4f})"
    )
    all_results["label_propagation"] = build_result(
        G, "label_propagation", lp_communities, lp_mod
    )
    if lp_mod > best_overall["mod"]:
        best_overall = {
            "mod": lp_mod, "label": "Label Propagation", "communities": lp_communities
        }

    # Run Spectral Clustering
    spectral_runs = run_spectral(G, k_values=[2, 3, 4, 5], seed=args.seed)
    best_spectral = max(spectral_runs, key=lambda x: x[2])
    k, communities, mod = best_spectral
    print_community_detail(G, communities, f"Spectral best (k={k}, mod={mod:.4f})")
    for k, communities, mod in spectral_runs:
        all_results[f"spectral_k{k}"] = build_result(
            G, f"spectral_k{k}", communities, mod
        )
    if best_spectral[2] > best_overall["mod"]:
        best_overall = {
            "mod": best_spectral[2],
            "label": f"Spectral k={best_spectral[0]}",
            "communities": best_spectral[1]
        }

    # Run Greedy Modularity
    gm_communities, gm_mod = run_greedy_modularity(G)
    print_community_detail(
        G, gm_communities,
        f"Greedy Modularity (k={len(gm_communities)}, mod={gm_mod:.4f})"
    )
    all_results["greedy_modularity"] = build_result(
        G, "greedy_modularity", gm_communities, gm_mod
    )
    if gm_mod > best_overall["mod"]:
        best_overall = {
            "mod": gm_mod, "label": "Greedy Modularity", "communities": gm_communities
        }

    # Print final summary
    print(f"\n{'=' * 60}")
    print(
        f"  BEST RESULT: {best_overall['label']}  "
        f"(modularity={best_overall['mod']:.4f})"
    )
    print(f"{'=' * 60}")
    if best_overall["communities"]:
        print_community_detail(G, best_overall["communities"], "Best community breakdown")

    with open(args.out, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSaved -> {args.out}")
    print(f"Isolated nodes (no strong edges): {len(isolated_nodes)}")
    if isolated_nodes:
        for n in isolated_nodes:
            meta = G_full.nodes.get(n, {})
            print(
                f"  {meta.get('name', n)} "
                f"({meta.get('party', '?')}-{meta.get('state', '?')})"
            )


if __name__ == "__main__":
    main()
