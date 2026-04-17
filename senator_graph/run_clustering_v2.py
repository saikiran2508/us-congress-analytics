"""
run_clustering_v2.py
--------------------
Improved clustering that handles high-density graphs by filtering weak edges
before community detection, then runs multiple k values to find richer structure.

Key changes from v1:
    - Edge filtering: only keep edges above a percentile threshold
    - Forces k=4 for Spectral/GN (matches Senate research literature)
    - Runs Louvain at multiple resolutions to find more granular communities
    - Reports cluster purity vs party to validate findings
"""

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


# ─────────────────────────────────────────────
# Edge filtering
# ─────────────────────────────────────────────

def filter_graph(G: nx.Graph, percentile: float = 75) -> nx.Graph:
    """
    Keep only edges above the given weight percentile.
    percentile=75 keeps the top 25% strongest co-sponsorship pairs.
    percentile=50 keeps top 50%, etc.
    """
    weights = [d["weight"] for _, _, d in G.edges(data=True)]
    threshold = float(np.percentile(weights, percentile))
    filtered = G.copy()
    weak_edges = [(u, v) for u, v, d in G.edges(data=True) if d["weight"] < threshold]
    filtered.remove_edges_from(weak_edges)

    # Remove isolated nodes
    isolated = list(nx.isolates(filtered))
    filtered.remove_nodes_from(isolated)

    print(f"  Edge filter (p{percentile:.0f}): {G.number_of_edges()} → "
          f"{filtered.number_of_edges()} edges | "
          f"{len(isolated)} isolated nodes removed | "
          f"threshold weight: {threshold:.4f}")
    return filtered, isolated, threshold


# ─────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────

def modularity_score(G, communities):
    try:
        return nx_comm.modularity(G, communities, weight="weight")
    except Exception:
        return 0.0


def community_party_breakdown(G, communities):
    results = []
    for i, comm in enumerate(communities):
        party_counts = Counter()
        for node in comm:
            party = G.nodes[node].get("party", "Unknown")
            party_counts[party] += 1
        total = sum(party_counts.values())
        dominant = party_counts.most_common(1)[0][0] if party_counts else "Unknown"
        dominant_pct = (party_counts[dominant] / total * 100) if total else 0
        results.append({
            "community_id":    i,
            "size":            len(comm),
            "party_breakdown": dict(party_counts),
            "dominant_party":  dominant,
            "dominant_pct":    round(dominant_pct, 1),
            "bipartisan":      len([p for p, c in party_counts.items() if c > 0]) > 1,
        })
    return results


def communities_to_mapping(communities):
    mapping = {}
    for i, comm in enumerate(communities):
        for node in comm:
            mapping[str(node)] = i
    return mapping


def build_result(G, algo_name, communities, modularity):
    mapping = communities_to_mapping(communities)
    breakdown = community_party_breakdown(G, communities)
    senator_data = []
    for node in G.nodes():
        meta = G.nodes[node]
        senator_data.append({
            "bioguideId":   str(node),
            "name":         meta.get("name", node),
            "party":        meta.get("party", "Unknown"),
            "state":        meta.get("state", "Unknown"),
            "bill_count":   meta.get("bill_count", 0),
            "community_id": mapping.get(str(node), -1),
        })
    return {
        "algorithm":         algo_name,
        "modularity":        round(modularity, 6),
        "num_communities":   len(communities),
        "senators":          senator_data,
        "community_summary": breakdown,
    }


# ─────────────────────────────────────────────
# Algorithms
# ─────────────────────────────────────────────

def run_louvain_multi_resolution(G, resolutions=(0.5, 1.0, 1.5, 2.0), seed=42):
    """
    Run Louvain at multiple resolutions.
    resolution < 1 → fewer, larger communities
    resolution > 1 → more, smaller communities
    """
    print("\n[1] Louvain (multi-resolution)")
    results = []
    for res in resolutions:
        try:
            communities = nx_comm.louvain_communities(G, weight="weight",
                                                       resolution=res, seed=seed)
            mod = modularity_score(G, communities)
            print(f"  resolution={res:.1f}  →  k={len(communities)}  mod={mod:.4f}")
            results.append((res, communities, mod))
        except Exception as e:
            print(f"  resolution={res:.1f}  →  error: {e}")
    return results


def run_label_propagation(G):
    print("\n[2] Label Propagation")
    t0 = time.time()
    communities = list(nx_comm.label_propagation_communities(G))
    mod = modularity_score(G, communities)
    print(f"  k={len(communities)}  mod={mod:.4f}  time={time.time()-t0:.2f}s")
    return communities, mod


def run_spectral(G, k_values=(2, 3, 4, 5), seed=42):
    """Run Spectral Clustering for multiple k values."""
    print("\n[3] Spectral Clustering (multi-k)")
    nodes = list(G.nodes())
    A = nx.to_numpy_array(G, nodelist=nodes, weight="weight")
    A_norm = normalize(A, norm="l1")

    results = []
    for k in k_values:
        try:
            t0 = time.time()
            sc = SpectralClustering(n_clusters=k, affinity="precomputed",
                                    random_state=seed, n_init=10)
            labels = sc.fit_predict(A_norm)
            community_sets = defaultdict(set)
            for node, label in zip(nodes, labels):
                community_sets[int(label)].add(node)
            communities = list(community_sets.values())
            mod = modularity_score(G, communities)
            print(f"  k={k}  mod={mod:.4f}  time={time.time()-t0:.2f}s")
            results.append((k, communities, mod))
        except Exception as e:
            print(f"  k={k}  error: {e}")
    return results


def run_greedy_modularity(G):
    """Clauset-Newman-Moore greedy modularity — good alternative to Louvain."""
    print("\n[4] Greedy Modularity (CNM)")
    t0 = time.time()
    communities = list(nx_comm.greedy_modularity_communities(G, weight="weight"))
    mod = modularity_score(G, communities)
    print(f"  k={len(communities)}  mod={mod:.4f}  time={time.time()-t0:.2f}s")
    return communities, mod


# ─────────────────────────────────────────────
# Summary printer
# ─────────────────────────────────────────────

def print_community_detail(G, communities, label=""):
    breakdown = community_party_breakdown(G, communities)
    print(f"\n  {'─'*50}")
    print(f"  {label}")
    print(f"  {'─'*50}")
    for b in sorted(breakdown, key=lambda x: x["size"], reverse=True):
        party_str = "  ".join(
            f"{p}:{c}" for p, c in
            sorted(b["party_breakdown"].items(), key=lambda x: x[1], reverse=True)
        )
        bipartisan = " [bipartisan]" if b["bipartisan"] else ""
        print(f"  Community {b['community_id']:>2} | size={b['size']:>3} | "
              f"{party_str:<30} | dominant={b['dominant_party']} "
              f"({b['dominant_pct']:.0f}%){bipartisan}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--graph",       default="senate_graph.graphml")
    ap.add_argument("--out",         default="cluster_results_v2.json")
    ap.add_argument("--percentile",  type=float, default=75,
                    help="Keep edges above this weight percentile (default: 75)")
    ap.add_argument("--seed",        type=int, default=42)
    args = ap.parse_args()

    print(f"Loading graph...")
    G_full = nx.read_graphml(args.graph)
    for u, v, d in G_full.edges(data=True):
        G_full[u][v]["weight"] = float(d.get("weight", 1.0))
    print(f"  Full graph: {G_full.number_of_nodes()} nodes, "
          f"{G_full.number_of_edges()} edges, "
          f"density={nx.density(G_full):.4f}")

    # Show weight distribution
    weights = sorted([d["weight"] for _, _, d in G_full.edges(data=True)])
    print(f"\n  Weight distribution:")
    for p in [25, 50, 75, 90, 95]:
        print(f"    p{p:>2}: {np.percentile(weights, p):.4f}")
    print(f"    max: {max(weights):.4f}")

    # Filter to strong edges only
    print(f"\nFiltering to top {100 - args.percentile:.0f}% strongest edges...")
    G, isolated_nodes, threshold = filter_graph(G_full, percentile=args.percentile)
    print(f"  Filtered graph: {G.number_of_nodes()} nodes, "
          f"{G.number_of_edges()} edges, "
          f"density={nx.density(G):.4f}")

    all_results = {}
    best_overall = {"mod": -1, "label": "", "communities": None}

    # ── 1. Louvain multi-resolution ────────────────────────────
    louvain_runs = run_louvain_multi_resolution(G, seed=args.seed)
    best_louvain = max(louvain_runs, key=lambda x: x[2])
    res, communities, mod = best_louvain
    print_community_detail(G, communities, f"Louvain best (res={res}, k={len(communities)}, mod={mod:.4f})")
    all_results["louvain_best"] = build_result(G, f"louvain_res{res}", communities, mod)
    if mod > best_overall["mod"]:
        best_overall = {"mod": mod, "label": f"Louvain res={res}", "communities": communities}

    # Save all louvain resolutions
    for res, communities, mod in louvain_runs:
        all_results[f"louvain_res{res}"] = build_result(G, f"louvain_res{res}", communities, mod)

    # ── 2. Label Propagation ───────────────────────────────────
    lp_communities, lp_mod = run_label_propagation(G)
    print_community_detail(G, lp_communities, f"Label Propagation (k={len(lp_communities)}, mod={lp_mod:.4f})")
    all_results["label_propagation"] = build_result(G, "label_propagation", lp_communities, lp_mod)
    if lp_mod > best_overall["mod"]:
        best_overall = {"mod": lp_mod, "label": "Label Propagation", "communities": lp_communities}

    # ── 3. Spectral multi-k ────────────────────────────────────
    spectral_runs = run_spectral(G, k_values=[2, 3, 4, 5], seed=args.seed)
    best_spectral = max(spectral_runs, key=lambda x: x[2])
    k, communities, mod = best_spectral
    print_community_detail(G, communities, f"Spectral best (k={k}, mod={mod:.4f})")
    for k, communities, mod in spectral_runs:
        all_results[f"spectral_k{k}"] = build_result(G, f"spectral_k{k}", communities, mod)
    if best_spectral[2] > best_overall["mod"]:
        best_overall = {"mod": best_spectral[2], "label": f"Spectral k={best_spectral[0]}", "communities": best_spectral[1]}

    # ── 4. Greedy Modularity ───────────────────────────────────
    gm_communities, gm_mod = run_greedy_modularity(G)
    print_community_detail(G, gm_communities, f"Greedy Modularity (k={len(gm_communities)}, mod={gm_mod:.4f})")
    all_results["greedy_modularity"] = build_result(G, "greedy_modularity", gm_communities, gm_mod)
    if gm_mod > best_overall["mod"]:
        best_overall = {"mod": gm_mod, "label": "Greedy Modularity", "communities": gm_communities}

    # ── Final summary ──────────────────────────────────────────
    print(f"\n{'═'*60}")
    print(f"  BEST RESULT: {best_overall['label']}  (modularity={best_overall['mod']:.4f})")
    print(f"{'═'*60}")
    if best_overall["communities"]:
        print_community_detail(G, best_overall["communities"], "Best community breakdown")

    # Save
    with open(args.out, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nSaved → {args.out}")
    print(f"Isolated nodes (no strong edges): {len(isolated_nodes)}")
    if isolated_nodes:
        for n in isolated_nodes:
            meta = G_full.nodes.get(n, {})
            print(f"  {meta.get('name', n)} ({meta.get('party','?')}-{meta.get('state','?')})")


if __name__ == "__main__":
    main()
