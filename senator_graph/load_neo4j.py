"""
load_neo4j.py
-------------
Loads the Senate co-sponsorship graph into Neo4j with community labels.

Creates:
    (:Senator) nodes  — one per senator, with all metadata + community_id
    [:CO_SPONSORS]    — weighted edges between senators

Node properties:
    bioguideId, name, party, state, billCount,
    communityId, communityLabel, degree

Edge properties:
    weight       (normalized Jaccard-like score)
    rawCount     (raw number of shared bills)

Usage:
    python load_neo4j.py --uri neo4j+s://xxxx.databases.neo4j.io \
                         --user neo4j --password <password>

    # Or set env vars:
    NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD
"""

import os
import json
import argparse
from dotenv import load_dotenv
import networkx as nx
from neo4j import GraphDatabase

load_dotenv()

# ─────────────────────────────────────────────
# Community label mapping
# Based on identify_clusters.py findings
# Update these if your community IDs differ
# ─────────────────────────────────────────────

COMMUNITY_LABELS = {
    # louvain_res0.5 on p75-filtered graph
    0:  "Democratic Caucus",
    5:  "Great Plains / Mountain West Republicans",
    1:  "Nevada Delegation",
    2:  "Southern Republican Pair",      # Hyde-Smith + Britt
    3:  "Arizona Delegation",
    4:  "West Virginia Delegation",
    6:  "Alaska Delegation",
    7:  "Colorado Delegation",
    8:  "Executive Branch Departure",    # Rubio + Vance (artifact)
    -1: "Isolated / No Strong Ties",
}

ISOLATED_SENATORS = [
    "Hawley", "Young", "Paul", "Curtis", "Johnson", "McConnell"
]


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def load_cluster_results(path: str, algo_key: str = "louvain_res0.5") -> dict[str, int]:
    """Returns bioguideId -> community_id mapping."""
    with open(path) as f:
        results = json.load(f)

    result = results.get(algo_key)
    if not result:
        available = list(results.keys())
        # fallback: best by modularity
        result = max(results.values(), key=lambda r: r.get("modularity", 0))
        print(f"  Key '{algo_key}' not found. Using best: {result['algorithm']}")
        print(f"  Available keys: {available}")

    mapping = {}
    for senator in result["senators"]:
        mapping[senator["bioguideId"]] = senator["community_id"]
    return mapping


def get_community_label(community_id: int) -> str:
    return COMMUNITY_LABELS.get(community_id, f"Community {community_id}")


# ─────────────────────────────────────────────
# Neo4j loader
# ─────────────────────────────────────────────

class Neo4jLoader:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print(f"Connected to Neo4j at {uri}")

    def close(self):
        self.driver.close()

    def verify_connection(self):
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS ok")
            result.single()
        print("  Connection verified.")

    def clear_existing(self):
        """Drop existing Senator nodes and CO_SPONSORS edges."""
        with self.driver.session() as session:
            session.run("MATCH (s:Senator) DETACH DELETE s")
        print("  Cleared existing :Senator nodes and relationships.")

    def create_constraints(self):
        """Unique constraint on bioguideId."""
        with self.driver.session() as session:
            session.run("""
                CREATE CONSTRAINT senator_bioguide IF NOT EXISTS
                FOR (s:Senator) REQUIRE s.bioguideId IS UNIQUE
            """)
        print("  Constraint created: Senator.bioguideId is unique.")

    def load_senators(self, G: nx.Graph, community_map: dict[str, int],
                      isolated: list[str]):
        """
        Create one :Senator node per graph node, plus isolated senators.
        """
        nodes = []

        # Senators in the filtered graph
        for node_id in G.nodes():
            meta = G.nodes[node_id]
            bio = str(node_id)
            cid = community_map.get(bio, -1)
            nodes.append({
                "bioguideId":     bio,
                "name":           meta.get("name", bio),
                "party":          meta.get("party", "Unknown"),
                "state":          meta.get("state", "Unknown"),
                "billCount":      int(meta.get("bill_count", 0)),
                "degree":         int(G.degree(node_id)),
                "communityId":    cid,
                "communityLabel": get_community_label(cid),
                "isIsolated":     False,
            })

        # Isolated senators (not in filtered graph — add with -1 community)
        # Load from senator_stats.json to get their metadata
        stats_path = "senator_stats.json"
        if os.path.exists(stats_path):
            with open(stats_path) as f:
                all_stats = json.load(f)
            graph_ids = set(str(n) for n in G.nodes())
            for bio, meta in all_stats.items():
                if bio not in graph_ids:
                    nodes.append({
                        "bioguideId":     bio,
                        "name":           meta.get("name", bio),
                        "party":          meta.get("party", "Unknown"),
                        "state":          meta.get("state", "Unknown"),
                        "billCount":      int(meta.get("bill_count", 0)),
                        "degree":         0,
                        "communityId":    -1,
                        "communityLabel": "Isolated / No Strong Ties",
                        "isIsolated":     True,
                    })

        with self.driver.session() as session:
            session.run("""
                UNWIND $nodes AS s
                MERGE (n:Senator {bioguideId: s.bioguideId})
                SET n.name           = s.name,
                    n.party          = s.party,
                    n.state          = s.state,
                    n.billCount      = s.billCount,
                    n.degree         = s.degree,
                    n.communityId    = s.communityId,
                    n.communityLabel = s.communityLabel,
                    n.isIsolated     = s.isIsolated
            """, nodes=nodes)

        print(f"  Loaded {len(nodes)} senator nodes.")
        return len(nodes)

    def load_edges(self, G: nx.Graph, batch_size: int = 500):
        """
        Create [:CO_SPONSORS] relationships between senators.
        Bidirectional — both directions created for easy Cypher queries.
        """
        edges = []
        for u, v, data in G.edges(data=True):
            edge = {
                "aId":      str(u),
                "bId":      str(v),
                "weight":   float(data.get("weight", 0.0)),
                "rawCount": int(data.get("raw_count", 0)),
            }
            edges.append(edge)

        total = 0
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            with self.driver.session() as session:
                session.run("""
                    UNWIND $edges AS e
                    MATCH (a:Senator {bioguideId: e.aId})
                    MATCH (b:Senator {bioguideId: e.bId})
                    MERGE (a)-[r:CO_SPONSORS]-(b)
                    SET r.weight   = e.weight,
                        r.rawCount = e.rawCount
                """, edges=batch)
            total += len(batch)

        print(f"  Loaded {total} CO_SPONSORS edges.")
        return total

    def add_community_labels_as_nodes(self):
        """
        Optional: create :Community nodes and link senators to them.
        Useful for GraphRAG — Aditi can query 'community → senators'.
        """
        with self.driver.session() as session:
            # Create Community nodes
            session.run("""
                MATCH (s:Senator)
                WITH DISTINCT s.communityId AS cid, s.communityLabel AS clabel
                MERGE (c:Community {communityId: cid})
                SET c.label = clabel
            """)
            # Link senators to their community
            session.run("""
                MATCH (s:Senator)
                MATCH (c:Community {communityId: s.communityId})
                MERGE (s)-[:BELONGS_TO]->(c)
            """)
        print("  Created :Community nodes and :BELONGS_TO relationships.")

    def verify_load(self):
        """Print summary stats from Neo4j."""
        with self.driver.session() as session:
            senators = session.run("MATCH (s:Senator) RETURN count(s) AS n").single()["n"]
            edges    = session.run("MATCH ()-[r:CO_SPONSORS]-() RETURN count(r)/2 AS n").single()["n"]
            comms    = session.run("MATCH (c:Community) RETURN count(c) AS n").single()["n"]
            isolated = session.run("MATCH (s:Senator {isIsolated:true}) RETURN count(s) AS n").single()["n"]

        print(f"\n  Neo4j load summary:")
        print(f"    Senator nodes:      {senators}")
        print(f"    CO_SPONSORS edges:  {edges}")
        print(f"    Community nodes:    {comms}")
        print(f"    Isolated senators:  {isolated}")


# ─────────────────────────────────────────────
# Useful Cypher queries (printed at end)
# ─────────────────────────────────────────────

SAMPLE_QUERIES = """
╔══════════════════════════════════════════════════════════════════╗
  SAMPLE CYPHER QUERIES FOR NEO4J BROWSER / GRAPHRAG
╚══════════════════════════════════════════════════════════════════╝

-- All senators in a community
MATCH (s:Senator {communityId: 5})
RETURN s.name, s.party, s.state, s.billCount
ORDER BY s.billCount DESC;

-- Top co-sponsorship pairs by weight
MATCH (a:Senator)-[r:CO_SPONSORS]-(b:Senator)
WHERE a.bioguideId < b.bioguideId
RETURN a.name, b.name, r.weight, r.rawCount
ORDER BY r.weight DESC LIMIT 20;

-- Cross-party edges (bipartisan co-sponsorship)
MATCH (a:Senator)-[r:CO_SPONSORS]-(b:Senator)
WHERE a.party <> b.party AND a.bioguideId < b.bioguideId
RETURN a.name, a.party, b.name, b.party, r.weight
ORDER BY r.weight DESC LIMIT 20;

-- Community summary
MATCH (s:Senator)
RETURN s.communityId, s.communityLabel,
       count(s) AS size,
       collect(DISTINCT s.party) AS parties
ORDER BY size DESC;

-- Most connected senator (highest degree)
MATCH (s:Senator)-[r:CO_SPONSORS]-()
RETURN s.name, s.party, s.state, count(r) AS connections
ORDER BY connections DESC LIMIT 10;

-- Isolated senators
MATCH (s:Senator {isIsolated: true})
RETURN s.name, s.party, s.state, s.billCount
ORDER BY s.billCount DESC;

-- Path between two senators
MATCH p = shortestPath(
  (a:Senator {name: 'Elizabeth A. Warren'})-[:CO_SPONSORS*]-(b:Senator {name: 'Mitch McConnell'})
)
RETURN p;

-- Community → senators (for GraphRAG)
MATCH (c:Community)<-[:BELONGS_TO]-(s:Senator)
RETURN c.label, collect(s.name) AS senators
ORDER BY c.communityId;
"""


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Load Senate co-sponsorship graph into Neo4j")
    ap.add_argument("--uri",      default=os.getenv("NEO4J_URI"),
                    help="Neo4j URI, e.g. neo4j+s://xxxx.databases.neo4j.io")
    ap.add_argument("--user",     default=os.getenv("NEO4J_USER", "neo4j"))
    ap.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"))
    ap.add_argument("--graph",    default="senate_graph.graphml")
    ap.add_argument("--results",  default="cluster_results_v2.json")
    ap.add_argument("--algo",     default="louvain_res0.5")
    ap.add_argument("--clear",    action="store_true",
                    help="Clear existing Senator nodes before loading")
    ap.add_argument("--no-community-nodes", action="store_true",
                    help="Skip creating :Community nodes")
    args = ap.parse_args()

    if not args.uri:
        raise SystemExit(
            "Neo4j URI required. Pass --uri or set NEO4J_URI env var.\n"
            "Get your URI from: https://console.neo4j.io"
        )
    if not args.password:
        raise SystemExit(
            "Neo4j password required. Pass --password or set NEO4J_PASSWORD env var."
        )

    # Load graph
    print(f"Loading graph from {args.graph}...")
    G = nx.read_graphml(args.graph)
    for u, v, d in G.edges(data=True):
        G[u][v]["weight"] = float(d.get("weight", 1.0))
    print(f"  {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    # Load community assignments
    print(f"\nLoading cluster results from {args.results} (key: {args.algo})...")
    community_map = load_cluster_results(args.results, args.algo)
    print(f"  {len(community_map)} senators with community assignments")

    # Connect and load
    loader = Neo4jLoader(args.uri, args.user, args.password)
    try:
        loader.verify_connection()

        if args.clear:
            print("\nClearing existing data...")
            loader.clear_existing()

        print("\nCreating constraints...")
        loader.create_constraints()

        print("\nLoading senator nodes...")
        loader.load_senators(G, community_map, ISOLATED_SENATORS)

        print("\nLoading co-sponsorship edges...")
        loader.load_edges(G)

        if not args.no_community_nodes:
            print("\nCreating Community nodes...")
            loader.add_community_labels_as_nodes()

        loader.verify_load()

    finally:
        loader.close()

    print(SAMPLE_QUERIES)
    print("Done. Open Neo4j Browser and run the queries above to explore.")


if __name__ == "__main__":
    main()
