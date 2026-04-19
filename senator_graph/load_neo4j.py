## @package senator_graph.load_neo4j
#  Loads the Senate co-sponsorship graph into Neo4j with community labels.
#
#  Reads the GraphML graph file and cluster results JSON produced by
#  build_graph.py and run_clustering_v2.py, then creates Senator nodes
#  and CO_SPONSORS relationships in a Neo4j AuraDB instance.
#
#  Neo4j schema created:
#    (:Senator)      — one node per senator with metadata + community assignment
#    [:CO_SPONSORS]  — weighted edge between senators who co-sponsored bills
#    (:Community)    — one node per cluster (optional, for GraphRAG queries)
#    [:BELONGS_TO]   — links each Senator to their Community node
#
#  Node properties (Senator):
#    bioguideId, name, party, state, billCount,
#    communityId, communityLabel, degree, isIsolated
#
#  Edge properties (CO_SPONSORS):
#    weight    — normalized Jaccard-like co-sponsorship score
#    rawCount  — raw number of shared bills
#
#  Usage:
#    python load_neo4j.py --graph senate_graph.graphml \
#                         --results cluster_results_v2.json
#    # Or set NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD in .env

import os
import json
import argparse

import networkx as nx
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Community label mapping
#
# Maps numeric community IDs (from Louvain clustering) to descriptive labels.
# Update these labels if you re-run clustering with different parameters.
# ---------------------------------------------------------------------------

## Maps community ID integers to human-readable cluster labels.
COMMUNITY_LABELS = {
    0: "Democratic Caucus",
    1: "Nevada Delegation",
    2: "Southern Republican Pair",
    3: "Arizona Delegation",
    4: "West Virginia Delegation",
    5: "Great Plains / Mountain West Republicans",
    6: "Alaska Delegation",
    7: "Colorado Delegation",
    8: "Executive Branch Departure",
    -1: "Isolated / No Strong Ties",
}

## List of senator last names known to have weak co-sponsorship ties.
#  These are included in Neo4j with communityId=-1 even if not in the graph.
ISOLATED_SENATORS = [
    "Hawley", "Young", "Paul", "Curtis", "Johnson", "McConnell"
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

## Loads the bioguideId to community_id mapping from a cluster results file.
#
#  Falls back to the algorithm with the highest modularity score if the
#  requested algorithm key is not found in the results file.
#
#  @param path      str - path to the cluster results JSON file
#  @param algo_key  str - algorithm key to load (default "louvain_res0.5")
#  @return          dict - mapping of bioguideId str to community_id int
def load_cluster_results(path: str, algo_key: str = "louvain_res0.5") -> dict:
    with open(path) as f:
        results = json.load(f)

    result = results.get(algo_key)
    if not result:
        available = list(results.keys())
        # Fallback: use the algorithm with the highest modularity score
        result = max(results.values(), key=lambda r: r.get("modularity", 0))
        print(f"  Key '{algo_key}' not found. Using best modularity result.")
        print(f"  Available keys: {available}")

    mapping: dict = {}
    for senator in result["senators"]:
        mapping[senator["bioguideId"]] = senator["community_id"]
    return mapping


## Returns the human-readable label for a community ID.
#
#  Falls back to a generic "Community N" label for unknown IDs.
#
#  @param community_id  int - numeric community ID from clustering
#  @return              str - descriptive label for the community
def get_community_label(community_id: int) -> str:
    return COMMUNITY_LABELS.get(community_id, f"Community {community_id}")


# ---------------------------------------------------------------------------
# Neo4j loader class
# ---------------------------------------------------------------------------

## Manages the Neo4j connection and all graph loading operations.
#
#  Wraps the Neo4j Python driver and provides methods for clearing existing
#  data, creating constraints, loading senator nodes, loading edges, and
#  creating optional Community nodes for GraphRAG queries.
class Neo4jLoader:

    ## Initializes the Neo4j driver and opens a connection.
    #
    #  @param uri       str - Neo4j connection URI e.g. "neo4j+s://xxxx.databases.neo4j.io"
    #  @param user      str - Neo4j username (default "neo4j")
    #  @param password  str - Neo4j password
    def __init__(self, uri: str, user: str, password: str) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        print(f"Connected to Neo4j at {uri}")

    ## Closes the Neo4j driver connection.
    def close(self) -> None:
        self.driver.close()

    ## Verifies the Neo4j connection by running a simple query.
    #
    #  @throws Exception if the connection cannot be established
    def verify_connection(self) -> None:
        with self.driver.session() as session:
            result = session.run("RETURN 1 AS ok")
            result.single()
        print("  Connection verified.")

    ## Deletes all existing Senator nodes and CO_SPONSORS relationships.
    #
    #  Used before a fresh load to avoid duplicate or stale data.
    def clear_existing(self) -> None:
        with self.driver.session() as session:
            session.run("MATCH (s:Senator) DETACH DELETE s")
        print("  Cleared existing :Senator nodes and relationships.")

    ## Creates a unique constraint on Senator.bioguideId.
    #
    #  Prevents duplicate senator nodes when using MERGE statements.
    def create_constraints(self) -> None:
        with self.driver.session() as session:
            session.run("""
                CREATE CONSTRAINT senator_bioguide IF NOT EXISTS
                FOR (s:Senator) REQUIRE s.bioguideId IS UNIQUE
            """)
        print("  Constraint created: Senator.bioguideId is unique.")

    ## Creates one Senator node per graph node, plus any isolated senators.
    #
    #  Senators in the filtered graph receive their community assignment.
    #  Senators not in the filtered graph (isolated) are loaded with
    #  communityId=-1 using metadata from senator_stats.json if available.
    #
    #  @param G              nx.Graph - the co-sponsorship graph
    #  @param community_map  dict     - bioguideId to community_id mapping
    #  @param isolated       list     - list of senator last names to mark isolated
    #  @return               int      - total number of senator nodes created
    def load_senators(
        self,
        G: nx.Graph,
        community_map: dict,
        isolated: list
    ) -> int:
        nodes: list = []

        # Add senators present in the filtered graph
        for node_id in G.nodes():
            meta = G.nodes[node_id]
            bio = str(node_id)
            cid = community_map.get(bio, -1)
            nodes.append({
                "bioguideId": bio,
                "name": meta.get("name", bio),
                "party": meta.get("party", "Unknown"),
                "state": meta.get("state", "Unknown"),
                "billCount": int(meta.get("bill_count", 0)),
                "degree": int(G.degree(node_id)),
                "communityId": cid,
                "communityLabel": get_community_label(cid),
                "isIsolated": False,
            })

        # Add senators not in filtered graph using senator_stats.json
        stats_path = "senator_stats.json"
        if os.path.exists(stats_path):
            with open(stats_path) as f:
                all_stats = json.load(f)
            graph_ids = {str(n) for n in G.nodes()}
            for bio, meta in all_stats.items():
                if bio not in graph_ids:
                    nodes.append({
                        "bioguideId": bio,
                        "name": meta.get("name", bio),
                        "party": meta.get("party", "Unknown"),
                        "state": meta.get("state", "Unknown"),
                        "billCount": int(meta.get("bill_count", 0)),
                        "degree": 0,
                        "communityId": -1,
                        "communityLabel": "Isolated / No Strong Ties",
                        "isIsolated": True,
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

    ## Creates CO_SPONSORS relationships between senator nodes.
    #
    #  Processes edges in batches to avoid memory issues with large graphs.
    #  Relationships are bidirectional for easy Cypher traversal queries.
    #
    #  @param G           nx.Graph - the co-sponsorship graph
    #  @param batch_size  int      - number of edges per batch (default 500)
    #  @return            int      - total number of edges created
    def load_edges(self, G: nx.Graph, batch_size: int = 500) -> int:
        edges: list = []
        for u, v, data in G.edges(data=True):
            edges.append({
                "aId": str(u),
                "bId": str(v),
                "weight": float(data.get("weight", 0.0)),
                "rawCount": int(data.get("raw_count", 0)),
            })

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

    ## Creates Community nodes and BELONGS_TO relationships.
    #
    #  Optional step that creates one Community node per cluster and links
    #  each Senator to their community. Useful for GraphRAG — allows queries
    #  like "which senators belong to the Democratic Caucus community?".
    def add_community_labels_as_nodes(self) -> None:
        with self.driver.session() as session:
            session.run("""
                MATCH (s:Senator)
                WITH DISTINCT s.communityId AS cid, s.communityLabel AS clabel
                MERGE (c:Community {communityId: cid})
                SET c.label = clabel
            """)
            session.run("""
                MATCH (s:Senator)
                MATCH (c:Community {communityId: s.communityId})
                MERGE (s)-[:BELONGS_TO]->(c)
            """)
        print("  Created :Community nodes and :BELONGS_TO relationships.")

    ## Prints a summary of the loaded data from Neo4j.
    def verify_load(self) -> None:
        with self.driver.session() as session:
            senators = session.run(
                "MATCH (s:Senator) RETURN count(s) AS n"
            ).single()["n"]
            edges = session.run(
                "MATCH ()-[r:CO_SPONSORS]-() RETURN count(r)/2 AS n"
            ).single()["n"]
            comms = session.run(
                "MATCH (c:Community) RETURN count(c) AS n"
            ).single()["n"]
            isolated = session.run(
                "MATCH (s:Senator {isIsolated:true}) RETURN count(s) AS n"
            ).single()["n"]

        print("\n  Neo4j load summary:")
        print(f"    Senator nodes:      {senators}")
        print(f"    CO_SPONSORS edges:  {edges}")
        print(f"    Community nodes:    {comms}")
        print(f"    Isolated senators:  {isolated}")


# ---------------------------------------------------------------------------
# Sample Cypher queries
# ---------------------------------------------------------------------------

## Sample Cypher queries printed after a successful load for reference.
SAMPLE_QUERIES = """
SAMPLE CYPHER QUERIES FOR NEO4J BROWSER / GRAPHRAG

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

-- Most connected senator
MATCH (s:Senator)-[r:CO_SPONSORS]-()
RETURN s.name, s.party, s.state, count(r) AS connections
ORDER BY connections DESC LIMIT 10;

-- Community to senators (for GraphRAG)
MATCH (c:Community)<-[:BELONGS_TO]-(s:Senator)
RETURN c.label, collect(s.name) AS senators
ORDER BY c.communityId;
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

## Entry point — loads graph and cluster results into Neo4j.
#
#  Reads the GraphML graph file and cluster results JSON, connects to Neo4j,
#  and creates Senator nodes, CO_SPONSORS edges, and optional Community nodes.
#
#  CLI arguments:
#    --uri                  str  - Neo4j URI (default from NEO4J_URI env var)
#    --user                 str  - Neo4j username (default from NEO4J_USERNAME env)
#    --password             str  - Neo4j password (default from NEO4J_PASSWORD env)
#    --graph                str  - GraphML input file (default senate_graph.graphml)
#    --results              str  - cluster results JSON (default cluster_results_v2.json)
#    --algo                 str  - algorithm key to load (default louvain_res0.5)
#    --clear                     - clear existing Senator nodes before loading
#    --no-community-nodes        - skip creating Community nodes
def main() -> None:
    ap = argparse.ArgumentParser(
        description="Load Senate co-sponsorship graph into Neo4j"
    )
    ap.add_argument("--uri", default=os.getenv("NEO4J_URI"),
                    help="Neo4j URI e.g. neo4j+s://xxxx.databases.neo4j.io")
    ap.add_argument("--user", default=os.getenv("NEO4J_USERNAME", "neo4j"))
    ap.add_argument("--password", default=os.getenv("NEO4J_PASSWORD"))
    ap.add_argument("--graph", default="senate_graph.graphml")
    ap.add_argument("--results", default="cluster_results_v2.json")
    ap.add_argument("--algo", default="louvain_res0.5")
    ap.add_argument("--clear", action="store_true",
                    help="Clear existing Senator nodes before loading")
    ap.add_argument("--no-community-nodes", action="store_true",
                    help="Skip creating :Community nodes")
    args = ap.parse_args()

    if not args.uri:
        raise SystemExit(
            "Neo4j URI required. Pass --uri or set NEO4J_URI env var."
        )
    if not args.password:
        raise SystemExit(
            "Neo4j password required. Pass --password or set NEO4J_PASSWORD env var."
        )

    print(f"Loading graph from {args.graph}...")
    G = nx.read_graphml(args.graph)
    for u, v, d in G.edges(data=True):
        G[u][v]["weight"] = float(d.get("weight", 1.0))
    print(f"  {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    print(f"\nLoading cluster results from {args.results} (key: {args.algo})...")
    community_map = load_cluster_results(args.results, args.algo)
    print(f"  {len(community_map)} senators with community assignments")

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
