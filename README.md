# US Congress Analytics

A data pipeline and graph analysis platform for US Congressional data. Collects
representative and bill data from public APIs, stores it in AWS DynamoDB, builds
a Senate co-sponsorship graph in Neo4j, runs community detection algorithms to
identify senator clusters, and exposes the data through a REST API.

---

## Project Structure

```
us-congress-analytics/
├── api/                            # REST API (Flask + AWS Lambda)
│   └── main.py
├── ingestion/                      # Data collection scripts
│   ├── bioguide_members.py         # Scrapes Bioguide website for member bios
│   ├── current_reps_ingestion.py   # Fetches current members from Congress.gov
│   ├── populate_repterms.py        # Populates RepTerms table from Reps table
│   └── bills_senate.py             # Fetches Senate bills from Congress.gov
├── senator_graph/                  # Neo4j graph + community detection
│   ├── build_graph.py              # Builds co-sponsorship graph
│   ├── load_neo4j.py               # Loads graph into Neo4j
│   ├── identify_clusters.py        # Runs community detection algorithms
│   ├── run_clustering_v2.py        # Executes clustering pipeline
│   ├── analyze_clusters.py         # Analyzes cluster results
│   └── visualize_interactive_v5.py # Generates interactive HTML visualization
├── tests/                          # Pytest test suite (75 tests)
│   ├── test_api.py                 # REST API route and helper tests
│   ├── test_ingestion.py           # Data ingestion and format validation tests
│   └── test_senator_graph.py       # Graph construction and clustering tests
├── .github/workflows/              # GitHub Actions CI/CD
├── requirements.txt                # Python dependencies
├── pyproject.toml                  # Project metadata
└── README.md
```

---

## 1. Requirements

**Programming Language:** Python 3.12

**Required Libraries:**

| Library | Version | Purpose |
|---|---|---|
| boto3 | >=1.34 | AWS DynamoDB access |
| flask | >=3.0 | REST API framework |
| flask-cors | >=4.0 | Cross-origin resource sharing |
| neo4j | >=5.0 | Neo4j graph database driver |
| networkx | >=3.3 | Graph construction and analysis |
| scikit-learn | >=1.4 | Community detection algorithms |
| numpy | >=1.26 | Numerical computations |
| requests | >=2.31 | HTTP requests to Congress.gov API |
| playwright | >=1.40 | Browser automation for Bioguide scraping |
| python-dotenv | >=1.0 | Environment variable management |
| pytest | >=8.0 | Testing framework |
| moto | >=5.0 | AWS mocking for tests |
| flake8 | >=7.0 | Code style enforcement |

**Installation:**

```bash
# Clone the repository
git clone https://github.com/saikiran2508/us-congress-analytics.git
cd us-congress-analytics

# Create and activate virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# Install all dependencies
python -m pip install -r requirements.txt

# Install Playwright browser
playwright install chromium
```

**Environment Variables:**

Create a `.env` file at the root:

```
CONGRESS_API_KEY=your_congress_api_key
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
```

---

## 2. Data Collection / Preparation

There are four data collection scripts. Run them in this order:

### Step 1 — Scrape Bioguide member bios

Scrapes the [Bioguide website](https://bioguide.congress.gov) using Playwright
to collect biographical data for all historical US representatives. Stores
records in the `Reps` DynamoDB table.

```bash
python ingestion/bioguide_members.py --letters A-Z --table Reps --region us-east-2
```

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `--letters` | A | Letters to scan e.g. `A-Z` or `A,B,C` |
| `--table` | required | DynamoDB table name |
| `--region` | us-east-2 | AWS region |
| `--headless` | false | Run browser without visible window |
| `--stop-after-misses` | 3 | Stop after N consecutive misses |

### Step 2 — Fetch current members from Congress.gov API

Fetches current members for a given congress from the Congress.gov API and
updates missing fields in the `Reps` table. Never overwrites existing data.

```bash
python ingestion/current_reps_ingestion.py --congress 119 --table Reps

# Preview changes without writing to DynamoDB
python ingestion/current_reps_ingestion.py --congress 119 --table Reps --dry-run
```

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `--congress` | 119 | Congress number to fetch |
| `--table` | required | DynamoDB table name |
| `--total` | 545 | Max members to process, 0 = all |
| `--dry-run` | false | Preview without writing to DynamoDB |

### Step 3 — Populate RepTerms table

Reads all records from the `Reps` table and creates one record per congress
term per representative in the `RepTerms` table. This enables fast map queries
that run in under 1 second instead of 7 seconds.

```bash
python ingestion/populate_repterms.py
```

### Step 4 — Fetch Senate bills

Fetches Senate bills from the Congress.gov API into the `bills` DynamoDB table.
Supports incremental updates — only fetches bills newer than the last run.

```bash
# Fetch latest 100 bills
python ingestion/bills_senate.py --congress 119 --table bills --total 100

# Full ingestion of all bills
python ingestion/bills_senate.py --congress 119 --table bills --full

# Incremental from a specific date
python ingestion/bills_senate.py --congress 119 --table bills \
    --from-date 2025-01-01T00:00:00Z
```

**Arguments:**

| Argument | Default | Description |
|---|---|---|
| `--congress` | 119 | Congress number |
| `--table` | bills | DynamoDB table name |
| `--total` | 10 | Max bills to fetch, 0 = all |
| `--full` | false | Force full ingestion |
| `--from-date` | None | Only fetch bills updated after this date |

### Data verification test

Verifies that data records have all required fields and correct format
before running the graph pipeline:

```bash
python -m pytest tests/test_ingestion.py -v
```

---

## 3. Model Training — Senate Co-sponsorship Graph

Builds a weighted graph where each node is a senator and each edge represents
the number of bills they co-sponsored together. Runs community detection
algorithms to identify clusters of senators with shared policy interests.

### Step 1 — Build the co-sponsorship graph

Fetches bill and co-sponsorship data and builds a NetworkX weighted graph:

```bash
python senator_graph/build_graph.py
```

### Step 2 — Load graph into Neo4j

Loads the graph nodes and edges into Neo4j AuraDB:

```bash
python senator_graph/load_neo4j.py
```

### Step 3 — Run community detection

Runs Louvain, Label Propagation, Spectral Clustering, and Greedy Modularity
algorithms and saves all results for comparison:

```bash
python senator_graph/run_clustering_v2.py
```

### Step 4 — Identify and label clusters

```bash
python senator_graph/identify_clusters.py
```

### Model verification test

Verifies graph node/edge attributes and clustering output format:

```bash
python -m pytest tests/test_senator_graph.py -v
```

---

## 4. Data Exploration / Visualization

### Interactive HTML graph

Generates an interactive visualization of the Senate co-sponsorship network.
Nodes are colored by community cluster and sized by number of connections.

```bash
python senator_graph/visualize_interactive_v5.py
```

This generates `senate_graph_v5.html` which opens in any browser. Features:
- Click nodes to see senator details (name, state, party, cluster)
- Click legend items to highlight a single community
- Hover tooltips on nodes and edges
- Zoom and pan the network

### Cluster analysis

Prints a detailed breakdown of each community including top senators,
dominant party, alignment score, and bipartisan connectors:

```bash
python senator_graph/analyze_clusters.py

# Report for a specific algorithm only
python senator_graph/analyze_clusters.py --algo louvain_res0.5

# Include cross-party analysis
python senator_graph/analyze_clusters.py --algo louvain_res0.5 --cross-party
```

---

## 5. Results Postprocessing / Visualization — REST API

The REST API exposes the processed congressional data stored in DynamoDB
for consumption by the frontend map application.

### Run locally

```bash
cd api
python main.py
```

API runs at `http://localhost:5001`

### Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/` | GET | Health check |
| `/reps/map?congress=119&chamber=senate` | GET | Members for US map display |
| `/reps/congresses` | GET | List of available congress numbers |
| `/reps?name=Warren&party=Democrat` | GET | Search with optional filters |
| `/reps/<bioguide_id>` | GET | Single representative by bioguide ID |

### Example requests

```bash
# Get all Senate members for the 119th Congress
curl "http://localhost:5001/reps/map?congress=119&chamber=senate"

# Search by name
curl "http://localhost:5001/reps?name=Warren"

# Get a single representative
curl "http://localhost:5001/reps/W000817"
```

### API verification test

```bash
python -m pytest tests/test_api.py -v
```

---

## Running All Tests

```bash
python -m pytest tests/ -v --durations=0
```

75 tests covering API routes, data format validation, graph construction,
and clustering output format. All tests run without real AWS or Neo4j
connections using moto for DynamoDB mocking.

---

## CI/CD

GitHub Actions runs automatically on every push:
- **Flake8** — enforces code style on pushes to `main`
- **Pytest** — runs full test suite on every branch

---

## Tech Stack

| Component | Technology |
|---|---|
| Language | Python 3.12 |
| REST API | Flask 3.0 |
| Serverless | AWS Lambda + API Gateway |
| Database | AWS DynamoDB |
| Graph Database | Neo4j AuraDB |
| Graph Analysis | NetworkX, scikit-learn |
| Data Collection | Playwright, Requests |
| Testing | Pytest, moto |
| CI/CD | GitHub Actions |