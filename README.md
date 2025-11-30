# Airspace Transition Analysis

A real-time pipeline for collecting, processing, and visualizing U.S. airspace traffic data. This system continuously monitors aircraft movements across 20 defined airspace regions, identifies transitions between them, and provides interactive exploration and static analysis.

**Tech Stack:** OpenSky Network API, Redpanda Kafka, DuckDB, Polars, GeoPandas, Streamlit, Cartopy

## Quick Start

### Prerequisites

1. **OpenSky API Credentials:** Create a `.env` file in the project root:
   ```
   OPENSKY_CLIENTID=your_client_id
   OPENSKY_CLIENTSECRET=your_client_secret
   ```

2. **Docker & Docker Compose** (for Redpanda Kafka cluster)

3. **Python** with dependencies from `requirements.txt`

### Setup

1. Start the Kafka cluster:
   ```sh
   docker compose up -d
   ```
   Redpanda Console available at http://localhost:8080

2. Run the pipeline in order (each in a separate terminal):
   ```sh
   python producer.py    # Fetch OpenSky data
   python consumer.py    # Stream to DuckDB
   python transform.py   # Enrich & analyze
   python analysis.py    # Generate visualizations
   streamlit run app.py  # Launch dashboard (optional)
   ```

## Pipeline Architecture

```
OpenSky API → Kafka Topic → DuckDB → Enrichment → Analysis/Dashboard
```

### Stage 1: Data Production (`producer.py`)

Polls the OpenSky API every 90 seconds for live state vectors over the continental U.S. (24.5°N–49°N, 125°W–66.9°W) and publishes them to the `airspace-events` Kafka topic.

- **Authentication:** OAuth 2.0 client credentials with automatic token refresh
- **Rate Limiting:** 90-second polling interval respects API limits (~4,000 calls/day)
- **Partitioning:** Messages keyed by ICAO24 identifier

### Stage 2: Data Consumption (`consumer.py`)

Consumes messages from Kafka and persists them to DuckDB in batches.

- **Storage:** Creates `airspace` table automatically with 17 columns (ICAO24, callsign, position, altitude, velocity, etc.)
- **Performance:** Batch inserts of 2,000 records; graceful shutdown flushes final batch
- **Primary Key:** `(icao24, last_contact)` prevents duplicates

### Stage 3: Data Enrichment (`transform.py`)

- `consumer.py` script **must be stopped** prior to running `transform.py`. This ensures the transform script can connect to the DuckDB database.

Enriches flight data with U.S. airspace geometries via spatial join and calculates traffic metrics.

- **Airspace Data:** Fetches official 20 Center sectors from ESRI/ArcGIS REST API
- **Spatial Join:** Assigns each aircraft to airspace region; fills missing as "Outside National Airspace"
- **Transitions:** Identifies entry/exit events by tracking state changes per aircraft
- **Calculations:** Per-hour rates for entrances, exits, and events; normalized by airspace area

**Outputs:**
- `opensky_enriched.parquet` — Flight records with airspace labels
- `airspace_enriched.parquet` — GeoParquet with geometries and traffic metrics
- `transitions.parquet` — All detected transitions (entries/exits) with timestamps

### Stage 4: Static Analysis (`analysis.py`)

Generates publication-quality visualizations and saves to `./img/`.

**Outputs:**
- `Planes_by_Airspace.png` — Latest aircraft colored by assigned airspace region
- `Cruising_Density.png` — Hexbin heatmap of planes above 10,000 ft altitude
- `Traffic_Density.png` — Choropleth map showing events per hour per area by region

### Stage 5: Interactive Dashboard (`app.py`)

Streamlit web application for real-time exploration of airspace transitions.

**Features:**
- **Airspace Selection:** Sidebar buttons to select any of 20 U.S. Center sectors
- **Entry/Exit Tables:** Real-time tables of aircraft entering/exiting (last 5 minutes)
- **Live Map:** Current aircraft positions with color-coded transition status:
  - Green = entered in last 5 minutes
  - Red = exited in last 5 minutes
  - Gray = neither (transiting)
  - Arrows show heading/true track
- **Geographic Context:** Airspace boundaries, coastlines, and state borders

Run with: `streamlit run app.py` (available at http://localhost:8501)

## File Structure

```
├── producer.py              # OpenSky API polling
├── consumer.py              # Kafka consumer → DuckDB
├── transform.py             # Spatial join & enrichment
├── analysis.py              # Static visualizations
├── app.py                   # Interactive dashboard
├── docker-compose.yml       # 3-node Redpanda cluster
├── requirements.txt         # Python dependencies
├── .env                     # API credentials (not in repo)
├── airspace-events.duckdb   # Database file (created at runtime)
├── *.log                    # Log files per script
├── img/                     # Analysis output plots
├── *.parquet                # Enriched data outputs
└── README.md               # This file
```

## Configuration

### Environment Variables (`.env`)
```
OPENSKY_CLIENTID=your_id
OPENSKY_CLIENTSECRET=your_secret
KAFKA_BROKER=127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092
```

### Docker Services
- **3-node Redpanda Kafka cluster** — High availability (ports 19092, 29092, 39092)
- **Redpanda Console** — Web UI for monitoring (http://localhost:8080)
- **Persistent volumes** — Data persists across restarts
- **Topic:** `airspace-events` with 3 partitions, replication factor 3, infinite retention

## Dependencies

| Package | Purpose |
|---------|---------|
| `polars` | Fast DataFrame operations and aggregations |
| `pandas` | Data manipulation and CSV handling |
| `geopandas` | Geospatial operations and spatial joins |
| `shapely` | Geometry objects and geometric operations |
| `numpy` | Numerical computing and array operations |
| `matplotlib` | Static plotting and visualization |
| `cartopy` | Geographic data visualization and map rendering |
| `streamlit` | Interactive web dashboard framework |
| `requests` | HTTP client for API calls |
| `python-dotenv` | Load environment variables from `.env` |
| `duckdb` | Lightweight SQL database for persistence |
| `quixstreams` | High-level Kafka producer/consumer API |

Install with: `pip install -r requirements.txt`

## Notes

- All pipeline stages must run in order; downstream scripts depend on upstream outputs
- `producer.py` and `consumer.py` run indefinitely (stop with Ctrl+C)
- `consumer.py` script must be stopped prior to running `transform.py`. This ensures the transform script can connect to the DuckDB database.
- Parquet files maintain geospatial attributes for accurate mapping
- All scripts log to both console and `.log` files
