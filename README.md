# job-finder

Prototype for an intelligent job discovery tool.

## Architecture

- Modular source connectors for job APIs and compliant scrapers (Adzuna and ZipRecruiter implemented)
- Parsing layer normalizes data and extracts skills
- PostgreSQL backend with full-text and vector search (pgvector)
- Search module provides keyword and semantic queries
- FastAPI and CLI front-ends
- Daily pipeline orchestrates ingestion

## Technology Stack

- Python 3.11
- requests & BeautifulSoup for HTTP and scraping
- SQLAlchemy with PostgreSQL (or any DB via SQLAlchemy URI)
- spaCy & sentence-transformers for NLP and embeddings
- FastAPI & uvicorn for the service layer

## Usage

1. Set environment variables for the API credentials (Adzuna and ZipRecruiter):
   ```bash
   export ADZUNA_APP_ID="your_app_id"
   export ADZUNA_APP_KEY="your_app_key"
   export ZIPRECRUITER_API_KEY="your_api_key"
   ```
2. Run the ingestion pipeline:
   ```bash
   python -m job_discovery_project.jobs_pipeline
   ```
3. Query stored jobs with a keyword search:
   ```python
   from job_discovery_project.search.search_index import search_jobs
   print(search_jobs("python"))
   ```
