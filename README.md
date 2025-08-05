# job-finder

Prototype for an intelligent job discovery tool.

## Architecture

- Modular source connectors for job APIs and compliant scrapers
- Parsing layer normalizes data and extracts skills
- PostgreSQL backend with full-text and vector search (pgvector)
- Search module provides keyword and semantic queries
- FastAPI and CLI front-ends
- Daily pipeline orchestrates ingestion

## Technology Stack

- Python 3.11
- requests & BeautifulSoup for HTTP and scraping
- SQLAlchemy with PostgreSQL
- spaCy & sentence-transformers for NLP and embeddings
- FastAPI & uvicorn for the service layer
