from typing import List

from .data_sources.adzuna_client import AdzunaClient
from .parsers.normalize import normalize_job
from .db.models import Job
from .db.db_client import get_session, init_db


def run_pipeline() -> List[Job]:
    """Run a single ingestion cycle and return normalized jobs."""
    init_db()
    source = AdzunaClient()
    raw_jobs = source.fetch_jobs()
    jobs = [Job(**normalize_job(raw, source.source_name)) for raw in raw_jobs]
    with get_session() as session:
        session.add_all(jobs)
        session.commit()
    return jobs


if __name__ == "__main__":
    run_pipeline()
