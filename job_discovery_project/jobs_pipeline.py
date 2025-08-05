from typing import List

from .data_sources.indeed_client import IndeedClient
from .parsers.normalize import normalize_job
from .db.models import Job


def run_pipeline() -> List[Job]:
    """Run a single ingestion cycle and return normalized jobs."""
    source = IndeedClient()
    raw_jobs = source.fetch_jobs()
    jobs = [Job(**normalize_job(raw, source.source_name)) for raw in raw_jobs]
    # TODO: persist jobs to the database
    return jobs


if __name__ == "__main__":
    run_pipeline()
