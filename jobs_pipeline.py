from typing import List
import logging
from datetime import datetime

from data_sources.adzuna_client import AdzunaClient
from data_sources.ziprecruiter_client import ZipRecruiterClient
from data_sources.usajobs_client import USAJobsClient
from data_sources.jobspikr_client import JobsPikrClient
from data_sources.base import JobSource
from parsers.normalize import normalize_job, deduplicate_jobs
from db.models import Job
from db.db_client import get_session, init_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_pipeline(
        query: str = "software engineer",
        location: str = "United States",
        save_to_db: bool = True
) -> List[Job]:
    """
    Run a single ingestion cycle with skills extraction and return normalized jobs.

    Args:
        query: Search query for jobs
        location: Location filter for jobs
        save_to_db: Whether to save jobs to database

    Returns:
        List of Job objects with extracted skills
    """
    logger.info(f"Starting pipeline run at {datetime.now()}")
    logger.info(f"Query: '{query}', Location: '{location}'")

    # Initialize database
    init_db()

    # Configure data sources
    sources: List[JobSource] = [
        AdzunaClient(),
        ZipRecruiterClient(),
        USAJobsClient(),
        JobsPikrClient(),
    ]

    all_normalized_jobs = []
    total_raw_jobs = 0

    for source in sources:
        source_name = source.source_name
        logger.info(f"Fetching jobs from {source_name}...")

        try:
            # Fetch raw jobs from source
            raw_jobs = source.fetch_jobs(query=query, location=location)

            if not raw_jobs:
                logger.warning(f"No jobs returned from {source_name} (check API credentials)")
                continue

            raw_count = len(list(raw_jobs)) if hasattr(raw_jobs, '__len__') else 0
            total_raw_jobs += raw_count
            logger.info(f"Fetched {raw_count} raw jobs from {source_name}")

            # Normalize jobs and extract skills
            for raw_job in raw_jobs:
                try:
                    normalized = normalize_job(raw_job, source_name)

                    # Log skills extraction success
                    skills_count = len(normalized.get("skills", []))
                    if skills_count > 0:
                        logger.debug(f"Extracted {skills_count} skills from job: {normalized.get('title', 'Unknown')}")

                    all_normalized_jobs.append(normalized)

                except Exception as e:
                    logger.error(f"Error normalizing job from {source_name}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error fetching from {source_name}: {e}")
            continue

    # Deduplicate jobs
    logger.info(f"Deduplicating {len(all_normalized_jobs)} normalized jobs...")
    unique_jobs = deduplicate_jobs(all_normalized_jobs)
    logger.info(f"Retained {len(unique_jobs)} unique jobs after deduplication")

    # Convert to Job models
    job_models = []
    for job_data in unique_jobs:
        try:
            job = Job(**job_data)
            job_models.append(job)
        except Exception as e:
            logger.error(f"Error creating Job model: {e}")
            continue

    # Save to database if requested
    if save_to_db and job_models:
        logger.info(f"Saving {len(job_models)} jobs to database...")
        try:
            with get_session() as session:
                session.add_all(job_models)
                session.commit()
                logger.info("Successfully saved jobs to database")

                # Log some statistics
                total_in_db = session.query(Job).count()
                jobs_with_skills = session.query(Job).filter(
                    Job.skills != None,
                    Job.skills != []
                ).count()
                logger.info(f"Database now contains {total_in_db} total jobs")
                logger.info(f"{jobs_with_skills} jobs have extracted skills")

        except Exception as e:
            logger.error(f"Error saving to database: {e}")

    # Log summary statistics
    logger.info("=" * 50)
    logger.info("Pipeline Summary:")
    logger.info(f"  Total raw jobs fetched: {total_raw_jobs}")
    logger.info(f"  Jobs after normalization: {len(all_normalized_jobs)}")
    logger.info(f"  Unique jobs after deduplication: {len(unique_jobs)}")
    logger.info(f"  Jobs successfully processed: {len(job_models)}")

    # Log skills statistics
    total_skills = sum(len(job.skills or []) for job in job_models)
    jobs_with_skills = sum(1 for job in job_models if job.skills)
    avg_skills = total_skills / len(job_models) if job_models else 0

    logger.info(f"  Jobs with skills: {jobs_with_skills}/{len(job_models)}")
    logger.info(f"  Average skills per job: {avg_skills:.1f}")
    logger.info("=" * 50)

    return job_models


def run_incremental_update():
    """Run an incremental update, checking for new jobs only."""
    logger.info("Running incremental update...")

    # This could be enhanced to:
    # - Track last run timestamp
    # - Only fetch jobs posted after last run
    # - Update existing jobs if needed

    with get_session() as session:
        existing_urls = set(
            url for (url,) in session.query(Job.url).filter(Job.url != None).all()
        )
        logger.info(f"Found {len(existing_urls)} existing job URLs in database")

    # Run normal pipeline
    new_jobs = run_pipeline(save_to_db=False)

    # Filter out existing jobs
    truly_new = [
        job for job in new_jobs
        if job.url not in existing_urls
    ]

    if truly_new:
        logger.info(f"Found {len(truly_new)} new jobs to add")
        with get_session() as session:
            session.add_all(truly_new)
            session.commit()
    else:
        logger.info("No new jobs found")

    return truly_new


def test_pipeline():
    """Test the pipeline with a small batch."""
    logger.info("Running pipeline test with limited results...")

    # Run with test parameters
    jobs = run_pipeline(
        query="python developer",
        location="New York",
        save_to_db=False
    )

    # Display sample results
    logger.info(f"\nSample of extracted jobs:")
    for job in jobs[:3]:
        logger.info(f"\nJob: {job.title}")
        logger.info(f"  Company: {job.company}")
        logger.info(f"  Location: {job.location}")
        logger.info(f"  Skills: {', '.join(job.skills) if job.skills else 'None'}")
        logger.info(f"  URL: {job.url}")

    return jobs


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Job Discovery Pipeline")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--incremental", action="store_true", help="Run incremental update")
    parser.add_argument("--query", default="software engineer", help="Job search query")
    parser.add_argument("--location", default="United States", help="Job location")
    parser.add_argument("--no-save", action="store_true", help="Don't save to database")

    args = parser.parse_args()

    if args.test:
        test_pipeline()
    elif args.incremental:
        run_incremental_update()
    else:
        run_pipeline(
            query=args.query,
            location=args.location,
            save_to_db=not args.no_save
        )