"""
Updated Jobs Pipeline
=====================
This is your updated jobs_pipeline.py file that integrates with the new enhanced database.

INSTRUCTIONS FOR CLAUDE CODE:
1. Replace your current jobs_pipeline.py with this updated version
2. Key changes are marked with # [CHANGED] comments
3. This uses the new DatabaseClient and proper skills handling
"""

from typing import List, Dict, Any
import logging
from datetime import datetime

from data_sources.adzuna_client import AdzunaClient
from data_sources.ziprecruiter_client import ZipRecruiterClient
from data_sources.usajobs_client import USAJobsClient
from data_sources.jobspikr_client import JobsPikrClient
from data_sources.base import JobSource
from parsers.normalize import normalize_job, deduplicate_jobs, \
    batch_normalize_jobs  # [CHANGED] Added batch_normalize_jobs
from db.models import Job, ScrapingLog  # [CHANGED] Added ScrapingLog
from db.db_client import get_db_client  # [CHANGED] Using new client instead of get_session, init_db

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

    # [CHANGED] Initialize database client instead of just init_db
    client = get_db_client()
    client.init_db()

    # Track pipeline start time for logging
    pipeline_start = datetime.utcnow()

    # Configure data sources
    sources: List[JobSource] = [
        AdzunaClient(),
        ZipRecruiterClient(),
        USAJobsClient(),
        JobsPikrClient(),
    ]

    all_normalized_jobs = []
    total_raw_jobs = 0
    source_stats = {}  # [CHANGED] Track stats per source

    for source in sources:
        source_name = source.source_name
        logger.info(f"Fetching jobs from {source_name}...")

        # [CHANGED] Track source-specific stats
        source_start = datetime.utcnow()
        source_fetched = 0
        source_errors = 0

        try:
            # Fetch raw jobs from source
            raw_jobs = source.fetch_jobs(query=query, location=location)

            if not raw_jobs:
                logger.warning(f"No jobs returned from {source_name} (check API credentials)")
                # [CHANGED] Log the failed attempt
                client.log_scraping_run(
                    source=source_name,
                    jobs_fetched=0,
                    jobs_inserted=0,
                    status='no_data',
                    error_message="No jobs returned - check API credentials"
                )
                continue

            # Convert to list if needed
            raw_jobs_list = list(raw_jobs) if not isinstance(raw_jobs, list) else raw_jobs
            source_fetched = len(raw_jobs_list)
            total_raw_jobs += source_fetched
            logger.info(f"Fetched {source_fetched} raw jobs from {source_name}")

            # [CHANGED] Use batch normalization for better performance
            try:
                normalized_batch = batch_normalize_jobs(raw_jobs_list, source_name)
                all_normalized_jobs.extend(normalized_batch)

                # Log skills extraction success
                jobs_with_skills = sum(1 for job in normalized_batch if job.get("skills"))
                logger.info(f"Extracted skills for {jobs_with_skills}/{len(normalized_batch)} jobs from {source_name}")

            except Exception as e:
                logger.error(f"Error in batch normalization for {source_name}: {e}")
                # Fall back to individual normalization
                for raw_job in raw_jobs_list:
                    try:
                        normalized = normalize_job(raw_job, source_name)
                        all_normalized_jobs.append(normalized)
                    except Exception as e:
                        logger.error(f"Error normalizing job from {source_name}: {e}")
                        source_errors += 1
                        continue

            # [CHANGED] Store source statistics
            source_stats[source_name] = {
                'fetched': source_fetched,
                'errors': source_errors,
                'duration': (datetime.utcnow() - source_start).total_seconds()
            }

        except Exception as e:
            logger.error(f"Error fetching from {source_name}: {e}")
            # [CHANGED] Log the failed source
            client.log_scraping_run(
                source=source_name,
                jobs_fetched=0,
                jobs_inserted=0,
                status='failed',
                error_message=str(e)
            )
            continue

    # Deduplicate jobs
    logger.info(f"Deduplicating {len(all_normalized_jobs)} normalized jobs...")
    unique_jobs = deduplicate_jobs(all_normalized_jobs)
    logger.info(f"Retained {len(unique_jobs)} unique jobs after deduplication")

    # [CHANGED] Save to database using new batch upsert method
    inserted = 0
    updated = 0
    job_models = []

    if save_to_db and unique_jobs:
        logger.info(f"Saving {len(unique_jobs)} jobs to database...")
        try:
            # Use the new batch upsert method
            inserted, updated = client.batch_upsert_jobs(unique_jobs)
            logger.info(f"Database operation complete: {inserted} inserted, {updated} updated")

            # [CHANGED] Get the actual job models for return value
            with client.get_session() as session:
                # Fetch the jobs we just inserted/updated
                job_models = session.query(Job).order_by(Job.created_at.desc()).limit(len(unique_jobs)).all()

                # Log comprehensive statistics
                total_in_db = session.query(Job).count()
                jobs_with_skills = session.query(Job).filter(
                    Job.skills != None
                ).count()

                # [CHANGED] Get top skills for logging
                top_skills = client.get_top_skills(limit=10)

                logger.info(f"Database now contains {total_in_db} total jobs")
                logger.info(f"{jobs_with_skills} jobs have extracted skills")
                if top_skills:
                    logger.info(f"Top skills: {', '.join([f'{skill}({count})' for skill, count in top_skills[:5]])}")

        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            # Still try to create models for return value
            for job_data in unique_jobs:
                try:
                    job = Job(**job_data)
                    job_models.append(job)
                except Exception as e:
                    logger.error(f"Error creating Job model: {e}")
                    continue
    else:
        # Create Job models without saving
        for job_data in unique_jobs:
            try:
                job = Job(**job_data)
                job_models.append(job)
            except Exception as e:
                logger.error(f"Error creating Job model: {e}")
                continue

    # [CHANGED] Log the complete pipeline run
    pipeline_duration = (datetime.utcnow() - pipeline_start).total_seconds()
    client.log_scraping_run(
        source='pipeline_complete',
        jobs_fetched=total_raw_jobs,
        jobs_inserted=inserted,
        jobs_updated=updated,
        status='completed'
    )

    # Log summary statistics
    logger.info("=" * 50)
    logger.info("Pipeline Summary:")
    logger.info(f"  Total raw jobs fetched: {total_raw_jobs}")
    logger.info(f"  Jobs after normalization: {len(all_normalized_jobs)}")
    logger.info(f"  Unique jobs after deduplication: {len(unique_jobs)}")
    logger.info(f"  Jobs inserted to DB: {inserted}")
    logger.info(f"  Jobs updated in DB: {updated}")
    logger.info(f"  Pipeline duration: {pipeline_duration:.2f} seconds")

    # [CHANGED] Log per-source statistics
    if source_stats:
        logger.info("  Source Statistics:")
        for source, stats in source_stats.items():
            logger.info(f"    {source}: {stats['fetched']} fetched, "
                        f"{stats['errors']} errors, {stats['duration']:.2f}s")

    # Log skills statistics
    if job_models:
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

    # [CHANGED] Use the new client
    client = get_db_client()

    # Get existing URLs using the client
    with client.get_session() as session:
        existing_urls = set(
            url for (url,) in session.query(Job.url).filter(Job.url != None).all()
        )
        logger.info(f"Found {len(existing_urls)} existing job URLs in database")

    # Run normal pipeline without immediate save
    new_jobs = run_pipeline(save_to_db=False)

    # Filter to only truly new jobs
    new_job_data = []
    for job in new_jobs:
        if job.url and job.url not in existing_urls:
            # Convert Job model back to dict for batch upsert
            job_dict = {
                'title': job.title,
                'company': job.company,
                'location': job.location,
                'description': job.description,
                'url': job.url,
                'source': job.source,
                'posting_date': job.posting_date,
                'skills': [skill.name for skill in job.skills] if job.skills else []
            }
            new_job_data.append(job_dict)

    # [CHANGED] Use batch upsert for new jobs
    if new_job_data:
        logger.info(f"Found {len(new_job_data)} new jobs to add")
        inserted, updated = client.batch_upsert_jobs(new_job_data)
        logger.info(f"Added {inserted} new jobs, updated {updated} existing jobs")
    else:
        logger.info("No new jobs found")

    return new_job_data


def test_pipeline():
    """Test the pipeline with a small batch."""
    logger.info("Running pipeline test with limited results...")

    # [CHANGED] Use the new client for testing
    client = get_db_client()

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
        logger.info(f"  Skills: {', '.join([s.name for s in job.skills]) if job.skills else 'None'}")
        logger.info(f"  URL: {job.url}")

    # [CHANGED] Test database operations
    if jobs:
        logger.info("\nTesting database operations...")
        test_job_data = {
            'title': jobs[0].title,
            'company': jobs[0].company,
            'location': jobs[0].location,
            'description': jobs[0].description,
            'url': f"test_{datetime.now().timestamp()}",  # Unique URL for test
            'source': 'test',
            'skills': [s.name for s in jobs[0].skills] if jobs[0].skills else []
        }

        test_job = client.upsert_job(test_job_data)
        logger.info(f"✓ Successfully created test job with ID: {test_job.id}")

        # Test search
        search_results = client.search_jobs(query="python", limit=5)
        logger.info(f"✓ Search returned {len(search_results)} results")

        # Test analytics
        top_skills = client.get_top_skills(limit=5)
        if top_skills:
            logger.info(f"✓ Top skill: {top_skills[0][0]} appears in {top_skills[0][1]} jobs")

    return jobs


def view_statistics():
    """[CHANGED] New function to view ingestion statistics."""
    client = get_db_client()

    logger.info("\n" + "=" * 50)
    logger.info("Ingestion Statistics (Last 7 Days)")
    logger.info("=" * 50)

    stats = client.get_ingestion_stats(days=7)

    logger.info(f"Total runs: {stats['total_runs']}")
    logger.info(f"Successful: {stats['successful_runs']}")
    logger.info(f"Failed: {stats['failed_runs']}")
    logger.info(f"Jobs fetched: {stats['total_jobs_fetched']}")
    logger.info(f"Jobs inserted: {stats['total_jobs_inserted']}")

    if stats['by_source']:
        logger.info("\nBy Source:")
        for source, source_stats in stats['by_source'].items():
            logger.info(f"  {source}:")
            logger.info(f"    Runs: {source_stats['runs']}")
            logger.info(f"    Fetched: {source_stats['jobs_fetched']}")
            logger.info(f"    Inserted: {source_stats['jobs_inserted']}")

    # Also show top skills
    logger.info("\nTop 10 Skills in Database:")
    top_skills = client.get_top_skills(limit=10)
    for i, (skill, count) in enumerate(top_skills, 1):
        logger.info(f"  {i}. {skill}: {count} jobs")

    logger.info("=" * 50)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Job Discovery Pipeline")
    parser.add_argument("--test", action="store_true", help="Run in test mode")
    parser.add_argument("--incremental", action="store_true", help="Run incremental update")
    parser.add_argument("--stats", action="store_true", help="View statistics")  # [CHANGED] New option
    parser.add_argument("--query", default="software engineer", help="Job search query")
    parser.add_argument("--location", default="United States", help="Job location")
    parser.add_argument("--no-save", action="store_true", help="Don't save to database")

    args = parser.parse_args()

    if args.test:
        test_pipeline()
    elif args.incremental:
        run_incremental_update()
    elif args.stats:  # [CHANGED] New option
        view_statistics()
    else:
        run_pipeline(
            query=args.query,
            location=args.location,
            save_to_db=not args.no_save
        )