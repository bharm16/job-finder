# ==================== FILE: db/db_client.py ====================
"""
Enhanced Database Client with Advanced Queries
"""
from contextlib import contextmanager
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Generator, Tuple
import logging

from sqlalchemy import create_engine, text, and_, or_, func, select
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from config import settings
from db.models import Base, Job, Skill, JobSkill, User, UserSkill, UserJobInteraction, ScrapingLog

# Configure logging
logger = logging.getLogger(__name__)


class DatabaseClient:
    """Enhanced database client with connection pooling and advanced queries."""

    def __init__(self, db_url: str = None):
        """Initialize database client with connection pooling."""
        self.db_url = db_url or settings.db_url

        # Configure connection pool
        pool_size = 10 if 'postgresql' in self.db_url else 5

        self.engine = create_engine(
            self.db_url,
            echo=False,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=20,
            pool_pre_ping=True,  # Verify connections before using
            future=True
        )

        # Create session factory with scoping for thread safety
        session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.SessionLocal = scoped_session(session_factory)

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session with automatic cleanup."""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            session.close()

    def init_db(self) -> None:
        """Create database tables if they don't exist."""
        Base.metadata.create_all(bind=self.engine)
        logger.info("Database initialized")

    def drop_all_tables(self) -> None:
        """Drop all tables (use with caution!)."""
        Base.metadata.drop_all(bind=self.engine)
        logger.warning("All database tables dropped")

    # ========== Job Operations ==========

    def upsert_job(self, job_data: Dict[str, Any]) -> Job:
        """Insert or update a job based on URL uniqueness."""
        with self.get_session() as session:
            # Extract skills from job_data
            skills_data = job_data.pop('skills', [])

            # Check if job exists by URL
            existing_job = None
            if job_data.get('url'):
                existing_job = session.query(Job).filter_by(url=job_data['url']).first()

            if existing_job:
                # Update existing job
                for key, value in job_data.items():
                    if hasattr(existing_job, key):
                        setattr(existing_job, key, value)
                job = existing_job
                logger.debug(f"Updated job: {job.title} at {job.company}")
            else:
                # Create new job
                job = Job(**job_data)
                session.add(job)
                logger.debug(f"Created job: {job.title} at {job.company}")

            session.flush()  # Get job ID if new

            # Handle skills
            if skills_data:
                self._update_job_skills(session, job, skills_data)

            # Generate embedding if description exists and not already set
            if job.description and not job.embedding:
                try:
                    from search.vectorizer import embed
                    job.embedding = embed(f"{job.title} {job.description[:1000]}")
                except Exception as e:
                    logger.warning(f"Could not generate embedding: {e}")

            return job

    def batch_upsert_jobs(self, jobs_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Batch insert/update jobs for better performance."""
        inserted = 0
        updated = 0

        with self.get_session() as session:
            for job_data in jobs_data:
                try:
                    # Check if exists
                    url = job_data.get('url')
                    if url and session.query(Job).filter_by(url=url).first():
                        updated += 1
                    else:
                        inserted += 1

                    self.upsert_job(job_data)

                except IntegrityError as e:
                    logger.error(f"Integrity error for job: {e}")
                    session.rollback()
                    continue
                except Exception as e:
                    logger.error(f"Error upserting job: {e}")
                    continue

        logger.info(f"Batch upsert complete: {inserted} inserted, {updated} updated")
        return inserted, updated

    def _update_job_skills(self, session: Session, job: Job, skills_list: List[str]) -> None:
        """Update skills for a job."""
        # Clear existing skills
        session.query(JobSkill).filter_by(job_id=job.id).delete()

        for skill_name in skills_list:
            # Find or create skill
            skill = session.query(Skill).filter_by(name=skill_name).first()
            if not skill:
                skill = Skill(name=skill_name)
                session.add(skill)
                session.flush()

            # Create job-skill relationship
            job_skill = JobSkill(
                job_id=job.id,
                skill_id=skill.id,
                is_required=True
            )
            session.add(job_skill)

            # Update skill job count
            skill.job_count = session.query(JobSkill).filter_by(skill_id=skill.id).count()

    def get_job_by_id(self, job_id: int) -> Optional[Job]:
        """Get a job by ID with skills loaded."""
        with self.get_session() as session:
            return session.query(Job).filter_by(id=job_id).first()

    def get_recent_jobs(self, days: int = 7, limit: int = 100) -> List[Job]:
        """Get jobs posted in the last N days."""
        with self.get_session() as session:
            cutoff_date = date.today() - timedelta(days=days)
            return session.query(Job).filter(
                Job.posting_date >= cutoff_date
            ).order_by(
                Job.posting_date.desc()
            ).limit(limit).all()

    def search_jobs(
            self,
            query: Optional[str] = None,
            location: Optional[str] = None,
            company: Optional[str] = None,
            skills: Optional[List[str]] = None,
            source: Optional[str] = None,
            days_old: Optional[int] = None,
            limit: int = 50,
            offset: int = 0
    ) -> List[Job]:
        """Advanced job search with multiple filters."""
        with self.get_session() as session:
            q = session.query(Job)

            # Text search
            if query:
                search_filter = or_(
                    Job.title.ilike(f"%{query}%"),
                    Job.description.ilike(f"%{query}%"),
                    Job.company.ilike(f"%{query}%")
                )
                q = q.filter(search_filter)

            # Location filter
            if location:
                q = q.filter(Job.location.ilike(f"%{location}%"))

            # Company filter
            if company:
                q = q.filter(Job.company.ilike(f"%{company}%"))

            # Skills filter (jobs that have ALL specified skills)
            if skills:
                for skill_name in skills:
                    skill_subq = session.query(JobSkill.job_id).join(Skill).filter(
                        Skill.name.ilike(f"%{skill_name}%")
                    ).subquery()
                    q = q.filter(Job.id.in_(select(skill_subq)))

            # Source filter
            if source:
                q = q.filter(Job.source == source)

            # Date filter
            if days_old:
                cutoff_date = date.today() - timedelta(days=days_old)
                q = q.filter(Job.posting_date >= cutoff_date)

            # Order by relevance/date and paginate
            jobs = q.order_by(
                Job.posting_date.desc(),
                Job.relevance_score.desc()
            ).offset(offset).limit(limit).all()

            return jobs

    # ========== Skill Operations ==========

    def get_top_skills(self, limit: int = 20) -> List[Tuple[str, int]]:
        """Get most in-demand skills by job count."""
        with self.get_session() as session:
            results = session.query(
                Skill.name,
                Skill.job_count
            ).order_by(
                Skill.job_count.desc()
            ).limit(limit).all()

            return [(name, count) for name, count in results]

    def get_skill_trends(self, skill_name: str, days: int = 30) -> Dict[str, Any]:
        """Get trend data for a specific skill."""
        with self.get_session() as session:
            skill = session.query(Skill).filter_by(name=skill_name).first()
            if not skill:
                return {}

            # Get job count by date
            cutoff_date = date.today() - timedelta(days=days)

            daily_counts = session.query(
                func.date(Job.posting_date).label('date'),
                func.count(Job.id).label('count')
            ).join(
                JobSkill
            ).filter(
                JobSkill.skill_id == skill.id,
                Job.posting_date >= cutoff_date
            ).group_by(
                func.date(Job.posting_date)
            ).all()

            return {
                'skill': skill_name,
                'total_jobs': skill.job_count,
                'daily_counts': [
                    {'date': str(d), 'count': c} for d, c in daily_counts
                ]
            }

    # ========== User Operations ==========

    def create_user(self, email: Optional[str] = None, **kwargs) -> User:
        """Create a new user profile."""
        with self.get_session() as session:
            user = User(email=email, **kwargs)
            session.add(user)
            session.flush()
            return user

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get user by email."""
        with self.get_session() as session:
            return session.query(User).filter_by(email=email).first()

    def update_user_skills(self, user_id: int, skills: List[Dict[str, Any]]) -> None:
        """Update user's skill profile."""
        with self.get_session() as session:
            # Clear existing skills
            session.query(UserSkill).filter_by(user_id=user_id).delete()

            for skill_data in skills:
                skill_name = skill_data.get('name')
                skill = session.query(Skill).filter_by(name=skill_name).first()

                if not skill:
                    skill = Skill(name=skill_name)
                    session.add(skill)
                    session.flush()

                user_skill = UserSkill(
                    user_id=user_id,
                    skill_id=skill.id,
                    proficiency_level=skill_data.get('proficiency_level'),
                    years_experience=skill_data.get('years_experience')
                )
                session.add(user_skill)

    def save_job_for_user(self, user_id: int, job_id: int) -> None:
        """Save a job to user's saved list."""
        with self.get_session() as session:
            interaction = session.query(UserJobInteraction).filter_by(
                user_id=user_id,
                job_id=job_id
            ).first()

            if not interaction:
                interaction = UserJobInteraction(
                    user_id=user_id,
                    job_id=job_id
                )
                session.add(interaction)

            interaction.saved = True
            interaction.saved_at = datetime.utcnow()

    def get_user_saved_jobs(self, user_id: int, limit: int = 50) -> List[Job]:
        """Get user's saved jobs."""
        with self.get_session() as session:
            return session.query(Job).join(
                UserJobInteraction
            ).filter(
                UserJobInteraction.user_id == user_id,
                UserJobInteraction.saved == True
            ).order_by(
                UserJobInteraction.saved_at.desc()
            ).limit(limit).all()

    # ========== Analytics Operations ==========

    def get_ingestion_stats(self, days: int = 7) -> Dict[str, Any]:
        """Get statistics about data ingestion."""
        with self.get_session() as session:
            cutoff_date = datetime.utcnow() - timedelta(days=days)

            logs = session.query(ScrapingLog).filter(
                ScrapingLog.started_at >= cutoff_date
            ).all()

            stats = {
                'total_runs': len(logs),
                'successful_runs': len([l for l in logs if l.status == 'completed']),
                'failed_runs': len([l for l in logs if l.status == 'failed']),
                'total_jobs_fetched': sum(l.jobs_fetched for l in logs),
                'total_jobs_inserted': sum(l.jobs_inserted for l in logs),
                'by_source': {}
            }

            # Group by source
            for log in logs:
                source = log.source
                if source not in stats['by_source']:
                    stats['by_source'][source] = {
                        'runs': 0,
                        'jobs_fetched': 0,
                        'jobs_inserted': 0
                    }

                stats['by_source'][source]['runs'] += 1
                stats['by_source'][source]['jobs_fetched'] += log.jobs_fetched
                stats['by_source'][source]['jobs_inserted'] += log.jobs_inserted

            return stats

    def log_scraping_run(
            self,
            source: str,
            jobs_fetched: int = 0,
            jobs_inserted: int = 0,
            jobs_updated: int = 0,
            status: str = 'completed',
            error_message: Optional[str] = None
    ) -> None:
        """Log a scraping run for monitoring."""
        with self.get_session() as session:
            log = ScrapingLog(
                source=source,
                jobs_fetched=jobs_fetched,
                jobs_inserted=jobs_inserted,
                jobs_updated=jobs_updated,
                status=status,
                error_message=error_message,
                completed_at=datetime.utcnow() if status in ['completed', 'failed'] else None
            )

            if log.completed_at:
                log.duration_seconds = (
                        log.completed_at - log.started_at
                ).total_seconds()

            session.add(log)


# Create a singleton instance
_db_client = None


def get_db_client() -> DatabaseClient:
    """Get or create the database client singleton."""
    global _db_client
    if _db_client is None:
        _db_client = DatabaseClient()
    return _db_client


# Convenience functions for backward compatibility
def init_db() -> None:
    """Initialize the database."""
    client = get_db_client()
    client.init_db()


def get_session():
    """Get a database session (for backward compatibility)."""
    client = get_db_client()
    return client.SessionLocal()


# Export commonly used functions
__all__ = [
    'DatabaseClient',
    'get_db_client',
    'init_db',
    'get_session'
]