from typing import List, Set

from db.db_client import get_session
from db.models import Job


def recommend_jobs(profile_skills: List[str], limit: int = 10) -> List[Job]:
    """Return jobs matching any of the provided skills."""
    wanted: Set[str] = {s.lower() for s in profile_skills}
    with get_session() as session:
        jobs = session.query(Job).all()
    matches = [
        job
        for job in jobs
        if wanted.intersection({s.lower() for s in (job.skills or [])})
    ]
    return matches[:limit]
