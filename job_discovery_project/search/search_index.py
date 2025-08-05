from typing import List

from sqlalchemy import or_, select

from ..db.db_client import get_session
from ..db.models import Job


def search_jobs(query: str, limit: int = 10) -> List[Job]:
    """Return jobs matching the query in title or description."""
    with get_session() as session:
        stmt = (
            select(Job)
            .where(
                or_(
                    Job.title.ilike(f"%{query}%"),
                    Job.description.ilike(f"%{query}%"),
                )
            )
            .limit(limit)
        )
        return list(session.scalars(stmt))
