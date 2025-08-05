from typing import Iterable, Dict, Any

from .base import JobSource


class LinkedInClient(JobSource):
    """Placeholder client for LinkedIn job listings."""

    source_name = "linkedin"

    def fetch_jobs(self, query: str = "software engineer", location: str = "United States") -> Iterable[Dict[str, Any]]:
        """Stub implementation that returns no results."""
        # TODO: Implement scraping respecting LinkedIn terms of service
        return []
