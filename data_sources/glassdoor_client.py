from typing import Iterable, Dict, Any

from .base import JobSource


class GlassdoorClient(JobSource):
    """Placeholder client for Glassdoor job listings."""

    source_name = "glassdoor"

    def fetch_jobs(self, query: str = "software engineer", location: str = "United States") -> Iterable[Dict[str, Any]]:
        """Stub implementation that returns no results."""
        # TODO: Implement scraping respecting Glassdoor terms of service
        return []
