from typing import Iterable, Dict, Any

import requests

from .base import JobSource
from ..config import settings


class IndeedClient(JobSource):
    """Minimal client for the (deprecated) Indeed API."""

    source_name = "indeed"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or settings.indeed_api_key

    def fetch_jobs(self, query: str = "software engineer", location: str = "United States") -> Iterable[Dict[str, Any]]:
        """Return a list of raw job postings.

        This is currently a stub that does not perform any network requests.
        """
        # TODO: Implement real API call when API access is configured
        return []
