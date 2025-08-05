from typing import Iterable, Dict, Any, Union

import requests

from .base import JobSource
from config import settings


class ZipRecruiterClient(JobSource):
    """Client for the ZipRecruiter job search API."""

    source_name = "ziprecruiter"

    def __init__(self, api_key: Union[str, None] = None) -> None:
        self.api_key = api_key or settings.ziprecruiter_api_key

    def fetch_jobs(
        self,
        search: str = "software engineer",
        location: str = "United States",
        jobs_per_page: int = 20,
    ) -> Iterable[Dict[str, Any]]:
        """Fetch job postings from ZipRecruiter.

        Returns an empty list if API credentials are missing.
        """
        if not self.api_key:
            return []

        url = "https://api.ziprecruiter.com/jobs/v1"
        params = {
            "api_key": self.api_key,
            "search": search,
            "location": location,
            "jobs_per_page": jobs_per_page,
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("jobs", [])
