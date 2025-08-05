from typing import Iterable, Dict, Any

import requests

from .base import JobSource
from ..config import settings


class AdzunaClient(JobSource):
    """Client for the Adzuna job search API."""

    source_name = "adzuna"

    def __init__(self, app_id: str | None = None, app_key: str | None = None) -> None:
        self.app_id = app_id or settings.adzuna_app_id
        self.app_key = app_key or settings.adzuna_app_key

    def fetch_jobs(
        self,
        query: str = "software engineer",
        location: str = "United States",
        results_per_page: int = 20,
    ) -> Iterable[Dict[str, Any]]:
        """Fetch job postings from the Adzuna API.

        Returns an empty list if API credentials are missing.
        """
        if not self.app_id or not self.app_key:
            return []

        url = "https://api.adzuna.com/v1/api/jobs/us/search/1"
        params = {
            "app_id": self.app_id,
            "app_key": self.app_key,
            "what": query,
            "where": location,
            "results_per_page": results_per_page,
            "content-type": "application/json",
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
