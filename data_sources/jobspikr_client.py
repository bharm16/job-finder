from typing import Iterable, Dict, Any

import requests

from .base import JobSource
from config import settings


class JobsPikrClient(JobSource):
    """Client for the JobsPikr commercial job listings API."""

    source_name = "jobspikr"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or settings.jobspikr_api_key

    def fetch_jobs(
        self,
        query: str = "software engineer",
        location: str = "United States",
        results_per_page: int = 20,
    ) -> Iterable[Dict[str, Any]]:
        """Fetch job postings from the JobsPikr API.

        Returns an empty list if API credentials are missing.
        """
        if not self.api_key:
            return []

        url = "https://api.jobspikr.com/v2/data"
        headers = {"x-api-key": self.api_key}
        params = {
            "query": query,
            "country": location,
            "per_page": results_per_page,
        }
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("data", [])
