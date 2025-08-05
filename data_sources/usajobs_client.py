from typing import Iterable, Dict, Any, Union

import requests

from .base import JobSource
from config import settings


class USAJobsClient(JobSource):
    """Client for the USAJOBS search API."""

    source_name = "usajobs"

    def __init__(
        self,
        api_key: Union[str, None] = None,
        user_agent: Union[str, None] = None,
    ) -> None:
        self.api_key = api_key or settings.usajobs_api_key
        self.user_agent = user_agent or settings.usajobs_user_agent

    def fetch_jobs(
        self,
        query: str = "software engineer",
        location: str = "United States",
        results_per_page: int = 20,
    ) -> Iterable[Dict[str, Any]]:
        """Fetch job postings from the USAJOBS API.

        Returns an empty list if API credentials are missing.
        """
        if not self.api_key or not self.user_agent:
            return []

        url = "https://data.usajobs.gov/api/search"
        headers = {"Authorization-Key": self.api_key, "User-Agent": self.user_agent}
        params = {
            "Keyword": query,
            "LocationName": location,
            "ResultsPerPage": results_per_page,
        }
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("SearchResult", {}).get("SearchResultItems", [])
