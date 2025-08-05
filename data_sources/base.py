from abc import ABC, abstractmethod
from typing import Iterable, Dict, Any


class JobSource(ABC):
    """Abstract base class for job data providers."""

    source_name: str

    @abstractmethod
    def fetch_jobs(self) -> Iterable[Dict[str, Any]]:
        """Return an iterable of raw job postings."""
        raise NotImplementedError
