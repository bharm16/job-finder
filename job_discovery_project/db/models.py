from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional


@dataclass
class Job:
    """Representation of a normalized job posting."""

    title: str
    company: str
    location: str
    description: str
    url: str
    source: str
    skills: List[str] = field(default_factory=list)
    posting_date: Optional[date] = None
