from datetime import datetime, date
from typing import Any, Dict, Optional


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "")).date()
    except ValueError:
        return None


def normalize_job(raw: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Convert a raw job payload into the internal schema."""
    if source == "adzuna":
        return {
            "title": raw.get("title", ""),
            "company": raw.get("company", {}).get("display_name", ""),
            "location": raw.get("location", {}).get("display_name", ""),
            "description": raw.get("description", ""),
            "skills": [],
            "source": source,
            "url": raw.get("redirect_url", ""),
            "posting_date": _parse_date(raw.get("created")),
        }

    if source == "ziprecruiter":
        company = raw.get("hiring_company", {})
        return {
            "title": raw.get("name", ""),
            "company": company.get("name", ""),
            "location": raw.get("location", ""),
            "description": raw.get("snippet", ""),
            "skills": [],
            "source": source,
            "url": raw.get("url", ""),
            "posting_date": _parse_date(raw.get("posted_time")),
        }

    return {
        "title": raw.get("title", ""),
        "company": raw.get("company", ""),
        "location": raw.get("location", ""),
        "description": raw.get("description", ""),
        "skills": raw.get("skills", []),
        "source": source,
        "url": raw.get("url", ""),
        "posting_date": raw.get("posting_date"),
    }
