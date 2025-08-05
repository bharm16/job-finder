from typing import Dict, Any


def normalize_job(raw: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Convert a raw job payload into the internal schema."""
    return {
        "title": raw.get("title", ""),
        "company": raw.get("company", ""),
        "location": raw.get("location", ""),
        "description": raw.get("description", ""),
        "skills": raw.get("skills", []),
        "source": source,
        "url": raw.get("url", ""),
    }
