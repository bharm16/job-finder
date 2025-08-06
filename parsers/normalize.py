from datetime import datetime, date
from typing import Any, Dict, Optional
from parsers.skills_extractor import extract_skills


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "")).date()
    except ValueError:
        return None


def normalize_job(raw: Dict[str, Any], source: str) -> Dict[str, Any]:
    """Convert a raw job payload into the internal schema with skills extraction."""

    # Base normalization based on source
    if source == "adzuna":
        normalized = {
            "title": raw.get("title", ""),
            "company": raw.get("company", {}).get("display_name", ""),
            "location": raw.get("location", {}).get("display_name", ""),
            "description": raw.get("description", ""),
            "source": source,
            "url": raw.get("redirect_url", ""),
            "posting_date": _parse_date(raw.get("created")),
        }
    elif source == "ziprecruiter":
        company = raw.get("hiring_company", {})
        normalized = {
            "title": raw.get("name", ""),
            "company": company.get("name", ""),
            "location": raw.get("location", ""),
            "description": raw.get("snippet", ""),
            "source": source,
            "url": raw.get("url", ""),
            "posting_date": _parse_date(raw.get("posted_time")),
        }
    elif source == "usajobs":
        # Handle USAJobs specific structure
        position_info = raw.get("MatchedObjectDescriptor", {})
        normalized = {
            "title": position_info.get("PositionTitle", ""),
            "company": position_info.get("OrganizationName", "Federal Government"),
            "location": _extract_usajobs_location(position_info),
            "description": position_info.get("UserArea", {}).get("Details", {}).get("JobSummary", ""),
            "source": source,
            "url": position_info.get("PositionURI", ""),
            "posting_date": _parse_date(position_info.get("PublicationStartDate")),
        }
    elif source == "jobspikr":
        normalized = {
            "title": raw.get("title", "") or raw.get("job_title", ""),
            "company": raw.get("company_name", "") or raw.get("company", ""),
            "location": raw.get("location", "") or raw.get("job_location", ""),
            "description": raw.get("description", "") or raw.get("job_description", ""),
            "source": source,
            "url": raw.get("url", "") or raw.get("job_link", ""),
            "posting_date": _parse_date(raw.get("post_date") or raw.get("posted_date")),
        }
    else:
        # Generic normalization for unknown sources
        normalized = {
            "title": raw.get("title", ""),
            "company": raw.get("company", ""),
            "location": raw.get("location", ""),
            "description": raw.get("description", ""),
            "source": source,
            "url": raw.get("url", ""),
            "posting_date": raw.get("posting_date"),
        }

    # Extract skills from title and description
    text_for_extraction = f"{normalized.get('title', '')} {normalized.get('description', '')}"

    # Check if raw data already has skills (some APIs provide them)
    provided_skills = raw.get("skills", [])
    if isinstance(provided_skills, list) and provided_skills:
        # If skills are provided, use them but also extract additional ones
        extracted_skills = extract_skills(text_for_extraction)
        # Combine and deduplicate
        all_skills = list(set(provided_skills + extracted_skills))
    else:
        # Extract skills from text
        all_skills = extract_skills(text_for_extraction)

    normalized["skills"] = all_skills

    return normalized


def _extract_usajobs_location(position_info: Dict[str, Any]) -> str:
    """Extract location from USAJobs position data."""
    locations = position_info.get("PositionLocation", [])
    if locations and isinstance(locations, list):
        first_location = locations[0]
        city = first_location.get("CityName", "")
        state = first_location.get("StateName", "")
        country = first_location.get("CountryCode", "")

        location_parts = [p for p in [city, state, country] if p]
        return ", ".join(location_parts)
    return ""


def batch_normalize_jobs(raw_jobs: list, source: str) -> list:
    """Normalize a batch of jobs from the same source."""
    return [normalize_job(job, source) for job in raw_jobs]


def deduplicate_jobs(jobs: list) -> list:
    """Remove duplicate jobs based on URL or title+company combination."""
    seen_urls = set()
    seen_combinations = set()
    unique_jobs = []

    for job in jobs:
        url = job.get("url", "")
        title_company = (job.get("title", "").lower(), job.get("company", "").lower())

        # Skip if we've seen this exact URL
        if url and url in seen_urls:
            continue

        # Skip if we've seen this title+company combination
        if title_company in seen_combinations:
            continue

        if url:
            seen_urls.add(url)
        seen_combinations.add(title_company)
        unique_jobs.append(job)

    return unique_jobs