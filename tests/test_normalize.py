from parsers.normalize import normalize_job


def test_normalize_adzuna():
    raw = {
        "title": "Software Engineer",
        "company": {"display_name": "ABC"},
        "location": {"display_name": "NY"},
        "description": "desc",
        "redirect_url": "url",
        "created": "2024-01-01T00:00:00Z",
    }
    job = normalize_job(raw, "adzuna")
    assert job["title"] == "Software Engineer"
    assert job["company"] == "ABC"
    assert job["location"] == "NY"
    assert job["url"] == "url"
    assert job["posting_date"].isoformat() == "2024-01-01"


def test_normalize_ziprecruiter():
    raw = {
        "name": "Dev",
        "hiring_company": {"name": "XYZ"},
        "location": "SF",
        "snippet": "desc",
        "url": "u",
        "posted_time": "2024-02-02T00:00:00Z",
    }
    job = normalize_job(raw, "ziprecruiter")
    assert job["title"] == "Dev"
    assert job["company"] == "XYZ"
    assert job["location"] == "SF"
    assert job["url"] == "u"
    assert job["posting_date"].isoformat() == "2024-02-02"


def test_normalize_generic():
    raw = {
        "title": "Analyst",
        "company": "ACME",
        "location": "LA",
        "description": "desc",
        "url": "link",
        "posting_date": None,
    }
    job = normalize_job(raw, "other")
    assert job["title"] == "Analyst"
    assert job["company"] == "ACME"
    assert job["location"] == "LA"
    assert job["url"] == "link"
