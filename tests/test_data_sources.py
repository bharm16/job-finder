from types import SimpleNamespace

import pytest

from data_sources.adzuna_client import AdzunaClient
from data_sources.ziprecruiter_client import ZipRecruiterClient
from data_sources.usajobs_client import USAJobsClient
from data_sources.jobspikr_client import JobsPikrClient


class DummyResponse(SimpleNamespace):
    def json(self):
        return self.data

    def raise_for_status(self):
        pass


def test_adzuna_fetch(monkeypatch):
    sample = {"results": [{"title": "A"}]}

    def fake_get(url, params=None, timeout=0):
        return DummyResponse(data=sample)

    monkeypatch.setattr("requests.get", fake_get)
    client = AdzunaClient(app_id="id", app_key="key")
    jobs = client.fetch_jobs()
    assert jobs == sample["results"]


def test_ziprecruiter_fetch(monkeypatch):
    sample = {"jobs": [{"name": "B"}]}

    def fake_get(url, params=None, timeout=0):
        return DummyResponse(data=sample)

    monkeypatch.setattr("requests.get", fake_get)
    client = ZipRecruiterClient(api_key="key")
    jobs = client.fetch_jobs()
    assert jobs == sample["jobs"]


def test_usajobs_fetch(monkeypatch):
    sample = {"SearchResult": {"SearchResultItems": [{"MatchedObjectId": 1}]}}

    def fake_get(url, headers=None, params=None, timeout=0):
        return DummyResponse(data=sample)

    monkeypatch.setattr("requests.get", fake_get)
    client = USAJobsClient(api_key="key", user_agent="agent")
    jobs = client.fetch_jobs()
    assert jobs == sample["SearchResult"]["SearchResultItems"]


def test_jobspikr_fetch(monkeypatch):
    sample = {"data": [{"title": "C"}]}

    def fake_get(url, headers=None, params=None, timeout=0):
        return DummyResponse(data=sample)

    monkeypatch.setattr("requests.get", fake_get)
    client = JobsPikrClient(api_key="key")
    jobs = client.fetch_jobs()
    assert jobs == sample["data"]


def test_missing_credentials_return_empty():
    assert AdzunaClient().fetch_jobs() == []
    assert ZipRecruiterClient().fetch_jobs() == []
    assert USAJobsClient().fetch_jobs() == []
    assert JobsPikrClient().fetch_jobs() == []
