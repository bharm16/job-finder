from db.db_client import get_session
from db.models import Job
from search.search_index import search_jobs
from search.recommend import recommend_jobs


def setup_jobs():
    with get_session() as session:
        job1 = Job(title="Python Dev", company="A", description="Python role", skills=["python"])
        job2 = Job(title="Java Dev", company="B", description="Java role", skills=["java"])
        session.add_all([job1, job2])
        session.commit()


def test_search_jobs():
    setup_jobs()
    results = search_jobs("Python")
    assert len(results) == 1
    assert results[0].title == "Python Dev"


def test_recommend_jobs():
    setup_jobs()
    results = recommend_jobs(["java"])
    assert len(results) == 1
    assert results[0].company == "B"
