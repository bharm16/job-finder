from db.db_client import get_session, init_db
from db.models import Job


def test_init_and_crud():
    init_db()
    with get_session() as session:
        job = Job(title="T", company="C", location="L")
        session.add(job)
        session.commit()
        assert session.query(Job).count() == 1
