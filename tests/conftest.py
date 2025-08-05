import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db import db_client
from db.models import Base


@pytest.fixture(autouse=True)
def in_memory_db(monkeypatch):
    engine = create_engine("sqlite:///:memory:", future=True)
    TestingSessionLocal = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)

    def get_session():
        return TestingSessionLocal()

    monkeypatch.setattr(db_client, "engine", engine)
    monkeypatch.setattr(db_client, "SessionLocal", TestingSessionLocal)
    monkeypatch.setattr(db_client, "get_session", get_session)
    yield
