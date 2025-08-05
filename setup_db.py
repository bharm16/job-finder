#!/usr/bin/env python3
from sqlalchemy import text
from db.db_client import init_db, get_session, engine
from db.models import Job

def test_connection():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        print("Database connection successful!")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

def setup_database():
    print("Setting up SQLite database...")
    if test_connection():
        init_db()
        print("Tables created!")
        with get_session() as session:
            count = session.query(Job).count()
            print(f"Total jobs in database: {count}")
    else:
        print("Failed to connect to database")

if __name__ == "__main__":
    setup_database()