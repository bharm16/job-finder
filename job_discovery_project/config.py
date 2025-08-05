from dataclasses import dataclass
import os

@dataclass
class Settings:
    """Application configuration loaded from environment variables."""
    db_url: str = os.getenv("JOB_FINDER_DB_URL", "postgresql://localhost/job_finder")
    indeed_api_key: str = os.getenv("INDEED_API_KEY", "")
    linkedin_username: str = os.getenv("LINKEDIN_USERNAME", "")
    linkedin_password: str = os.getenv("LINKEDIN_PASSWORD", "")
    adzuna_app_id: str = os.getenv("ADZUNA_APP_ID", "")
    adzuna_app_key: str = os.getenv("ADZUNA_APP_KEY", "")


settings = Settings()
