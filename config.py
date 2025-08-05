from dataclasses import dataclass
import os
from dotenv import load_dotenv

load_dotenv()

@dataclass
class Settings:
    db_url: str = os.getenv("JOB_FINDER_DB_URL", "postgresql://postgres:postgres@localhost:5432/job_finder")
    adzuna_app_id: str = os.getenv("ADZUNA_APP_ID", "")
    adzuna_app_key: str = os.getenv("ADZUNA_APP_KEY", "")
    ziprecruiter_api_key: str = os.getenv("ZIPRECRUITER_API_KEY", "")
    usajobs_api_key: str = os.getenv("USAJOBS_API_KEY", "")
    usajobs_user_agent: str = os.getenv("USAJOBS_USER_AGENT", "")
    jobspikr_api_key: str = os.getenv("JOBSPIKR_API_KEY", "")
    indeed_api_key: str = os.getenv("INDEED_API_KEY", "")
    linkedin_username: str = os.getenv("LINKEDIN_USERNAME", "")
    linkedin_password: str = os.getenv("LINKEDIN_PASSWORD", "")

settings = Settings()
