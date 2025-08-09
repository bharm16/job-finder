# ==================== FILE: db/migrate.py ====================
"""
Database Migration and Setup Script
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime
from typing import List, Dict, Any, Optional
import json
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from config import settings
from db.models import Base, Job, Skill, JobSkill, User, ScrapingLog
from parsers.skills_extractor import SKILLS_TAXONOMY
from search.vectorizer import embed


class DatabaseMigrator:
    """Handle database migrations and setup."""

    def __init__(self, db_url: str = None):
        """Initialize the migrator with database connection."""
        self.db_url = db_url or settings.db_url
        self.engine = create_engine(self.db_url, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.inspector = inspect(self.engine)

    def setup_extensions(self) -> bool:
        """Setup PostgreSQL extensions (pgvector for semantic search)."""
        try:
            with self.engine.connect() as conn:
                # Check if we're using PostgreSQL
                if 'postgresql' in self.db_url:
                    print("Setting up PostgreSQL extensions...")

                    # Create pgvector extension if not exists
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
                    conn.commit()

                    # Create pg_trgm for fuzzy text search
                    conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
                    conn.commit()

                    print("✓ PostgreSQL extensions configured")
                    return True
                else:
                    print("ℹ Using SQLite - vector search will use fallback implementation")
                    return True

        except SQLAlchemyError as e:
            print(f"✗ Error setting up extensions: {e}")
            return False

    def create_skills_table(self, session: Session) -> Dict[str, Skill]:
        """Create and populate the skills table from taxonomy."""
        print("Creating skills table...")

        skill_map = {}

        for category, skills_list in SKILLS_TAXONOMY.items():
            for skill_name in skills_list:
                # Check if skill already exists
                existing_skill = session.query(Skill).filter_by(name=skill_name).first()

                if not existing_skill:
                    skill = Skill(
                        name=skill_name,
                        category=category,
                        job_count=0
                    )
                    session.add(skill)
                    skill_map[skill_name.lower()] = skill
                else:
                    skill_map[skill_name.lower()] = existing_skill

        session.commit()
        print(f"✓ Created/verified {len(skill_map)} skills")
        return skill_map

    def migrate_job_skills(self, session: Session, skill_map: Dict[str, Skill]) -> int:
        """Migrate skills from JSON field to many-to-many relationship."""
        print("Migrating job skills...")

        jobs = session.query(Job).all()
        migrated_count = 0

        for job in jobs:
            # Check if job already has skills in the new structure
            if job.skills:
                continue

            # Get skills from the old JSON field if it exists
            if hasattr(job, '_skills_json'):
                old_skills = job._skills_json
            else:
                # Try to get from description using skill extractor
                from parsers.skills_extractor import extract_skills
                text = f"{job.title} {job.description or ''}"
                old_skills = extract_skills(text)

            if old_skills:
                for skill_name in old_skills:
                    skill_lower = skill_name.lower()

                    # Find or create skill
                    if skill_lower in skill_map:
                        skill = skill_map[skill_lower]
                    else:
                        # Create new skill if not in taxonomy
                        skill = Skill(name=skill_name, category="other")
                        session.add(skill)
                        session.flush()
                        skill_map[skill_lower] = skill

                    # Create job-skill relationship
                    job_skill = session.query(JobSkill).filter_by(
                        job_id=job.id,
                        skill_id=skill.id
                    ).first()

                    if not job_skill:
                        job_skill = JobSkill(
                            job_id=job.id,
                            skill_id=skill.id,
                            is_required=True
                        )
                        session.add(job_skill)

                        # Update skill job count
                        skill.job_count += 1

                migrated_count += 1

                # Generate embedding for semantic search
                if job.description and not job.embedding:
                    try:
                        job.embedding = embed(f"{job.title} {job.description[:1000]}")
                    except Exception as e:
                        print(f"Warning: Could not generate embedding for job {job.id}: {e}")

        session.commit()
        print(f"✓ Migrated skills for {migrated_count} jobs")
        return migrated_count

    def create_indexes(self) -> bool:
        """Create additional indexes for performance."""
        try:
            with self.engine.connect() as conn:
                if 'postgresql' in self.db_url:
                    print("Creating performance indexes...")

                    # GIN index for full-text search on job title and description
                    conn.execute(text("""
                                      CREATE INDEX IF NOT EXISTS idx_job_title_gin
                                          ON jobs USING gin(to_tsvector('english', title))
                                      """))

                    conn.execute(text("""
                                      CREATE INDEX IF NOT EXISTS idx_job_description_gin
                                          ON jobs USING gin(to_tsvector('english', COALESCE (description, '')))
                                      """))

                    # Vector index for semantic search (if pgvector is available)
                    try:
                        conn.execute(text("""
                                          CREATE INDEX IF NOT EXISTS idx_job_embedding
                                              ON jobs USING ivfflat (embedding vector_cosine_ops)
                                              WITH (lists = 100)
                                          """))
                    except:
                        print("   Note: IVFFlat index requires data in table first")

                    # Trigram indexes for fuzzy matching
                    conn.execute(text("""
                                      CREATE INDEX IF NOT EXISTS idx_skill_name_trgm
                                          ON skills USING gin(name gin_trgm_ops)
                                      """))

                    conn.commit()
                    print("✓ Performance indexes created")

            return True

        except SQLAlchemyError as e:
            print(f"Warning: Some indexes could not be created: {e}")
            return False

    def migrate_database(self) -> bool:
        """Run the complete migration process."""
        print("\n" + "=" * 50)
        print("Starting Database Migration")
        print("=" * 50 + "\n")

        try:
            # Step 1: Setup extensions
            if not self.setup_extensions():
                return False

            # Step 2: Create all tables
            print("Creating database schema...")
            Base.metadata.create_all(bind=self.engine)
            print("✓ Database schema created/updated")

            # Step 3: Migrate data
            with self.SessionLocal() as session:
                # Create skills table
                skill_map = self.create_skills_table(session)

                # Migrate job skills if there are existing jobs
                job_count = session.query(Job).count()
                if job_count > 0:
                    self.migrate_job_skills(session, skill_map)
                else:
                    print("ℹ No existing jobs to migrate")

            # Step 4: Create indexes
            self.create_indexes()

            # Step 5: Log migration
            with self.SessionLocal() as session:
                log_entry = ScrapingLog(
                    source="migration",
                    status="completed",
                    jobs_fetched=0,
                    jobs_inserted=0,
                    jobs_updated=job_count,
                    completed_at=datetime.utcnow()
                )
                session.add(log_entry)
                session.commit()

            print("\n" + "=" * 50)
            print("✓ Migration completed successfully!")
            print("=" * 50 + "\n")

            return True

        except Exception as e:
            print(f"\n✗ Migration failed: {e}")
            return False

    def verify_migration(self) -> Dict[str, Any]:
        """Verify the migration was successful."""
        with self.SessionLocal() as session:
            results = {
                "jobs_count": session.query(Job).count(),
                "skills_count": session.query(Skill).count(),
                "job_skills_count": session.query(JobSkill).count(),
                "users_count": session.query(User).count(),
                "tables": self.inspector.get_table_names(),
                "has_pgvector": 'postgresql' in self.db_url
            }

            # Check for vector embeddings
            jobs_with_embeddings = session.query(Job).filter(
                Job.embedding.isnot(None)
            ).count()
            results["jobs_with_embeddings"] = jobs_with_embeddings

            return results

    def rollback_migration(self) -> bool:
        """Rollback the migration if needed."""
        print("Rolling back migration...")

        try:
            # Drop new tables in reverse order of dependencies
            with self.engine.connect() as conn:
                tables_to_drop = [
                    "scraping_logs",
                    "saved_searches",
                    "user_job_interactions",
                    "user_skills",
                    "job_skills",
                    "users",
                    "skills"
                ]

                for table in tables_to_drop:
                    if table in self.inspector.get_table_names():
                        conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))
                        print(f"✓ Dropped table: {table}")

                conn.commit()

            print("✓ Rollback completed")
            return True

        except Exception as e:
            print(f"✗ Rollback failed: {e}")
            return False


def main():
    """Main migration entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Database migration tool")
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Rollback the migration"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify migration status"
    )
    parser.add_argument(
        "--db-url",
        type=str,
        help="Override database URL from config"
    )

    args = parser.parse_args()

    migrator = DatabaseMigrator(args.db_url)

    if args.rollback:
        success = migrator.rollback_migration()
        sys.exit(0 if success else 1)

    elif args.verify:
        results = migrator.verify_migration()
        print("\nMigration Status:")
        print("-" * 30)
        for key, value in results.items():
            print(f"{key}: {value}")
        sys.exit(0)

    else:
        success = migrator.migrate_database()

        if success:
            # Show verification
            results = migrator.verify_migration()
            print("\nDatabase Status:")
            print("-" * 30)
            for key, value in results.items():
                print(f"{key}: {value}")

        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()