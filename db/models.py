from __future__ import annotations
from datetime import date, datetime
from typing import List, Optional
from sqlalchemy import (
    Date, DateTime, Integer, String, Text, Float, Boolean,
    ForeignKey, UniqueConstraint, Index, JSON, func
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
import uuid


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""
    pass


class Job(Base):
    """ORM model representing a normalized job posting."""
    
    __tablename__ = "jobs"
    
    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Core fields
    title: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    company: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    location: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    url: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    source: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    
    # Date fields
    posting_date: Mapped[Optional[date]] = mapped_column(Date, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        server_default=func.now(),
        nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False
    )
    
    # Additional metadata
    salary_min: Mapped[Optional[int]] = mapped_column(Integer)
    salary_max: Mapped[Optional[int]] = mapped_column(Integer)
    salary_currency: Mapped[Optional[str]] = mapped_column(String(10))
    remote_type: Mapped[Optional[str]] = mapped_column(String(50))  # remote, hybrid, onsite
    experience_level: Mapped[Optional[str]] = mapped_column(String(50))  # junior, mid, senior, lead
    employment_type: Mapped[Optional[str]] = mapped_column(String(50))  # full-time, part-time, contract
    
    # Computed fields
    relevance_score: Mapped[Optional[float]] = mapped_column(Float)
    days_old: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Vector embedding for semantic search (stored as JSON for compatibility)
    embedding: Mapped[Optional[dict]] = mapped_column(JSON)
    
    # Relationships
    skills: Mapped[List["Skill"]] = relationship(
        secondary="job_skills",
        back_populates="jobs",
        lazy="selectin"  # Eager load skills
    )
    
    user_interactions: Mapped[List["UserJobInteraction"]] = relationship(
        back_populates="job",
        cascade="all, delete-orphan"
    )
    
    # Indexes
    __table_args__ = (
        Index('idx_job_posting_date_desc', posting_date.desc()),
        Index('idx_job_source_date', source, posting_date.desc()),
        Index('idx_job_company_title', company, title),
        Index('idx_job_created_at', created_at.desc()),
    )
    
    def __repr__(self):
        return f"<Job(id={self.id}, title='{self.title}', company='{self.company}')>"


class Skill(Base):
    """ORM model for skills with categories."""
    
    __tablename__ = "skills"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True, index=True)
    category: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    aliases: Mapped[Optional[dict]] = mapped_column(JSON)  # Store as JSON for compatibility
    
    # Statistics
    job_count: Mapped[int] = mapped_column(Integer, default=0)
    demand_score: Mapped[Optional[float]] = mapped_column(Float)  # Calculated based on frequency
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    
    # Relationships
    jobs: Mapped[List["Job"]] = relationship(
        secondary="job_skills",
        back_populates="skills"
    )
    
    user_skills: Mapped[List["UserSkill"]] = relationship(
        back_populates="skill",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return f"<Skill(id={self.id}, name='{self.name}', category='{self.category}')>"


class JobSkill(Base):
    """Association table for job-skill many-to-many relationship."""
    
    __tablename__ = "job_skills"
    
    job_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("jobs.id", ondelete="CASCADE"),
        primary_key=True
    )
    skill_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("skills.id", ondelete="CASCADE"),
        primary_key=True
    )
    
    # Additional fields for the relationship
    relevance: Mapped[Optional[float]] = mapped_column(Float)  # How relevant is this skill to the job
    is_required: Mapped[bool] = mapped_column(Boolean, default=False)
    years_required: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Indexes
    __table_args__ = (
        Index('idx_job_skill_composite', job_id, skill_id),
    )


class User(Base):
    """User model for personalization features."""
    
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    uuid: Mapped[str] = mapped_column(
        String(36),
        default=lambda: str(uuid.uuid4()),
        unique=True,
        nullable=False,
        index=True
    )
    
    # Profile information
    email: Mapped[Optional[str]] = mapped_column(String(255), unique=True, index=True)
    name: Mapped[Optional[str]] = mapped_column(String(255))
    location: Mapped[Optional[str]] = mapped_column(String(255))
    
    # Preferences
    target_roles: Mapped[Optional[dict]] = mapped_column(JSON)  # Store as JSON for compatibility
    target_companies: Mapped[Optional[dict]] = mapped_column(JSON)  # Store as JSON for compatibility
    target_locations: Mapped[Optional[dict]] = mapped_column(JSON)  # Store as JSON for compatibility
    min_salary: Mapped[Optional[int]] = mapped_column(Integer)
    remote_preference: Mapped[Optional[str]] = mapped_column(String(50))
    experience_level: Mapped[Optional[str]] = mapped_column(String(50))
    
    # Profile embedding for similarity matching
    profile_embedding: Mapped[Optional[dict]] = mapped_column(JSON)
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    last_active: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    
    # Relationships
    skills: Mapped[List["UserSkill"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan"
    )
    
    job_interactions: Mapped[List["UserJobInteraction"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan"
    )
    
    saved_searches: Mapped[List["SavedSearch"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan"
    )
    
    def __repr__(self):
        return f"<User(id={self.id}, email='{self.email}')>"


class UserSkill(Base):
    """User's skills with proficiency levels."""
    
    __tablename__ = "user_skills"
    
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True
    )
    skill_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("skills.id", ondelete="CASCADE"),
        primary_key=True
    )
    
    proficiency_level: Mapped[Optional[str]] = mapped_column(String(50))  # beginner, intermediate, expert
    years_experience: Mapped[Optional[int]] = mapped_column(Integer)
    last_used: Mapped[Optional[date]] = mapped_column(Date)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="skills")
    skill: Mapped["Skill"] = relationship(back_populates="user_skills")
    
    __table_args__ = (
        UniqueConstraint('user_id', 'skill_id', name='uq_user_skill'),
    )


class UserJobInteraction(Base):
    """Track user interactions with job postings."""
    
    __tablename__ = "user_job_interactions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    job_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("jobs.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Interaction types
    viewed: Mapped[bool] = mapped_column(Boolean, default=False)
    saved: Mapped[bool] = mapped_column(Boolean, default=False)
    applied: Mapped[bool] = mapped_column(Boolean, default=False)
    hidden: Mapped[bool] = mapped_column(Boolean, default=False)
    
    # Timestamps
    viewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    saved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    applied_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # User feedback
    rating: Mapped[Optional[int]] = mapped_column(Integer)  # 1-5 stars
    notes: Mapped[Optional[str]] = mapped_column(Text)
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="job_interactions")
    job: Mapped["Job"] = relationship(back_populates="user_interactions")
    
    __table_args__ = (
        UniqueConstraint('user_id', 'job_id', name='uq_user_job'),
        Index('idx_interaction_user_saved', user_id, saved),
    )


class SavedSearch(Base):
    """User's saved search queries for notifications."""
    
    __tablename__ = "saved_searches"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    user_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )
    
    # Search parameters
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    query: Mapped[Optional[str]] = mapped_column(Text)
    filters: Mapped[Optional[dict]] = mapped_column(JSON)  # Store complex filters as JSON
    
    # Notification settings
    notify_email: Mapped[bool] = mapped_column(Boolean, default=True)
    notify_frequency: Mapped[str] = mapped_column(String(50), default="daily")  # daily, weekly, instant
    
    last_notified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    
    # Relationships
    user: Mapped["User"] = relationship(back_populates="saved_searches")
    
    def __repr__(self):
        return f"<SavedSearch(id={self.id}, name='{self.name}')>"


class ScrapingLog(Base):
    """Log scraping runs for monitoring and debugging."""
    
    __tablename__ = "scraping_logs"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    source: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Statistics
    jobs_fetched: Mapped[int] = mapped_column(Integer, default=0)
    jobs_inserted: Mapped[int] = mapped_column(Integer, default=0)
    jobs_updated: Mapped[int] = mapped_column(Integer, default=0)
    jobs_failed: Mapped[int] = mapped_column(Integer, default=0)
    
    # Status and errors
    status: Mapped[str] = mapped_column(String(50), default="running")  # running, completed, failed
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    
    # Performance metrics
    duration_seconds: Mapped[Optional[float]] = mapped_column(Float)
    
    __table_args__ = (
        Index('idx_scraping_log_source_date', source, started_at.desc()),
    )
