from __future__ import annotations

from datetime import date
from typing import List, Optional

from sqlalchemy import Date, Integer, Text, JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""
    pass


class Job(Base):
    """ORM model representing a normalized job posting."""

    __tablename__ = "jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    company: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[Optional[str]] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(Text)
    url: Mapped[Optional[str]] = mapped_column(Text)
    source: Mapped[Optional[str]] = mapped_column(Text)
    posting_date: Mapped[Optional[date]] = mapped_column(Date)
    skills: Mapped[Optional[List[str]]] = mapped_column(JSON, default=list)
