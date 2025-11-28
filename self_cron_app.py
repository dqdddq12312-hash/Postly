"""FastAPI application with a self-managed cron loop for scheduled social posts."""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

LOGGER = logging.getLogger("postly.self_cron")
logging.basicConfig(level=logging.INFO)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5432/postly",
)

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class ScheduledJob(Base):
    """Table storing scheduled publish jobs."""

    __tablename__ = "scheduled_posts"

    id = Column(Integer, primary_key=True)
    platform = Column(String(50), nullable=False)
    payload = Column(Text, nullable=False)
    scheduled_time = Column(DateTime(timezone=True), nullable=False, index=True)

    posted = Column(Boolean, default=False, index=True)
    posted_at = Column(DateTime(timezone=True), nullable=True)

    locked = Column(Boolean, default=False, index=True)
    locked_at = Column(DateTime(timezone=True), nullable=True)

    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    failed = Column(Boolean, default=False, index=True)
    failure_reason = Column(Text, nullable=True)

    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    cron_task = start_self_cron()
    app.state.self_cron_task = cron_task
    LOGGER.info("Self-Cron loop started")

    try:
        yield
    finally:
        cron_task.cancel()
        with suppress(asyncio.CancelledError):
            await cron_task
        await engine.dispose()
        LOGGER.info("Self-Cron loop stopped")


app = FastAPI(title="Postly Self-Cron", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class JobCreate(BaseModel):
    platform: str = Field(..., max_length=50)
    payload: str
    scheduled_time: datetime
    max_retries: int = Field(3, ge=1, le=10)


class JobResponse(BaseModel):
    id: int
    platform: str
    scheduled_time: datetime
    posted: bool
    failed: bool
    retry_count: int

    @classmethod
    def from_model(cls, job: ScheduledJob) -> "JobResponse":
        return cls(
            id=job.id,
            platform=job.platform,
            scheduled_time=job.scheduled_time,
            posted=job.posted,
            failed=job.failed,
            retry_count=job.retry_count,
        )


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with SessionLocal() as session:
        yield session


@app.post("/jobs", response_model=JobResponse, status_code=status.HTTP_201_CREATED)
async def create_job(payload: JobCreate, session: AsyncSession = Depends(get_session)):
    job = ScheduledJob(
        platform=payload.platform,
        payload=payload.payload,
        scheduled_time=payload.scheduled_time,
        max_retries=payload.max_retries,
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)
    return JobResponse.from_model(job)


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job(job_id: int, session: AsyncSession = Depends(get_session)):
    result = await session.get(ScheduledJob, job_id)
    if not result:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return JobResponse.from_model(result)


async def publish_to_social(job: ScheduledJob) -> None:
    """Placeholder job processor. Replace with actual social publish logic."""
    LOGGER.info("Publishing job %s to %s", job.id, job.platform)
    await asyncio.sleep(0.1)


async def fetch_and_lock_jobs(session: AsyncSession) -> list[ScheduledJob]:
    now = datetime.now(timezone.utc)
    stmt = (
        select(ScheduledJob.id)
        .where(
            ScheduledJob.scheduled_time <= now,
            ScheduledJob.posted.is_(False),
            ScheduledJob.failed.is_(False),
            ScheduledJob.locked.is_(False),
        )
        .order_by(ScheduledJob.scheduled_time)
        .limit(20)
    )
    job_ids = [row[0] for row in (await session.execute(stmt)).all()]
    locked_job_ids: list[int] = []

    for job_id in job_ids:
        stmt_lock = (
            update(ScheduledJob)
            .where(
                ScheduledJob.id == job_id,
                ScheduledJob.locked.is_(False),
                ScheduledJob.posted.is_(False),
                ScheduledJob.failed.is_(False),
            )
            .values(locked=True, locked_at=now)
            .returning(ScheduledJob)
        )
        result = await session.execute(stmt_lock)
        job = result.scalar_one_or_none()
        if job:
            locked_job_ids.append(job.id)

    if not locked_job_ids:
        await session.commit()
        return []

    await session.commit()
    jobs_stmt = select(ScheduledJob).where(ScheduledJob.id.in_(locked_job_ids))
    rows = await session.execute(jobs_stmt)
    return rows.scalars().all()


async def handle_job(session: AsyncSession, job: ScheduledJob) -> None:
    try:
        await publish_to_social(job)
    except Exception as exc:  # pragma: no cover - logging
        LOGGER.exception("Job %s failed: %s", job.id, exc)
        new_retry = job.retry_count + 1
        stmt_fail = (
            update(ScheduledJob)
            .where(ScheduledJob.id == job.id)
            .values(
                locked=False,
                locked_at=None,
                retry_count=new_retry,
                failure_reason=str(exc),
                failed=(new_retry >= job.max_retries),
            )
        )
        await session.execute(stmt_fail)
    else:
        stmt_success = (
            update(ScheduledJob)
            .where(ScheduledJob.id == job.id)
            .values(
                posted=True,
                posted_at=datetime.now(timezone.utc),
                locked=False,
                locked_at=None,
            )
        )
        await session.execute(stmt_success)
        LOGGER.info("Job %s posted successfully", job.id)
    finally:
        await session.commit()


async def cron_tick() -> None:
    async with SessionLocal() as session:
        jobs = await fetch_and_lock_jobs(session)
        if not jobs:
            LOGGER.debug("No jobs ready")
            return
        for job in jobs:
            await handle_job(session, job)


async def cron_loop() -> None:
    while True:
        try:
            await cron_tick()
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Self-Cron tick crashed; continuing")
        await asyncio.sleep(60)


def start_self_cron() -> asyncio.Task:
    loop = asyncio.get_running_loop()
    return loop.create_task(cron_loop(), name="postly_self_cron")
