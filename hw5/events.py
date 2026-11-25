from datetime import UTC, datetime, timedelta
from random import choice, randint
from typing import Literal, Self
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

EventType = Literal[
    "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"
]
DeviceType = Literal["MOBILE", "DESKTOP", "TV", "TABLET"]


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", str_min_length=1)

    event_id: UUID
    user_id: str
    movie_id: str
    event_type: EventType
    timestamp: AwareDatetime = Field(
        ge=datetime(1900, 1, 1, tzinfo=UTC),
        lt=datetime(2300, 1, 1, tzinfo=UTC),
    )
    device_type: DeviceType
    session_id: str
    progress_seconds: int | None = Field(default=None, strict=True, ge=0, le=2**31 - 1)

    @field_validator("timestamp")
    @classmethod
    def use_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def require_view_progress(self) -> Self:
        if self.event_type.startswith("VIEW_") and self.progress_seconds is None:
            raise ValueError("progress_seconds is required for viewing events")
        return self


def sample_session():
    """Generate one viewing session with simulated timestamps."""
    shared = {
        "user_id": f"user-{randint(1, 20)}",
        "movie_id": f"movie-{randint(1, 5)}",
        "device_type": choice(("MOBILE", "DESKTOP", "TV", "TABLET")),
        "session_id": str(uuid4()),
    }
    duration = randint(3600, 7200)
    pause_at = duration // 3
    started_at = datetime.now(UTC) - timedelta(seconds=duration + 121)
    for event_type, elapsed, progress in (
        ("SEARCHED", -10, None),
        ("VIEW_STARTED", 0, 0),
        ("VIEW_PAUSED", pause_at, pause_at),
        ("VIEW_RESUMED", pause_at + 121, pause_at + 1),
        ("VIEW_FINISHED", duration + 120, duration),
        ("LIKED", duration + 121, None),
    ):
        yield MovieEvent(
            **shared,
            event_id=uuid4(),
            event_type=event_type,
            timestamp=started_at + timedelta(seconds=elapsed),
            progress_seconds=progress,
        )
