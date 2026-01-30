from __future__ import annotations

import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


EVENT_TYPES = ["page_view", "click", "signup", "error"]
PAGES = ["/", "/pricing", "/docs", "/blog", "/login", "/signup", "/account", "/checkout"]
REFERRERS = ["direct", "google", "linkedin", "twitter", "newsletter", "github"]
DEVICES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["DE", "NL", "FR", "PL", "SE", "DK", "GB", "US"]

DATA_DIR = Path("data")


@dataclass
class Event:
    event_id: str
    event_time_utc: str
    ingested_at_utc: str
    user_id: str
    event_type: str
    page: Optional[str] = None
    referrer: Optional[str] = None
    device: Optional[str] = None
    country: Optional[str] = None
    error_code: Optional[str] = None


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_daily_events(
    day: datetime,
    n_users: int = 200,
    n_events: int = 5000,
    seed: int = 42,
    duplicate_rate: float = 0.01,
    late_event_rate: float = 0.02,
    missing_field_rate: float = 0.005,
) -> list[Event]:
    """
    Generates realistic-ish application events for a single day.

    Includes:
    - duplicates (retries)
    - late arriving events (event_time belongs to previous day)
    - occasional missing optional fields
    """
    random.seed(seed)

    day_start = day.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    users = [f"user_{i:05d}" for i in range(1, n_users + 1)]
    now = datetime.now(timezone.utc)

    events: list[Event] = []

    for _ in range(n_events):
        user_id = random.choice(users)
        event_type = random.choices(EVENT_TYPES, weights=[0.70, 0.22, 0.03, 0.05], k=1)[0]

        # event_time normally within the day
        event_time = day_start + timedelta(seconds=random.randint(0, 24 * 3600 - 1))

        # late arriving events: some belong to the previous day
        if random.random() < late_event_rate:
            event_time = event_time - timedelta(hours=random.randint(1, 12))

        # ingestion time is "now-ish", not equal to event time
        ingested_at = now + timedelta(seconds=random.randint(-60, 60))

        page = random.choice(PAGES) if event_type in ("page_view", "click", "signup") else None
        referrer = random.choice(REFERRERS)
        device = random.choices(DEVICES, weights=[0.55, 0.40, 0.05], k=1)[0]
        country = random.choices(COUNTRIES, weights=[0.55, 0.08, 0.08, 0.07, 0.05, 0.05, 0.07, 0.05], k=1)[0]

        error_code = None
        if event_type == "error":
            error_code = random.choice(["E_TIMEOUT", "E_AUTH", "E_5XX", "E_VALIDATION"])

        e = Event(
            event_id=str(uuid.uuid4()),
            event_time_utc=_iso(event_time),
            ingested_at_utc=_iso(ingested_at),
            user_id=user_id,
            event_type=event_type,
            page=page,
            referrer=referrer,
            device=device,
            country=country,
            error_code=error_code,
        )

        # missing field simulation (keep it rare)
        if random.random() < missing_field_rate:
            # drop one optional field
            field_to_drop = random.choice(["page", "referrer", "device", "country"])
            setattr(e, field_to_drop, None)

        events.append(e)

    # duplicates simulation: copy some events with same event_id (retry)
    n_dupes = int(n_events * duplicate_rate)
    for _ in range(n_dupes):
        original = random.choice(events)
        dup = Event(**asdict(original))
        # tweak ingested_at to simulate retry later
        dup.ingested_at_utc = _iso(datetime.now(timezone.utc) + timedelta(seconds=random.randint(1, 120)))
        events.append(dup)

    # shuffle so duplicates are not adjacent
    random.shuffle(events)

    return events


def write_jsonl(events: list[Event], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(asdict(e), ensure_ascii=False) + "\n")


def main():
    # Default: generate today’s events (UTC date)
    day = datetime.now(timezone.utc)
    day_str = day.strftime("%Y%m%d")

    out_path = DATA_DIR / f"events_{day_str}.jsonl"
    events = generate_daily_events(day=day, seed=random.randint(1, 10_000))

    write_jsonl(events, out_path)
    print(f"Wrote {len(events)} events to {out_path}")


if __name__ == "__main__":
    main()
