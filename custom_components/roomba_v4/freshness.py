"""Expose receipt and sample ages without treating unchanged robot state as offline."""
from datetime import UTC, datetime
from typing import Any


def telemetry_freshness(data: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    live = data.get("live_state") or {}
    meta = live.get("_meta") or {}
    status = data.get("status") or {}
    received = meta.get("last_update") or status.get("last_update")
    sample = (live.get("livemap") or {}).get("path_timestamp")

    def age(value: Any) -> tuple[str | None, float | None]:
        try:
            stamp = datetime.fromtimestamp(value, UTC) if isinstance(value, (int, float)) else datetime.fromisoformat(value)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=UTC)
            return stamp.isoformat(), round(max(0, (now - stamp).total_seconds()), 1)
        except (TypeError, ValueError, OverflowError, OSError):
            return None, None

    received_at, received_age = age(received)
    status_at, status_age = age(meta.get("status_received_at"))
    sampled_at, sample_age = age(sample)
    active = (status.get("vacuum_state") or data.get("vacuum_state")) in {"cleaning", "returning", "paused"}
    return {
        "telemetry_received_at": received_at,
        "telemetry_age_seconds": received_age,
        "telemetry_stale": received_age is None or received_age > (60 if active else 300),
        "status_received_at": status_at,
        "status_age_seconds": status_age,
        "status_stale": status_age is None or status_age > (60 if active else 300),
        "position_sampled_at": sampled_at,
        "position_age_seconds": sample_age,
        "position_stale": sample_age is None or sample_age > 30,
        "robot_reported_at": meta.get("robot_reported_at"),
    }
